package gorundb

import (
	"context"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jswidler/gorun/errors"
	"github.com/jswidler/gorun/logger"
	"github.com/jswidler/gorun/tenantctx"
)

type JobView struct {
	db *Db
}

type JobTrigger struct {
	Id             string    `db:"id" json:"id"`
	TenantId       *string   `db:"tenant_id" json:"tenantId"`
	CreatedAt      time.Time `db:"created_at" json:"createdAt"`
	UpdatedAt      time.Time `db:"updated_at" json:"updatedAt"`
	TriggerType    string    `db:"trigger_type" json:"triggerType"`
	TriggerData    string    `db:"trigger_data" json:"triggerData"`
	ScheduledUntil time.Time `db:"scheduled_until" json:"scheduledUntil"`

	JobType string `db:"job_type" json:"jobType"`
	JobArgs string `db:"job_args" json:"jobArgs"`
}
type JobData struct {
	Id        string    `db:"id" json:"id"`
	TenantId  *string   `db:"tenant_id" json:"tenantId"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
	Status    string    `db:"status" json:"status"`
	Nonce     int64     `db:"nonce" json:"-"`

	TriggerId *string `db:"trigger_id" json:"triggerId"`

	RunAt time.Time `db:"run_at" json:"runAt"`

	Type   string  `db:"type" json:"type"`
	Args   string  `db:"args" json:"args"`
	Result *string `db:"result" json:"result"`
}

func (view JobView) AcquireJobsToRun(ctx context.Context, jobLimit int) ([]*JobData, error) {
	nonce := rand.Int63()
	jobs, err := queryMany[JobData](ctx, view.db.db, `WITH to_run AS (
			SELECT * from "gorun_job_data" WHERE "status" = 'scheduled' AND "run_at" < NOW() LIMIT $2 FOR UPDATE
		)
		UPDATE "gorun_job_data" j SET "status" = 'running', "updated_at" = NOW(), "nonce" = $1 FROM to_run WHERE j.id = to_run.id RETURNING j.*`, nonce, jobLimit)

	if len(jobs) == jobLimit {
		// Warn if we hit the limit
		logger.Ctx(ctx).Warn().Msg("full batch of jobs acquired")
	}

	return jobs, err
}

func (view JobView) MarkIncompleteJobs(ctx context.Context, jobTimeout time.Duration) ([]*JobData, error) {
	nonce := rand.Int63()
	tenMinAgo := time.Now().Add(-1 * jobTimeout)
	return queryMany[JobData](ctx, view.db.db, `WITH stuck AS (
			SELECT * from "gorun_job_data" WHERE "status" = 'running' AND "updated_at" < $1 FOR UPDATE
		)
		UPDATE "gorun_job_data" j SET "status" = 'failed', "updated_at" = NOW(), result = $2, nonce = $3 FROM stuck WHERE j.id = stuck.id RETURNING j.*`,
		tenMinAgo, "job timed out", nonce)
}

func (view JobView) UpdateJob(ctx context.Context, job *JobData) error {
	return updateById(ctx, view.db.db, "gorun_job_data", job)
}

func (view JobView) GetJobById(ctx context.Context, jobId string) (*JobData, error) {
	return byId[JobData](ctx, view.db.db, "gorun_job_data", jobId)
}

func (view JobView) ListJobs(ctx context.Context, startTime, endTime time.Time) ([]*JobData, error) {
	tenantId := tenantctx.GetTenant(ctx)
	if tenantId == "" {
		return queryMany[JobData](ctx, view.db.db, `SELECT * FROM "gorun_job_data" WHERE "run_at" >= $1 AND "run_at" < $2`, startTime, endTime)
	}
	return queryMany[JobData](ctx, view.db.db, `SELECT * FROM "gorun_job_data" WHERE "tenant_id" = $1 AND "run_at" >= $2 AND "run_at" < $3`, tenantId, startTime, endTime)
}

func (view JobView) GetTriggerById(ctx context.Context, triggerId string) (*JobTrigger, error) {
	var trigger *JobTrigger
	err := view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		var err error
		trigger, err = byId[JobTrigger](ctx, tx, "gorun_trigger", triggerId)
		return err
	})
	return trigger, err
}

func (view JobView) ListTriggers(ctx context.Context) ([]*JobTrigger, error) {
	tenantId := tenantctx.GetTenant(ctx)
	if tenantId != "" {
		return queryMany[JobTrigger](ctx, view.db.db, `SELECT * FROM "gorun_trigger" WHERE "tenant_id" = $1`, tenantId)
	}
	return queryMany[JobTrigger](ctx, view.db.db, `SELECT * FROM "gorun_trigger"`)
}

func (view JobView) DeleteTriggerById(ctx context.Context, triggerId string) error {
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		// Attempt to stop any jobs from running that are scheduled by the trigger, but have run yet.
		err := delete(ctx, tx, `DELETE FROM gorun_job_data WHERE trigger_id = $1 AND status = 'scheduled'`, triggerId)
		if err != nil {
			return err
		}
		return delete(ctx, tx, `DELETE FROM gorun_trigger WHERE id = $1`, triggerId)
	})
}

func (view JobView) InsertTriggerWithJobs(ctx context.Context, jobTrigger *JobTrigger, jobs []*JobData) error {
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		err := view.InsertTrigger(ctx, jobTrigger)
		if err != nil {
			return err
		}
		return view.InsertJobs(ctx, jobs)
	})
}

func (view JobView) MaybeUpsertTriggerWithJobs(ctx context.Context, jobTrigger *JobTrigger, jobs []*JobData) error {
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		trig, err := view.GetTriggerById(ctx, jobTrigger.Id)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if trig == nil {
			return view.InsertTriggerWithJobs(ctx, jobTrigger, jobs)
		}

		if trig.TriggerType == jobTrigger.TriggerType && trig.TriggerData == jobTrigger.TriggerData && trig.JobType == jobTrigger.JobType && trig.JobArgs == jobTrigger.JobArgs {
			return nil
		}
		err = delete(ctx, tx, `DELETE FROM gorun_job_data WHERE trigger_id = $1 AND status = 'scheduled'`, jobTrigger.Id)
		if err != nil {
			return err
		}
		return updateById(ctx, tx, "gorun_trigger", jobTrigger)
	})
}

func (view JobView) ScheduleNewJobsFromTrigger(ctx context.Context, jobTrigger *JobTrigger, prevScheduleUntil time.Time, jobs []*JobData) error {
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		err := view.UpdateScheduledUntil(ctx, jobTrigger.Id, jobTrigger.ScheduledUntil, prevScheduleUntil)
		if err != nil {
			return err
		}
		return view.InsertJobs(ctx, jobs)
	})
}

func (view JobView) InsertTrigger(ctx context.Context, jobTrigger *JobTrigger) error {
	logger.Ctx(ctx).Info().Str("triggerId", jobTrigger.Id).
		Str("jobType", jobTrigger.JobType).
		Str("triggerType", jobTrigger.TriggerType).
		Msg("inserting job trigger")
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return insert(ctx, tx, "gorun_trigger", jobTrigger)
	})
}

func (view JobView) UpsertTrigger(ctx context.Context, jobTrigger *JobTrigger) error {
	logger.Ctx(ctx).Info().Str("triggerId", jobTrigger.Id).
		Str("jobType", jobTrigger.JobType).
		Str("triggerType", jobTrigger.TriggerType).
		Msg("upserting job trigger")
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return upsert(ctx, tx, "gorun_trigger", jobTrigger, "(id)")
	})
}

func (view JobView) InsertJobs(ctx context.Context, jobs []*JobData) error {
	logger.Ctx(ctx).Info().Int("jobCount", len(jobs)).Msg("inserting jobs")
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		return insertBulk(ctx, tx, "gorun_job_data", jobs)
	})
}

func (view JobView) TriggerById(ctx context.Context, triggerId string) (*JobTrigger, error) {
	var trigger *JobTrigger
	err := view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		var err error
		trigger, err = byId[JobTrigger](ctx, tx, "gorun_trigger", triggerId)
		return err
	})
	return trigger, err
}

func (view JobView) GetTriggersToUpdate(ctx context.Context, t time.Time) ([]*JobTrigger, error) {
	var triggers []*JobTrigger
	err := view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		var err error
		triggers, err = queryMany[JobTrigger](ctx, tx, `SELECT * FROM "gorun_trigger" WHERE "scheduled_until" < $1`, t)
		return err
	})
	return triggers, err
}

func (view JobView) UpdateScheduledUntil(ctx context.Context, triggerId string, nextScheduledUntil time.Time, prevScheduledUntil time.Time) error {
	return view.db.useTx(ctx, func(ctx context.Context, tx *sqlx.Tx) error {
		r, err := tx.Exec(`UPDATE "gorun_trigger" SET "scheduled_until" = $1, updated_at = NOW() WHERE "id" = $2 AND "scheduled_until" = $3`,
			nextScheduledUntil, triggerId, prevScheduledUntil)
		if err != nil {
			return errors.Wrap(ErrDatabaseError, errors.WithCause(err))
		} else if n, err := r.RowsAffected(); err != nil {
			return errors.Wrap(ErrDatabaseError, errors.WithCause(err))
		} else if n != 1 {
			return errors.Wrap(ErrConflict, errors.WithMessage("scheduled_until was updated by another process"))
		}

		return nil
	})
}
