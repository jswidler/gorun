package gorun

import (
	"context"
	"encoding/json"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jswidler/gorun/errors"
	"github.com/jswidler/gorun/gorundb"
	"github.com/jswidler/gorun/logger"
	"github.com/jswidler/gorun/tenantctx"
	"github.com/jswidler/gorun/triggers"
	"github.com/jswidler/gorun/triggers/crontrigger"
	"github.com/jswidler/gorun/ulid"
)

var ErrUnregisteredJobType = errors.Sentinel("unregistered job type")
var ErrGorunInternalError = errors.Sentinel("internal gorun job service error")

type gorunner struct {
	db *gorundb.Db

	batchSize  int
	batchFreq  time.Duration
	jobTimeout time.Duration

	ticker    *time.Ticker
	done      chan (struct{})
	waitGroup *sync.WaitGroup

	jobInit      func(ctx context.Context, jobType string, jobId string) context.Context
	argProcessor func(ctx context.Context, jobType string, jobId string, args any) error
	jobComplete  func(ctx context.Context, jobType string, jobId string, result string, err error)
}

type options struct {
	batchSize      int
	batchFreq      time.Duration
	jobTimeout     time.Duration
	disableLogging bool

	jobInit      func(ctx context.Context, jobType string, jobId string) context.Context
	argProcessor func(ctx context.Context, jobType string, jobId string, args any) error
	jobComplete  func(ctx context.Context, jobType string, jobId string, result string, err error)
}

func newGorunner(db *gorundb.Db, opts []Option) *gorunner {
	o := options{
		batchSize:  10,
		batchFreq:  1 * time.Second,
		jobTimeout: 10 * time.Minute,
	}
	for _, opt := range opts {
		opt(&o)
	}

	logger.DisableLogging = o.disableLogging

	return &gorunner{
		db:           db,
		batchSize:    o.batchSize,
		batchFreq:    o.batchFreq,
		jobTimeout:   o.jobTimeout,
		jobInit:      o.jobInit,
		argProcessor: o.argProcessor,
		jobComplete:  o.jobComplete,
	}
}

func (g *gorunner) Start(ctx context.Context) error {
	if g.ticker != nil {
		return nil
	}
	g.ticker = time.NewTicker(g.batchFreq)
	g.done = make(chan struct{})
	g.waitGroup = &sync.WaitGroup{}

	ctx = withGoRunner(ctx, g)

	go func() {
		logger.Default().Info().Msg("starting job server")
		for {
			select {
			case <-g.done:
				return
			case <-g.ticker.C:
				err := g.runBatch(ctx)
				if err != nil {
					logger.Ctx(ctx).Error().Err(err).Msg("job batch returned an error")
				}
			}
		}
	}()

	var err error
	defer func() {
		if err != nil {
			g.Close()
		}
	}()
	err = g.ScheduleRepeatedWithKey(ctx, "gorun:processTriggers", 30*time.Second, ProcessTriggers{})
	if err != nil {
		return err
	}
	err = g.ScheduleRepeatedWithKey(ctx, "gorun:markIncompleteJobs", 5*time.Minute, MarkIncompleteJobs{})
	if err != nil {
		return err
	}
	// It's a good idea to process triggers when starting the service.
	return g.ProcessTriggers(ctx)
}

func (g *gorunner) Close() {
	if g.ticker == nil {
		return
	}
	g.ticker.Stop()
	close(g.done)
	g.waitGroup.Wait()
}

func (g gorunner) ScheduleImmediately(ctx context.Context, job JobData) (jobId string, err error) {
	return g.schedule(ctx, ulid.New(), triggers.NewRunOnceTrigger(0), job)
}

func (g gorunner) ScheduleAfter(ctx context.Context, delay time.Duration, job JobData) (jobId string, err error) {
	return g.schedule(ctx, ulid.New(), triggers.NewRunOnceTrigger(delay), job)
}

func (g gorunner) ScheduleCron(ctx context.Context, cronExpr string, loc *time.Location, job JobData) (triggerId string, err error) {
	trigger, err := crontrigger.NewWithLoc(cronExpr, loc)
	if err != nil {
		return
	}
	triggerId = ulid.New()
	_, err = g.schedule(ctx, triggerId, trigger, job)
	return
}

func (g gorunner) ScheduleCronWithKey(ctx context.Context, triggerId string, cronExpr string, loc *time.Location, job JobData) (err error) {
	trigger, err := crontrigger.NewWithLoc(cronExpr, loc)
	if err != nil {
		return
	}
	_, err = g.schedule(ctx, triggerId, trigger, job)
	return
}

func (g gorunner) ScheduleRepeated(ctx context.Context, interval time.Duration, job JobData) (triggerId string, err error) {
	triggerId = ulid.New()
	_, err = g.schedule(ctx, triggerId, triggers.NewRepeatTrigger(interval), job)
	return
}

func (g gorunner) ScheduleRepeatedWithKey(ctx context.Context, triggerId string, interval time.Duration, job JobData) (err error) {
	_, err = g.schedule(ctx, triggerId, triggers.NewRepeatTrigger(interval), job)
	return
}

func (g gorunner) schedule(ctx context.Context, triggerId string, trigger Trigger, job JobData) (jobId string, err error) {
	if v, ok := job.(Validateable); ok {
		err = v.Validate()
		if err != nil {
			return
		}
	}

	trig, jobData, err := g.firstRun(ctx, triggerId, trigger, job)
	if err != nil {
		return
	}
	jobId = jobData[0].Id
	if trig == nil {
		err = g.db.JobView.InsertJobs(ctx, jobData)
	} else {
		err = g.db.JobView.MaybeUpsertTriggerWithJobs(ctx, trig, jobData)
	}
	return
}

func (g gorunner) GetJob(ctx context.Context, jobId string) (*gorundb.JobData, error) {
	return g.db.JobView.GetJobById(ctx, jobId)
}

func (g gorunner) DeleteTrigger(ctx context.Context, triggerId string) error {
	return g.db.JobView.DeleteTriggerById(ctx, triggerId)
}

func (g gorunner) ProcessTriggers(ctx context.Context) error {
	now := time.Now()
	minScheduleTime := now.Add(3 * time.Minute)

	triggers, err := g.db.JobView.GetTriggersToUpdate(ctx, minScheduleTime)
	if err != nil {
		return err
	}

	for _, t := range triggers {
		err := g.scheduleJobsFromTrigger(ctx, t, now, minScheduleTime)
		if err != nil {
			if errors.Is(err, gorundb.ErrConflict) {
				// It is not entirely unexpected we fail to update a trigger, since this function may be called concurrently.
				logger.Ctx(ctx).Info().Str("triggerId", t.Id).Msg("trigger scheduled by concurrent process")
				continue
			}
			return err
		}
	}
	return nil
}

func (g gorunner) ListJobs(ctx context.Context, start, end time.Time) ([]*gorundb.JobData, error) {
	return g.db.JobView.ListJobs(ctx, start, end)
}

func (g gorunner) ListTriggers(ctx context.Context) ([]*gorundb.JobTrigger, error) {
	return g.db.JobView.ListTriggers(ctx)
}

func (g gorunner) MarkIncompleteJobs(ctx context.Context) error {
	jobs, err := g.db.JobView.MarkIncompleteJobs(ctx, g.jobTimeout)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		logger.Ctx(ctx).Error().Str("failedJobId", job.Id).Str("failedJobType", job.Type).Msg("job timed out")
	}
	return nil
}

func (g gorunner) scheduleJobsFromTrigger(ctx context.Context, trigger *gorundb.JobTrigger, now, minScheduleTime time.Time) error {
	logger.Ctx(ctx).Info().
		Str("jobType", trigger.JobType). // jobType is used by the job running this command
		Str("triggerType", trigger.TriggerType).
		Str("triggerId", trigger.Id).
		Msg("scheduling job for trigger")

	prevScheduleUntil := trigger.ScheduledUntil // stored to prevent race conditions from scheduling the same job twice
	trig, err := triggers.LoadTrigger(trigger.TriggerType, trigger.TriggerData)
	if err != nil {
		return err
	}

	jobList := []*gorundb.JobData{}
	next := trigger.ScheduledUntil
	for {
		next, err = trig.NextFireTime(next)
		if err != nil {
			if len(jobList) == 0 {
				// the first fire time should not be an error, otherwise expect the error to indicate the trigger should not be repeated by the run-once trigger
				return err
			}
			break
		}
		// if we missed the time to run the job, run it immediately, and don't schedule it again until the future.
		if next.Before(now) {
			next = now
		}
		jobList = append(jobList, newJobFromTrigger(trigger, next))
		trigger.ScheduledUntil = next
		if next.After(minScheduleTime) {
			break
		}
	}

	return g.db.JobView.ScheduleNewJobsFromTrigger(ctx, trigger, prevScheduleUntil, jobList)
}

func (g *gorunner) runBatch(ctx context.Context) error {
	l := logger.Ctx(ctx).With().Str("batchId", ulid.New()).Logger()
	ctx = l.WithContext(ctx)

	jobs, err := g.db.JobView.AcquireJobsToRun(ctx, g.batchSize)
	if err != nil {
		logger.Ctx(ctx).Error().Err(err).Msg("error acquiring jobs")
		return err
	}
	if len(jobs) == 0 {
		logger.Ctx(ctx).Info().Msg("no jobs to run")
		return nil
	}

	g.waitGroup.Add(len(jobs))
	logger.Ctx(ctx).Info().Int("jobCount", len(jobs)).Msg("running job batch")
	for i := range jobs {
		go func(i int) {
			defer g.waitGroup.Done()

			ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()

			g.runJob(ctx, jobs[i])
		}(i)
	}
	return nil
}

func (g *gorunner) runJob(ctx context.Context, job *gorundb.JobData) {
	start := time.Now()
	l := logger.Ctx(ctx).With().Str("jobId", job.Id).Str("jobType", job.Type)
	if job.TriggerId != nil {
		l = l.Str("triggerId", *job.TriggerId)
	}
	if job.TenantId != nil {
		l = l.Str("tenantId", *job.TenantId)
		ctx = tenantctx.WithTenant(ctx, *job.TenantId)
	}
	ctx = l.Logger().WithContext(ctx)

	logger.Ctx(ctx).Info().Msg("job starting")

	var err error
	if g.jobInit != nil {
		ctx = g.jobInit(ctx, job.Type, job.Id)
	}

	result := ""
	defer func() {
		if r := recover(); r != nil {
			err = errors.Panic(r, debug.Stack())
		}
		g.writeJobResult(ctx, job, start, result, err)
		if g.jobComplete != nil {
			g.jobComplete(ctx, job.Type, job.Id, result, err)
		}
	}()

	argsFn, executeFn, err := getHandlerFunctions(job.Type)
	if err != nil {
		return
	}

	// This should return a pointer to a freshly allocated T struct.
	argsPtrT := argsFn.Call(nil)[0].Interface()
	err = json.Unmarshal([]byte(job.Args), argsPtrT)
	if err != nil {
		err = errors.Wrap(err)
		return
	}
	if v, ok := argsPtrT.(Validateable); ok {
		err = v.Validate()
		if err != nil {
			err = errors.Wrap(err)
			return
		}
	}
	if g.argProcessor != nil {
		err = g.argProcessor(ctx, job.Type, job.Id, argsPtrT)
		if err != nil {
			err = errors.Wrap(err)
			return
		}
	}

	fnReturns := executeFn.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(argsPtrT)})

	result = fnReturns[0].Interface().(string)
	err, _ = fnReturns[1].Interface().(error)
}

func getHandlerFunctions(jobType string) (argsFn, executeFn reflect.Value, err error) {
	handler, err := getHandler(jobType)
	if err != nil {
		return
	}
	val := reflect.ValueOf(handler)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	argsFn = val.FieldByName("ArgsFn")
	if !argsFn.IsValid() || argsFn.Kind() != reflect.Func {
		err = errors.Wrap(ErrGorunInternalError)
		return
	}
	executeFn = val.FieldByName("Execute")
	if !executeFn.IsValid() || executeFn.Kind() != reflect.Func {
		err = errors.Wrap(ErrGorunInternalError)
		return
	}
	return
}

func (g gorunner) writeJobResult(ctx context.Context, job *gorundb.JobData, start time.Time, result string, err error) {
	if err == nil {
		job.Status = string(StatusCompleted)
		if result == "" {
			result = "success"
		}
	} else {
		job.Status = string(StatusFailed)
		if result == "" {
			result = err.Error()
		}
	}
	job.Result = &result

	err2 := g.db.JobView.UpdateJob(ctx, job)
	if err2 != nil {
		// writeJobResult does not return an error because it is run as a defer.
		logger.Ctx(ctx).Error().Err(err2).Msg("failed to write job result")
	}

	dur := time.Since(start)
	if err == nil {
		logger.Ctx(ctx).Info().Str("result", result).Dur("duration", dur).Msg("job completed")
	} else {
		logger.Ctx(ctx).Error().Str("result", result).Dur("duration", dur).Err(err).Msg("job failed")
	}
}

// firsRun determines the first run time time of a job from the trigger and creates the job data and trigger to be saved to the gorundb.
func (g gorunner) firstRun(ctx context.Context, triggerId string, trigger Trigger, jobData JobData) (*gorundb.JobTrigger, []*gorundb.JobData, error) {
	dbTrigger, err := g.toDbTrigger(tenantctx.GetTenant(ctx), triggerId, trigger, jobData)
	if err != nil {
		return nil, nil, err
	}

	next, err := trigger.NextFireTime(time.Now())
	if err != nil {
		return nil, nil, err
	}

	dbTrigger.ScheduledUntil = next
	jobList := []*gorundb.JobData{newJobFromTrigger(dbTrigger, next)}

	// Don't save run-once triggers to the database, just save the job data.
	if dbTrigger.TriggerType == "run-once" {
		dbTrigger = nil
		jobList[0].TriggerId = nil
	}

	return dbTrigger, jobList, nil
}

var jobHandlers = map[string]any{}

func getHandler(jobType string) (any, error) {
	handler, ok := jobHandlers[jobType]
	if !ok {
		return nil, errors.Wrap(ErrUnregisteredJobType)
	}
	return handler, nil
}

func (g gorunner) toDbTrigger(tenantId string, triggerId string, trigger Trigger, jobData JobData) (*gorundb.JobTrigger, error) {
	jobArgs, err := json.Marshal(jobData)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	triggerArgs, err := trigger.Serialize()
	if err != nil {
		return nil, err
	}
	var tenantIdRef *string
	if tenantId != "" {
		tenantIdRef = &tenantId
	}
	return &gorundb.JobTrigger{
		Id:          triggerId,
		TenantId:    tenantIdRef,
		TriggerType: trigger.Type(),
		TriggerData: triggerArgs,
		JobType:     jobData.JobType(),
		JobArgs:     string(jobArgs),
	}, nil
}

func newJobFromTrigger(trigger *gorundb.JobTrigger, runAt time.Time) *gorundb.JobData {
	return &gorundb.JobData{
		Id:        ulid.New(),
		TenantId:  trigger.TenantId,
		Status:    string(StatusScheduled),
		TriggerId: &trigger.Id,
		RunAt:     runAt,
		Type:      trigger.JobType,
		Args:      trigger.JobArgs,
	}
}

type ctxKey int

const gorunCtxKey ctxKey = iota

func withGoRunner(ctx context.Context, service *gorunner) context.Context {
	return context.WithValue(ctx, gorunCtxKey, service)
}

func getGoRunner(ctx context.Context) *gorunner {
	service, _ := ctx.Value(gorunCtxKey).(*gorunner)
	return service
}
