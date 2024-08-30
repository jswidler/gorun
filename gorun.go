package gorun

import (
	"context"
	"database/sql"
	"time"

	"github.com/jswidler/gorun/gorundb"
	"github.com/jswidler/gorun/triggers"
)

type GoRunService interface {
	ScheduleImmediately(ctx context.Context, job JobData) (jobId string, err error)
	ScheduleAfter(ctx context.Context, delay time.Duration, job JobData) (jobId string, err error)
	ScheduleCron(ctx context.Context, cronExpr string, loc *time.Location, job JobData) (triggerId string, err error)
	ScheduleRepeated(ctx context.Context, interval time.Duration, job JobData) (triggerId string, err error)

	ScheduleCronWithKey(ctx context.Context, triggerId string, cronExpr string, loc *time.Location, job JobData) error
	ScheduleRepeatedWithKey(ctx context.Context, triggerId string, interval time.Duration, job JobData) error

	GetJob(ctx context.Context, jobId string) (*gorundb.JobData, error)
	ListJobs(ctx context.Context, start, end time.Time) ([]*gorundb.JobData, error)
	ListTriggers(ctx context.Context) ([]*gorundb.JobTrigger, error)
	DeleteTrigger(ctx context.Context, triggerId string) error

	Start(ctx context.Context) error
	Close()
}

type Handler[T JobData] struct {
	Execute func(ctx context.Context, args *T) (string, error)
}

type handlerInternal[T JobData] struct {
	Execute func(ctx context.Context, args *T) (string, error)
	ArgsFn  func() *T
}

type JobData interface {
	// Return a constant name for the job that will process the request - this is used to uniquely identify the job handler
	JobType() string

	// Optionally, implement a function to validate the job data
	// Validate() error
}

type Validateable interface {
	Validate() error
}

func RegisterHandler[T JobData](h *Handler[T]) {
	var t T
	hi := handlerInternal[T]{
		Execute: h.Execute,
		ArgsFn: func() *T {
			var t T
			return &t
		},
	}
	jobType := t.JobType()
	if _, ok := jobHandlers[jobType]; ok {
		panic("handler already registered for job type " + jobType)
	}
	jobHandlers[jobType] = hi
}

const (
	StatusScheduled = "scheduled"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

type Trigger = triggers.Trigger

func New(db *sql.DB, opts ...Option) (GoRunService, error) {
	gdb, err := gorundb.New(db)
	if err != nil {
		return nil, err
	}
	return newGorunner(gdb, opts), nil
}

func NewFromEnv(opts ...Option) (GoRunService, error) {
	db, err := gorundb.NewFromEnv()
	if err != nil {
		return nil, err
	}
	return newGorunner(db, opts), nil
}

type Option func(*options)

// How often each job server will check for new jobs to run
func WithBatchFreq(freq time.Duration) Option {
	return func(o *options) {
		o.batchFreq = freq
	}
}

// How many new jobs to run in each batch.  Currently there is no limit on the number of concurrent jobs.
func WithBatchSize(size int) Option {
	return func(o *options) {
		o.batchSize = size
	}
}

// The maximum amount of time a job can run before it is considered to have timed out
func WithJobTimeout(jobTimeout time.Duration) Option {
	return func(o *options) {
		o.jobTimeout = jobTimeout
	}
}

type JobTracer func(ctx context.Context, jobId string) (context.Context, func(ctx context.Context, jobId string, result string, err error))

func WithJobTracing(tracer JobTracer) Option {
	return func(o *options) {
		o.tracer = tracer
	}
}

func DisableLogging() Option {
	return func(o *options) {
		o.disableLogging = true
	}
}
