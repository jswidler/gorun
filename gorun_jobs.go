package gorun

import (
	"context"
)

func init() {
	RegisterHandler(&ProcessTriggersHandler)
	RegisterHandler(&MarkIncompleteJobsHandler)
}

type ProcessTriggers struct{}

func (a ProcessTriggers) JobType() string {
	return "gorun:ProcessTriggers"
}

var ProcessTriggersHandler = Handler[ProcessTriggers]{
	Execute: func(ctx context.Context, args *ProcessTriggers) (string, error) {
		err := getGoRunner(ctx).ProcessTriggers(ctx)
		if err != nil {
			return "", err
		}
		return "done", nil
	},
}

type MarkIncompleteJobs struct{}

func (a MarkIncompleteJobs) JobType() string {
	return "gorun:MarkIncompleteJobs"
}

var MarkIncompleteJobsHandler = Handler[MarkIncompleteJobs]{
	Execute: func(ctx context.Context, args *MarkIncompleteJobs) (string, error) {
		err := getGoRunner(ctx).MarkIncompleteJobs(ctx)
		if err != nil {
			return "", err
		}
		return "done", nil
	},
}
