package triggers

import (
	"time"

	"github.com/jswidler/gorun/errors"
)

// Trigger represents the mechanism by which Jobs are scheduled.
type Trigger interface {
	Type() string

	// NextFireTime returns the next time at which the Trigger is scheduled to fire.
	NextFireTime(prev time.Time) (time.Time, error)

	Serialize() (string, error)

	Deserialize(data string) (Trigger, error)
}

var triggerHandlers = map[string]Trigger{}

var ErrInvalidTriggerType = errors.Sentinel("invalid trigger type", 500)

func RegisterTriggerHandler(handlers ...Trigger) {
	for i := range handlers {
		triggerHandlers[handlers[i].Type()] = handlers[i]
	}
}

func LoadTrigger(triggerType string, data string) (Trigger, error) {
	handler, ok := triggerHandlers[triggerType]
	if !ok {
		return nil, errors.Wrap(ErrInvalidTriggerType)
	}
	return handler.Deserialize(data)
}
