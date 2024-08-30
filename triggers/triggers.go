package triggers

import (
	"encoding/json"
	"time"

	"github.com/jswidler/gorun/errors"
)

func init() {
	RegisterTriggerHandler(&RunOnceTrigger{}, &RepeatTrigger{})
}

type RepeatTrigger struct {
	Interval time.Duration
}

// Verify RepeatTrigger satisfies the Trigger interface.
var _ Trigger = (*RepeatTrigger)(nil)

// NewRepeatTrigger returns a new RepeatTrigger using the given interval.
func NewRepeatTrigger(interval time.Duration) *RepeatTrigger {
	return &RepeatTrigger{
		Interval: interval,
	}
}

func (t *RepeatTrigger) Type() string {
	return "repeat"
}

// NextFireTime returns the next time at which the RepeatTrigger is scheduled to fire.
func (st *RepeatTrigger) NextFireTime(prev time.Time) (time.Time, error) {
	next := prev.Add(st.Interval)
	return next, nil
}

func (st *RepeatTrigger) Serialize() (string, error) {
	data, err := json.Marshal(st)
	return string(data), errors.Wrap(err)
}

func (st *RepeatTrigger) Deserialize(data string) (Trigger, error) {
	trig := RepeatTrigger{}
	err := json.Unmarshal([]byte(data), &trig)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &trig, nil
}

// RunOnceTrigger implements the quartz.Trigger interface.
// This type of Trigger can only be fired once and will expire immediately.
type RunOnceTrigger struct {
	Delay   time.Duration
	Expired bool
}

// Verify RunOnceTrigger satisfies the Trigger interface.
var _ Trigger = (*RunOnceTrigger)(nil)

func (ot *RunOnceTrigger) Type() string {
	return "run-once"
}

// NewRunOnceTrigger returns a new RunOnceTrigger with the given delay time.
func NewRunOnceTrigger(delay time.Duration) *RunOnceTrigger {
	return &RunOnceTrigger{
		Delay:   delay,
		Expired: false,
	}
}

// NextFireTime returns the next time at which the RunOnceTrigger is scheduled to fire.
// Sets exprired to true afterwards.
func (ot *RunOnceTrigger) NextFireTime(prev time.Time) (time.Time, error) {
	if !ot.Expired {
		next := prev.Add(ot.Delay)
		ot.Expired = true
		return next, nil
	}

	return time.Time{}, errors.New("RunOnce trigger is expired")
}

func (ot *RunOnceTrigger) Serialize() (string, error) {
	data, err := json.Marshal(ot)
	return string(data), errors.Wrap(err)
}

func (ot *RunOnceTrigger) Deserialize(data string) (Trigger, error) {
	trig := RunOnceTrigger{}
	err := json.Unmarshal([]byte(data), &trig)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &trig, nil
}
