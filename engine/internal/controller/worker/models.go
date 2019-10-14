package worker

import (
	"github.com/veritone/edge-messages"
	"fmt"
)


// errorReason is the struct has failure reason and error
type ErrorReason struct {
	Err error
	FailureReason messages.TaskFailureReason
}

func (e *ErrorReason) String() string {
	return fmt.Sprintf("%v: %s", e.FailureReason, e.Err.Error())
}
type Worker interface {
	Run() ErrorReason
}
