package sheduler

import (
	"time"

	uuid "github.com/satori/go.uuid"
)

type Sheduler interface {
	Shedule(
		taskID uuid.UUID,
		payload []byte,
		startDateTime time.Time,
		notifyTo ...string,
	) error

	Start() <-chan struct{}
}
