package liteaq

import (
	"time"
)

// Job represents the unit of work to be performed.
type Job struct {
	Type    string
	Payload []byte
}

func NewJob(typ string, data []byte) *Job {
	return &Job{
		Type:    typ,
		Payload: data,
	}
}

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
)

type JobDetails struct {
	ID           int64
	Type         string
	Payload      []byte
	CreatedAt    time.Time
	DispatchedAt *time.Time
	CompletedAt  *time.Time

	Status Status
}
