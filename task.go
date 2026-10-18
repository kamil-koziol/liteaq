package liteaq

import (
	"time"
)

// Job represents the unit of work to be performed.
type Job struct {
	// Type routes the job to a handler registered on the ServeMux.
	Type    string
	Payload []byte

	// MaxRetries is the number of times a failed job is retried.
	// Use InfiniteRetries to retry forever.
	// Use NoRetries to disallow retries.
	MaxRetries int
}

type JobOption func(*Job)

const InfiniteRetries int = -1
const NoRetries int = 0

func WithMaxRetries(maxRetries int) JobOption {
	return func(j *Job) {
		j.MaxRetries = maxRetries
	}
}

func NewJob(typ string, data []byte, opts ...JobOption) *Job {
	j := &Job{
		Type:       typ,
		Payload:    data,
		MaxRetries: 20,
	}

	for _, opt := range opts {
		opt(j)
	}

	return j
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
	Retry        int64
	MaxRetries   int64
	FailedAt     *time.Time
	RootId       *int64
	ExecuteAt    time.Time

	Status Status
}
