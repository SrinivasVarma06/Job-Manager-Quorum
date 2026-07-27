package job

import (
	"time"
)

type Job struct {
	ID         int
	Type       string
	Priority   int
	Status     Status
	RetryCount int
	MaxRetries int
	NextRunAt  time.Time
	LastError  string
}

type Status string

const (
	Pending   Status = "PENDING"
	Scheduled Status = "SCHEDULED"
	Running   Status = "RUNNING"
	Completed Status = "COMPLETED"
	Failed    Status = "FAILED"
	Cancelled Status = "CANCELLED"
	Retrying  Status = "RETRYING"
)

func NewJob(id int, jobType string, priority int) Job {
	return Job{
		ID:         id,
		Type:       jobType,
		Priority:   priority,
		Status:     Pending,
		MaxRetries: 3,
	}
}
