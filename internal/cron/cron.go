package cron

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CronJob struct {
	ID       string    `json:"id"`
	Schedule string    `json:"schedule"`
	Type     string    `json:"type"`
	Priority int       `json:"priority"`
	NextRun  time.Time `json:"next_run"`
}

type SubmitFunc func(jobType string, priority int) error

type Scheduler struct {
	mu      sync.RWMutex
	jobs    map[string]*CronJob
	submit  SubmitFunc
	tickDur time.Duration
}

func New(submit SubmitFunc) *Scheduler {
	return &Scheduler{
		jobs:    make(map[string]*CronJob),
		submit:  submit,
		tickDur: time.Second,
	}
}

func (s *Scheduler) Add(j CronJob) error {
	if strings.TrimSpace(j.ID) == "" {
		return errors.New("cron job id is required")
	}
	if strings.TrimSpace(j.Type) == "" {
		return errors.New("cron job type is required")
	}
	if j.Priority < 0 {
		return errors.New("cron priority must be >= 0")
	}

	next := nextRun(j.Schedule, time.Now())
	if next.IsZero() {
		return errors.New("unsupported cron schedule")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[j.ID]; exists {
		return errors.New("cron job id already exists")
	}
	j.NextRun = next
	s.jobs[j.ID] = &j
	return nil
}

func (s *Scheduler) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
}

func (s *Scheduler) List() []CronJob {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]CronJob, 0, len(s.jobs))
	for _, cronJob := range s.jobs {
		jobs = append(jobs, *cronJob)
	}
	slices.SortFunc(jobs, func(a, b CronJob) int {
		return strings.Compare(a.ID, b.ID)
	})
	return jobs
}

func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(s.tickDur)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			now := time.Now()
			due := make([]CronJob, 0)

			s.mu.Lock()
			for _, cj := range s.jobs {
				if now.Before(cj.NextRun) {
					continue
				}

				cj.NextRun = nextRun(cj.Schedule, cj.NextRun)
				due = append(due, *cj)
			}
			s.mu.Unlock()

			for _, cronJob := range due {
				if err := s.submit(cronJob.Type, cronJob.Priority); err != nil {
					slog.Error("Cron submit failed", "cron_id", cronJob.ID, "error", err)
				}
			}
		}
	}
}

// nextRun calculates the next execution time aligned to top-of-minute wall clock boundaries.
func nextRun(schedule string, from time.Time) time.Time {
	schedule = strings.TrimSpace(schedule)
	// Truncate to current minute
	fromMin := from.Truncate(time.Minute)

	if schedule == "* * * * *" {
		return fromMin.Add(time.Minute)
	}

	if strings.HasPrefix(schedule, "*/") {
		fields := strings.Fields(schedule)
		if len(fields) != 5 {
			return time.Time{}
		}

		interval, err := strconv.Atoi(strings.TrimPrefix(fields[0], "*/"))
		if err != nil || interval <= 0 {
			return time.Time{}
		}

		// Align to top-of-hour interval (e.g. 0m, 5m, 10m...)
		currentMin := fromMin.Minute()
		nextMin := ((currentMin / interval) + 1) * interval
		if nextMin >= 60 {
			// Roll over to next hour
			return time.Date(fromMin.Year(), fromMin.Month(), fromMin.Day(), fromMin.Hour()+1, 0, 0, 0, fromMin.Location())
		}
		return time.Date(fromMin.Year(), fromMin.Month(), fromMin.Day(), fromMin.Hour(), nextMin, 0, 0, fromMin.Location())
	}
	return time.Time{}
}
