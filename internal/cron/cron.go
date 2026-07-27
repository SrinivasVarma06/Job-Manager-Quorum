package cron

import (
	"context"
	"errors"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CronJob struct {
	ID       string
	Schedule string
	Type     string
	Priority int
	NextRun  time.Time
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
					log.Printf("cron submit failed for %s: %v", cronJob.ID, err)
				}
			}
		}
	}
}

func nextRun(schedule string, from time.Time) time.Time {

	schedule = strings.TrimSpace(schedule)

	if schedule == "* * * * *" {
		return from.Add(time.Minute)
	}

	if strings.HasPrefix(schedule, "*/") {

		fields := strings.Fields(schedule)

		if len(fields) != 5 {
			return time.Time{}
		}

		minutes, err := strconv.Atoi(strings.TrimPrefix(fields[0], "*/"))

		if err != nil || minutes <= 0 {
			return time.Time{}
		}

		return from.Add(time.Duration(minutes) * time.Minute)
	}
	return time.Time{}
}
