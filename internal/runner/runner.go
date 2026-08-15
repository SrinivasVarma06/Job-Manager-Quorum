package runner

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"quorum/internal/executor"
	"quorum/internal/job"
	"quorum/internal/metrics"
	"quorum/internal/store"
	"quorum/internal/tracing"
)

type Runner struct {
	Store       store.Store
	Results     chan job.Result
	Executor    executor.Executor
	mu          sync.Mutex
	activeTasks map[int]context.CancelFunc // jobID -> cancel function
}

func New(
	s store.Store,
	results chan job.Result,
	exec executor.Executor,
) *Runner {
	return &Runner{
		Store:       s,
		Results:     results,
		Executor:    exec,
		activeTasks: make(map[int]context.CancelFunc),
	}
}

// Cancel cancels the active execution context of a running job on this worker.
func (r *Runner) Cancel(jobID int) bool {
	r.mu.Lock()
	cancel, ok := r.activeTasks[jobID]
	if ok {
		delete(r.activeTasks, jobID)
	}
	r.mu.Unlock()

	if ok && cancel != nil {
		cancel()
		slog.Info("Cancelled active job execution", "job_id", jobID)
		// We'll count cancellations when the Execute loop returns with context.Canceled
		return true
	}
	return false
}

func (r *Runner) sendResult(result job.Result) {
	select {
	case r.Results <- result:
	default:
		slog.Error("Results channel full, dropping job result",
			"job_id", result.JobID,
			"success", result.Success)
	}
}

func (r *Runner) Execute(workerID int, j job.Job) {
	ctx, span := tracing.Tracer().Start(context.Background(), "runner.execute")
	defer span.End()

	span.SetAttributes(
		attribute.Int("job.id", j.ID),
		attribute.String("job.type", j.Type),
	)

	start := time.Now()
	defer func() {
		span.SetAttributes(attribute.Int64("execution.duration_ms", time.Since(start).Milliseconds()))
	}()

	execCtx, cancel := context.WithCancel(ctx)

	r.mu.Lock()
	r.activeTasks[j.ID] = cancel
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.activeTasks, j.ID)
		r.mu.Unlock()
		cancel()
		metrics.ObserveDuration(time.Since(start))
	}()

	slog.Info("Executing job", "worker_id", workerID, "job_id", j.ID, "type", j.Type)

	err := r.Executor.Execute(execCtx, j)
	if err != nil {
		// Distinguish cancellation from execution failure
		if errors.Is(err, context.Canceled) {
			j.Status = job.Cancelled
			if r.Store != nil {
				_ = r.Store.Update(j)
			}
			metrics.JobsCancelled.Inc()
			slog.Info("Job cancelled", "worker_id", workerID, "job_id", j.ID)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			r.sendResult(job.Result{JobID: j.ID, Success: false, Error: err})
			return
		}

		j.Status = job.Failed
		j.LastError = err.Error()
		if r.Store != nil {
			_ = r.Store.Update(j)
		}

		metrics.JobsFailed.Inc()
		slog.Warn("Job failed", "worker_id", workerID, "job_id", j.ID, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		r.sendResult(job.Result{
			JobID:   j.ID,
			Success: false,
			Error:   err,
		})
		return
	}

	j.Status = job.Completed
	if r.Store != nil {
		_ = r.Store.Update(j)
	}

	metrics.JobsCompleted.Inc()
	slog.Info("Job completed", "worker_id", workerID, "job_id", j.ID)
	span.SetStatus(codes.Ok, "")

	r.sendResult(job.Result{
		JobID:   j.ID,
		Success: true,
	})
}
