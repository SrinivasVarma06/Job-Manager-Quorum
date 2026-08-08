package handlers

import (
	"fmt"
	"net/http"
	"quorum/internal/engine"
	"quorum/internal/job"
)

// MetricsHandler exposes system telemetry in standard Prometheus text format at GET /metrics.
func MetricsHandler(e *engine.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		nodes := e.WorkerManager.Nodes()
		activeWorkers := 0
		deadWorkers := 0
		for _, n := range nodes {
			if n.Alive {
				activeWorkers++
			} else {
				deadWorkers++
			}
		}

		jobs := e.Jobs()
		pending := 0
		scheduled := 0
		completed := 0
		failed := 0
		cancelled := 0

		for _, j := range jobs {
			switch j.Status {
			case job.Pending:
				pending++
			case job.Scheduled:
				scheduled++
			case job.Completed:
				completed++
			case job.Failed:
				failed++
			case job.Cancelled:
				cancelled++
			}
		}

		isLeader := 1
		term := uint64(1)
		if e.RaftNode != nil {
			if !e.RaftNode.IsLeader() {
				isLeader = 0
			}
			term = e.RaftNode.LeaderTerm()
		}

		deadJobs := e.DeadJobs()

		// Write Prometheus Metrics
		fmt.Fprintf(w, "# HELP quorum_jobs_total Total jobs by desired status.\n")
		fmt.Fprintf(w, "# TYPE quorum_jobs_total gauge\n")
		fmt.Fprintf(w, "quorum_jobs_total{status=\"pending\"} %d\n", pending)
		fmt.Fprintf(w, "quorum_jobs_total{status=\"scheduled\"} %d\n", scheduled)
		fmt.Fprintf(w, "quorum_jobs_total{status=\"completed\"} %d\n", completed)
		fmt.Fprintf(w, "quorum_jobs_total{status=\"failed\"} %d\n", failed)
		fmt.Fprintf(w, "quorum_jobs_total{status=\"cancelled\"} %d\n", cancelled)

		fmt.Fprintf(w, "\n# HELP quorum_workers_active Total active workers.\n")
		fmt.Fprintf(w, "# TYPE quorum_workers_active gauge\n")
		fmt.Fprintf(w, "quorum_workers_active %d\n", activeWorkers)

		fmt.Fprintf(w, "\n# HELP quorum_workers_dead Total dead workers.\n")
		fmt.Fprintf(w, "# TYPE quorum_workers_dead gauge\n")
		fmt.Fprintf(w, "quorum_workers_dead %d\n", deadWorkers)

		fmt.Fprintf(w, "\n# HELP quorum_dlq_size Number of jobs in Dead Letter Queue.\n")
		fmt.Fprintf(w, "# TYPE quorum_dlq_size gauge\n")
		fmt.Fprintf(w, "quorum_dlq_size %d\n", len(deadJobs))

		fmt.Fprintf(w, "\n# HELP quorum_raft_is_leader Whether current node is Raft leader (1=yes, 0=no).\n")
		fmt.Fprintf(w, "# TYPE quorum_raft_is_leader gauge\n")
		fmt.Fprintf(w, "quorum_raft_is_leader %d\n", isLeader)

		fmt.Fprintf(w, "\n# HELP quorum_raft_term Current Raft election term.\n")
		fmt.Fprintf(w, "# TYPE quorum_raft_term gauge\n")
		fmt.Fprintf(w, "quorum_raft_term %d\n", term)
	}
}
