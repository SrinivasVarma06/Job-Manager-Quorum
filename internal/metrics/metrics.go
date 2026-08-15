package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	JobsSubmitted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quorum_jobs_submitted_total",
		Help: "Total number of jobs submitted to the system.",
	})

	JobsCompleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quorum_jobs_completed_total",
		Help: "Total number of jobs that completed successfully.",
	})

	JobsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quorum_jobs_failed_total",
		Help: "Total number of jobs that failed during execution.",
	})

	JobsCancelled = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quorum_jobs_cancelled_total",
		Help: "Total number of jobs that were cancelled.",
	})

	QueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quorum_queue_depth",
		Help: "Current number of jobs waiting in the queue.",
	})

	ActiveWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quorum_active_workers",
		Help: "Current number of healthy workers.",
	})

	JobExecutionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "quorum_job_execution_duration_seconds",
		Help:    "Histogram of job execution durations in seconds.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})
)

func init() {
	// Register metrics with the default Prometheus registry.
	prometheus.MustRegister(
		JobsSubmitted,
		JobsCompleted,
		JobsFailed,
		JobsCancelled,
		QueueDepth,
		ActiveWorkers,
		JobExecutionDuration,
	)
}

// Handler returns an http.Handler that serves metrics in Prometheus format.
func Handler() http.Handler {
	return promhttp.Handler()
}

// ObserveDuration is a helper to record a duration.Duration or time.Duration into the histogram.
func ObserveDuration(d time.Duration) {
	JobExecutionDuration.Observe(d.Seconds())
}
