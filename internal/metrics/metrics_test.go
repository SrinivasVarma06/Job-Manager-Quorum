package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCounterIncrement(t *testing.T) {
	// Record baseline
	base := testutil.ToFloat64(JobsSubmitted)
	JobsSubmitted.Inc()
	if got := testutil.ToFloat64(JobsSubmitted); got != base+1 {
		t.Fatalf("JobsSubmitted expected %v got %v", base+1, got)
	}

	base = testutil.ToFloat64(JobsCompleted)
	JobsCompleted.Add(2)
	if got := testutil.ToFloat64(JobsCompleted); got != base+2 {
		t.Fatalf("JobsCompleted expected %v got %v", base+2, got)
	}

	base = testutil.ToFloat64(JobsFailed)
	JobsFailed.Inc()
	if got := testutil.ToFloat64(JobsFailed); got != base+1 {
		t.Fatalf("JobsFailed expected %v got %v", base+1, got)
	}

	base = testutil.ToFloat64(JobsCancelled)
	JobsCancelled.Inc()
	if got := testutil.ToFloat64(JobsCancelled); got != base+1 {
		t.Fatalf("JobsCancelled expected %v got %v", base+1, got)
	}
}

func TestGaugeUpdates(t *testing.T) {
	base := testutil.ToFloat64(QueueDepth)
	QueueDepth.Inc()
	if got := testutil.ToFloat64(QueueDepth); got != base+1 {
		t.Fatalf("QueueDepth expected %v got %v", base+1, got)
	}
	QueueDepth.Dec()
	if got := testutil.ToFloat64(QueueDepth); got != base {
		t.Fatalf("QueueDepth expected %v got %v", base, got)
	}

	aw := testutil.ToFloat64(ActiveWorkers)
	ActiveWorkers.Inc()
	if got := testutil.ToFloat64(ActiveWorkers); got != aw+1 {
		t.Fatalf("ActiveWorkers expected %v got %v", aw+1, got)
	}
	ActiveWorkers.Dec()
	if got := testutil.ToFloat64(ActiveWorkers); got != aw {
		t.Fatalf("ActiveWorkers expected %v got %v", aw, got)
	}
}

func TestHistogramRecords(t *testing.T) {
	// Record an observation
	JobExecutionDuration.Observe(0.05)
	count := testutil.CollectAndCount(JobExecutionDuration)
	if count == 0 {
		t.Fatal("expected histogram to have at least one metric collected")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h := Handler()
	s := httptest.NewServer(h)
	defer s.Close()

	// Give prometheus client a moment to register metrics if needed
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get(s.URL)
	if err != nil {
		t.Fatalf("http get error: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	sbody := string(body)

	want := []string{
		"quorum_jobs_submitted_total",
		"quorum_jobs_completed_total",
		"quorum_jobs_failed_total",
		"quorum_jobs_cancelled_total",
		"quorum_queue_depth",
		"quorum_active_workers",
		"quorum_job_execution_duration_seconds",
	}

	for _, w := range want {
		if !strings.Contains(sbody, w) {
			t.Fatalf("metrics output missing %s", w)
		}
	}
}
