package handlers_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"quorum/internal/config"
	"quorum/internal/engine"
	"quorum/internal/handlers"
)

// newTestEngine creates a minimal in-memory engine suitable for handler tests.
func newTestEngine(t *testing.T) *engine.Engine {
	t.Helper()
	cfg := config.Default()
	cfg.StorageType = "memory"
	cfg.RaftEnabled = false
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { _ = e.Stop() })
	return e
}

// postJob performs a POST /jobs with the given body and returns the recorder.
func postJob(t *testing.T, e *engine.Engine, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handlers.SubmitJobHandler(e)(rr, req)
	return rr
}

// ---------------------------------------------------------------------------
// HTTP idempotency tests
// ---------------------------------------------------------------------------

func TestHTTP_SubmitJob_FirstRequest_Returns201(t *testing.T) {
	e := newTestEngine(t)

	rr := postJob(t, e, map[string]any{
		"type":            "email",
		"priority":        5,
		"idempotency_key": "http-key-1",
	})

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d body=%s", rr.Code, rr.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if _, ok := resp["id"]; !ok {
		t.Fatal("response missing 'id' field")
	}
	if resp["idempotency_key"] != "http-key-1" {
		t.Fatalf("expected idempotency_key='http-key-1', got %v", resp["idempotency_key"])
	}
}

func TestHTTP_SubmitJob_SecondRequestSameKey_Returns200(t *testing.T) {
	e := newTestEngine(t)

	body := map[string]any{
		"type":            "email",
		"priority":        5,
		"idempotency_key": "http-key-dup",
	}

	rr1 := postJob(t, e, body)
	if rr1.Code != http.StatusCreated {
		t.Fatalf("first request: expected 201, got %d", rr1.Code)
	}

	var resp1 map[string]any
	_ = json.Unmarshal(rr1.Body.Bytes(), &resp1)
	firstID := resp1["id"]

	rr2 := postJob(t, e, body)
	if rr2.Code != http.StatusOK {
		t.Fatalf("second request: expected 200 OK (duplicate), got %d body=%s", rr2.Code, rr2.Body.String())
	}

	var resp2 map[string]any
	_ = json.Unmarshal(rr2.Body.Bytes(), &resp2)

	if resp2["id"] != firstID {
		t.Fatalf("expected same job ID on duplicate, got first=%v second=%v", firstID, resp2["id"])
	}

	// Store must contain exactly one job.
	if len(e.Jobs()) != 1 {
		t.Fatalf("expected 1 job in store, got %d", len(e.Jobs()))
	}
}

func TestHTTP_SubmitJob_NoKey_EachRequestCreatesNewJob(t *testing.T) {
	e := newTestEngine(t)

	body := map[string]any{"type": "email", "priority": 5}

	rr1 := postJob(t, e, body)
	if rr1.Code != http.StatusCreated {
		t.Fatalf("first no-key request: expected 201, got %d", rr1.Code)
	}

	rr2 := postJob(t, e, body)
	if rr2.Code != http.StatusCreated {
		t.Fatalf("second no-key request: expected 201, got %d", rr2.Code)
	}

	var r1, r2 map[string]any
	_ = json.Unmarshal(rr1.Body.Bytes(), &r1)
	_ = json.Unmarshal(rr2.Body.Bytes(), &r2)

	if r1["id"] == r2["id"] {
		t.Fatal("no-key requests must create distinct jobs")
	}

	if len(e.Jobs()) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(e.Jobs()))
	}
}

func TestHTTP_SubmitJob_MissingType_Returns400(t *testing.T) {
	e := newTestEngine(t)

	rr := postJob(t, e, map[string]any{"priority": 5, "idempotency_key": "k"})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}
