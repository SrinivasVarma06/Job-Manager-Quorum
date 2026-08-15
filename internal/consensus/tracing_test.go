package consensus_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/hashicorp/raft"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"quorum/internal/consensus"
	"quorum/internal/job"
	"quorum/internal/oteltest"
	"quorum/internal/store"
)

func TestApplyAddJobCreatesRaftApplySpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	fsm := consensus.NewFSM(store.NewMemoryStore())
	cmd := consensus.Command{
		Type: consensus.CmdAddJob,
		Job:  job.NewJob(1, "email", 5),
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}

	fsm.Apply(&raft.Log{Index: 10, Term: 3, Data: data})

	span := findConsensusSpanByName(sr.Ended(), "raft.apply")
	if span == nil {
		t.Fatal("expected raft.apply span")
	}
	if !hasConsensusAttr(span, "raft.index", "10") {
		t.Fatal("expected raft.index attribute")
	}
	if !hasConsensusAttr(span, "raft.term", "3") {
		t.Fatal("expected raft.term attribute")
	}
	if !hasConsensusAttr(span, "command.type", string(consensus.CmdAddJob)) {
		t.Fatal("expected command.type attribute")
	}
	if !hasConsensusAttr(span, "job.id", "1") {
		t.Fatal("expected job.id attribute")
	}
	if !hasConsensusAttr(span, "job.type", "email") {
		t.Fatal("expected job.type attribute")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func TestApplyCancelJobCreatesRaftApplySpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	jobStore := store.NewMemoryStore()
	j := job.NewJob(42, "report", 3)
	if err := jobStore.Add(j); err != nil {
		t.Fatalf("add job: %v", err)
	}

	fsm := consensus.NewFSM(jobStore)
	cmd := consensus.Command{
		Type: consensus.CmdCancelJob,
		ID:   42,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}

	fsm.Apply(&raft.Log{Index: 5, Term: 2, Data: data})

	span := findConsensusSpanByName(sr.Ended(), "raft.apply")
	if span == nil {
		t.Fatal("expected raft.apply span")
	}
	if !hasConsensusAttr(span, "command.type", string(consensus.CmdCancelJob)) {
		t.Fatal("expected command.type attribute")
	}
	if !hasConsensusAttr(span, "job.id", "42") {
		t.Fatal("expected job.id attribute")
	}
	if span.Status().Code != codes.Ok {
		t.Fatalf("expected Ok status, got %v", span.Status().Code)
	}
}

func TestApplyInvalidJSONRecordsErrorSpan(t *testing.T) {
	sr := oteltest.InstallSpanRecorder(t)

	fsm := consensus.NewFSM(store.NewMemoryStore())
	fsm.Apply(&raft.Log{Index: 1, Term: 1, Data: []byte("not-json")})

	span := findConsensusSpanByName(sr.Ended(), "raft.apply")
	if span == nil {
		t.Fatal("expected raft.apply span")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("expected Error status, got %v", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Fatal("expected recorded error event")
	}
}

func findConsensusSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, sp := range spans {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

func hasConsensusAttr(span sdktrace.ReadOnlySpan, key, expected string) bool {
	for _, kv := range span.Attributes() {
		if string(kv.Key) != key {
			continue
		}
		switch kv.Value.Type() {
		case attribute.STRING:
			return kv.Value.AsString() == expected
		case attribute.INT64:
			return strconv.FormatInt(kv.Value.AsInt64(), 10) == expected
		default:
			return false
		}
	}
	return false
}
