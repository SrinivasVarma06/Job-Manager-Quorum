package events

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type EventType string

const (
	EventWorkerRegistered  EventType = "WORKER_REGISTERED"
	EventWorkerEvicted     EventType = "WORKER_EVICTED"
	EventJobSubmitted      EventType = "JOB_SUBMITTED"
	EventLeaseGranted      EventType = "LEASE_GRANTED"
	EventLeaseReleased     EventType = "LEASE_RELEASED"
	EventLeaderChanged     EventType = "LEADER_CHANGED"
	EventQueueRebuilt      EventType = "QUEUE_REBUILT"
	EventDispatchResumed   EventType = "DISPATCH_RESUMED"
	EventFailoverTriggered EventType = "FAILOVER_TRIGGERED"
)

type Event struct {
	Type      EventType         `json:"type"`
	Message   string            `json:"message"`
	Timestamp time.Time         `json:"timestamp"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Broadcaster struct {
	mu          sync.Mutex
	subscribers map[chan Event]struct{}
}

var globalBroadcaster = &Broadcaster{
	subscribers: make(map[chan Event]struct{}),
}

func Global() *Broadcaster {
	return globalBroadcaster
}

func (b *Broadcaster) Subscribe() chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan Event, 100)
	b.subscribers[ch] = struct{}{}
	return ch
}

func (b *Broadcaster) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.subscribers, ch)
	close(ch)
}

func (b *Broadcaster) Broadcast(evt Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}

	for ch := range b.subscribers {
		select {
		case ch <- evt:
		default:
			// Drop slow subscribers to prevent blocking
		}
	}
}

// HTTPHandler serves Server-Sent Events (SSE) to web clients at GET /events.
func (b *Broadcaster) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		ch := b.Subscribe()
		defer b.Unsubscribe(ch)

		// Send initial connected event
		initEvt, _ := json.Marshal(Event{
			Type:      "CONNECTED",
			Message:   "Stream connected to Quorum Control Plane Event Broadcaster",
			Timestamp: time.Now(),
		})
		fmt.Fprintf(w, "data: %s\n\n", initEvt)
		w.(http.Flusher).Flush()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				data, err := json.Marshal(evt)
				if err == nil {
					fmt.Fprintf(w, "data: %s\n\n", data)
					w.(http.Flusher).Flush()
				}
			}
		}
	}
}
