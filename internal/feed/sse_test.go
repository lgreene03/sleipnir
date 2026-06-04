package feed

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSSEDecoder(t *testing.T) {
	d := &sseDecoder{}

	if _, ok := d.push(":keepalive"); ok {
		t.Fatal("comment line should not complete a frame")
	}
	if _, ok := d.push("event: feature"); ok {
		t.Fatal("event line should not complete a frame")
	}
	if _, ok := d.push("data: {\"a\":1}"); ok {
		t.Fatal("data line should not complete a frame")
	}
	frame, ok := d.push("")
	if !ok {
		t.Fatal("blank line after data should complete a frame")
	}
	if frame.event != "feature" || frame.data != `{"a":1}` {
		t.Fatalf("unexpected frame: %+v", frame)
	}

	// Multi-line data joins with newlines; absent event defaults to "message".
	d.push("data: line1")
	d.push("data: line2")
	frame, ok = d.push("")
	if !ok {
		t.Fatal("expected completed frame for multi-line data")
	}
	if frame.event != "message" || frame.data != "line1\nline2" {
		t.Fatalf("unexpected multi-line frame: %+v", frame)
	}

	if _, ok := d.push(""); ok {
		t.Fatal("blank line with no data should not complete a frame")
	}
}

// writeFeatureFrame emits one SSE feature frame and flushes it.
func writeFeatureFrame(t *testing.T, w http.ResponseWriter, name string, value float64) {
	t.Helper()
	fmt.Fprintf(w, "event: feature\n")
	fmt.Fprintf(w, "data: {\"eventId\":\"id-%s\",\"eventTime\":\"2026-06-04T12:00:00Z\",\"featureName\":\"%s\",\"featureVersion\":\"v1\",\"value\":%v,\"windowStart\":\"2026-06-04T11:59:00Z\",\"windowEnd\":\"2026-06-04T12:00:00Z\"}\n\n", name, name, value)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func TestStreamClient_StreamsEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, ": keepalive\n\n")
		writeFeatureFrame(t, w, "vwap.1m", 101.5)
		writeFeatureFrame(t, w, "obi.1m", 0.7)
		<-r.Context().Done()
	}))
	defer srv.Close()

	events := make(chan FeatureEvent, 8)
	c := NewStreamClient(Config{BaseURL: srv.URL}, func(ev FeatureEvent) { events <- ev }, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	got := make(map[string]float64)
	for range 2 {
		select {
		case ev := <-events:
			if ev.Value == nil {
				t.Fatalf("event %s missing scalar value", ev.FeatureName)
			}
			got[ev.FeatureName] = *ev.Value
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for feature events")
		}
	}
	if got["vwap.1m"] != 101.5 || got["obi.1m"] != 0.7 {
		t.Fatalf("unexpected events: %+v", got)
	}
}

func TestStreamClient_Reconnects(t *testing.T) {
	var mu sync.Mutex
	conns := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conns++
		n := conns
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if n == 1 {
			return // drop immediately to force a reconnect
		}
		writeFeatureFrame(t, w, "obi.1m", 0.9)
		<-r.Context().Done()
	}))
	defer srv.Close()

	events := make(chan FeatureEvent, 4)
	c := NewStreamClient(Config{
		BaseURL:   srv.URL,
		BaseDelay: time.Millisecond,
		MaxDelay:  10 * time.Millisecond,
	}, func(ev FeatureEvent) { events <- ev }, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	select {
	case ev := <-events:
		if ev.Value == nil || *ev.Value != 0.9 {
			t.Fatalf("unexpected event after reconnect: %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event after reconnect")
	}

	mu.Lock()
	defer mu.Unlock()
	if conns < 2 {
		t.Fatalf("expected at least 2 connection attempts, got %d", conns)
	}
}

func TestStreamClient_Non2xxRetriesThenStops(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewStreamClient(Config{
		BaseURL:   srv.URL,
		BaseDelay: time.Millisecond,
		MaxDelay:  5 * time.Millisecond,
	}, func(FeatureEvent) { t.Fatal("handler must not run on a 5xx stream") }, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

func TestLatestStore(t *testing.T) {
	s := NewLatestStore()
	v1, v2 := 1.0, 2.0
	s.Record(FeatureEvent{FeatureName: "obi.1m", Value: &v1})
	s.Record(FeatureEvent{FeatureName: "obi.1m", Value: &v2}) // overwrite
	s.Record(FeatureEvent{FeatureName: "vwap.1m", Value: &v1})

	snap := s.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 features, got %d", len(snap))
	}
	if got := snap["obi.1m"]; got.Value == nil || *got.Value != 2.0 {
		t.Fatalf("obi.1m should reflect the latest value: %+v", got)
	}

	// Snapshot is a copy: mutating it must not affect the store.
	delete(snap, "obi.1m")
	if _, ok := s.Snapshot()["obi.1m"]; !ok {
		t.Fatal("Snapshot must return an independent copy")
	}
}
