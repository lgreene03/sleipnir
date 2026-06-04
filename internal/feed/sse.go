// Package feed consumes Muninn's live feature stream over Server-Sent Events
// (GET /api/v1/features/stream, muninn ADR-0009) and surfaces the most recent
// value per feature for operator visibility.
//
// This is sleipnir Phase 9. The transport (StreamClient) lands first; the first
// concrete consumer is LatestStore, a thread-safe "latest value per feature"
// snapshot exposed by the HTTP API at /feature/latest. It deliberately does NOT
// feed the trading path: sleipnir does not pick trades (see ROADMAP non-goals).
// Strategy lives in huginn. This feed is read-only situational awareness.
//
// The wire format and the SSE line decoder mirror muninn-py's
// muninn.streaming.MuninnStreamClient — the reference implementation promoted
// by the same trigger (T3) — so the two clients agree frame for frame.
package feed

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"sleipnir/internal/telemetry"
)

const (
	streamPath       = "/api/v1/features/stream"
	featureEventName = "feature"
)

// FeatureEvent is a decoded Muninn FeatureComputedEvent
// (io.muninn.shared.event.FeatureComputedEvent). Scalar features populate
// Value; map features populate Values.
type FeatureEvent struct {
	EventID        string             `json:"eventId"`
	EventTime      time.Time          `json:"eventTime"`
	FeatureName    string             `json:"featureName"`
	FeatureVersion string             `json:"featureVersion"`
	Value          *float64           `json:"value,omitempty"`
	Values         map[string]float64 `json:"values,omitempty"`
	WindowStart    time.Time          `json:"windowStart"`
	WindowEnd      time.Time          `json:"windowEnd"`
}

// Config configures a StreamClient. Zero backoff values fall back to the same
// discipline as the Binance WS reconnect loop (500ms base, 60s max).
type Config struct {
	// BaseURL is Muninn's host, e.g. "http://localhost:8080". The stream path
	// is appended; a trailing slash is tolerated.
	BaseURL string
	// Feature, when non-empty, restricts the stream to one feature name via
	// the ?feature= query parameter. Empty streams all features.
	Feature string
	// BaseDelay / MaxDelay bound the exponential reconnect backoff.
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// StreamClient maintains a connection to Muninn's SSE feature stream and calls
// a handler for every decoded feature event, reconnecting with the same
// exponential-backoff-with-jitter discipline as the exchange WS connector.
type StreamClient struct {
	url       string
	baseDelay time.Duration
	maxDelay  time.Duration
	client    *http.Client
	logger    *slog.Logger
	handler   func(FeatureEvent)
}

// NewStreamClient builds a client that streams from cfg.BaseURL and calls
// handler for every decoded feature event.
func NewStreamClient(cfg Config, handler func(FeatureEvent), logger *slog.Logger) *StreamClient {
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 500 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 60 * time.Second
	}
	url := strings.TrimRight(cfg.BaseURL, "/") + streamPath
	if cfg.Feature != "" {
		url += "?feature=" + cfg.Feature
	}
	return &StreamClient{
		url:       url,
		baseDelay: cfg.BaseDelay,
		maxDelay:  cfg.MaxDelay,
		// No client-level timeout: an SSE connection is idle between events and
		// muninn keeps it warm with keepalive comments; a request timeout would
		// sever a healthy-but-quiet stream. Cancellation is the ctx's job.
		client:  &http.Client{},
		logger:  logger,
		handler: handler,
	}
}

// Run connects and streams until ctx is cancelled, reconnecting with
// exponential backoff (×2) plus ±10% jitter, capped at MaxDelay. A connection
// that stayed up longer than 30s resets the backoff to BaseDelay — the same
// "stable connection" rule the Binance WS connector uses. It blocks; run it in
// a goroutine.
func (c *StreamClient) Run(ctx context.Context) {
	const (
		factor        = 2.0
		jitterPercent = 0.10
		stableReset   = 30 * time.Second
	)
	c.logger.Info("Starting Muninn SSE feature-stream consumer", "url", c.url)

	retryDelay := c.baseDelay
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Muninn SSE feature-stream consumer stopped")
			return
		default:
		}

		connectedAt := time.Now()
		err := c.streamOnce(ctx)
		telemetry.FeatureStreamConnected.Set(0)
		if ctx.Err() != nil {
			c.logger.Info("Muninn SSE feature-stream consumer stopped")
			return
		}
		telemetry.FeatureStreamDrops.Inc()

		// A connection that stayed up beyond the stable window resets the
		// backoff so a long-lived stream that finally drops reconnects promptly.
		if time.Since(connectedAt) > stableReset {
			retryDelay = c.baseDelay
		}

		jitterVal := (rand.Float64() * 2.0 * jitterPercent) - jitterPercent //nolint:gosec // jitter only; not security-sensitive
		delay := time.Duration(float64(retryDelay) * (1.0 + jitterVal))
		if delay > c.maxDelay {
			delay = c.maxDelay
		}
		c.logger.Warn("Muninn SSE feature stream disconnected, backing off",
			"error", err, "backoff", delay.String())

		select {
		case <-ctx.Done():
			c.logger.Info("Muninn SSE feature-stream consumer stopped")
			return
		case <-time.After(delay):
		}

		retryDelay = time.Duration(float64(retryDelay) * factor)
		if retryDelay > c.maxDelay {
			retryDelay = c.maxDelay
		}
	}
}

// streamOnce performs a single connect-and-read cycle, returning when the
// stream ends or errors.
func (c *StreamClient) streamOnce(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	telemetry.FeatureStreamConnected.Set(1)
	c.logger.Info("Muninn SSE feature stream connected", "url", c.url)

	dec := &sseDecoder{}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		frame, ok := dec.push(scanner.Text())
		if !ok || frame.event != featureEventName {
			continue
		}
		var ev FeatureEvent
		if err := json.Unmarshal([]byte(frame.data), &ev); err != nil {
			c.logger.Warn("Dropping malformed SSE feature frame", "error", err)
			continue
		}
		telemetry.KafkaMessagesProcessed.WithLabelValues("features.stream", "consume", "success").Inc()
		c.handler(ev)
	}
	return scanner.Err()
}

// sseFrame is one completed Server-Sent Events frame.
type sseFrame struct {
	event string
	data  string
}

// sseDecoder is an incremental Server-Sent Events line decoder. Fed one line at
// a time (no trailing newline, as bufio.Scanner yields), push returns a
// completed frame when a blank line terminates it. Comment lines (starting with
// ":", e.g. keepalives) and fields other than event/data are ignored per the
// SSE spec. Mirrors muninn-py's _SseDecoder.
type sseDecoder struct {
	event string
	data  []string
}

func (d *sseDecoder) push(line string) (sseFrame, bool) {
	if line == "" {
		if len(d.data) == 0 {
			d.event = ""
			return sseFrame{}, false
		}
		frame := sseFrame{event: d.event, data: strings.Join(d.data, "\n")}
		if frame.event == "" {
			frame.event = "message"
		}
		d.event = ""
		d.data = nil
		return frame, true
	}
	if strings.HasPrefix(line, ":") {
		return sseFrame{}, false
	}
	field, value, _ := strings.Cut(line, ":")
	value = strings.TrimPrefix(value, " ")
	switch field {
	case "event":
		d.event = value
	case "data":
		d.data = append(d.data, value)
	}
	return sseFrame{}, false
}

// LatestStore keeps the most recent FeatureEvent per feature name. It is the
// first concrete consumer of the stream — operator visibility into what muninn
// is currently emitting, surfaced at /feature/latest. Safe for concurrent use.
type LatestStore struct {
	mu     sync.RWMutex
	latest map[string]FeatureEvent
}

// NewLatestStore returns an empty LatestStore.
func NewLatestStore() *LatestStore {
	return &LatestStore{latest: make(map[string]FeatureEvent)}
}

// Record stores ev as the latest value for its feature name. Use it as the
// StreamClient handler.
func (s *LatestStore) Record(ev FeatureEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest[ev.FeatureName] = ev
}

// Snapshot returns a copy of the current latest-value map.
func (s *LatestStore) Snapshot() map[string]FeatureEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]FeatureEvent, len(s.latest))
	for k, v := range s.latest {
		out[k] = v
	}
	return out
}
