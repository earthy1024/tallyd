// Package metronome implements adapter.Adapter for Metronome's usage
// event ingest API: POST https://api.metronome.com/v1/ingest, a JSON
// array of events (1-100 per request), Bearer-token authenticated.
package metronome

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/earthy1024/tallyd/adapter"
)

// DefaultEndpoint is Metronome's usage event ingest endpoint.
const DefaultEndpoint = "https://api.metronome.com/v1/ingest"

// MaxBatchSize is Metronome's documented hard limit: 1-100 events per
// ingest request.
const MaxBatchSize = 100

// wireEvent is Metronome's on-the-wire event shape.
type wireEvent struct {
	TransactionID string            `json:"transaction_id"`
	CustomerID    string            `json:"customer_id"`
	EventType     string            `json:"event_type"`
	Timestamp     string            `json:"timestamp"`
	Properties    map[string]string `json:"properties,omitempty"`
}

// Adapter implements adapter.Adapter for Metronome.
type Adapter struct {
	Endpoint   string
	Token      string
	HTTPClient *http.Client

	// MaxBatch optionally lowers the batch size below MaxBatchSize; it is
	// always capped at MaxBatchSize regardless of what it's set to, since
	// that's Metronome's hard API limit, not a tunable.
	MaxBatch int
}

// New returns a Metronome Adapter. token is sent as a Bearer token on
// every request; endpoint defaults to DefaultEndpoint if empty.
func New(endpoint, token string) *Adapter {
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	return &Adapter{
		Endpoint:   endpoint,
		Token:      token,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (a *Adapter) MaxBatchSize() int {
	if a.MaxBatch <= 0 || a.MaxBatch > MaxBatchSize {
		return MaxBatchSize
	}
	return a.MaxBatch
}

func (a *Adapter) Encode(events []adapter.Event) ([]byte, error) {
	wire := make([]wireEvent, len(events))
	for i, e := range events {
		wire[i] = wireEvent{
			TransactionID: e.ID,
			CustomerID:    e.CustomerID,
			EventType:     e.EventName,
			Timestamp:     e.Timestamp.UTC().Format(time.RFC3339),
			Properties:    stringifyProperties(e.Properties),
		}
	}
	return json.Marshal(wire)
}

// stringifyProperties converts every value to its string form: Metronome
// requires all keys and values in "properties" to be strings, even when
// the underlying value is numeric or boolean.
func stringifyProperties(props map[string]any) map[string]string {
	if len(props) == 0 {
		return nil
	}
	out := make(map[string]string, len(props))
	for k, v := range props {
		out[k] = stringifyValue(v)
	}
	return out
}

func stringifyValue(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("%v", t)
		}
		return string(b)
	}
}

func (a *Adapter) Send(ctx context.Context, body []byte) (adapter.BatchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Endpoint, bytes.NewReader(body))
	if err != nil {
		return adapter.BatchResult{}, fmt.Errorf("metronome: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.Token)

	resp, err := a.HTTPClient.Do(req)
	if err != nil {
		return adapter.BatchResult{}, fmt.Errorf("metronome: send: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Metronome's documented response for a successful ingest doesn't
		// specify a per-event result body, so a 2xx means the whole batch
		// was accepted. Leaving Results empty means batcher.flush treats
		// every event as Ok (see its fallback for events an adapter
		// doesn't report explicitly).
		// TODO: if Metronome starts returning per-event outcomes, parse
		// respBody here for granular per-event dispositions instead.
		return adapter.BatchResult{}, nil
	}

	return adapter.BatchResult{}, &sendError{status: resp.StatusCode, body: string(respBody)}
}

// sendError carries the HTTP status Classify needs, since batcher.flush
// calls Classify(err, 0) on a transport-level Send error rather than
// passing the status separately.
type sendError struct {
	status int
	body   string
}

func (e *sendError) Error() string {
	return fmt.Sprintf("metronome: unexpected status %d: %s", e.status, e.body)
}

func (a *Adapter) Classify(err error, status int) adapter.Disposition {
	if se, ok := err.(*sendError); ok {
		status = se.status
	}
	switch {
	case status == http.StatusTooManyRequests || status >= 500:
		return adapter.Retry
	case status >= 400:
		return adapter.DeadLetter
	default:
		// No usable HTTP status (e.g. a network-level error): retry.
		return adapter.Retry
	}
}
