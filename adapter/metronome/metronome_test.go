package metronome_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/adapter/metronome"
)

func testEvent() adapter.Event {
	return adapter.Event{
		ID:         "evt-1",
		CustomerID: "cust_1",
		EventName:  "api_call",
		Timestamp:  time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		Properties: map[string]any{
			"endpoint":    "/charge",
			"compute_ms":  float64(912), // json.Unmarshal shape: numbers decode as float64
			"cache_hit":   true,
			"retry_count": nil,
		},
	}
}

func TestEncodeMapsFieldsAndStringifiesProperties(t *testing.T) {
	a := metronome.New("", "token")

	body, err := a.Encode([]adapter.Event{testEvent()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var decoded []map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal encoded body: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("got %d events, want 1", len(decoded))
	}

	got := decoded[0]
	if got["transaction_id"] != "evt-1" {
		t.Errorf("transaction_id = %v, want evt-1", got["transaction_id"])
	}
	if got["customer_id"] != "cust_1" {
		t.Errorf("customer_id = %v, want cust_1", got["customer_id"])
	}
	if got["event_type"] != "api_call" {
		t.Errorf("event_type = %v, want api_call", got["event_type"])
	}
	if got["timestamp"] != "2026-07-11T12:00:00Z" {
		t.Errorf("timestamp = %v, want 2026-07-11T12:00:00Z", got["timestamp"])
	}

	props, ok := got["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties not an object: %v", got["properties"])
	}
	wantProps := map[string]any{
		"endpoint":    "/charge",
		"compute_ms":  "912",
		"cache_hit":   "true",
		"retry_count": "",
	}
	for k, want := range wantProps {
		if props[k] != want {
			t.Errorf("properties[%q] = %v (%T), want %v", k, props[k], props[k], want)
		}
	}
}

func TestMaxBatchSizeCapsAtDocumentedLimit(t *testing.T) {
	a := metronome.New("", "token")
	a.MaxBatch = 500 // above Metronome's real limit
	if got := a.MaxBatchSize(); got != metronome.MaxBatchSize {
		t.Errorf("MaxBatchSize() = %d, want %d (capped)", got, metronome.MaxBatchSize)
	}

	a.MaxBatch = 50
	if got := a.MaxBatchSize(); got != 50 {
		t.Errorf("MaxBatchSize() = %d, want 50", got)
	}
}

func TestSendSuccess(t *testing.T) {
	var gotAuth, gotMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	a := metronome.New(server.URL, "secret-token")
	body, err := a.Encode([]adapter.Event{testEvent()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	result, err := a.Send(context.Background(), body)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(result.Results) != 0 {
		t.Errorf("expected no explicit per-event results on success, got %+v", result.Results)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %s, want POST", gotMethod)
	}
	if gotAuth != "Bearer secret-token" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer secret-token")
	}
}

func TestSendAndClassifyByStatus(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantResult adapter.Disposition
	}{
		{"too_many_requests", http.StatusTooManyRequests, adapter.Retry},
		{"server_error", http.StatusInternalServerError, adapter.Retry},
		{"bad_request", http.StatusBadRequest, adapter.DeadLetter},
		{"unauthorized", http.StatusUnauthorized, adapter.DeadLetter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(`{"error":"nope"}`))
			}))
			defer server.Close()

			a := metronome.New(server.URL, "token")
			body, err := a.Encode([]adapter.Event{testEvent()})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			_, sendErr := a.Send(context.Background(), body)
			if sendErr == nil {
				t.Fatalf("expected send error for status %d", tt.status)
			}

			got := a.Classify(sendErr, 0)
			if got != tt.wantResult {
				t.Errorf("Classify() = %v, want %v", got, tt.wantResult)
			}
		})
	}
}

func TestClassifyNetworkError(t *testing.T) {
	a := metronome.New("http://127.0.0.1:0", "token") // nothing listening
	body, err := a.Encode([]adapter.Event{testEvent()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	_, sendErr := a.Send(context.Background(), body)
	if sendErr == nil {
		t.Fatalf("expected a network-level send error")
	}
	if got := a.Classify(sendErr, 0); got != adapter.Retry {
		t.Errorf("Classify() for network error = %v, want Retry", got)
	}
}
