package status_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/dlq"
	"github.com/tallyd/tallyd/internal/status"
)

type fakeWAL struct{ unacked int }

func (f *fakeWAL) UnackedCount() int { return f.unacked }

type fakeStore struct {
	regular map[string][]dlq.Record
	poison  map[string][]dlq.Record
}

func (f *fakeStore) List(provider string) ([]dlq.Record, error)       { return f.regular[provider], nil }
func (f *fakeStore) ListPoison(provider string) ([]dlq.Record, error) { return f.poison[provider], nil }

func testEvent(id string) adapter.Event {
	return adapter.Event{ID: id, CustomerID: "cust_1", EventName: "api_call"}
}

func TestStatusReportsWALAndPerProviderDLQDepth(t *testing.T) {
	h := &status.Handler{
		WAL: &fakeWAL{unacked: 7},
		DLQ: &fakeStore{
			regular: map[string][]dlq.Record{"metronome": {{Event: testEvent("evt-1")}, {Event: testEvent("evt-2")}}},
			poison:  map[string][]dlq.Record{"metronome": {{Event: testEvent("evt-3")}}},
		},
		Providers: []string{"metronome"},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var result status.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.WALUnackedEntries != 7 {
		t.Errorf("WALUnackedEntries = %d, want 7", result.WALUnackedEntries)
	}
	got := result.DLQ["metronome"]
	if got.Depth != 2 || got.PoisonDepth != 1 {
		t.Errorf("DLQ[metronome] = %+v, want {Depth:2 PoisonDepth:1}", got)
	}
}

func TestStatusOnlyAcceptsGet(t *testing.T) {
	h := &status.Handler{WAL: &fakeWAL{}, DLQ: &fakeStore{}, Providers: nil}
	req := httptest.NewRequest(http.MethodPost, "/v1/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
