package dlqshow_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/dlq"
	"github.com/tallyd/tallyd/internal/dlqshow"
)

type fakeStore struct {
	regular map[string][]dlq.Record
	poison  map[string][]dlq.Record
}

func (f *fakeStore) List(provider string) ([]dlq.Record, error)       { return f.regular[provider], nil }
func (f *fakeStore) ListPoison(provider string) ([]dlq.Record, error) { return f.poison[provider], nil }

func testEvent(id string) adapter.Event {
	return adapter.Event{ID: id, CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Now()}
}

func doGet(t *testing.T, h http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestShowReturnsRegularRecordsByDefault(t *testing.T) {
	store := &fakeStore{
		regular: map[string][]dlq.Record{"orb": {{Provider: "orb", Event: testEvent("evt-1")}}},
		poison:  map[string][]dlq.Record{"orb": {{Provider: "orb", Event: testEvent("evt-poisoned")}}},
	}
	h := &dlqshow.Handler{DLQ: store, KnownProviders: map[string]bool{"orb": true}}

	rec := doGet(t, h, "/v1/dlq?provider=orb")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var result dlqshow.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(result.Records) != 1 || result.Records[0].Event.ID != "evt-1" {
		t.Errorf("Records = %+v, want only evt-1 (poisoned entry excluded)", result.Records)
	}
}

func TestShowIncludePoison(t *testing.T) {
	store := &fakeStore{
		regular: map[string][]dlq.Record{"orb": {{Provider: "orb", Event: testEvent("evt-1")}}},
		poison:  map[string][]dlq.Record{"orb": {{Provider: "orb", Event: testEvent("evt-poisoned")}}},
	}
	h := &dlqshow.Handler{DLQ: store, KnownProviders: map[string]bool{"orb": true}}

	rec := doGet(t, h, "/v1/dlq?provider=orb&include_poison=true")

	var result dlqshow.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(result.Records) != 2 {
		t.Errorf("Records = %+v, want 2 (poisoned entry included)", result.Records)
	}
}

func TestShowEmptyReturnsEmptyArrayNotNull(t *testing.T) {
	store := &fakeStore{}
	h := &dlqshow.Handler{DLQ: store, KnownProviders: map[string]bool{"orb": true}}

	rec := doGet(t, h, "/v1/dlq?provider=orb")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); !jsonHasEmptyArray(got) {
		t.Errorf("body = %s, want records to be [] not null", got)
	}
}

func jsonHasEmptyArray(body string) bool {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		return false
	}
	return string(raw["records"]) == "[]"
}

func TestShowRejectsUnknownProvider(t *testing.T) {
	store := &fakeStore{regular: map[string][]dlq.Record{"metronome": {{Provider: "metronome", Event: testEvent("evt-1")}}}}
	h := &dlqshow.Handler{DLQ: store, KnownProviders: map[string]bool{"orb": true}} // metronome not known

	rec := doGet(t, h, "/v1/dlq?provider=metronome")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestShowRequiresProviderParam(t *testing.T) {
	h := &dlqshow.Handler{DLQ: &fakeStore{}, KnownProviders: map[string]bool{"orb": true}}
	rec := doGet(t, h, "/v1/dlq")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestShowOnlyAcceptsGet(t *testing.T) {
	h := &dlqshow.Handler{DLQ: &fakeStore{}, KnownProviders: map[string]bool{"orb": true}}
	req := httptest.NewRequest(http.MethodPost, "/v1/dlq?provider=orb", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
