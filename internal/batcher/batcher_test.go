package batcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/batcher"
)

type fakeAdapter struct {
	maxBatch       int
	dispositionFor func(eventID string, attempt int) adapter.Disposition

	mu        sync.Mutex
	sendCalls [][]adapter.Event
	attempts  map[string]int
}

func newFakeAdapter(maxBatch int, dispositionFor func(string, int) adapter.Disposition) *fakeAdapter {
	return &fakeAdapter{maxBatch: maxBatch, dispositionFor: dispositionFor, attempts: make(map[string]int)}
}

func (f *fakeAdapter) Encode(events []adapter.Event) ([]byte, error) {
	return json.Marshal(events)
}

func (f *fakeAdapter) Send(_ context.Context, body []byte) (adapter.BatchResult, error) {
	var events []adapter.Event
	if err := json.Unmarshal(body, &events); err != nil {
		return adapter.BatchResult{}, err
	}

	f.mu.Lock()
	f.sendCalls = append(f.sendCalls, events)
	f.mu.Unlock()

	results := make([]adapter.EventResult, len(events))
	for i, e := range events {
		f.mu.Lock()
		f.attempts[e.ID]++
		attempt := f.attempts[e.ID]
		f.mu.Unlock()

		d := adapter.Ok
		if f.dispositionFor != nil {
			d = f.dispositionFor(e.ID, attempt)
		}
		results[i] = adapter.EventResult{EventID: e.ID, Disposition: d}
	}
	return adapter.BatchResult{Results: results}, nil
}

func (f *fakeAdapter) Classify(_ error, _ int) adapter.Disposition { return adapter.Retry }
func (f *fakeAdapter) MaxBatchSize() int                           { return f.maxBatch }

func (f *fakeAdapter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sendCalls)
}

type ackCall struct {
	eventID, provider string
	disposition       adapter.Disposition
}

type fakeAcker struct {
	mu    sync.Mutex
	acked []ackCall
}

func (f *fakeAcker) Ack(eventID, provider string, disposition adapter.Disposition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, ackCall{eventID, provider, disposition})
	return nil
}

func (f *fakeAcker) find(eventID string) (ackCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.acked {
		if a.eventID == eventID {
			return a, true
		}
	}
	return ackCall{}, false
}

type dlqCall struct {
	provider, eventID, reason string
}

type fakeDLQ struct {
	mu   sync.Mutex
	puts []dlqCall
}

func (f *fakeDLQ) Put(provider string, event adapter.Event, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, dlqCall{provider, event.ID, reason})
	return nil
}

func (f *fakeDLQ) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.puts)
}

func testEvent(id string) adapter.Event {
	return adapter.Event{ID: id, CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Now()}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// TestCloseFlushesQueuedEventsBeforeLingerElapses proves the shutdown
// path a graceful signal (SIGINT/SIGTERM) relies on: events still sitting
// in the queue, well before their linger window or max-batch size would
// have triggered a flush on their own, must still be sent before Close()
// returns — not silently dropped.
func TestCloseFlushesQueuedEventsBeforeLingerElapses(t *testing.T) {
	ad := newFakeAdapter(100, nil) // always Ok; batch size never reached
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	// Linger is deliberately huge so only Close()'s drain-and-flush could
	// possibly resolve these before the test's own timeout would.
	b := batcher.New("orb", ad, time.Hour, acker, dlq, batcher.RetryPolicy{})

	b.Enqueue(testEvent("evt-1"))
	b.Enqueue(testEvent("evt-2"))

	b.Close()

	if got := ad.callCount(); got != 1 {
		t.Fatalf("adapter Send called %d times, want exactly 1 (one final flush on Close)", got)
	}
	for _, id := range []string{"evt-1", "evt-2"} {
		call, ok := acker.find(id)
		if !ok || call.disposition != adapter.Ok {
			t.Errorf("%s: expected Ok ack after Close, got %+v (ok=%v)", id, call, ok)
		}
	}
}

func TestFlushesOnMaxBatchSize(t *testing.T) {
	ad := newFakeAdapter(2, nil) // always Ok
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, time.Hour, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))
	b.Enqueue(testEvent("evt-2"))

	waitFor(t, time.Second, func() bool { return ad.callCount() == 1 })

	if _, ok := acker.find("evt-1"); !ok {
		t.Errorf("evt-1 not acked")
	}
	if _, ok := acker.find("evt-2"); !ok {
		t.Errorf("evt-2 not acked")
	}
}

func TestFlushesOnLinger(t *testing.T) {
	ad := newFakeAdapter(100, nil) // always Ok, batch size never reached
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, 20*time.Millisecond, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, time.Second, func() bool {
		_, ok := acker.find("evt-1")
		return ok
	})
}

func TestDeadLetterDisposition(t *testing.T) {
	ad := newFakeAdapter(10, func(string, int) adapter.Disposition { return adapter.DeadLetter })
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, 5*time.Millisecond, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, time.Second, func() bool { return dlq.count() == 1 })

	call, ok := acker.find("evt-1")
	if !ok || call.disposition != adapter.DeadLetter {
		t.Errorf("expected evt-1 acked DeadLetter, got %+v (ok=%v)", call, ok)
	}
}

func TestRetryThenOk(t *testing.T) {
	ad := newFakeAdapter(10, func(_ string, attempt int) adapter.Disposition {
		if attempt < 3 {
			return adapter.Retry
		}
		return adapter.Ok
	})
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	retry := batcher.RetryPolicy{InitialBackoff: 2 * time.Millisecond, MaxBackoff: 5 * time.Millisecond, MaxElapsed: time.Second}
	b := batcher.New("orb", ad, 2*time.Millisecond, acker, dlq, retry)
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, 2*time.Second, func() bool {
		call, ok := acker.find("evt-1")
		return ok && call.disposition == adapter.Ok
	})
	if dlq.count() != 0 {
		t.Errorf("expected no dead-lettering, got %d", dlq.count())
	}
}

// slowAdapter tracks how many Send calls are concurrently in-flight, to
// prove a single Batcher never fires two provider requests at once even
// when new events queue up faster than a slow provider responds.
type slowAdapter struct {
	maxBatch  int
	sendDelay time.Duration

	mu            sync.Mutex
	sendCalls     int
	concurrent    int
	maxConcurrent int
	batchSizes    []int
}

func (a *slowAdapter) Encode(events []adapter.Event) ([]byte, error) {
	return json.Marshal(events)
}

func (a *slowAdapter) Send(_ context.Context, body []byte) (adapter.BatchResult, error) {
	var events []adapter.Event
	if err := json.Unmarshal(body, &events); err != nil {
		return adapter.BatchResult{}, err
	}

	a.mu.Lock()
	a.sendCalls++
	a.concurrent++
	if a.concurrent > a.maxConcurrent {
		a.maxConcurrent = a.concurrent
	}
	a.batchSizes = append(a.batchSizes, len(events))
	a.mu.Unlock()

	time.Sleep(a.sendDelay)

	a.mu.Lock()
	a.concurrent--
	a.mu.Unlock()

	results := make([]adapter.EventResult, len(events))
	for i, e := range events {
		results[i] = adapter.EventResult{EventID: e.ID, Disposition: adapter.Ok}
	}
	return adapter.BatchResult{Results: results}, nil
}

func (a *slowAdapter) Classify(_ error, _ int) adapter.Disposition { return adapter.Retry }
func (a *slowAdapter) MaxBatchSize() int                           { return a.maxBatch }

func (a *slowAdapter) snapshot() (calls, maxConcurrent int, sizes []int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sendCalls, a.maxConcurrent, append([]int(nil), a.batchSizes...)
}

// TestNoConcurrentSendsForSameProvider answers a specific question about
// the design: if 100 events arrive while a send to the provider is
// already in flight, does a second request go out at the same time? No —
// the Batcher's single run() goroutine calls flush() synchronously, so it
// structurally cannot start a second Send until the first returns. The
// 100 events queue up in the channel buffer and, once the first Send
// completes, get coalesced into one subsequent batch — never sent
// concurrently, and never sent one-by-one either.
func TestNoConcurrentSendsForSameProvider(t *testing.T) {
	// linger triggers the first (lone) flush; maxBatch is comfortably
	// above 100 so the second flush coalesces all 100 queued events into
	// a single batch rather than triggering on size.
	ad := &slowAdapter{maxBatch: 200, sendDelay: 150 * time.Millisecond}
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	b := batcher.New("orb", ad, 20*time.Millisecond, acker, dlq, batcher.RetryPolicy{})
	defer b.Close()

	b.Enqueue(testEvent("evt-0"))
	time.Sleep(40 * time.Millisecond) // let the linger-triggered first Send actually start

	for i := 1; i <= 100; i++ {
		b.Enqueue(testEvent(fmt.Sprintf("evt-%d", i)))
	}

	waitFor(t, 3*time.Second, func() bool {
		for i := 1; i <= 100; i++ {
			if _, ok := acker.find(fmt.Sprintf("evt-%d", i)); !ok {
				return false
			}
		}
		return true
	})

	calls, maxConcurrent, sizes := ad.snapshot()
	if maxConcurrent != 1 {
		t.Errorf("max concurrent Send calls = %d, want 1 (no concurrent sends to the same provider)", maxConcurrent)
	}
	if calls != 2 {
		t.Errorf("Send called %d time(s), want exactly 2 (the lone kickoff event, then all 100 queued-while-busy events coalesced into one batch)", calls)
	}
	if len(sizes) == 2 && sizes[1] != 100 {
		t.Errorf("second Send batch size = %d, want 100 (all events queued while the first Send was in flight)", sizes[1])
	}
	t.Logf("Send call batch sizes: %v", sizes)
}

func TestRetryBudgetExhaustion(t *testing.T) {
	ad := newFakeAdapter(10, func(string, int) adapter.Disposition { return adapter.Retry })
	acker := &fakeAcker{}
	dlq := &fakeDLQ{}

	retry := batcher.RetryPolicy{InitialBackoff: 2 * time.Millisecond, MaxBackoff: 3 * time.Millisecond, MaxElapsed: 15 * time.Millisecond}
	b := batcher.New("orb", ad, 2*time.Millisecond, acker, dlq, retry)
	defer b.Close()

	b.Enqueue(testEvent("evt-1"))

	waitFor(t, 2*time.Second, func() bool { return dlq.count() == 1 })

	call, ok := acker.find("evt-1")
	if !ok || call.disposition != adapter.DeadLetter {
		t.Errorf("expected evt-1 eventually acked DeadLetter after retry exhaustion, got %+v (ok=%v)", call, ok)
	}
}
