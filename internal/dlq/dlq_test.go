package dlq_test

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/dlq"
)

func testEvent(id string) adapter.Event {
	return adapter.Event{ID: id, CustomerID: "cust_1", EventName: "api_call", Timestamp: time.Now()}
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()

	lines := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines++
	}
	return lines
}

func TestPutAppendsAndTracksDepth(t *testing.T) {
	dir := t.TempDir()
	d, err := dlq.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Put("orb", testEvent("evt-1"), "4xx from provider", true); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := d.Put("orb", testEvent("evt-2"), "retry budget exhausted", false); err != nil {
		t.Fatalf("put: %v", err)
	}

	if got := d.Depth("orb"); got != 2 {
		t.Errorf("Depth(orb) = %d, want 2", got)
	}
	if got := d.Depth("metronome"); got != 0 {
		t.Errorf("Depth(metronome) = %d, want 0", got)
	}
	if lines := countLines(t, filepath.Join(dir, "orb.jsonl")); lines != 2 {
		t.Errorf("orb.jsonl has %d lines, want 2", lines)
	}
}

func TestPermanentFailurePoisonsAfterTwoAttempts(t *testing.T) {
	dir := t.TempDir()
	d, err := dlq.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	evt := testEvent("evt-1")
	if err := d.Put("orb", evt, "dead-lettered by provider", true); err != nil {
		t.Fatalf("put 1: %v", err)
	}
	regular, err := d.List("orb")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(regular) != 1 || regular[0].Poisoned {
		t.Fatalf("after 1 permanent failure: List() = %+v, want 1 non-poisoned entry", regular)
	}

	if err := d.Put("orb", evt, "dead-lettered by provider", true); err != nil {
		t.Fatalf("put 2: %v", err)
	}
	regular, err = d.List("orb")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(regular) != 0 {
		t.Errorf("after 2nd permanent failure: List() = %+v, want empty (should be poisoned)", regular)
	}
	poison, err := d.ListPoison("orb")
	if err != nil {
		t.Fatalf("list poison: %v", err)
	}
	if len(poison) != 1 || !poison[0].Poisoned || poison[0].Attempts != 2 {
		t.Errorf("ListPoison() = %+v, want 1 poisoned entry with Attempts=2", poison)
	}
}

func TestExhaustedFailurePoisonsAfterThreeAttempts(t *testing.T) {
	dir := t.TempDir()
	d, err := dlq.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	evt := testEvent("evt-1")
	for i := 0; i < 2; i++ {
		if err := d.Put("orb", evt, "retry budget exhausted", false); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if poison, _ := d.ListPoison("orb"); len(poison) != 0 {
		t.Fatalf("after 2 exhausted failures: ListPoison() = %+v, want empty", poison)
	}

	if err := d.Put("orb", evt, "retry budget exhausted", false); err != nil {
		t.Fatalf("put 3: %v", err)
	}
	poison, err := d.ListPoison("orb")
	if err != nil {
		t.Fatalf("list poison: %v", err)
	}
	if len(poison) != 1 || poison[0].Attempts != 3 {
		t.Errorf("after 3rd exhausted failure: ListPoison() = %+v, want 1 entry with Attempts=3", poison)
	}
}

func TestRemoveDeletesFromRegularAndPoisonFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := dlq.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	// evt-1 stays non-poisoned; evt-2 gets poisoned (2 permanent failures).
	if err := d.Put("orb", testEvent("evt-1"), "reason", true); err != nil {
		t.Fatalf("put: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := d.Put("orb", testEvent("evt-2"), "reason", true); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// evt-1's one entry plus evt-2's one (poisoned) entry: 2, not 3 —
	// evt-2's first, non-poisoned record was purged from the regular file
	// the moment its second failure crossed the poison threshold.
	if got := d.Depth("orb"); got != 2 {
		t.Fatalf("Depth(orb) = %d, want 2", got)
	}

	if err := d.Remove("orb", []string{"evt-1", "evt-2"}); err != nil {
		t.Fatalf("remove: %v", err)
	}

	regular, err := d.List("orb")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(regular) != 0 {
		t.Errorf("List() after remove = %+v, want empty", regular)
	}
	poison, err := d.ListPoison("orb")
	if err != nil {
		t.Fatalf("list poison: %v", err)
	}
	if len(poison) != 0 {
		t.Errorf("ListPoison() after remove = %+v, want empty", poison)
	}
	if got := d.Depth("orb"); got != 0 {
		t.Errorf("Depth(orb) after remove = %d, want 0", got)
	}

	// A Put after Remove must still work correctly (file handles reopened
	// cleanly after the rewrite-and-rename in Remove).
	if err := d.Put("orb", testEvent("evt-3"), "reason", true); err != nil {
		t.Fatalf("put after remove: %v", err)
	}
	if got := d.Depth("orb"); got != 1 {
		t.Errorf("Depth(orb) after put-after-remove = %d, want 1", got)
	}
}

func TestReopenReconstructsAttemptsDepthAndPoisonState(t *testing.T) {
	dir := t.TempDir()
	d, err := dlq.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := d.Put("orb", testEvent("evt-1"), "reason", true); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := d.Put("orb", testEvent("evt-2"), "reason", true); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := d.Put("orb", testEvent("evt-2"), "reason", true); err != nil {
		t.Fatalf("put (poison evt-2): %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := dlq.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = d2.Close() }()

	// evt-1's one non-poisoned record plus evt-2's poisoned record: 2
	// current entries, not 3 — evt-2's earlier non-poisoned record was
	// purged from the regular file the moment it crossed the poison
	// threshold, so it never survives to be counted after a restart.
	if got := d2.Depth("orb"); got != 2 {
		t.Fatalf("Depth(orb) after reopen = %d, want 2", got)
	}

	// evt-2 was already poisoned before the restart at Attempts=2; a
	// third failure for it should build on the reconstructed attempt
	// count, not restart from 1, and land straight back in the poison
	// file.
	if err := d2.Put("orb", testEvent("evt-2"), "reason", true); err != nil {
		t.Fatalf("put after reopen: %v", err)
	}
	poison, err := d2.ListPoison("orb")
	if err != nil {
		t.Fatalf("list poison: %v", err)
	}
	found := false
	for _, rec := range poison {
		if rec.Event.ID == "evt-2" && rec.Attempts == 3 {
			found = true
		}
	}
	if !found {
		t.Errorf("ListPoison() = %+v, want an evt-2 entry with Attempts=3 (reconstructed count continued)", poison)
	}
}
