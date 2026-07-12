// Package dlq provides durable on-disk parking for events a provider has
// permanently rejected or that exhausted their retry budget.
package dlq

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tallyd/tallyd/adapter"
)

const (
	// permanentPoisonThreshold poisons an event after this many total
	// dead-letters when the failure looked permanent (provider
	// rejection, encode error) — a second failure of the same kind is
	// already strong evidence that retrying again won't help.
	permanentPoisonThreshold = 2
	// exhaustedPoisonThreshold is more lenient: a retry-budget-exhausted
	// dead-letter failed on time, not necessarily on the payload, so it
	// gets a couple more chances before being written off. Neither
	// threshold is configurable yet — fixed defaults for this first cut.
	exhaustedPoisonThreshold = 3
)

// Record is one dead-lettered event as stored on disk.
type Record struct {
	Provider  string        `json:"provider"`
	Event     adapter.Event `json:"event"`
	Reason    string        `json:"reason"`
	Timestamp time.Time     `json:"timestamp"`
	// Attempts is the cumulative number of times this event (by ID) has
	// been dead-lettered for this provider across the DLQ's whole
	// history, reconstructed from disk on restart — not just this
	// process's lifetime.
	Attempts int `json:"attempts"`
	// Poisoned is true once Attempts reached the threshold for its
	// failure category. Poisoned records live in a separate
	// "<provider>.poison.jsonl" file and are excluded from replay unless
	// explicitly requested.
	Poisoned bool `json:"poisoned,omitempty"`
}

// DLQ appends dead-lettered events as JSON lines to a per-provider file
// (plus a separate per-provider poison file once an event has failed
// repeatedly enough — see permanentPoisonThreshold/exhaustedPoisonThreshold).
type DLQ struct {
	dir string

	mu       sync.Mutex
	handles  map[string]*os.File      // keyed by full file path
	depth    map[string]int           // provider -> current (not cumulative) entry count
	attempts map[string]map[string]int // provider -> event ID -> cumulative dead-letter count
}

// Open opens (creating if needed) a DLQ rooted at dir, reconstructing
// depth and attempt counts from whatever's already on disk so a restart
// doesn't lose track of prior failures.
func Open(dir string) (*DLQ, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("dlq: mkdir %s: %w", dir, err)
	}

	d := &DLQ{
		dir:      dir,
		handles:  make(map[string]*os.File),
		depth:    make(map[string]int),
		attempts: make(map[string]map[string]int),
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("dlq: read %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		provider := strings.TrimSuffix(strings.TrimSuffix(e.Name(), ".jsonl"), ".poison")

		records, err := d.readRecords(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		if d.attempts[provider] == nil {
			d.attempts[provider] = make(map[string]int)
		}
		for _, rec := range records {
			if rec.Attempts > d.attempts[provider][rec.Event.ID] {
				d.attempts[provider][rec.Event.ID] = rec.Attempts
			}
			d.depth[provider]++
		}
	}

	return d, nil
}

func (d *DLQ) regularPath(provider string) string {
	return filepath.Join(d.dir, provider+".jsonl")
}

func (d *DLQ) poisonPath(provider string) string {
	return filepath.Join(d.dir, provider+".poison.jsonl")
}

// Put durably records that event was dead-lettered for provider, with a
// human-readable reason. permanent should be true for failures unlikely
// to succeed on replay without intervention (the provider rejected it
// outright, or tallyd failed to even encode it) and false for failures
// that were purely about running out of retry time — the two get
// different poison thresholds.
func (d *DLQ) Put(provider string, event adapter.Event, reason string, permanent bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.attempts[provider] == nil {
		d.attempts[provider] = make(map[string]int)
	}
	d.attempts[provider][event.ID]++
	attempts := d.attempts[provider][event.ID]

	threshold := exhaustedPoisonThreshold
	if permanent {
		threshold = permanentPoisonThreshold
	}
	poisoned := attempts >= threshold

	rec := Record{
		Provider:  provider,
		Event:     event,
		Reason:    reason,
		Timestamp: time.Now().UTC(),
		Attempts:  attempts,
		Poisoned:  poisoned,
	}

	path := d.regularPath(provider)
	if poisoned {
		path = d.poisonPath(provider)
		// This event just crossed its poison threshold: purge any earlier
		// (non-poisoned) record(s) for the same ID from the regular file,
		// so the poison file becomes its sole current home instead of
		// leaving a stale, superseded entry behind for List() to return.
		purged, err := d.removeFromFile(d.regularPath(provider), map[string]bool{event.ID: true})
		if err != nil {
			return err
		}
		d.depth[provider] -= purged
	}
	if err := d.appendRecord(path, rec); err != nil {
		return err
	}

	d.depth[provider]++
	return nil
}

func (d *DLQ) appendRecord(path string, rec Record) error {
	f, ok := d.handles[path]
	if !ok {
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("dlq: open %s: %w", path, err)
		}
		d.handles[path] = f
	}

	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("dlq: marshal: %w", err)
	}
	line = append(line, '\n')
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("dlq: write %s: %w", path, err)
	}
	return f.Sync()
}

// readRecords is an internal helper: callers already hold d.mu (or, for
// the Open-time scan, haven't published d yet, so no lock is needed).
func (d *DLQ) readRecords(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("dlq: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var records []Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("dlq: decode %s: %w", path, err)
		}
		records = append(records, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("dlq: scan %s: %w", path, err)
	}
	return records, nil
}

// List returns provider's non-poisoned dead-lettered events — the
// default set of candidates for replay.
func (d *DLQ) List(provider string) ([]Record, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.readRecords(d.regularPath(provider))
}

// ListPoison returns provider's poisoned dead-lettered events — excluded
// from replay unless explicitly requested.
func (d *DLQ) ListPoison(provider string) ([]Record, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.readRecords(d.poisonPath(provider))
}

// Remove deletes the given event IDs for provider from both the regular
// and poison files (whichever actually contains each one), typically
// called after successfully replaying them. Rewrites the affected
// file(s) rather than truncating in place.
func (d *DLQ) Remove(provider string, eventIDs []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(eventIDs) == 0 {
		return nil
	}
	remove := make(map[string]bool, len(eventIDs))
	for _, id := range eventIDs {
		remove[id] = true
	}

	var removedTotal int
	for _, path := range []string{d.regularPath(provider), d.poisonPath(provider)} {
		removed, err := d.removeFromFile(path, remove)
		if err != nil {
			return err
		}
		removedTotal += removed
	}

	d.depth[provider] -= removedTotal
	if d.depth[provider] < 0 {
		d.depth[provider] = 0
	}
	return nil
}

func (d *DLQ) removeFromFile(path string, remove map[string]bool) (int, error) {
	records, err := d.readRecords(path)
	if err != nil {
		return 0, err
	}

	kept := make([]Record, 0, len(records))
	removedCount := 0
	for _, rec := range records {
		if remove[rec.Event.ID] {
			removedCount++
			continue
		}
		kept = append(kept, rec)
	}
	if removedCount == 0 {
		return 0, nil
	}

	// Close any open append handle before rewriting the file out from
	// under it.
	if f, ok := d.handles[path]; ok {
		_ = f.Close()
		delete(d.handles, path)
	}

	tmpPath := path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, fmt.Errorf("dlq: create %s: %w", tmpPath, err)
	}
	for _, rec := range kept {
		line, err := json.Marshal(rec)
		if err != nil {
			_ = tmp.Close()
			return 0, fmt.Errorf("dlq: marshal: %w", err)
		}
		line = append(line, '\n')
		if _, err := tmp.Write(line); err != nil {
			_ = tmp.Close()
			return 0, fmt.Errorf("dlq: write %s: %w", tmpPath, err)
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("dlq: sync %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("dlq: close %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return 0, fmt.Errorf("dlq: rename %s -> %s: %w", tmpPath, path, err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, fmt.Errorf("dlq: reopen %s: %w", path, err)
	}
	d.handles[path] = f

	return removedCount, nil
}

// Depth returns the current (not cumulative) number of events
// dead-lettered for provider, across both the regular and poison files.
// Feeds the dlq_depth{provider} metric.
func (d *DLQ) Depth(provider string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.depth[provider]
}

// Close closes every open file handle.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	var firstErr error
	for _, f := range d.handles {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
