// Package status serves a human/script-friendly JSON snapshot of the
// running daemon's health signals — an alternative to scraping /metrics'
// Prometheus text format when all you want is "is anything backed up".
package status

import (
	"encoding/json"
	"net/http"

	"github.com/tallyd/tallyd/internal/dlq"
)

// WAL is the subset of *wal.WAL this handler needs.
type WAL interface {
	UnackedCount() int
}

// DLQStore is the subset of *dlq.DLQ this handler needs.
type DLQStore interface {
	List(provider string) ([]dlq.Record, error)
	ListPoison(provider string) ([]dlq.Record, error)
}

// Handler serves GET /v1/status.
type Handler struct {
	WAL       WAL
	DLQ       DLQStore
	Providers []string
}

// ProviderDLQ is the per-provider breakdown in Result.DLQ.
type ProviderDLQ struct {
	Depth       int `json:"depth"`
	PoisonDepth int `json:"poison_depth"`
}

// Result is the JSON response body.
type Result struct {
	WALUnackedEntries int                    `json:"wal_unacked_entries"`
	DLQ               map[string]ProviderDLQ `json:"dlq"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	result := Result{
		WALUnackedEntries: h.WAL.UnackedCount(),
		DLQ:               make(map[string]ProviderDLQ, len(h.Providers)),
	}
	for _, provider := range h.Providers {
		records, err := h.DLQ.List(provider)
		if err != nil {
			http.Error(w, "list dlq: "+err.Error(), http.StatusInternalServerError)
			return
		}
		poisoned, err := h.DLQ.ListPoison(provider)
		if err != nil {
			http.Error(w, "list poisoned dlq: "+err.Error(), http.StatusInternalServerError)
			return
		}
		result.DLQ[provider] = ProviderDLQ{Depth: len(records), PoisonDepth: len(poisoned)}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
