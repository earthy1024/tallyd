// Package dlqshow serves an admin endpoint for inspecting dead-lettered
// events without replaying them.
package dlqshow

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tallyd/tallyd/internal/dlq"
)

// Store is the subset of *dlq.DLQ this handler needs.
type Store interface {
	List(provider string) ([]dlq.Record, error)
	ListPoison(provider string) ([]dlq.Record, error)
}

// Handler serves GET /v1/dlq?provider=X[&include_poison=true].
type Handler struct {
	DLQ Store

	// KnownProviders gates which provider names are accepted, same
	// rationale as dlqreplay.Handler.KnownProviders: a misspelled name
	// should be a clear 400, not a silent empty result.
	KnownProviders map[string]bool
}

// Result is the JSON response body.
type Result struct {
	Provider string       `json:"provider"`
	Records  []dlq.Record `json:"records"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	provider := r.URL.Query().Get("provider")
	if provider == "" {
		http.Error(w, "provider query parameter is required", http.StatusBadRequest)
		return
	}
	if !h.KnownProviders[provider] {
		http.Error(w, fmt.Sprintf("unknown provider %q", provider), http.StatusBadRequest)
		return
	}

	records, err := h.DLQ.List(provider)
	if err != nil {
		http.Error(w, fmt.Sprintf("list dlq: %v", err), http.StatusInternalServerError)
		return
	}

	if r.URL.Query().Get("include_poison") == "true" {
		poisoned, err := h.DLQ.ListPoison(provider)
		if err != nil {
			http.Error(w, fmt.Sprintf("list poisoned dlq: %v", err), http.StatusInternalServerError)
			return
		}
		records = append(records, poisoned...)
	}

	if records == nil {
		records = []dlq.Record{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Result{Provider: provider, Records: records})
}
