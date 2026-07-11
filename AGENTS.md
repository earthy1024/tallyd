# AGENTS.md

Guidance for AI coding agents working in this repository.

## Commands

```sh
go build ./...
go test ./... -race
go vet ./...
golangci-lint run ./...
```

Run a single test:

```sh
go test ./internal/wal/... -run TestCrashRecovery -v
```

Run the daemon locally:

```sh
go build -o tallyd ./cmd/tallyd
cp config.example.yaml config.yaml   # edit buffer.dir etc.
./tallyd -config config.yaml
```

Commits require DCO sign-off (`git commit -s`) and Conventional Commits
subject lines (`feat:`, `fix:`, `chore:`, `test:`, `docs:`, `refactor:`) —
see CONTRIBUTING.md. CI enforces both on PRs.

## Architecture

tallyd is a durability-first pipeline: `receiver -> WAL -> dispatcher ->
batcher -> adapter`. The full wiring lives in `internal/pipeline.Build`;
`cmd/tallyd/main.go` is a thin flag/signal wrapper around it.

**The core invariant**: `internal/wal.WAL.Append` only returns after its
record is fsync'd to disk. The HTTP receiver (`internal/receiver`) acks
the caller with 2xx *only* after every event in the request durably
appends — never before. This is what makes it safe to treat a 2xx as "this
event will survive a crash." `internal/wal/wal_test.go`'s
`TestCrashRecovery` proves this by SIGKILLing a real subprocess mid-run
and asserting replay recovers everything that was ever acked.

**Dual delivery paths, same durability boundary**: once an event is
durable, `pipeline.walDispatchSink.Append` does two things — it's the
`receiver.Sink` the HTTP layer talks to, and after a successful
`wal.Append` it also hands the event to the `dispatcher` for live
delivery. On restart, `dispatcher.ReplayPending(wal.Pending())` re-enqueues
anything left unresolved from a prior crash before the receiver accepts
new traffic. So live delivery and crash-recovery delivery are two
different code paths converging on the same `dispatcher.Dispatch` call.

**Per-provider ack state, not per-event**: each WAL entry tracks which
providers have acked/dead-lettered it independently (`wal.Entry.Pending`).
An entry is only garbage-collected once *every* target provider has
resolved it — this is what makes dual-write (sending the same event to
Orb and Metronome, say) correct instead of racy.

**One queue+batch+retry engine per provider**: `internal/dispatcher` just
fans an event out to the named provider `Enqueuer`s (normally
`*batcher.Batcher`); it does no batching itself. Each `Batcher` owns its
own flush-on-size-or-linger loop, exponential backoff+jitter retry (capped
below the provider's dedup window via `RetryPolicy.MaxElapsed` — retrying
past that window risks double-counting, so exhausted retries dead-letter
instead of looping forever), and DLQ handoff. A slow/down provider only
backs up its own queue, never a healthy one's.

**Structural typing keeps packages decoupled**: `receiver`, `batcher`, and
`dispatcher` never import `internal/wal`, `internal/dlq`, or
`internal/metrics` directly (dispatcher imports `wal` only for the
`wal.Entry` data type, not behavior). Instead each package declares small
local interfaces it needs (`receiver.Sink`, `receiver.MetricsRecorder`,
`batcher.Acker`, `batcher.DeadLetterSink`, `batcher.MetricsRecorder`,
`dispatcher.Enqueuer`), and the concrete types (`*wal.WAL`, `*dlq.DLQ`,
`*metrics.Metrics`, `*batcher.Batcher`) satisfy them structurally. When
adding a new producer/consumer relationship between packages, prefer
adding a narrow interface at the consumer over introducing a new import.
Metrics fields are always optional (nil-checked before use) so tests don't
need to wire a `*metrics.Metrics` just to exercise business logic.

**Two adapters exist so far**: `adapter.Adapter` is the vendor seam
(`Encode`/`Send`/`Classify`/`MaxBatchSize`). `adapter/stdout` prints
batches instead of calling a real billing API, which is enough to
exercise the whole pipeline without vendor credentials. `adapter/metronome`
calls Metronome's real ingest API (`POST /v1/ingest`, Bearer token, JSON
array of `transaction_id`/`customer_id`/`event_type`/`timestamp`/
`properties`) — batch size is hard-capped at 100 regardless of config
(`MaxBatchSize` constant) since that's Metronome's documented limit, not a
tunable, and every property value gets stringified before sending
(`stringifyProperties`) since Metronome requires that even for numbers and
booleans. Its `Send` treats a 2xx as the whole batch accepted (Metronome's
docs don't specify a per-event result body) and its `Classify` maps
429/5xx/network errors to `Retry` and other 4xx to `DeadLetter`. Orb is
the next adapter to add. `internal/pipeline.buildAdapter` is the factory
switch — adding Orb means adding a case there plus a new `adapter/<name>`
package, not touching the pipeline wiring itself.
`internal/pipeline.ProviderConfig`'s `Endpoint`/`TokenEnv` fields (the
latter resolved via `os.Getenv` in `buildAdapter`, erroring if unset) are
what make this config-driven without code changes per provider.

**Config**: `internal/pipeline/config.go` defines a YAML schema with a
custom `Duration` type (`UnmarshalYAML` via `time.ParseDuration`, since
`encoding/yaml` doesn't parse `"10s"`-style strings for the stdlib type
out of the box). `Config.applyDefaults()` fills in unset fields
per-provider; `cmd/tallyd/main.go` calls `pipeline.LoadConfig` if `-config`
is given, otherwise builds a zero-value `Config` and lets `pipeline.Build`
apply defaults.

**Testing patterns worth reusing**: fake `Acker`/`DeadLetterSink`/`Adapter`
implementations in `internal/batcher/batcher_test.go` and
`internal/dispatcher/dispatcher_test.go` (structural typing makes these
trivial); `internal/pipeline/pipeline_test.go` captures real `os.Stdout`
via `os.Pipe` to assert on what the stdout adapter printed; retry/linger
tests use a `waitFor(t, timeout, cond)` polling helper rather than fixed
sleeps, since the batcher's flush loop runs on its own goroutine.
