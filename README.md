# tallyd

Durable, vendor-agnostic daemon for forwarding usage-based billing events to any provider.

`tallyd` buffers local usage events durably (fsync'd write-ahead log)
before forwarding them to billing providers, so an accepted event is
never lost even if the process crashes mid-flight. Same pattern as an
OpenTelemetry Collector or Fluent Bit, specialized for billing ingestion.

## Status

Early. The receiver, WAL, dispatcher, batcher, retry/DLQ, and metrics
pipeline work end-to-end today. Two adapters exist: `stdout` (prints
batches instead of calling a real billing API, enough to exercise the
whole pipeline without vendor credentials) and `metronome` (calls
Metronome's real ingest API). Orb is the next unit of work.

## Quickstart

```sh
go build -o tallyd ./cmd/tallyd
cp config.example.yaml config.yaml   # adjust buffer.dir etc.
./tallyd -config config.yaml
```

```sh
curl -X POST http://127.0.0.1:8999/v1/events \
  -H "Content-Type: application/json" \
  -d '{"id":"evt-1","customer_id":"cust_1","event_name":"api_call","timestamp":"2026-07-11T12:00:00Z"}'
```

Metrics are served at `/metrics`.

### gRPC

Events can also be emitted over gRPC instead of HTTP — same validation,
routing, and durability guarantees either way (both transports call the
same core `receiver.Ingest`). Disabled by default; enable it via
`listen.grpc` in config or `-grpc-listen host:port`. Schema:
[`proto/tallyd/v1/events.proto`](proto/tallyd/v1/events.proto).

```sh
./tallyd -config config.yaml -grpc-listen 127.0.0.1:9000
```

```sh
grpcurl -plaintext -proto proto/tallyd/v1/events.proto \
  -d '{"events":[{"id":"evt-1","customer_id":"cust_1","event_name":"api_call","timestamp":"2026-07-11T12:00:00Z"}]}' \
  127.0.0.1:9000 tallyd.v1.Events/Ingest
```

## Shutdown

`tallyd` catches `SIGINT`/`SIGTERM` and shuts down gracefully: stop
accepting new HTTP/gRPC requests, flush every provider's queued events
(this happens immediately — it does **not** wait for the configured
`linger` window), then close the DLQ and WAL. `SIGKILL` cannot be
handled by any process on any OS (it's the kernel's unconditional,
uncatchable kill signal) — there's no code that could change that.

The one thing worth tuning for production: **the final flush has to
actually finish before your orchestrator's SIGKILL fallback fires.**
Docker's default `docker stop` grace period is 10s; Kubernetes'
`terminationGracePeriodSeconds` defaults to 30s. If a provider is slow
or unreachable at the moment of shutdown, that flush attempt can take
up to its own internal timeout, and if the grace period is shorter, the
process gets force-killed mid-flush. Verified this against a real
container:

- Healthy provider: `docker stop` completed in ~0.16s.
- Hung provider: `docker stop` used the full default 10s grace period,
  then `SIGKILL`'d the process (exit 137) before the flush completed.

Either way, **no data is lost** — every event was durably written to the
WAL before ever being dispatched, so even a mid-flush `SIGKILL` just
means that delivery attempt is abandoned; the event stays in the WAL
and gets redelivered on the next start (`dispatcher.ReplayPending`),
confirmed by restarting the killed container above and seeing
`wal_unacked_entries` come back with the event still pending. The
practical impact of a too-short grace period is delayed delivery, not
lost events — but for prompt delivery under provider slowness, set
`docker stop --time` (or Compose's `stop_grace_period`, or
`terminationGracePeriodSeconds` in Kubernetes) comfortably above your
adapters' worst-case send timeout, and remember the WAL directory must
be on a volume that survives the restart for replay to have anything
to recover.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Commits require DCO sign-off
(`git commit -s`).

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
