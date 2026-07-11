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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Commits require DCO sign-off
(`git commit -s`).

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
