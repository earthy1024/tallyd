package grpcserver_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tallyd/tallyd/adapter"
	"github.com/tallyd/tallyd/internal/grpcapi"
	"github.com/tallyd/tallyd/internal/grpcserver"
	"github.com/tallyd/tallyd/internal/receiver"
)

type fakeIngester struct {
	gotEvents []adapter.Event
	err       error
}

func (f *fakeIngester) Ingest(events []adapter.Event) error {
	f.gotEvents = events
	return f.err
}

func TestIngestConvertsFieldsAndDelegates(t *testing.T) {
	fi := &fakeIngester{}
	srv := grpcserver.New(fi)

	props, err := structpb.NewStruct(map[string]any{"endpoint": "/charge", "compute_ms": 42.0})
	if err != nil {
		t.Fatalf("build struct: %v", err)
	}

	ts := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	req := &grpcapi.IngestRequest{
		Events: []*grpcapi.Event{
			{
				Id:         "evt-1",
				CustomerId: "cust_1",
				EventName:  "api_call",
				Timestamp:  timestamppb.New(ts),
				Properties: props,
				Route:      []string{"orb"},
			},
		},
	}

	if _, err := srv.Ingest(context.Background(), req); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if len(fi.gotEvents) != 1 {
		t.Fatalf("got %d events, want 1", len(fi.gotEvents))
	}
	got := fi.gotEvents[0]
	if got.ID != "evt-1" || got.CustomerID != "cust_1" || got.EventName != "api_call" {
		t.Errorf("mismatched core fields: %+v", got)
	}
	if !got.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", got.Timestamp, ts)
	}
	if got.Properties["endpoint"] != "/charge" {
		t.Errorf("Properties[endpoint] = %v, want /charge", got.Properties["endpoint"])
	}
	if len(got.Route) != 1 || got.Route[0] != "orb" {
		t.Errorf("Route = %v, want [orb]", got.Route)
	}
}

func TestMissingTimestampConvertsToZeroNotEpoch(t *testing.T) {
	fi := &fakeIngester{}
	srv := grpcserver.New(fi)

	req := &grpcapi.IngestRequest{
		Events: []*grpcapi.Event{
			{Id: "evt-1", CustomerId: "cust_1", EventName: "api_call"}, // no Timestamp set
		},
	}

	_, _ = srv.Ingest(context.Background(), req)

	if len(fi.gotEvents) != 1 {
		t.Fatalf("got %d events, want 1", len(fi.gotEvents))
	}
	if !fi.gotEvents[0].Timestamp.IsZero() {
		t.Errorf("Timestamp = %v, want zero value (not Unix epoch)", fi.gotEvents[0].Timestamp)
	}
}

func TestValidationErrorMapsToInvalidArgument(t *testing.T) {
	fi := &fakeIngester{err: &receiver.ValidationError{}}
	srv := grpcserver.New(fi)

	_, err := srv.Ingest(context.Background(), &grpcapi.IngestRequest{})

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestUnavailableErrorMapsToUnavailable(t *testing.T) {
	fi := &fakeIngester{err: &receiver.UnavailableError{}}
	srv := grpcserver.New(fi)

	_, err := srv.Ingest(context.Background(), &grpcapi.IngestRequest{})

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %v", err)
	}
	if st.Code() != codes.Unavailable {
		t.Errorf("code = %v, want Unavailable", st.Code())
	}
}
