// Package grpcserver implements tallyd's gRPC ingress: the Events service
// defined in proto/tallyd/v1/events.proto. It's a thin transport shim —
// it converts between the generated wire types and adapter.Event, then
// delegates to the same Ingester (normally *receiver.Receiver) the HTTP
// transport uses, so validation, routing, and durability behave
// identically no matter which transport an event arrived through.
package grpcserver

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/earthy1024/tallyd/adapter"
	"github.com/earthy1024/tallyd/internal/grpcapi"
	"github.com/earthy1024/tallyd/internal/receiver"
)

// Ingester is the subset of *receiver.Receiver this server depends on.
type Ingester interface {
	Ingest(events []adapter.Event) error
}

// Server implements grpcapi.EventsServer.
type Server struct {
	grpcapi.UnimplementedEventsServer
	Receiver Ingester
}

// New returns a Server backed by the given Ingester.
func New(r Ingester) *Server {
	return &Server{Receiver: r}
}

func (s *Server) Ingest(_ context.Context, req *grpcapi.IngestRequest) (*grpcapi.IngestResponse, error) {
	events := toEvents(req.GetEvents())

	if err := s.Receiver.Ingest(events); err != nil {
		switch err.(type) {
		case *receiver.ValidationError:
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case *receiver.UnavailableError:
			return nil, status.Error(codes.Unavailable, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &grpcapi.IngestResponse{}, nil
}

func toEvents(wire []*grpcapi.Event) []adapter.Event {
	events := make([]adapter.Event, len(wire))
	for i, we := range wire {
		var props map[string]any
		if we.GetProperties() != nil {
			props = we.GetProperties().AsMap()
		}

		// A nil Timestamp must convert to Go's zero time.Time, not
		// (*timestamppb.Timestamp)(nil).AsTime()'s Unix epoch — otherwise
		// a client that omits the field would silently pass validate()'s
		// IsZero() check instead of being rejected as "timestamp is
		// required", unlike the HTTP transport where a missing JSON field
		// does decode to the real zero value.
		var ts time.Time
		if pts := we.GetTimestamp(); pts != nil {
			ts = pts.AsTime()
		}

		events[i] = adapter.Event{
			ID:         we.GetId(),
			CustomerID: we.GetCustomerId(),
			EventName:  we.GetEventName(),
			Timestamp:  ts,
			Properties: props,
			Route:      we.GetRoute(),
		}
	}
	return events
}
