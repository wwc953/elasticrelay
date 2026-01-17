package transform

import (
	"context"
	"io"
	"log"

	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
)

// Server implements the TransformService gRPC server.
type Server struct {
	pb.UnimplementedTransformServiceServer

	// engine is the transformation engine
	engine *Engine

	// configs holds the transform configurations
	configs []*TransformConfig
}

// ServerOption is a function that configures the Server.
type ServerOption func(*Server) error

// WithConfigs sets the transform configurations.
func WithConfigs(configs []*TransformConfig) ServerOption {
	return func(s *Server) error {
		s.configs = configs
		return nil
	}
}

// WithGlobalSettings sets the global transform settings.
func WithGlobalSettings(settings *GlobalTransformSettings) ServerOption {
	return func(s *Server) error {
		if s.engine != nil {
			s.engine.globalSettings = settings
		}
		return nil
	}
}

// NewServer creates a new Transform server with the given options.
func NewServer(opts ...ServerOption) (*Server, error) {
	s := &Server{
		configs: []*TransformConfig{},
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	// Create engine
	engine, err := NewEngine(s.configs, nil)
	if err != nil {
		return nil, err
	}
	s.engine = engine

	if s.engine.IsPassThrough() {
		log.Println("Transform Server created (pass-through mode - no rules configured)")
	} else {
		log.Printf("Transform Server created with %d transformation rules", len(s.configs))
	}

	return s, nil
}

// ApplyRules applies transformation rules to a stream of change events.
// This is the main gRPC streaming method that receives events from the orchestrator,
// transforms them, and sends them back.
func (s *Server) ApplyRules(stream pb.TransformService_ApplyRulesServer) error {
	ctx := stream.Context()
	log.Println("Transform: ApplyRules stream opened")

	// Collect all events first (as per the original design)
	var receivedEvents []*pb.ChangeEvent
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			// Client finished sending
			break
		}
		if err != nil {
			log.Printf("Transform: Error receiving from stream: %v", err)
			return err
		}
		receivedEvents = append(receivedEvents, event)
	}

	// Process and send events
	processedCount := 0
	filteredCount := 0

	for _, event := range receivedEvents {
		// Transform the event
		transformedEvent, shouldInclude, err := s.engine.Transform(ctx, event)
		if err != nil {
			log.Printf("Transform: Error transforming event PK=%s: %v", event.PrimaryKey, err)
			// For errors, we can either skip or return the original event
			// Here we skip the failed event and continue
			continue
		}

		// Skip filtered events
		if !shouldInclude {
			filteredCount++
			continue
		}

		// Send transformed event
		if err := stream.Send(transformedEvent); err != nil {
			log.Printf("Transform: Error sending to stream: %v", err)
			return err
		}
		processedCount++
	}

	log.Printf("Transform: ApplyRules stream closed. Processed: %d, Filtered: %d, Total: %d",
		processedCount, filteredCount, len(receivedEvents))
	return nil
}

// ValidateRules validates a set of transformation rules.
func (s *Server) ValidateRules(ctx context.Context, req *pb.ValidateRulesRequest) (*pb.ValidateRulesResponse, error) {
	// For now, just return valid
	// TODO: Implement actual validation logic
	return &pb.ValidateRulesResponse{
		Valid: true,
	}, nil
}

// LoadConfigs reloads the transformation configurations.
func (s *Server) LoadConfigs(configs []*TransformConfig) error {
	s.configs = configs
	return s.engine.LoadConfig(configs)
}

// GetStats returns the current engine statistics.
func (s *Server) GetStats() EngineStats {
	return s.engine.GetStats()
}

// TransformSingle transforms a single event (utility method for testing).
func (s *Server) TransformSingle(ctx context.Context, event *pb.ChangeEvent) (*pb.ChangeEvent, bool, error) {
	return s.engine.Transform(ctx, event)
}

// IsPassThrough returns true if the server is in pass-through mode.
func (s *Server) IsPassThrough() bool {
	return s.engine.IsPassThrough()
}
