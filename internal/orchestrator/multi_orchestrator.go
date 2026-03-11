package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
	"github.com/yogoosoft/elasticrelay/internal/config"
	"github.com/yogoosoft/elasticrelay/internal/connectors"
	"github.com/yogoosoft/elasticrelay/internal/connectors/mongodb"
	"github.com/yogoosoft/elasticrelay/internal/connectors/mysql"
	"github.com/yogoosoft/elasticrelay/internal/connectors/postgresql"
	"github.com/yogoosoft/elasticrelay/internal/dlq"
	"github.com/yogoosoft/elasticrelay/internal/parallel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Constants for batch processing and checkpointing
const (
	batchSize      = 100
	commitInterval = 5 * time.Second
)

// MultiOrchestrator supports multiple data sources and concurrent CDC processing
type MultiOrchestrator struct {
	pb.UnimplementedOrchestratorServiceServer

	connectorManager *connectors.ConnectorManager
	jobs             map[string]*MultiJob
	jobsMux          sync.RWMutex
	multiConfig      *config.MultiConfig
	grpcAddress      string // Address of the gRPC server where all services are running

	// Service clients (shared across jobs)
	sinkClients     map[string]pb.SinkServiceClient
	transformClient pb.TransformServiceClient

	// Dead Letter Queue management
	dlqManager *dlq.DLQManager
}

// MultiJob represents a synchronization job in multi-source environment
type MultiJob struct {
	ID          string
	Name        string
	SourceID    string
	SinkID      string
	Enabled     bool
	Description string

	ctx    context.Context
	cancel context.CancelFunc

	// Job-specific clients
	connectorInstance *connectors.ConnectorInstance
	sinkClient        pb.SinkServiceClient
	transformClient   pb.TransformServiceClient

	// Batch processing
	batch      []*pb.ChangeEvent
	batchMutex sync.Mutex
	lastCp     *pb.Checkpoint
	cpMutex    sync.RWMutex

	// Parallel snapshot processing
	parallelManager *parallel.ParallelSnapshotManager
	useParallel     bool

	// Job configuration access
	jobOptions map[string]interface{}
	sinkConfig map[string]interface{} // Sink configuration for index naming, etc.

	// Dead Letter Queue management (shared from orchestrator)
	dlqManager *dlq.DLQManager
}

// NewMultiOrchestrator creates a new multi-source orchestrator
func NewMultiOrchestrator(grpcAddress string) (*MultiOrchestrator, error) {
	mo := &MultiOrchestrator{
		connectorManager: connectors.NewConnectorManager(),
		jobs:             make(map[string]*MultiJob),
		sinkClients:      make(map[string]pb.SinkServiceClient),
		grpcAddress:      grpcAddress,
	}

	// Initialize DLQ manager with default config
	dlqConfig := &dlq.DLQConfig{
		StoragePath: "./dlq",
		MaxRetries:  3,
		RetryDelay:  30 * time.Second,
		Enabled:     true,
	}

	dlqManager, err := dlq.NewDLQManager(dlqConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DLQ manager: %w", err)
	}
	mo.dlqManager = dlqManager

	// Initialize shared transform client
	if err := mo.initializeTransformClient(); err != nil {
		return nil, fmt.Errorf("failed to initialize transform client: %w", err)
	}

	// Start DLQ retry processor if enabled
	if mo.dlqManager != nil {
		go mo.dlqManager.ProcessRetries(context.Background(), mo.retryDLQItem)

		// Start DLQ statistics logger (every hour)
		go mo.logDLQStats()

		// Start DLQ cleanup task (every 24 hours, keep 7 days)
		go mo.cleanupDLQ()

		// Start orphaned DLQ cleanup (every hour)
		go mo.cleanupOrphanedDLQItems()
	}

	return mo, nil
}

// updateDLQConfiguration updates the DLQ manager configuration from the loaded config
func (mo *MultiOrchestrator) updateDLQConfiguration(dlqConfig *config.DLQConfig) error {
	log.Printf("MultiOrchestrator: DLQ config received: %+v", dlqConfig)
	if dlqConfig == nil {
		log.Printf("MultiOrchestrator: No DLQ configuration found, using defaults")
		return nil
	}

	// Parse retry delay
	retryDelay := 30 * time.Second // default
	if dlqConfig.RetryDelay != "" {
		parsed, err := time.ParseDuration(dlqConfig.RetryDelay)
		if err != nil {
			log.Printf("MultiOrchestrator: Invalid DLQ retry_delay '%s', using default 30s: %v", dlqConfig.RetryDelay, err)
		} else {
			retryDelay = parsed
		}
	}

	// Create new DLQ manager with updated configuration
	newDLQConfig := &dlq.DLQConfig{
		StoragePath: dlqConfig.StoragePath,
		MaxRetries:  dlqConfig.MaxRetries,
		RetryDelay:  retryDelay,
		Enabled:     dlqConfig.Enabled,
	}

	// Apply defaults if values are zero
	if newDLQConfig.StoragePath == "" {
		newDLQConfig.StoragePath = "./dlq"
	}
	if newDLQConfig.MaxRetries == 0 {
		newDLQConfig.MaxRetries = 3
	}

	newDLQManager, err := dlq.NewDLQManager(newDLQConfig)
	if err != nil {
		return fmt.Errorf("failed to create new DLQ manager: %w", err)
	}

	// Replace the old DLQ manager
	mo.dlqManager = newDLQManager

	// Update all existing jobs with the new DLQ manager
	mo.jobsMux.Lock()
	for _, job := range mo.jobs {
		job.dlqManager = newDLQManager
	}
	mo.jobsMux.Unlock()

	// Start DLQ retry processor if enabled
	if mo.dlqManager != nil {
		go mo.dlqManager.ProcessRetries(context.Background(), mo.retryDLQItem)

		// Start DLQ statistics logger (every hour)
		go mo.logDLQStats()

		// Start DLQ cleanup task (every 24 hours, keep 7 days)
		go mo.cleanupDLQ()

		// Start orphaned DLQ cleanup (every hour)
		go mo.cleanupOrphanedDLQItems()
	}

	log.Printf("MultiOrchestrator: DLQ configuration updated - enabled: %t, storage: %s, max_retries: %d, retry_delay: %v",
		newDLQConfig.Enabled, newDLQConfig.StoragePath, newDLQConfig.MaxRetries, newDLQConfig.RetryDelay)

	return nil
}

// LoadConfiguration loads multi-source configuration
func (mo *MultiOrchestrator) LoadConfiguration(multiConfig *config.MultiConfig) error {
	mo.multiConfig = multiConfig

	log.Printf("MultiOrchestrator: Global config loaded: %+v", multiConfig.Global)
	// Update DLQ configuration from loaded config
	if err := mo.updateDLQConfiguration(multiConfig.Global.DLQConfig); err != nil {
		return fmt.Errorf("failed to update DLQ configuration: %w", err)
	}

	// Load data sources
	if err := mo.connectorManager.LoadFromMultiConfig(multiConfig); err != nil {
		return fmt.Errorf("failed to load data sources: %w", err)
	}

	// Initialize sink clients
	if err := mo.initializeSinkClients(multiConfig.Sinks); err != nil {
		return fmt.Errorf("failed to initialize sink clients: %w", err)
	}

	log.Printf("MultiOrchestrator: Configuration loaded - %d data sources, %d sinks, %d jobs",
		len(multiConfig.DataSources), len(multiConfig.Sinks), len(multiConfig.Jobs))

	return nil
}

// initializeSinkClients creates gRPC clients for all sink configurations
func (mo *MultiOrchestrator) initializeSinkClients(sinks []config.SinkConfig) error {
	for _, sink := range sinks {
		// For now, assume all sinks are on the same gRPC server
		// In production, you might have different endpoints per sink
		conn, err := grpc.Dial(mo.grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to sink %s: %w", sink.ID, err)
		}

		mo.sinkClients[sink.ID] = pb.NewSinkServiceClient(conn)
		log.Printf("MultiOrchestrator: Connected to sink '%s' (%s)", sink.ID, sink.Type)
	}

	return nil
}

// initializeTransformClient creates shared transform client
func (mo *MultiOrchestrator) initializeTransformClient() error {
	conn, err := grpc.Dial(mo.grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to transform service: %w", err)
	}

	mo.transformClient = pb.NewTransformServiceClient(conn)
	log.Printf("MultiOrchestrator: Connected to transform service")

	return nil
}

// CreateJob creates a new synchronization job from job configuration
func (mo *MultiOrchestrator) CreateJob(ctx context.Context, req *pb.CreateJobRequest) (*pb.Job, error) {
	// Parse job configuration
	var jobConfig config.SyncJobConfig
	if err := json.Unmarshal([]byte(req.Config), &jobConfig); err != nil {
		return nil, fmt.Errorf("failed to parse job config: %w", err)
	}

	jobID := jobConfig.ID
	if jobID == "" {
		jobID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}

	mo.jobsMux.Lock()
	defer mo.jobsMux.Unlock()

	if _, exists := mo.jobs[jobID]; exists {
		return nil, fmt.Errorf("job with ID '%s' already exists", jobID)
	}

	// Validate source and sink exist
	connectorInstance, err := mo.connectorManager.GetConnector(jobConfig.SourceID)
	if err != nil {
		return nil, fmt.Errorf("source not found: %w", err)
	}

	sinkClient, exists := mo.sinkClients[jobConfig.SinkID]
	if !exists {
		return nil, fmt.Errorf("sink '%s' not found", jobConfig.SinkID)
	}

	// Get sink configuration from multiConfig
	var sinkConfig map[string]interface{}
	if mo.multiConfig != nil {
		for _, sink := range mo.multiConfig.Sinks {
			if sink.ID == jobConfig.SinkID {
				sinkConfig = sink.Options
				break
			}
		}
	}

	jobCtx, cancel := context.WithCancel(context.Background())

	// Check if parallel processing is enabled in job options
	useParallel := true // Default to parallel processing
	if options, ok := jobConfig.Options["use_parallel"].(bool); ok {
		useParallel = options
	}

	job := &MultiJob{
		ID:                jobID,
		Name:              jobConfig.Name,
		SourceID:          jobConfig.SourceID,
		SinkID:            jobConfig.SinkID,
		Enabled:           jobConfig.Enabled,
		Description:       jobConfig.Description,
		ctx:               jobCtx,
		cancel:            cancel,
		connectorInstance: connectorInstance,
		sinkClient:        sinkClient,
		transformClient:   mo.transformClient,
		useParallel:       useParallel,
		jobOptions:        jobConfig.Options, // Copy job options for configuration access
		sinkConfig:        sinkConfig,        // Copy sink options for index naming, etc.
		dlqManager:        mo.dlqManager,     // Share DLQ manager from orchestrator
	}

	// Initialize parallel snapshot manager if enabled
	if useParallel {
		if err := job.initializeParallelManager(); err != nil {
			log.Printf("MultiOrchestrator: Failed to initialize parallel manager for job %s: %v", jobID, err)
			// Fall back to serial processing
			job.useParallel = false
		}
	}

	mo.jobs[jobID] = job

	// Register job with connector manager
	mo.connectorManager.AddJobToDataSource(jobConfig.SourceID, jobID)

	// Start job if enabled
	if job.Enabled {
		go job.run()
		log.Printf("MultiOrchestrator: Job '%s' started (%s -> %s)",
			jobID, jobConfig.SourceID, jobConfig.SinkID)
	} else {
		log.Printf("MultiOrchestrator: Job '%s' created but disabled", jobID)
	}

	return &pb.Job{
		Id:     jobID,
		Name:   jobConfig.Name,
		Status: getJobStatus(job),
		Config: req.Config,
	}, nil
}

// CreateJobsFromConfig creates all jobs from multi-config
func (mo *MultiOrchestrator) CreateJobsFromConfig() error {
	if mo.multiConfig == nil {
		return fmt.Errorf("no configuration loaded")
	}

	for _, jobConfig := range mo.multiConfig.Jobs {
		configJSON, err := json.Marshal(jobConfig)
		if err != nil {
			log.Printf("MultiOrchestrator: Failed to marshal job config %s: %v", jobConfig.ID, err)
			continue
		}

		req := &pb.CreateJobRequest{
			Name:   jobConfig.Name,
			Config: string(configJSON),
		}

		_, err = mo.CreateJob(context.Background(), req)
		if err != nil {
			log.Printf("MultiOrchestrator: Failed to create job %s: %v", jobConfig.ID, err)
			continue
		}
	}

	log.Printf("MultiOrchestrator: Created %d jobs from configuration", len(mo.multiConfig.Jobs))
	return nil
}

// run executes the main job loop with proper initial sync -> CDC flow
func (j *MultiJob) run() {
	log.Printf("MultiJob '%s': Starting synchronization from %s to %s", j.ID, j.SourceID, j.SinkID)
	defer log.Printf("MultiJob '%s': Stopped", j.ID)

	// Start batch flusher
	go j.batchFlusher()

	// Check if initial sync is enabled and needed
	if j.needsInitialSync() {
		log.Printf("MultiJob '%s': Starting initial snapshot sync", j.ID)
		if err := j.performInitialSync(); err != nil {
			log.Printf("MultiJob '%s': Initial sync failed: %v", j.ID, err)
			return
		}
		log.Printf("MultiJob '%s': Initial sync completed successfully", j.ID)
	}

	// Start CDC stream for incremental changes
	log.Printf("MultiJob '%s': Starting CDC for incremental changes", j.ID)
	j.startCDC()
}

// startCDC starts the CDC stream for this specific job
func (j *MultiJob) startCDC() {
	log.Printf("MultiJob '%s': Starting CDC stream for data source '%s'", j.ID, j.SourceID)

	// Create a custom stream handler for this job
	stream := &jobCDCStream{
		job: j,
	}

	var startCheckpoint *pb.Checkpoint
	if strings.EqualFold(j.connectorInstance.Type, "postgresql") {
		j.cpMutex.RLock()
		startCheckpoint = j.lastCp
		j.cpMutex.RUnlock()
		if startCheckpoint != nil && startCheckpoint.PostgresLsn != "" {
			log.Printf("MultiJob '%s': Reusing PostgreSQL checkpoint for CDC start: %s", j.ID, startCheckpoint.PostgresLsn)
		} else {
			startCheckpoint = nil
		}
	}

	// Handle different connector types
	switch connector := j.connectorInstance.Connector.(type) {
	case *mysql.Connector:
		log.Printf("MultiJob '%s': Starting MySQL CDC stream", j.ID)
		err := connector.Start(stream, nil)
		if err != nil {
			log.Printf("MultiJob '%s': MySQL CDC error: %v", j.ID, err)
		}
	case *postgresql.Connector:
		log.Printf("MultiJob '%s': Starting PostgreSQL CDC stream", j.ID)
		err := connector.Start(stream, startCheckpoint)
		if err != nil {
			log.Printf("MultiJob '%s': PostgreSQL CDC error: %v", j.ID, err)
		}
	case *mongodb.Connector:
		log.Printf("MultiJob '%s': Starting MongoDB CDC stream (Change Streams)", j.ID)
		err := connector.Start(stream, nil)
		if err != nil {
			log.Printf("MultiJob '%s': MongoDB CDC error: %v", j.ID, err)
		}
	default:
		log.Printf("MultiJob '%s': Unsupported connector type: %T", j.ID, connector)
		return
	}
}

// jobCDCStream implements the CDC stream interface for individual jobs
type jobCDCStream struct {
	job *MultiJob
}

// Send processes CDC events for the specific job
func (s *jobCDCStream) Send(event *pb.ChangeEvent) error {
	// Ensure SourceType is set for transform rule matching
	if event.Checkpoint != nil {
		event.Checkpoint.SourceType = s.job.SourceID
	} else {
		event.Checkpoint = &pb.Checkpoint{
			SourceType: s.job.SourceID,
		}
	}
	s.job.addToBatch(event)
	return nil
}

// Context returns the job context
func (s *jobCDCStream) Context() context.Context {
	return s.job.ctx
}

// RecvMsg implements the gRPC ServerStreamingServer interface
// This method is required by the interface but not used in server streaming
func (s *jobCDCStream) RecvMsg(m interface{}) error {
	// This should not be called for server streaming
	return fmt.Errorf("RecvMsg should not be called on server streaming")
}

// SendMsg implements the gRPC ServerStreamingServer interface
func (s *jobCDCStream) SendMsg(m interface{}) error {
	if event, ok := m.(*pb.ChangeEvent); ok {
		return s.Send(event)
	}
	return fmt.Errorf("invalid message type for SendMsg")
}

// SendHeader implements the gRPC ServerStreamingServer interface
func (s *jobCDCStream) SendHeader(md metadata.MD) error {
	// Not needed for our use case, but required by interface
	return nil
}

// SetHeader implements the gRPC ServerStreamingServer interface
func (s *jobCDCStream) SetHeader(md metadata.MD) error {
	// Not needed for our use case, but required by interface
	return nil
}

// SetTrailer implements the gRPC ServerStreamingServer interface
func (s *jobCDCStream) SetTrailer(md metadata.MD) {
	// Not needed for our use case, but required by interface
}

// addToBatch adds event to job's batch
func (j *MultiJob) addToBatch(event *pb.ChangeEvent) {
	j.batchMutex.Lock()
	defer j.batchMutex.Unlock()

	j.batch = append(j.batch, event)
	if len(j.batch) >= batchSize {
		j.flushBatch()
	}
}

// batchFlusher periodically flushes batches
func (j *MultiJob) batchFlusher() {
	ticker := time.NewTicker(commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			j.batchMutex.Lock()
			j.flushBatch()
			j.batchMutex.Unlock()
		case <-j.ctx.Done():
			return
		}
	}
}

// flushBatch processes and sends batch to transform and sink
func (j *MultiJob) flushBatch() {
	if len(j.batch) == 0 {
		return
	}

	log.Printf("MultiJob '%s': Flushing batch of %d events (%s -> %s)",
		j.ID, len(j.batch), j.SourceID, j.SinkID)

	// Enrich CDC events with _source_id for transform rule matching
	for _, event := range j.batch {
		j.enrichEventWithSourceID(event)
	}

	// Transform events
	transformedEvents := j.transformEvents(j.batch)
	if len(transformedEvents) == 0 {
		j.batch = nil
		return
	}

	// Send to sink
	if err := j.sendToSink(transformedEvents); err != nil {
		log.Printf("MultiJob '%s': Failed to send to sink: %v", j.ID, err)

		// Add failed events to Dead Letter Queue for automatic retry
		if j.dlqManager != nil {
			addedCount := 0
			for _, event := range transformedEvents {
				dlqErr := j.dlqManager.AddFailedEvent(
					j.ID,
					event,
					"sink",
					err.Error(),
					event.Checkpoint,
				)
				if dlqErr != nil {
					log.Printf("MultiJob '%s': Failed to add event to DLQ: %v", j.ID, dlqErr)
				} else {
					addedCount++
				}
			}
			log.Printf("MultiJob '%s': Added %d/%d failed events to DLQ for automatic retry (deduplicated)", j.ID, addedCount, len(transformedEvents))
		} else {
			log.Printf("MultiJob '%s': DLQ not available, events will be lost", j.ID)
		}

		// CRITICAL FIX: Clear batch even on failure to prevent reprocessing
		j.batch = nil
		log.Printf("MultiJob '%s': Batch cleared after DLQ processing to prevent duplicate retries", j.ID)
		return
	}

	// Update checkpoint
	lastEvent := transformedEvents[len(transformedEvents)-1]
	j.updateCheckpoint(lastEvent.Checkpoint)
	j.commitCheckpoint()

	// Clear batch
	j.batch = nil
}

// transformEvents applies transformation rules via the Transform Service
func (j *MultiJob) transformEvents(events []*pb.ChangeEvent) []*pb.ChangeEvent {
	if len(events) == 0 {
		return events
	}

	// If no transform client, pass through
	if j.transformClient == nil {
		log.Printf("MultiJob '%s': No transform client configured, passing through %d events", j.ID, len(events))
		return events
	}

	ctx, cancel := context.WithTimeout(j.ctx, 30*time.Second)
	defer cancel()

	// Open transform stream
	transformStream, err := j.transformClient.ApplyRules(ctx)
	if err != nil {
		log.Printf("MultiJob '%s': Failed to open transform stream: %v, passing through events", j.ID, err)
		return events
	}

	// Send all events to transform service
	for _, event := range events {
		if err := transformStream.Send(event); err != nil {
			log.Printf("MultiJob '%s': Failed to send event to transform: %v, passing through remaining events", j.ID, err)
			return events
		}
	}

	// Close send stream
	if err := transformStream.CloseSend(); err != nil {
		log.Printf("MultiJob '%s': Failed to close transform send stream: %v", j.ID, err)
		return events
	}

	// Collect transformed events
	transformedEvents := make([]*pb.ChangeEvent, 0, len(events))
	for {
		event, err := transformStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("MultiJob '%s': Failed to receive from transform: %v, returning partial results", j.ID, err)
			break
		}
		transformedEvents = append(transformedEvents, event)
	}

	if len(transformedEvents) != len(events) {
		log.Printf("MultiJob '%s': Transform filtered %d/%d events", j.ID, len(events)-len(transformedEvents), len(events))
	}

	return transformedEvents
}

// sendToSink sends events to the configured sink
func (j *MultiJob) sendToSink(events []*pb.ChangeEvent) error {
	stream, err := j.sinkClient.BulkWrite(j.ctx)
	if err != nil {
		return fmt.Errorf("failed to create sink stream: %w", err)
	}

	for _, event := range events {
		if err := stream.Send(event); err != nil {
			return fmt.Errorf("failed to send event: %w", err)
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

// enrichEventWithSourceID adds _source_id to event data for transform rule matching
// This is needed because Checkpoint.SourceType may be lost during gRPC transmission
func (j *MultiJob) enrichEventWithSourceID(event *pb.ChangeEvent) {
	if event.Data == "" {
		return
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(event.Data), &data); err != nil {
		return
	}
	// Only add _source_id if not already present
	if _, exists := data["_source_id"]; !exists {
		data["_source_id"] = j.SourceID
		if enrichedData, err := json.Marshal(data); err == nil {
			event.Data = string(enrichedData)
		}
	}
}

// updateCheckpoint updates job checkpoint
func (j *MultiJob) updateCheckpoint(cp *pb.Checkpoint) {
	j.cpMutex.Lock()
	defer j.cpMutex.Unlock()
	j.lastCp = cp
}

// commitCheckpoint commits checkpoint to persistent storage
func (j *MultiJob) commitCheckpoint() {
	j.cpMutex.RLock()
	defer j.cpMutex.RUnlock()

	if j.lastCp == nil {
		return
	}

	// Connect to the MySQL connector's gRPC service to commit checkpoint
	// All services are running on the same gRPC server
	conn, err := grpc.DialContext(j.ctx, "localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("MultiJob '%s': Failed to connect to connector service: %v", j.ID, err)
		return
	}
	defer conn.Close()

	connectorClient := pb.NewConnectorServiceClient(conn)
	_, err = connectorClient.CommitCheckpoint(j.ctx, &pb.CommitCheckpointRequest{
		JobId:      j.ID,
		Checkpoint: j.lastCp,
	})
	if err != nil {
		log.Printf("MultiJob '%s': Failed to commit checkpoint: %v", j.ID, err)
	} else {
		log.Printf("MultiJob '%s': Successfully committed checkpoint at %s",
			j.ID, j.formatCheckpointPosition(j.lastCp))
	}
}

func (j *MultiJob) formatCheckpointPosition(cp *pb.Checkpoint) string {
	if cp == nil {
		return "<nil>"
	}
	if cp.PostgresLsn != "" {
		return cp.PostgresLsn
	}
	if cp.MysqlBinlogFile != "" || cp.MysqlBinlogPos > 0 {
		return fmt.Sprintf("%s:%d", cp.MysqlBinlogFile, cp.MysqlBinlogPos)
	}
	if cp.MongoResumeToken != "" {
		return cp.MongoResumeToken
	}
	if cp.Position != "" {
		return cp.Position
	}
	return "<empty>"
}

// needsInitialSync checks if initial sync is needed for this job using multiple criteria
func (j *MultiJob) needsInitialSync() bool {
	log.Printf("MultiJob '%s': Checking if initial sync is needed", j.ID)

	// 1. Check configuration - explicit initial_sync setting
	if !j.isInitialSyncEnabledInConfig() {
		log.Printf("MultiJob '%s': Initial sync disabled in configuration", j.ID)
		return false
	}

	// 2. Check if force_initial_sync is enabled - this overrides all other checks
	if j.shouldForceInitialSync() {
		log.Printf("MultiJob '%s': force_initial_sync enabled, will perform initial sync", j.ID)
		return true
	}

	// 3. Check if valid checkpoint exists
	if j.hasValidCheckpoint() {
		log.Printf("MultiJob '%s': Valid checkpoint found, skipping initial sync", j.ID)
		return false
	}

	// 4. Check if target system already has data for this job
	if j.targetSystemHasData() {
		log.Printf("MultiJob '%s': Target system already has data, checking consistency", j.ID)
		// If target has data but no checkpoint, this might be a manual import
		// In production, you might want to compare source vs target counts
		log.Printf("MultiJob '%s': Target system has data but no checkpoint, skipping initial sync for safety", j.ID)
		return false
	}

	// 5. Default: If no checkpoint and no target data, definitely need initial sync
	log.Printf("MultiJob '%s': No checkpoint and no target data found, initial sync needed", j.ID)
	return true
}

// isInitialSyncEnabledInConfig checks if initial_sync is explicitly enabled in job configuration
func (j *MultiJob) isInitialSyncEnabledInConfig() bool {
	log.Printf("MultiJob '%s': Checking initial_sync configuration", j.ID)

	if j.jobOptions != nil {
		if initialSync, exists := j.jobOptions["initial_sync"]; exists {
			if enabled, ok := initialSync.(bool); ok {
				log.Printf("MultiJob '%s': initial_sync explicitly set to %t", j.ID, enabled)
				return enabled
			}
		}
	}

	// Default to true for safety if config is not available or not explicitly set
	log.Printf("MultiJob '%s': initial_sync not explicitly configured, defaulting to true", j.ID)
	return true
}

// getCheckpointManager returns the appropriate checkpoint manager for this job's connector type
func (j *MultiJob) getCheckpointManager() connectors.CheckpointManager {
	if j.connectorInstance == nil || j.connectorInstance.Config == nil {
		log.Printf("MultiJob '%s': No connector configuration available", j.ID)
		return nil
	}

	switch j.connectorInstance.Config.Type {
	case "mysql":
		return mysql.NewMySQLCheckpointManager()
	case "postgresql":
		return postgresql.NewPostgreSQLCheckpointManager()
	case "mongodb":
		// TODO: Implement MongoDB checkpoint manager
		log.Printf("MultiJob '%s': MongoDB checkpoint manager not yet implemented", j.ID)
		return nil
	default:
		log.Printf("MultiJob '%s': Unknown connector type '%s'", j.ID, j.connectorInstance.Config.Type)
		return nil
	}
}

// hasValidCheckpoint checks if a valid checkpoint exists for this job using the new CheckpointManager interface
func (j *MultiJob) hasValidCheckpoint() bool {
	// Load checkpoints from file
	checkpoints, err := j.loadCheckpointsFromFile()
	if err != nil {
		log.Printf("MultiJob '%s': Cannot load checkpoints file: %v", j.ID, err)
		return false
	}

	cp, exists := checkpoints[j.ID]
	if !exists {
		log.Printf("MultiJob '%s': No checkpoint found in file", j.ID)
		return false
	}

	// Get the appropriate checkpoint manager
	checkpointMgr := j.getCheckpointManager()
	if checkpointMgr == nil {
		// Fallback to legacy validation if no manager is available
		log.Printf("MultiJob '%s': No checkpoint manager available, using fallback validation", j.ID)
		return j.hasValidCheckpointLegacy(cp)
	}

	// Use the checkpoint manager to validate
	isValid := checkpointMgr.IsValid(cp)
	if isValid {
		log.Printf("MultiJob '%s': Valid %s checkpoint found", j.ID, checkpointMgr.GetSourceType())
	} else {
		log.Printf("MultiJob '%s': Invalid %s checkpoint", j.ID, checkpointMgr.GetSourceType())
	}

	return isValid
}

// hasValidCheckpointLegacy provides fallback validation for when checkpoint manager is not available
func (j *MultiJob) hasValidCheckpointLegacy(cp *pb.Checkpoint) bool {
	log.Printf("MultiJob '%s': Using legacy checkpoint validation", j.ID)

	// Check MySQL checkpoint
	if cp.MysqlBinlogFile != "" && cp.MysqlBinlogPos > 0 {
		log.Printf("MultiJob '%s': Valid MySQL checkpoint found (legacy): %s:%d",
			j.ID, cp.MysqlBinlogFile, cp.MysqlBinlogPos)
		return true
	}

	// Check PostgreSQL checkpoint
	if cp.PostgresLsn != "" && cp.PostgresLsn != "0/0" {
		log.Printf("MultiJob '%s': Valid PostgreSQL checkpoint found (legacy): LSN=%s",
			j.ID, cp.PostgresLsn)
		return true
	}

	// Check MongoDB checkpoint
	if cp.MongoResumeToken != "" {
		log.Printf("MultiJob '%s': Valid MongoDB checkpoint found (legacy): token=%s",
			j.ID, cp.MongoResumeToken)
		return true
	}

	log.Printf("MultiJob '%s': No valid checkpoint data found", j.ID)
	return false
}

// loadCheckpointsFromFile loads checkpoints directly from the JSON file
func (j *MultiJob) loadCheckpointsFromFile() (map[string]*pb.Checkpoint, error) {
	const checkpointFilePath = "checkpoints.json"

	data, err := os.ReadFile(checkpointFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]*pb.Checkpoint), nil
		}
		return nil, fmt.Errorf("failed to read checkpoints file: %w", err)
	}

	var checkpoints map[string]*pb.Checkpoint
	if err := json.Unmarshal(data, &checkpoints); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoints: %w", err)
	}
	return checkpoints, nil
}

// targetSystemHasData checks if the target Elasticsearch already has data for this job
func (j *MultiJob) targetSystemHasData() bool {
	log.Printf("MultiJob '%s': Checking target system for existing data", j.ID)

	if j.sinkClient == nil {
		log.Printf("MultiJob '%s': Sink client not available, assuming no data", j.ID)
		return false
	}

	// Get the configured tables for this job's source
	if j.connectorInstance == nil || j.connectorInstance.Config == nil {
		log.Printf("MultiJob '%s': No connector configuration, assuming no data", j.ID)
		return false
	}

	tableFilters := j.connectorInstance.Config.TableFilters
	if len(tableFilters) == 0 {
		log.Printf("MultiJob '%s': No table filters configured, assuming no data", j.ID)
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	totalDocuments := int64(0)
	indicesFound := 0

	// Check each configured table for existing data
	for _, tableName := range tableFilters {
		indexName := j.generateIndexName(tableName)

		// Use DescribeIndex to check if index exists and get document count
		req := &pb.DescribeIndexRequest{
			IndexName: indexName,
		}

		resp, err := j.sinkClient.DescribeIndex(ctx, req)
		if err != nil {
			log.Printf("MultiJob '%s': Failed to describe index '%s': %v", j.ID, indexName, err)
			continue
		}

		// Parse JSON response from IndexDefinition field
		var indexInfo map[string]interface{}
		if err := json.Unmarshal([]byte(resp.IndexDefinition), &indexInfo); err != nil {
			log.Printf("MultiJob '%s': Failed to parse index info for '%s': %v", j.ID, indexName, err)
			continue
		}

		exists, _ := indexInfo["exists"].(bool)
		documentCount, _ := indexInfo["document_count"].(float64) // JSON numbers are float64

		if exists {
			indicesFound++
			totalDocuments += int64(documentCount)
			log.Printf("MultiJob '%s': Found index '%s' with %d documents", j.ID, indexName, int64(documentCount))
		} else {
			log.Printf("MultiJob '%s': Index '%s' does not exist", j.ID, indexName)
		}
	}

	hasData := totalDocuments > 0
	log.Printf("MultiJob '%s': Target system check complete - %d indices found, %d total documents", j.ID, indicesFound, totalDocuments)

	return hasData
}

// generateIndexName creates an index name based on the table name (matches ES sink logic)
func (j *MultiJob) generateIndexName(tableName string) string {
	// Default prefix
	indexPrefix := "elasticrelay"

	// Extract prefix from sink configuration if available
	if j.sinkConfig != nil {
		if prefix, ok := j.sinkConfig["index_prefix"].(string); ok && prefix != "" {
			indexPrefix = prefix
		}
	}

	if tableName == "" {
		return indexPrefix + "-default"
	}
	return indexPrefix + "-" + strings.ToLower(tableName)
}

// shouldForceInitialSync determines if initial sync should be forced even when target has data
func (j *MultiJob) shouldForceInitialSync() bool {
	log.Printf("MultiJob '%s': Checking if initial sync should be forced", j.ID)

	// Check if force_initial_sync is explicitly enabled in configuration
	if j.jobOptions != nil {
		if forceSync, exists := j.jobOptions["force_initial_sync"]; exists {
			if enabled, ok := forceSync.(bool); ok && enabled {
				log.Printf("MultiJob '%s': force_initial_sync explicitly enabled", j.ID)
				return true
			}
		}

		// Check for consistency_check option
		if consistencyCheck, exists := j.jobOptions["consistency_check"]; exists {
			if enabled, ok := consistencyCheck.(bool); ok && enabled {
				log.Printf("MultiJob '%s': consistency_check enabled, performing data verification", j.ID)
				// TODO: Implement actual consistency check logic
				// For now, return false to be conservative
				return false
			}
		}
	}

	// For safety, if target has data but no checkpoint, we skip initial sync
	// to avoid duplicate data, unless explicitly forced via configuration
	log.Printf("MultiJob '%s': Target has data but no checkpoint and force not enabled - skipping initial sync to avoid duplicates", j.ID)
	return false
}

// initializeParallelManager initializes the parallel snapshot manager
func (j *MultiJob) initializeParallelManager() error {
	// For PostgreSQL, we should use the PostgreSQL-specific parallel manager
	// For now, disable parallel processing for PostgreSQL to avoid MySQL-specific queries
	if j.connectorInstance.Config.Type == "postgresql" {
		log.Printf("MultiJob '%s': PostgreSQL detected, disabling parallel processing to avoid compatibility issues", j.ID)
		j.useParallel = false
		j.parallelManager = nil
		return nil
	}

	// Only proceed with generic parallel manager for MySQL
	if j.connectorInstance.Config.Type != "mysql" {
		return fmt.Errorf("unsupported database type for parallel manager: %s", j.connectorInstance.Config.Type)
	}

	// Create MySQL database connection
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4",
		j.connectorInstance.Config.User,
		j.connectorInstance.Config.Password,
		j.connectorInstance.Config.Host,
		j.connectorInstance.Config.Port,
		j.connectorInstance.Config.Database)

	dbPool, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to create database connection: %w", err)
	}

	// Create ES client (simplified)
	esClient := parallel.NewSimpleESClient(
		"http://172.168.0.100:19200", // TODO: Get from sink config
		"elastic",
		"zIUPPogxwxCR",
	)

	// Create parallel snapshot manager for MySQL
	config := parallel.DefaultSnapshotConfig()
	j.parallelManager = parallel.NewParallelSnapshotManager(j.ID, config, dbPool, esClient)

	return nil
}

// performInitialSync performs initial snapshot sync for all configured tables
func (j *MultiJob) performInitialSync() error {
	// Get table filters from connector instance
	if j.connectorInstance == nil || j.connectorInstance.Config == nil {
		return fmt.Errorf("connector instance or config is nil")
	}

	tables := j.connectorInstance.Config.TableFilters
	if len(tables) == 0 {
		log.Printf("MultiJob '%s': No tables configured for sync", j.ID)
		return nil
	}

	// Use parallel processing if available and enabled
	if j.useParallel && j.parallelManager != nil {
		log.Printf("MultiJob '%s': Starting parallel initial sync for %d tables", j.ID, len(tables))
		return j.performParallelInitialSync(tables)
	}

	// Fallback to serial processing
	log.Printf("MultiJob '%s': Starting serial initial sync for %d tables", j.ID, len(tables))
	return j.performSerialInitialSync(tables)
}

// performParallelInitialSync performs parallel initial sync using the new architecture
func (j *MultiJob) performParallelInitialSync(tables []string) error {
	log.Printf("MultiJob '%s': Starting parallel snapshot sync for tables: %v", j.ID, tables)

	// Start parallel snapshot manager
	if err := j.parallelManager.Start(j.ctx, tables); err != nil {
		return fmt.Errorf("failed to start parallel snapshot manager: %w", err)
	}

	// Monitor progress
	return j.monitorParallelProgress()
}

// performSerialInitialSync performs the original serial sync (fallback)
func (j *MultiJob) performSerialInitialSync(tables []string) error {
	// Connect to the MySQL connector service
	conn, err := grpc.DialContext(j.ctx, "localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to connector service: %w", err)
	}
	defer conn.Close()

	connectorClient := pb.NewConnectorServiceClient(conn)

	// Perform snapshot for each table
	for _, tableName := range tables {
		log.Printf("MultiJob '%s': Starting snapshot for table '%s'", j.ID, tableName)

		if err := j.snapshotTable(connectorClient, tableName); err != nil {
			return fmt.Errorf("failed to snapshot table %s: %w", tableName, err)
		}

		log.Printf("MultiJob '%s': Completed snapshot for table '%s'", j.ID, tableName)
	}

	return nil
}

// monitorParallelProgress monitors the progress of parallel sync
func (j *MultiJob) monitorParallelProgress() error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-j.ctx.Done():
			return j.ctx.Err()
		case <-ticker.C:
			progress := j.parallelManager.GetStatistics()
			log.Printf("MultiJob '%s': Parallel sync progress - Tables: %d/%d, Chunks: %d/%d",
				j.ID, progress.TablesCompleted, progress.TablesTotal,
				progress.CompletedChunks, progress.TotalChunks)

			// Check if completed
			if progress.TablesCompleted == progress.TablesTotal && progress.TablesTotal > 0 {
				log.Printf("MultiJob '%s': Parallel snapshot sync completed", j.ID)
				return nil
			}
		}
	}
}

// snapshotTable performs snapshot for a single table
func (j *MultiJob) snapshotTable(connectorClient pb.ConnectorServiceClient, tableName string) error {
	primaryKeyColumns, err := j.getSnapshotPrimaryKeyColumns(tableName)
	if err != nil {
		return fmt.Errorf("failed to get primary key columns for table %s: %w", tableName, err)
	}

	// Start snapshot stream
	stream, err := connectorClient.BeginSnapshot(j.ctx, &pb.BeginSnapshotRequest{
		JobId:     j.ID,
		TableName: tableName,
	})
	if err != nil {
		return fmt.Errorf("failed to start snapshot stream: %w", err)
	}

	var totalRecords int
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving snapshot chunk: %w", err)
		}

		// Process snapshot chunk - convert to ChangeEvents and send to sink
		if err := j.processSnapshotChunk(chunk, tableName, primaryKeyColumns); err != nil {
			return fmt.Errorf("failed to process snapshot chunk: %w", err)
		}

		totalRecords += len(chunk.Records)
		log.Printf("MultiJob '%s': Processed %d records from table '%s' (total: %d)",
			j.ID, len(chunk.Records), tableName, totalRecords)
	}

	log.Printf("MultiJob '%s': Snapshot completed for table '%s': %d total records",
		j.ID, tableName, totalRecords)
	return nil
}

func (j *MultiJob) getSnapshotPrimaryKeyColumns(tableName string) ([]string, error) {
	if j.connectorInstance == nil || j.connectorInstance.Config == nil {
		return nil, fmt.Errorf("connector instance or config is nil")
	}

	switch strings.ToLower(j.connectorInstance.Type) {
	case "mysql":
		return j.getMySQLPrimaryKeyColumns(tableName)
	case "postgresql":
		return j.getPostgreSQLPrimaryKeyColumns(tableName)
	case "mongodb":
		return []string{"_id"}, nil
	default:
		return nil, fmt.Errorf("unsupported source type for snapshot primary key lookup: %s", j.connectorInstance.Type)
	}
}

func (j *MultiJob) getMySQLPrimaryKeyColumns(tableName string) ([]string, error) {
	cfg := j.connectorInstance.Config
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&collation=utf8mb4_unicode_ci&interpolateParams=true&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}
	defer db.Close()

	query := `
		SELECT COLUMN_NAME
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		  AND TABLE_NAME = ?
		  AND COLUMN_KEY = 'PRI'
		ORDER BY ORDINAL_POSITION
	`

	rows, err := db.QueryContext(j.ctx, query, cfg.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query mysql primary key metadata: %w", err)
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan mysql primary key column: %w", err)
		}
		pkColumns = append(pkColumns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate mysql primary key rows: %w", err)
	}

	if len(pkColumns) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", tableName)
	}

	return pkColumns, nil
}

func (j *MultiJob) getPostgreSQLPrimaryKeyColumns(tableName string) ([]string, error) {
	cfg := j.connectorInstance.Config
	schemaName := "public"
	baseTableName := tableName
	if strings.Contains(tableName, ".") {
		parts := strings.SplitN(tableName, ".", 2)
		schemaName = parts[0]
		baseTableName = parts[1]
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgresql connection: %w", err)
	}
	defer db.Close()

	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		 AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_schema = $1
		  AND tc.table_name = $2
		ORDER BY kcu.ordinal_position
	`

	rows, err := db.QueryContext(j.ctx, query, schemaName, baseTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query postgresql primary key metadata: %w", err)
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan postgresql primary key column: %w", err)
		}
		pkColumns = append(pkColumns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate postgresql primary key rows: %w", err)
	}

	if len(pkColumns) == 0 {
		return nil, fmt.Errorf("table %s has no primary key", tableName)
	}

	return pkColumns, nil
}

func extractSnapshotPrimaryKey(recordData map[string]interface{}, primaryKeyColumns []string) (string, error) {
	if len(primaryKeyColumns) == 0 {
		return "", fmt.Errorf("no primary key columns configured")
	}

	pkParts := make([]string, 0, len(primaryKeyColumns))
	for _, columnName := range primaryKeyColumns {
		value, exists := recordData[columnName]
		if !exists {
			return "", fmt.Errorf("primary key column %s not found in snapshot record", columnName)
		}
		pkParts = append(pkParts, fmt.Sprintf("%v", value))
	}

	return strings.Join(pkParts, ":"), nil
}

// processSnapshotChunk converts snapshot data to ChangeEvents and sends to sink
func (j *MultiJob) processSnapshotChunk(chunk *pb.SnapshotChunk, tableName string, primaryKeyColumns []string) error {
	events := make([]*pb.ChangeEvent, 0, len(chunk.Records))

	// Convert snapshot records to ChangeEvents
	for _, record := range chunk.Records {
		// Parse the JSON record to extract primary key
		var recordData map[string]interface{}
		if err := json.Unmarshal([]byte(record), &recordData); err != nil {
			log.Printf("MultiJob '%s': Failed to parse record JSON from table '%s': %v", j.ID, tableName, err)
			continue
		}

		// Add table name and source_id to record data for transform engine to match rules
		recordData["_table"] = tableName
		recordData["_source_id"] = j.SourceID

		// Re-serialize the record with table name and source_id included
		enrichedRecord, err := json.Marshal(recordData)
		if err != nil {
			log.Printf("MultiJob '%s': Failed to re-serialize record from table '%s': %v", j.ID, tableName, err)
			continue
		}

		primaryKey, err := extractSnapshotPrimaryKey(recordData, primaryKeyColumns)
		if err != nil {
			log.Printf("MultiJob '%s': Failed to extract primary key from table '%s': %v", j.ID, tableName, err)
			continue
		}

		// Create ChangeEvent for snapshot data
		checkpoint := &pb.Checkpoint{
			SourceType: j.SourceID, // Preserve source ID for transform rule matching
		}
		if strings.EqualFold(j.connectorInstance.Type, "postgresql") {
			snapshotLSN := chunk.Cursor
			if snapshotLSN == "" {
				snapshotLSN = chunk.SnapshotBinlogFile
			}
			checkpoint.Position = snapshotLSN
			checkpoint.PostgresLsn = snapshotLSN
			checkpoint.Timestamp = time.Now().Unix()
		} else {
			checkpoint.MysqlBinlogFile = chunk.SnapshotBinlogFile
			checkpoint.MysqlBinlogPos = chunk.SnapshotBinlogPos
		}

		event := &pb.ChangeEvent{
			Op:         "INSERT", // Snapshot data is treated as INSERT
			PrimaryKey: primaryKey,
			Data:       string(enrichedRecord), // Use enriched record with _table field
			Checkpoint: checkpoint,
		}

		events = append(events, event)
	}

	// Process events through normal pipeline (transform -> sink)
	if len(events) > 0 {
		transformedEvents := j.transformEvents(events)
		if err := j.sendToSink(transformedEvents); err != nil {
			return fmt.Errorf("failed to send snapshot events to sink: %w", err)
		}

		// Update checkpoint after successful snapshot chunk
		if len(transformedEvents) > 0 {
			lastEvent := transformedEvents[len(transformedEvents)-1]
			j.updateCheckpoint(lastEvent.Checkpoint)
			j.commitCheckpoint()
		}
	}

	return nil
}

// getJobStatus returns current job status
func getJobStatus(job *MultiJob) string {
	if !job.Enabled {
		return "DISABLED"
	}

	select {
	case <-job.ctx.Done():
		return "STOPPED"
	default:
		return "RUNNING"
	}
}

// ListJobs lists all jobs
func (mo *MultiOrchestrator) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	mo.jobsMux.RLock()
	defer mo.jobsMux.RUnlock()

	jobs := make([]*pb.Job, 0, len(mo.jobs))
	for _, job := range mo.jobs {
		jobs = append(jobs, &pb.Job{
			Id:     job.ID,
			Name:   job.Name,
			Status: getJobStatus(job),
			Config: fmt.Sprintf(`{"source_id":"%s","sink_id":"%s","enabled":%t}`,
				job.SourceID, job.SinkID, job.Enabled),
		})
	}

	return &pb.ListJobsResponse{
		Jobs:          jobs,
		NextPageToken: "",
	}, nil
}

// retryDLQItem attempts to retry a failed DLQ item
func (mo *MultiOrchestrator) retryDLQItem(item *dlq.DLQItem) error {
	log.Printf("MultiOrchestrator: Retrying DLQ item %s for job %s", item.ID, item.JobID)

	// Find the job
	mo.jobsMux.RLock()
	job, exists := mo.jobs[item.JobID]
	mo.jobsMux.RUnlock()

	if !exists {
		// Try to find a job with compatible configuration
		log.Printf("MultiOrchestrator: Job %s not found, attempting to find compatible job", item.JobID)

		mo.jobsMux.RLock()
		var compatibleJob *MultiJob
		for _, j := range mo.jobs {
			// Check if this job processes the same table/source as the DLQ item
			if item.Event != nil && item.Event.Data != "" {
				// Extract table info from the event data
				if j.Enabled {
					compatibleJob = j
					break
				}
			}
		}
		mo.jobsMux.RUnlock()

		if compatibleJob != nil {
			log.Printf("MultiOrchestrator: Found compatible job %s for DLQ item %s", compatibleJob.ID, item.ID)
			job = compatibleJob
		} else {
			log.Printf("MultiOrchestrator: No compatible job found for DLQ item %s, marking as exhausted", item.ID)
			return fmt.Errorf("no compatible job available for item %s", item.ID)
		}
	}

	// Create a single-event batch for retry
	events := []*pb.ChangeEvent{item.Event}

	// Try to process the event again
	switch item.ErrorType {
	case "transform":
		_, err := mo.processTransformBatch(job, events)
		return err
	case "sink":
		err := mo.processSinkBatch(job, events)
		return err
	default:
		return fmt.Errorf("unknown error type: %s", item.ErrorType)
	}
}

// processTransformBatch processes events through the transform service
func (mo *MultiOrchestrator) processTransformBatch(job *MultiJob, events []*pb.ChangeEvent) ([]*pb.ChangeEvent, error) {
	if len(events) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transformStream, err := job.transformClient.ApplyRules(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open transform stream: %w", err)
	}

	// Send all events
	for _, event := range events {
		if err := transformStream.Send(event); err != nil {
			return nil, fmt.Errorf("failed to send event to transform: %w", err)
		}
	}

	// Close send stream
	if err := transformStream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close transform send stream: %w", err)
	}

	// Collect transformed events
	transformedEvents := make([]*pb.ChangeEvent, 0, len(events))
	for {
		event, err := transformStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to receive from transform: %w", err)
		}
		transformedEvents = append(transformedEvents, event)
	}

	return transformedEvents, nil
}

// processSinkBatch processes events through the sink service
func (mo *MultiOrchestrator) processSinkBatch(job *MultiJob, events []*pb.ChangeEvent) error {
	if len(events) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sinkStream, err := job.sinkClient.BulkWrite(ctx)
	if err != nil {
		return fmt.Errorf("failed to open sink stream: %w", err)
	}

	// Send all events
	for _, event := range events {
		if err := sinkStream.Send(event); err != nil {
			return fmt.Errorf("failed to send event to sink: %w", err)
		}
	}

	// Close and receive response
	_, err = sinkStream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close sink stream: %w", err)
	}

	return nil
}

// logDLQStats periodically logs DLQ statistics for monitoring
func (mo *MultiOrchestrator) logDLQStats() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		if mo.dlqManager == nil {
			return
		}

		stats := mo.dlqManager.GetStats()
		log.Printf("DLQ Statistics: Total=%d, Pending=%d, Retrying=%d, Exhausted=%d, Resolved=%d, Discarded=%d",
			stats["total"], stats["pending"], stats["retrying"], stats["exhausted"], stats["resolved"], stats["discarded"])

		// Log warning if there are exhausted items
		if stats["exhausted"] > 0 {
			log.Printf("⚠️  DLQ Warning: %d events have exhausted retry attempts and require manual intervention", stats["exhausted"])
		}
	}
}

// cleanupDLQ periodically cleans up old resolved and discarded DLQ items
func (mo *MultiOrchestrator) cleanupDLQ() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		if mo.dlqManager == nil {
			return
		}

		// Clean up items older than 7 days
		removed := mo.dlqManager.CleanupResolved(7 * 24 * time.Hour)
		if removed > 0 {
			log.Printf("DLQ Cleanup: Removed %d old resolved/discarded items", removed)
		}
	}
}

// cleanupOrphanedDLQItems removes DLQ items that no longer have compatible jobs
func (mo *MultiOrchestrator) cleanupOrphanedDLQItems() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		if mo.dlqManager == nil {
			return
		}

		// Get all pending DLQ items
		pendingItems := mo.dlqManager.ListItems("pending", 0)
		orphanedCount := 0

		for _, item := range pendingItems {
			// Check if the job exists
			mo.jobsMux.RLock()
			_, jobExists := mo.jobs[item.JobID]

			// If job doesn't exist, check for compatible jobs
			hasCompatible := false
			if !jobExists {
				for _, j := range mo.jobs {
					if j.Enabled {
						hasCompatible = true
						break
					}
				}
			}
			mo.jobsMux.RUnlock()

			// If no job exists and no compatible jobs, mark as discarded
			if !jobExists && !hasCompatible {
				// Check if item has been orphaned for more than 1 hour
				if time.Since(item.FirstFailed) > time.Hour {
					log.Printf("DLQ Cleanup: Discarding orphaned item %s (job %s not found)", item.ID, item.JobID)
					mo.dlqManager.DiscardItem(item.ID)
					orphanedCount++
				}
			}
		}

		if orphanedCount > 0 {
			log.Printf("DLQ Cleanup: Discarded %d orphaned DLQ items", orphanedCount)
		}
	}
}
