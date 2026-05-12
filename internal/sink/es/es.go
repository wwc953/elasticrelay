package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt" // Keep fmt for OnFailure callback
	"io"
	"log"
	"strings"
	"sync"
	"time" // Re-add for BulkIndexer

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil" // Re-add for BulkIndexer
	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
	"github.com/yogoosoft/elasticrelay/internal/config"
)

// Server implements the SinkService for Elasticsearch.
type Server struct {
	pb.UnimplementedSinkServiceServer
	esClient    *elasticsearch.Client
	indexPrefix string
}

// NewServer creates a new Elasticsearch sink server.
func NewServer(cfg *config.Config) (*Server, error) {
	esCfg := elasticsearch.Config{
		Addresses: cfg.ESAddresses,
		Username:  cfg.ESUser,
		Password:  cfg.ESPassword,
	}

	// Default index prefix for legacy config
	return newServerFromESConfig(esCfg, "elasticrelay")
}

// NewServerFromSinkConfig creates a new Elasticsearch sink server from SinkConfig.
func NewServerFromSinkConfig(sinkCfg *config.SinkConfig) (*Server, error) {
	esCfg := elasticsearch.Config{
		Addresses: sinkCfg.Addresses,
		Username:  sinkCfg.User,
		Password:  sinkCfg.Password,
	}

	// Extract index prefix from options
	indexPrefix := "elasticrelay" // default value
	if sinkCfg.Options != nil {
		if prefix, ok := sinkCfg.Options["index_prefix"].(string); ok && prefix != "" {
			indexPrefix = prefix
		}
	}

	return newServerFromESConfig(esCfg, indexPrefix)
}

// newServerFromESConfig is the common function to create server from elasticsearch.Config
func newServerFromESConfig(esCfg elasticsearch.Config, indexPrefix string) (*Server, error) {
	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	res, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to get elasticsearch info: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch info() returned an error: %s", res.String())
	}

	log.Printf("Elasticsearch client initialized successfully. Server: %s", res.String())

	server := &Server{
		esClient:    es,
		indexPrefix: indexPrefix,
	}

	log.Printf("Elasticsearch sink initialized with index prefix: %s", indexPrefix)

	return server, nil
}

// extractTableName extracts table name from the JSON data
// Supports both _table (MySQL/PostgreSQL) and _collection (MongoDB)
func (s *Server) extractTableName(jsonData string) string {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		log.Printf("Failed to parse JSON data: %v", err)
		return ""
	}

	// Check _table first (MySQL/PostgreSQL)
	if tableName, ok := data["_table"].(string); ok {
		return tableName
	}
	// Check _collection for MongoDB
	if collectionName, ok := data["_collection"].(string); ok {
		return collectionName
	}
	return ""
}

// generateIndexName creates an index name based on the table name and index prefix
func (s *Server) generateIndexName(tableName string) string {
	if tableName == "" {
		return s.indexPrefix + "-default"
	}
	return s.indexPrefix + "-" + strings.ToLower(tableName)
}

// cleanDataForES removes metadata fields before storing in Elasticsearch.
// Uses json.Decoder with UseNumber() to preserve numeric representations
// (e.g. "3200.0" stays as-is instead of becoming float64(3200) → "3200"),
// preventing ES dynamic mapping type conflicts between long and float.
func (s *Server) cleanDataForES(jsonData string) string {
	var data map[string]interface{}
	dec := json.NewDecoder(strings.NewReader(jsonData))
	dec.UseNumber()
	if err := dec.Decode(&data); err != nil {
		log.Printf("Failed to parse JSON data for cleaning: %v", err)
		return jsonData
	}

	// Remove metadata fields
	delete(data, "_table")
	delete(data, "_collection") // MongoDB collection name
	delete(data, "_schema")
	delete(data, "_database")
	delete(data, "_source_id") // Transform rule matching metadata
	// Remove _id field from document body - ES uses _id as metadata field
	// The document ID should be set via DocumentID in the bulk request, not in the body
	delete(data, "_id")

	// Re-serialize without metadata
	cleanedData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to re-marshal cleaned data: %v", err)
		return jsonData
	}

	return string(cleanedData)
}

// BulkWrite implements the gRPC service endpoint for bulk writing.
func (s *Server) BulkWrite(stream pb.SinkService_BulkWriteServer) error {
	// Track bulk indexer failures
	var bulkFailures []string
	var bulkMutex sync.Mutex

	// Create a BulkIndexer without specifying a default index
	// (we'll specify index for each item individually)
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        s.esClient,
		NumWorkers:    2,               // Reduced workers for faster failure detection
		FlushBytes:    int(1e6),        // 1 MB flush threshold (reduced for quicker processing)
		FlushInterval: 3 * time.Second, // Periodic flush interval (reduced for faster failure detection)
	})
	if err != nil {
		return fmt.Errorf("error creating the bulk indexer: %w", err)
	}

	log.Println("Sink: BulkWrite stream opened and BulkIndexer started.")

	// Keep track of created indices to avoid repeated creation attempts
	createdIndices := make(map[string]bool)
	failedIndices := make(map[string]error)

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break // Finished receiving events
		}
		if err != nil {
			log.Printf("Sink: Error receiving from BulkWrite stream: %v", err)
			return err
		}

		var action string
		switch strings.ToUpper(event.Op) {
		case "INSERT", "UPDATE":
			action = "index"
		case "DELETE":
			action = "delete"
		default:
			log.Printf("Sink: Unknown operation type '%s', skipping.", event.Op)
			continue
		}

		// Extract table name from the data JSON
		tableName := s.extractTableName(event.Data)

		// Generate index name based on table name
		indexName := s.generateIndexName(tableName)

		// Ensure the index exists (only once per index)
		if !createdIndices[indexName] {
			// Check if we already failed to create this index
			if prevErr, failed := failedIndices[indexName]; failed {
				log.Printf("Sink: Skipping event for index %s due to previous failure: %v", indexName, prevErr)
				// Return error to trigger DLQ
				return fmt.Errorf("elasticsearch unavailable for index %s: %w", indexName, prevErr)
			}

			if err := s.ensureIndexExists(indexName); err != nil {
				log.Printf("Error: Failed to ensure index %s exists: %v", indexName, err)
				failedIndices[indexName] = err
				// Return error immediately to trigger DLQ
				return fmt.Errorf("failed to ensure index %s exists: %w", indexName, err)
			}
			createdIndices[indexName] = true
			log.Printf("Index %s ensured for table %s", indexName, tableName)
		}

		// Clean the data by removing metadata fields
		cleanedData := s.cleanDataForES(event.Data)

		// Add an item to the BulkIndexer
		err = bi.Add(
			stream.Context(),
			esutil.BulkIndexerItem{
				Action:     action,
				DocumentID: event.PrimaryKey,
				Body: bytes.NewReader(func() []byte {
					if action == "delete" {
						return nil // bytes.NewReader(nil) creates an empty reader
					}
					return []byte(cleanedData)
				}()),
				Index: indexName, // Use dynamically generated index name
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					// Optional: log success
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					bulkMutex.Lock()
					defer bulkMutex.Unlock()

					var errMsg string
					if err != nil {
						errMsg = fmt.Sprintf("ELASTICSEARCH BULK ERROR for PK %s, Op %s, Index %s: %s", item.DocumentID, item.Action, item.Index, err)
					} else {
						errMsg = fmt.Sprintf("ELASTICSEARCH BULK ERROR for PK %s, Op %s, Index %s: %s: %s", item.DocumentID, item.Action, item.Index, res.Error.Type, res.Error.Reason)
					}
					log.Printf(errMsg)
					bulkFailures = append(bulkFailures, errMsg)
				},
			},
		)

		if err != nil {
			log.Printf("Sink: Failed to add item to bulk indexer: %v", err)
			// This is a local error, might want to handle it differently
		}
	}

	// Close the bulk indexer to flush any remaining items
	if err := bi.Close(context.Background()); err != nil {
		log.Printf("Error closing bulk indexer: %s", err)
		return fmt.Errorf("failed to close bulk indexer: %w", err)
	}

	stats := bi.Stats()
	log.Printf("Sink: BulkWrite stream finished. Stats: %+v", stats)

	// Check if there were any failures
	bulkMutex.Lock()
	hasFailures := len(bulkFailures) > 0
	bulkMutex.Unlock()

	if hasFailures || stats.NumFailed > 0 {
		// Return error to trigger DLQ for all events in this batch
		return fmt.Errorf("bulk write failed: %d items failed, first error: %s", stats.NumFailed, bulkFailures[0])
	}

	return stream.SendAndClose(&pb.BulkWriteResponse{
		SuccessCount: int32(stats.NumIndexed + stats.NumUpdated),
		FailedCount:  int32(stats.NumFailed),
	})
}

func (s *Server) DescribeIndex(ctx context.Context, req *pb.DescribeIndexRequest) (*pb.DescribeIndexResponse, error) {
	indexName := req.IndexName
	log.Printf("Describing index: %s", indexName)

	// Check if index exists
	existsRes, err := s.esClient.Indices.Exists([]string{indexName})
	if err != nil {
		return nil, fmt.Errorf("failed to check if index exists: %w", err)
	}
	defer existsRes.Body.Close()

	indexInfo := map[string]interface{}{
		"index_name":     indexName,
		"exists":         existsRes.StatusCode == 200,
		"document_count": int64(0),
	}

	// If index doesn't exist, return early
	if existsRes.StatusCode != 200 {
		log.Printf("Index %s does not exist", indexName)
		indexInfoJSON, _ := json.Marshal(indexInfo)
		return &pb.DescribeIndexResponse{
			IndexDefinition: string(indexInfoJSON),
		}, nil
	}

	// Get index statistics to determine document count
	statsRes, err := s.esClient.Indices.Stats(
		s.esClient.Indices.Stats.WithIndex(indexName),
		s.esClient.Indices.Stats.WithMetric("docs"),
	)
	if err != nil {
		log.Printf("Failed to get stats for index %s: %v", indexName, err)
		// Return with exists=true but count=0 if stats fail
		indexInfoJSON, _ := json.Marshal(indexInfo)
		return &pb.DescribeIndexResponse{
			IndexDefinition: string(indexInfoJSON),
		}, nil
	}
	defer statsRes.Body.Close()

	if statsRes.IsError() {
		log.Printf("Stats request for index %s returned error: %s", indexName, statsRes.String())
		// Return with exists=true but count=0 if stats fail
		indexInfoJSON, _ := json.Marshal(indexInfo)
		return &pb.DescribeIndexResponse{
			IndexDefinition: string(indexInfoJSON),
		}, nil
	}

	// Parse stats response
	var statsResponse map[string]interface{}
	if err := json.NewDecoder(statsRes.Body).Decode(&statsResponse); err != nil {
		log.Printf("Failed to decode stats response for index %s: %v", indexName, err)
		// Return with exists=true but count=0 if parsing fails
		indexInfoJSON, _ := json.Marshal(indexInfo)
		return &pb.DescribeIndexResponse{
			IndexDefinition: string(indexInfoJSON),
		}, nil
	}

	// Extract document count from stats
	if indices, ok := statsResponse["indices"].(map[string]interface{}); ok {
		if indexStats, ok := indices[indexName].(map[string]interface{}); ok {
			if primaries, ok := indexStats["primaries"].(map[string]interface{}); ok {
				if docs, ok := primaries["docs"].(map[string]interface{}); ok {
					if count, ok := docs["count"].(float64); ok {
						indexInfo["document_count"] = int64(count)
						log.Printf("Index %s has %d documents", indexName, int64(count))
					}
				}
			}
		}
	}

	indexInfoJSON, _ := json.Marshal(indexInfo)
	return &pb.DescribeIndexResponse{
		IndexDefinition: string(indexInfoJSON),
	}, nil
}

func (s *Server) PutTemplate(ctx context.Context, req *pb.PutTemplateRequest) (*pb.PutTemplateResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *Server) SetupIlm(ctx context.Context, req *pb.SetupIlmRequest) (*pb.SetupIlmResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// ensureIndexExists checks if an index exists and creates it if it doesn't
func (s *Server) ensureIndexExists(indexName string) error {
	// Create context with timeout - use longer timeout for remote ES servers
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check if index exists
	res, err := s.esClient.Indices.Exists([]string{indexName}, s.esClient.Indices.Exists.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to check if index exists: %w", err)
	}
	defer res.Body.Close()

	// If index exists (status 200), nothing to do
	if res.StatusCode == 200 {
		log.Printf("Index %s already exists", indexName)
		return nil
	}

	// Index doesn't exist (status 404), create it
	if res.StatusCode == 404 {
		log.Printf("Index %s does not exist, creating...", indexName)
		return s.createDefaultIndex(indexName)
	}

	// Unexpected status code
	return fmt.Errorf("unexpected status code when checking index existence: %d", res.StatusCode)
}

// createDefaultIndex creates a new index with default settings and mappings
func (s *Server) createDefaultIndex(indexName string) error {
	indexBody := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 1,
			"refresh_interval": "5s"
		},
		"mappings": {
			"dynamic": true,
			"properties": {
				"timestamp": {
					"type": "date"
				},
				"operation": {
					"type": "keyword"
				},
				"table": {
					"type": "keyword"
				},
				"primary_key": {
					"type": "keyword"
				},
				"data": {
					"type": "object",
					"dynamic": true
				}
			}
		}
	}`

	// Create context with timeout - use longer timeout for remote ES servers
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := s.esClient.Indices.Create(
		indexName,
		s.esClient.Indices.Create.WithBody(strings.NewReader(indexBody)),
		s.esClient.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to create index %s: %s", indexName, string(body))
	}

	log.Printf("Successfully created index: %s", indexName)
	return nil
}

// Ensure Server implements the interface.
var _ pb.SinkServiceServer = (*Server)(nil)
