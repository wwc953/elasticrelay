package parallel

import (
	"time"
)

// TaskStatus represents the status of a table task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// ChunkStatus represents the status of a chunk task
type ChunkStatus string

const (
	ChunkStatusPending   ChunkStatus = "pending"
	ChunkStatusRunning   ChunkStatus = "running"
	ChunkStatusCompleted ChunkStatus = "completed"
	ChunkStatusFailed    ChunkStatus = "failed"
	ChunkStatusRetrying  ChunkStatus = "retrying"
)

// SnapshotConfig contains configuration for parallel snapshot processing
type SnapshotConfig struct {
	MaxConcurrentTables int    `json:"max_concurrent_tables"` // Maximum concurrent tables: 3
	MaxConcurrentChunks int    `json:"max_concurrent_chunks"` // Maximum concurrent chunks per table: 8
	ChunkSize           int    `json:"chunk_size"`            // Chunk size: 100000
	WorkerPoolSize      int    `json:"worker_pool_size"`      // Worker pool size: 12
	AdaptiveScheduling  bool   `json:"adaptive_scheduling"`   // Adaptive scheduling: true
	StreamingMode       bool   `json:"streaming_mode"`        // Streaming mode: true
	ChunkStrategy       string `json:"chunk_strategy"`        // Chunk strategy: id_based, time_based, hash_based
}

// DefaultSnapshotConfig returns default configuration for snapshot processing
func DefaultSnapshotConfig() *SnapshotConfig {
	return &SnapshotConfig{
		MaxConcurrentTables: 3,
		MaxConcurrentChunks: 8,
		ChunkSize:           100000,
		WorkerPoolSize:      12,
		AdaptiveScheduling:  true,
		StreamingMode:       true,
		ChunkStrategy:       "id_based",
	}
}

// TableTask represents a table synchronization task
type TableTask struct {
	JobID            string     `json:"job_id"`
	TableName        string     `json:"table_name"`
	PrimaryKeyColumn string     `json:"primary_key_column,omitempty"`
	TotalRows        int64      `json:"total_rows"`
	ChunkSize        int        `json:"chunk_size"`
	Priority         int        `json:"priority"`     // Priority (lower for large tables)
	Dependencies     []string   `json:"dependencies"` // Dependent tables (foreign key relationships)
	Status           TaskStatus `json:"status"`
	CreatedAt        time.Time  `json:"created_at"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	CompletedAt      *time.Time `json:"completed_at,omitempty"`
	ErrorMsg         string     `json:"error_msg,omitempty"`
}

// ChunkTask represents a chunk of data to be processed
type ChunkTask struct {
	ID               string      `json:"id"` // Unique identifier
	TableTask        *TableTask  `json:"table_task"`
	ChunkID          int         `json:"chunk_id"`
	StartID          int64       `json:"start_id"` // Starting primary key ID
	EndID            int64       `json:"end_id"`   // Ending primary key ID
	UseFullTableScan bool        `json:"use_full_table_scan,omitempty"`
	RetryCount       int         `json:"retry_count"` // Retry count
	Status           ChunkStatus `json:"status"`
	WorkerID         int         `json:"worker_id,omitempty"`
	StartedAt        *time.Time  `json:"started_at,omitempty"`
	CompletedAt      *time.Time  `json:"completed_at,omitempty"`
	ProcessedRows    int64       `json:"processed_rows"`
	ErrorMsg         string      `json:"error_msg,omitempty"`
}

// IndexInfo contains information about table indexes
type IndexInfo struct {
	HasAutoIncrementPK bool   `json:"has_auto_increment_pk"`
	HasTimestampIndex  bool   `json:"has_timestamp_index"`
	HasUniqueIndex     bool   `json:"has_unique_index"`
	PrimaryKeyColumn   string `json:"primary_key_column"`
	TimestampColumn    string `json:"timestamp_column,omitempty"`
}

// Record represents a data record
type Record struct {
	ID        string                 `json:"id"`
	TableName string                 `json:"table_name"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

// GetPrimaryKeyID extracts primary key ID from record data
func (r *Record) GetPrimaryKeyID() int64 {
	if id, exists := r.Data["id"]; exists {
		switch v := id.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		}
	}
	return 0
}

// ESClient interface for Elasticsearch operations
type ESClient interface {
	BulkIndex(indexName string, documents []*ESDocument) error
}

// ESDocument represents a document for Elasticsearch
type ESDocument struct {
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}

// DataTransformer handles data transformation
type DataTransformer struct {
	// Add transformation rules here
}

// NewDataTransformer creates a new data transformer
func NewDataTransformer() *DataTransformer {
	return &DataTransformer{}
}

// TransformBatch transforms a batch of records
func (dt *DataTransformer) TransformBatch(records []*Record) ([]*ESDocument, error) {
	documents := make([]*ESDocument, 0, len(records))

	for _, record := range records {
		doc := &ESDocument{
			ID:     record.ID,
			Source: record.Data,
		}
		documents = append(documents, doc)
	}

	return documents, nil
}

// TableInfo contains information about a table for parallel processing
type TableInfo struct {
	Name          string       `json:"name"`
	Schema        string       `json:"schema"`
	Database      string       `json:"database"`
	TotalRows     int64        `json:"total_rows"`
	EstimatedRows int64        `json:"estimated_rows"`
	PrimaryKey    []string     `json:"primary_key"`
	Columns       []ColumnInfo `json:"columns"`
	HasTimestamp  bool         `json:"has_timestamp"`
}

// ColumnInfo contains information about a table column
type ColumnInfo struct {
	Name         string      `json:"name"`
	DataType     string      `json:"data_type"`
	IsNullable   bool        `json:"is_nullable"`
	IsPrimaryKey bool        `json:"is_primary_key"`
	DefaultValue interface{} `json:"default_value,omitempty"`
}

// ChunkInfo contains information about a data chunk for parallel processing
type ChunkInfo struct {
	ID          string `json:"id"`
	TableName   string `json:"table_name"`
	WhereClause string `json:"where_clause"`
	StartID     int64  `json:"start_id"`
	EndID       int64  `json:"end_id"`
	RowCount    int64  `json:"row_count"`
	ChunkSize   int    `json:"chunk_size"`
}

// Statistics represents parallel snapshot statistics
type Statistics struct {
	TablesTotal      int64 `json:"tables_total"`
	TablesCompleted  int64 `json:"tables_completed"`
	TablesInProgress int64 `json:"tables_in_progress"`
	TablesFailed     int64 `json:"tables_failed"`
	TotalChunks      int64 `json:"total_chunks"`
	CompletedChunks  int64 `json:"completed_chunks"`
	FailedChunks     int64 `json:"failed_chunks"`
	ActiveWorkers    int   `json:"active_workers"`
	QueuedChunks     int   `json:"queued_chunks"`
	RemainingRows    int64 `json:"remaining_rows"`
	TotalRetries     int64 `json:"total_retries"`
}
