package parallel

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"
	"unicode/utf8"
)

// SnapshotWorker processes chunks of data
type SnapshotWorker struct {
	ID          int
	manager     *ParallelSnapshotManager
	dbPool      *sql.DB
	esClient    ESClient
	transformer *DataTransformer

	// Statistics
	processedChunks int64
	processedRows   int64
	errorCount      int64
	startTime       time.Time
}

// NewSnapshotWorker creates a new snapshot worker
func NewSnapshotWorker(id int, manager *ParallelSnapshotManager) *SnapshotWorker {
	return &SnapshotWorker{
		ID:          id,
		manager:     manager,
		dbPool:      manager.dbPool,
		esClient:    manager.esClient,
		transformer: NewDataTransformer(),
		startTime:   time.Now(),
	}
}

// Run starts the worker main loop
func (w *SnapshotWorker) Run(ctx context.Context) {
	log.Printf("SnapshotWorker #%d started", w.ID)
	defer log.Printf("SnapshotWorker #%d stopped", w.ID)

	for {
		select {
		case chunk := <-w.manager.chunkQueue:
			if chunk == nil {
				return // Channel closed
			}
			w.processChunk(ctx, chunk)

		case <-ctx.Done():
			return
		}
	}
}

// processChunk processes a single chunk
func (w *SnapshotWorker) processChunk(ctx context.Context, chunk *ChunkTask) {
	startTime := time.Now()

	// Update chunk status
	w.updateChunkStatus(chunk, ChunkStatusRunning)

	if chunk.UseFullTableScan {
		log.Printf("Worker #%d processing chunk %s (full table scan)", w.ID, chunk.ID)
	} else {
		log.Printf("Worker #%d processing chunk %s (PK range: %d-%d)",
			w.ID, chunk.ID, chunk.StartID, chunk.EndID)
	}

	err := w.doProcessChunk(ctx, chunk)

	if err != nil {
		w.handleChunkError(chunk, err)
	} else {
		// Processing successful
		now := time.Now()
		chunk.CompletedAt = &now
		chunk.Status = ChunkStatusCompleted
		w.processedChunks++

		// Update progress
		w.manager.progressMgr.UpdateChunkProgress(chunk)

		log.Printf("Worker #%d completed chunk %s in %v (%d rows)",
			w.ID, chunk.ID, time.Since(startTime), chunk.ProcessedRows)
	}
}

// doProcessChunk actually processes the chunk data
func (w *SnapshotWorker) doProcessChunk(ctx context.Context, chunk *ChunkTask) error {
	// Build query SQL
	query := w.buildChunkQuery(chunk)

	// Execute query
	rows, err := w.dbPool.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns failed: %w", err)
	}

	// Process data in streaming fashion
	batch := make([]*Record, 0, 1000)
	processedRows := int64(0)

	for rows.Next() {
		// Scan row data
		record, err := w.scanRow(rows, columns, chunk.TableTask.TableName, chunk.TableTask.PrimaryKeyColumn)
		if err != nil {
			log.Printf("scan row error: %v", err)
			continue
		}

		batch = append(batch, record)
		processedRows++

		// Process when batch is full
		if len(batch) >= 1000 {
			if err := w.processBatch(ctx, batch, chunk); err != nil {
				return fmt.Errorf("process batch failed: %w", err)
			}
			batch = batch[:0] // Reset batch
		}

		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	// Process remaining data
	if len(batch) > 0 {
		if err := w.processBatch(ctx, batch, chunk); err != nil {
			return fmt.Errorf("process final batch failed: %w", err)
		}
	}

	chunk.ProcessedRows = processedRows
	w.processedRows += processedRows

	return rows.Err()
}

// buildChunkQuery builds the SQL query for a chunk
func (w *SnapshotWorker) buildChunkQuery(chunk *ChunkTask) string {
	table := chunk.TableTask.TableName
	primaryKeyColumn := chunk.TableTask.PrimaryKeyColumn

	if chunk.UseFullTableScan {
		if primaryKeyColumn == "" {
			return fmt.Sprintf("SELECT * FROM %s", table)
		}
		return fmt.Sprintf(`
		SELECT * FROM %s
		ORDER BY %s
	`, table, primaryKeyColumn)
	}

	// Use primary key range-based query with index optimization
	return fmt.Sprintf(`
		SELECT * FROM %s 
		WHERE %s >= %d AND %s < %d 
		ORDER BY %s
	`, table, primaryKeyColumn, chunk.StartID, primaryKeyColumn, chunk.EndID, primaryKeyColumn)
}

// scanRow scans a database row into a Record
func (w *SnapshotWorker) scanRow(rows *sql.Rows, columns []string, tableName string, primaryKeyColumn string) (*Record, error) {
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	// Build record data
	data := make(map[string]interface{})
	data["_table"] = tableName
	data["_timestamp"] = time.Now().UnixNano()

	for i, colName := range columns {
		val := values[i]
		if val == nil {
			data[colName] = nil
		} else {
			data[colName] = w.convertValue(val)
		}
	}

	primaryKey, err := extractRecordPrimaryKey(data, primaryKeyColumn)
	if err != nil {
		return nil, err
	}

	return &Record{
		ID:        primaryKey,
		TableName: tableName,
		Data:      data,
		Timestamp: time.Now(),
	}, nil
}

func extractRecordPrimaryKey(data map[string]interface{}, primaryKeyColumn string) (string, error) {
	if primaryKeyColumn == "" {
		return "", fmt.Errorf("primary key column is not configured")
	}

	value, exists := data[primaryKeyColumn]
	if !exists {
		return "", fmt.Errorf("primary key column %s not found in row data", primaryKeyColumn)
	}

	return fmt.Sprintf("%v", value), nil
}

// convertValue converts database values to appropriate types
func (w *SnapshotWorker) convertValue(val interface{}) interface{} {
	switch v := val.(type) {
	case []byte:
		// Handle UTF-8 encoded byte data properly
		s := string(v)
		// Try to parse as numbers first
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i
		} else if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		} else if b, err := strconv.ParseBool(s); err == nil {
			return b
		} else {
			// Ensure string is properly UTF-8 encoded
			if !utf8.Valid(v) {
				log.Printf("Invalid UTF-8 data, attempting to fix")
				// Try to fix encoding issues
				s = string([]rune(string(v)))
			}
			return s
		}
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return v
	}
}

// processBatch processes a batch of records
func (w *SnapshotWorker) processBatch(ctx context.Context, batch []*Record, chunk *ChunkTask) error {
	// Transform data
	transformedBatch, err := w.transformer.TransformBatch(batch)
	if err != nil {
		return fmt.Errorf("transform batch failed: %w", err)
	}

	// Write to ES
	indexName := fmt.Sprintf("elasticrelay-%s", chunk.TableTask.TableName)
	err = w.esClient.BulkIndex(indexName, transformedBatch)
	if err != nil {
		return fmt.Errorf("bulk index failed: %w", err)
	}

	return nil
}

// handleChunkError handles chunk processing errors
func (w *SnapshotWorker) handleChunkError(chunk *ChunkTask, err error) {
	w.errorCount++
	chunk.ErrorMsg = err.Error()

	// Check if we can retry
	if chunk.RetryCount < 3 {
		chunk.RetryCount++
		chunk.Status = ChunkStatusRetrying

		// Delay retry with exponential backoff
		go func() {
			delay := time.Duration(chunk.RetryCount) * time.Second * 5
			time.Sleep(delay)

			select {
			case w.manager.chunkQueue <- chunk:
			default:
				log.Printf("Failed to requeue chunk %s for retry", chunk.ID)
			}
		}()

		log.Printf("Worker #%d retrying chunk %s (attempt %d): %v",
			w.ID, chunk.ID, chunk.RetryCount, err)
	} else {
		// Retry exhausted, mark as failed
		chunk.Status = ChunkStatusFailed
		log.Printf("Worker #%d failed chunk %s after %d retries: %v",
			w.ID, chunk.ID, chunk.RetryCount, err)

		// Report to monitoring system (when implemented)
		// w.manager.monitor.ReportChunkFailure(chunk, err)
	}
}

// updateChunkStatus updates the status of a chunk
func (w *SnapshotWorker) updateChunkStatus(chunk *ChunkTask, status ChunkStatus) {
	chunk.Status = status
	chunk.WorkerID = w.ID

	now := time.Now()
	if status == ChunkStatusRunning {
		chunk.StartedAt = &now
	} else if status == ChunkStatusCompleted {
		chunk.CompletedAt = &now
	}
}

// GetStats returns worker statistics
func (w *SnapshotWorker) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"worker_id":        w.ID,
		"processed_chunks": w.processedChunks,
		"processed_rows":   w.processedRows,
		"error_count":      w.errorCount,
		"uptime":           time.Since(w.startTime).Seconds(),
		"chunks_per_sec":   float64(w.processedChunks) / time.Since(w.startTime).Seconds(),
		"rows_per_sec":     float64(w.processedRows) / time.Since(w.startTime).Seconds(),
	}
}
