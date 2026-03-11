package parallel

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// ParallelSnapshotManager manages parallel snapshot processing
type ParallelSnapshotManager struct {
	config   *SnapshotConfig
	jobID    string
	dbPool   *sql.DB
	esClient ESClient

	// Task queues
	tableQueue chan *TableTask
	chunkQueue chan *ChunkTask

	// Workers
	workers []*SnapshotWorker

	// Progress tracking
	progressMgr *ProgressManager

	// State management
	mutex   sync.RWMutex
	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	// Statistics
	stats      *Statistics
	statsMutex sync.RWMutex
}

// NewParallelSnapshotManager creates a new parallel snapshot manager
func NewParallelSnapshotManager(jobID string, config *SnapshotConfig, dbPool *sql.DB, esClient ESClient) *ParallelSnapshotManager {
	if config == nil {
		config = DefaultSnapshotConfig()
	}

	return &ParallelSnapshotManager{
		config:      config,
		jobID:       jobID,
		dbPool:      dbPool,
		esClient:    esClient,
		progressMgr: NewProgressManager(),
		stats:       &Statistics{},
	}
}

// Start starts the parallel snapshot manager
func (m *ParallelSnapshotManager) Start(ctx context.Context, tables []string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.running {
		return fmt.Errorf("manager already running")
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	m.running = true

	// Initialize task queues
	m.tableQueue = make(chan *TableTask, len(tables))
	m.chunkQueue = make(chan *ChunkTask, m.config.MaxConcurrentChunks*len(tables))

	// Start worker pool
	m.workers = make([]*SnapshotWorker, m.config.WorkerPoolSize)
	for i := 0; i < m.config.WorkerPoolSize; i++ {
		worker := NewSnapshotWorker(i, m)
		m.workers[i] = worker
		go worker.Run(m.ctx)
	}

	// Create table tasks
	for _, tableName := range tables {
		task, err := m.createTableTask(tableName)
		if err != nil {
			log.Printf("failed to create table task for %s: %v", tableName, err)
			continue
		}

		select {
		case m.tableQueue <- task:
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}

	// Start task processor
	go m.processTableTasks()

	log.Printf("ParallelSnapshotManager started with %d workers for %d tables",
		m.config.WorkerPoolSize, len(tables))

	return nil
}

// Stop stops the parallel snapshot manager
func (m *ParallelSnapshotManager) Stop() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.running {
		return nil
	}

	if m.cancel != nil {
		m.cancel()
	}

	m.running = false
	log.Printf("ParallelSnapshotManager stopped")

	return nil
}

// processTableTasks processes table tasks and creates chunks
func (m *ParallelSnapshotManager) processTableTasks() {
	defer func() {
		close(m.chunkQueue)
		log.Printf("Table task processor stopped")
	}()

	for {
		select {
		case task := <-m.tableQueue:
			if err := m.processTableTask(task); err != nil {
				log.Printf("Failed to process table task %s: %v", task.TableName, err)
				task.Status = TaskStatusFailed
				task.ErrorMsg = err.Error()
			}

		case <-m.ctx.Done():
			return
		}
	}
}

// processTableTask processes a single table task
func (m *ParallelSnapshotManager) processTableTask(task *TableTask) error {
	log.Printf("Processing table task: %s (rows: %d)", task.TableName, task.TotalRows)

	// Update task status
	task.Status = TaskStatusRunning
	now := time.Now()
	task.StartedAt = &now

	// Create chunks for the table
	chunks, err := m.createTableChunks(task)
	if err != nil {
		return fmt.Errorf("failed to create chunks: %w", err)
	}

	// Add chunks to the queue
	for _, chunk := range chunks {
		select {
		case m.chunkQueue <- chunk:
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}

	log.Printf("Created %d chunks for table %s", len(chunks), task.TableName)
	return nil
}

// createTableTask creates a new table task
func (m *ParallelSnapshotManager) createTableTask(tableName string) (*TableTask, error) {
	// Estimate total rows
	totalRows, err := m.estimateTableRows(tableName)
	if err != nil {
		log.Printf("Warning: failed to estimate rows for table %s: %v", tableName, err)
		totalRows = 0 // Will be calculated during processing
	}

	// Set priority (small tables first)
	priority := m.calculatePriority(totalRows)

	task := &TableTask{
		JobID:     m.jobID,
		TableName: tableName,
		TotalRows: totalRows,
		ChunkSize: m.optimizeChunkSize(totalRows),
		Priority:  priority,
		Status:    TaskStatusPending,
		CreatedAt: time.Now(),
	}

	return task, nil
}

// createTableChunks creates chunks for a table
func (m *ParallelSnapshotManager) createTableChunks(task *TableTask) ([]*ChunkTask, error) {
	// Analyze table indexes
	indexInfo, err := m.analyzeTableIndexes(task.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze indexes: %w", err)
	}

	if indexInfo.PrimaryKeyColumn == "" {
		return nil, fmt.Errorf("table %s has no primary key", task.TableName)
	}

	task.PrimaryKeyColumn = indexInfo.PrimaryKeyColumn

	var chunks []*ChunkTask

	// Use ID-based chunking strategy
	if indexInfo.HasAutoIncrementPK {
		chunks, err = m.createIDBasedChunks(task, indexInfo)
	} else {
		chunks = m.createFullTableChunk(task)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create chunks: %w", err)
	}

	return chunks, nil
}

func (m *ParallelSnapshotManager) createFullTableChunk(task *TableTask) []*ChunkTask {
	return []*ChunkTask{
		{
			ID:               fmt.Sprintf("%s_chunk_0", task.TableName),
			TableTask:        task,
			ChunkID:          0,
			UseFullTableScan: true,
			Status:           ChunkStatusPending,
		},
	}
}

// createIDBasedChunks creates chunks based on primary key ID ranges
func (m *ParallelSnapshotManager) createIDBasedChunks(task *TableTask, indexInfo *IndexInfo) ([]*ChunkTask, error) {
	// Get ID range
	minID, maxID, err := m.getIDRange(task.TableName, indexInfo.PrimaryKeyColumn)
	if err != nil {
		return nil, err
	}

	if minID == maxID {
		// Empty table or single row
		return []*ChunkTask{
			{
				ID:        fmt.Sprintf("%s_chunk_0", task.TableName),
				TableTask: task,
				ChunkID:   0,
				StartID:   minID,
				EndID:     maxID + 1,
				Status:    ChunkStatusPending,
			},
		}, nil
	}

	// Calculate chunk count
	totalRange := maxID - minID + 1
	chunkCount := int(totalRange / int64(task.ChunkSize))
	if chunkCount == 0 {
		chunkCount = 1
	}

	// Limit maximum chunks
	if chunkCount > m.config.MaxConcurrentChunks {
		chunkCount = m.config.MaxConcurrentChunks
	}

	var chunks []*ChunkTask
	step := totalRange / int64(chunkCount)

	for i := 0; i < chunkCount; i++ {
		startID := minID + int64(i)*step
		endID := minID + int64(i+1)*step

		// Last chunk includes remaining data
		if i == chunkCount-1 {
			endID = maxID + 1
		}

		chunk := &ChunkTask{
			ID:        fmt.Sprintf("%s_chunk_%d", task.TableName, i),
			TableTask: task,
			ChunkID:   i,
			StartID:   startID,
			EndID:     endID,
			Status:    ChunkStatusPending,
		}

		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// analyzeTableIndexes analyzes table indexes
func (m *ParallelSnapshotManager) analyzeTableIndexes(tableName string) (*IndexInfo, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			IS_NULLABLE,
			COLUMN_KEY,
			EXTRA
		FROM information_schema.COLUMNS 
		WHERE TABLE_SCHEMA = DATABASE() 
		  AND TABLE_NAME = ?
		  AND COLUMN_KEY = 'PRI'
		ORDER BY ORDINAL_POSITION
	`

	rows, err := m.dbPool.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table info: %w", err)
	}
	defer rows.Close()

	indexInfo := &IndexInfo{}

	for rows.Next() {
		var columnName, isNullable, columnKey, extra string
		err := rows.Scan(&columnName, &isNullable, &columnKey, &extra)
		if err != nil {
			continue
		}

		if columnKey == "PRI" {
			indexInfo.PrimaryKeyColumn = columnName
			if extra == "auto_increment" {
				indexInfo.HasAutoIncrementPK = true
			}
			break
		}
	}

	return indexInfo, nil
}

// estimateTableRows estimates the number of rows in a table
func (m *ParallelSnapshotManager) estimateTableRows(tableName string) (int64, error) {
	query := `
		SELECT TABLE_ROWS 
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA = DATABASE() 
		  AND TABLE_NAME = ?
	`

	var rows sql.NullInt64
	err := m.dbPool.QueryRow(query, tableName).Scan(&rows)
	if err != nil {
		// Fallback to COUNT(*) for accurate count (slower)
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
		var count int64
		err := m.dbPool.QueryRow(countQuery).Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to count rows: %w", err)
		}
		return count, nil
	}

	if !rows.Valid {
		return 0, nil
	}

	return rows.Int64, nil
}

// getIDRange gets the min and max ID from a table
func (m *ParallelSnapshotManager) getIDRange(tableName, pkColumn string) (int64, int64, error) {
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", pkColumn, pkColumn, tableName)

	var minID, maxID sql.NullInt64
	err := m.dbPool.QueryRow(query).Scan(&minID, &maxID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get ID range: %w", err)
	}

	if !minID.Valid || !maxID.Valid {
		return 0, 0, nil // Empty table
	}

	return minID.Int64, maxID.Int64, nil
}

// calculatePriority calculates task priority based on table size
func (m *ParallelSnapshotManager) calculatePriority(totalRows int64) int {
	// Smaller tables get higher priority (processed first)
	if totalRows < 10000 {
		return 100
	} else if totalRows < 100000 {
		return 80
	} else if totalRows < 1000000 {
		return 60
	} else if totalRows < 10000000 {
		return 40
	} else {
		return 20
	}
}

// optimizeChunkSize optimizes chunk size based on table size
func (m *ParallelSnapshotManager) optimizeChunkSize(totalRows int64) int {
	baseChunkSize := m.config.ChunkSize

	// Adjust chunk size based on table size
	if totalRows < 10000 {
		return min(baseChunkSize, 1000)
	} else if totalRows < 100000 {
		return min(baseChunkSize, 5000)
	} else if totalRows < 1000000 {
		return min(baseChunkSize, 10000)
	} else {
		return baseChunkSize
	}
}

// GetStatistics returns current processing statistics
func (m *ParallelSnapshotManager) GetStatistics() *Statistics {
	m.statsMutex.RLock()
	defer m.statsMutex.RUnlock()

	// Make a copy to avoid race conditions
	stats := *m.stats
	return &stats
}

// updateStatistics updates processing statistics
func (m *ParallelSnapshotManager) updateStatistics() {
	// This method will be called by workers to update statistics
	// Implementation details will be added when we implement the progress manager
}

// Utility functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
