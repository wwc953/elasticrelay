package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
	"github.com/yogoosoft/elasticrelay/internal/parallel"
)

// PostgreSQLSnapshotAdapter adapts PostgreSQL connector to work with parallel snapshot manager
type PostgreSQLSnapshotAdapter struct {
	config                  *PostgreSQLConfig
	pool                    *ManagedPool
	typeMapper              *AdvancedTypeMapper
	dbConnection            *sql.DB
	preferredConsistencyLSN string
}

// PostgreSQLConfig contains configuration specific to PostgreSQL snapshots
type PostgreSQLConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
	AppName  string
	Timeout  time.Duration
}

// NewPostgreSQLSnapshotAdapter creates a new PostgreSQL snapshot adapter
func NewPostgreSQLSnapshotAdapter(config *PostgreSQLConfig, pool *ManagedPool, typeMapper *AdvancedTypeMapper) *PostgreSQLSnapshotAdapter {
	return &PostgreSQLSnapshotAdapter{
		config:     config,
		pool:       pool,
		typeMapper: typeMapper,
	}
}

// SetPreferredConsistencyLSN allows snapshot callers to reuse a slot-aligned LSN.
func (psa *PostgreSQLSnapshotAdapter) SetPreferredConsistencyLSN(lsn string) {
	psa.preferredConsistencyLSN = lsn
}

// Initialize sets up the adapter for parallel snapshot processing
func (psa *PostgreSQLSnapshotAdapter) Initialize(ctx context.Context) error {
	// Create standard database connection for snapshots
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s application_name=%s connect_timeout=%d",
		psa.config.Host, psa.config.Port, psa.config.User, psa.config.Password,
		psa.config.Database, psa.config.SSLMode, psa.config.AppName, int(psa.config.Timeout.Seconds()))

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	psa.dbConnection = db
	log.Printf("PostgreSQL snapshot adapter initialized")
	return nil
}

// Close cleans up resources
func (psa *PostgreSQLSnapshotAdapter) Close() error {
	if psa.dbConnection != nil {
		return psa.dbConnection.Close()
	}
	return nil
}

// GetTableInfo retrieves information about a table for parallel processing
func (psa *PostgreSQLSnapshotAdapter) GetTableInfo(ctx context.Context, tableName string) (*parallel.TableInfo, error) {
	tableInfo := &parallel.TableInfo{
		Name:     tableName,
		Schema:   "public", // Default schema
		Database: psa.config.Database,
	}

	// Parse schema.table format
	if parts := strings.Split(tableName, "."); len(parts) == 2 {
		tableInfo.Schema = parts[0]
		tableInfo.Name = parts[1]
	}

	// Get table row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", psa.quoteName(tableName))
	var count int64
	if err := psa.dbConnection.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return nil, fmt.Errorf("failed to get table count: %w", err)
	}
	tableInfo.EstimatedRows = count

	// Get primary key columns
	pkQuery := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		WHERE tc.constraint_type = 'PRIMARY KEY' 
		AND tc.table_schema = $1 AND tc.table_name = $2
		ORDER BY kcu.ordinal_position`

	rows, err := psa.dbConnection.QueryContext(ctx, pkQuery, tableInfo.Schema, tableInfo.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		pkColumns = append(pkColumns, colName)
	}
	tableInfo.PrimaryKey = pkColumns

	// Get column information
	colQuery := `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`

	colRows, err := psa.dbConnection.QueryContext(ctx, colQuery, tableInfo.Schema, tableInfo.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get column info: %w", err)
	}
	defer colRows.Close()

	var columns []parallel.ColumnInfo
	for colRows.Next() {
		var colName, dataType, nullable string
		var defaultVal *string

		if err := colRows.Scan(&colName, &dataType, &nullable, &defaultVal); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		column := parallel.ColumnInfo{
			Name:         colName,
			DataType:     dataType,
			IsNullable:   nullable == "YES",
			DefaultValue: defaultVal,
		}
		columns = append(columns, column)
	}
	tableInfo.Columns = columns

	log.Printf("Table info for %s: %d rows, %d columns, PK: %v",
		tableName, count, len(columns), pkColumns)

	return tableInfo, nil
}

// CreateTableChunks creates chunks for parallel processing of a table
func (psa *PostgreSQLSnapshotAdapter) CreateTableChunks(ctx context.Context, tableInfo *parallel.TableInfo, chunkSize int) ([]*parallel.ChunkInfo, error) {
	if tableInfo.EstimatedRows == 0 {
		log.Printf("Table %s is empty, no snapshot chunks created", tableInfo.Name)
		return []*parallel.ChunkInfo{}, nil
	}

	if len(tableInfo.PrimaryKey) == 0 {
		// No primary key - create single chunk
		return []*parallel.ChunkInfo{
			{
				ID:          fmt.Sprintf("%s_chunk_0", tableInfo.Name),
				TableName:   tableInfo.Name,
				StartID:     0,
				EndID:       tableInfo.EstimatedRows,
				ChunkSize:   int(tableInfo.EstimatedRows),
				WhereClause: "",
			},
		}, nil
	}

	// For now, support single-column numeric primary keys
	if len(tableInfo.PrimaryKey) != 1 {
		log.Printf("Warning: multi-column primary key not fully supported, using single chunk")
		return []*parallel.ChunkInfo{
			{
				ID:          fmt.Sprintf("%s_chunk_0", tableInfo.Name),
				TableName:   tableInfo.Name,
				StartID:     0,
				EndID:       tableInfo.EstimatedRows,
				ChunkSize:   int(tableInfo.EstimatedRows),
				WhereClause: "",
			},
		}, nil
	}

	pkColumn := tableInfo.PrimaryKey[0]

	// Get min and max values of primary key
	minMaxQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		psa.quoteName(pkColumn), psa.quoteName(pkColumn), psa.quoteName(tableInfo.Name))

	var minID, maxID int64
	if err := psa.dbConnection.QueryRowContext(ctx, minMaxQuery).Scan(&minID, &maxID); err != nil {
		return nil, fmt.Errorf("failed to get min/max ID: %w", err)
	}

	// Calculate chunk boundaries
	var chunks []*parallel.ChunkInfo
	chunkID := 0

	for startID := minID; startID <= maxID; startID += int64(chunkSize) {
		endID := startID + int64(chunkSize) - 1
		if endID > maxID {
			endID = maxID
		}

		whereClause := fmt.Sprintf("%s >= %d AND %s <= %d",
			psa.quoteName(pkColumn), startID, psa.quoteName(pkColumn), endID)

		chunk := &parallel.ChunkInfo{
			ID:          fmt.Sprintf("%s_chunk_%d", tableInfo.Name, chunkID),
			TableName:   tableInfo.Name,
			StartID:     startID,
			EndID:       endID,
			ChunkSize:   chunkSize,
			WhereClause: whereClause,
		}
		chunks = append(chunks, chunk)
		chunkID++
	}

	log.Printf("Created %d chunks for table %s (ID range: %d-%d, chunk size: %d)",
		len(chunks), tableInfo.Name, minID, maxID, chunkSize)

	return chunks, nil
}

// ProcessChunk processes a single chunk of data
func (psa *PostgreSQLSnapshotAdapter) ProcessChunk(ctx context.Context, chunk *parallel.ChunkInfo, consistencyLSN string, stream pb.ConnectorService_BeginSnapshotServer) error {
	log.Printf("Processing chunk %s", chunk.ID)

	// Build query
	query := fmt.Sprintf("SELECT * FROM %s", psa.quoteName(chunk.TableName))
	if chunk.WhereClause != "" {
		query += " WHERE " + chunk.WhereClause
	}
	query += " ORDER BY " + psa.quoteName("id") // Assuming 'id' is the primary key

	// Execute query
	rows, err := psa.dbConnection.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute chunk query: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %w", err)
	}

	var processedRows int
	var records []string

	for rows.Next() {
		// Prepare scan destinations
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan row
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		dataMap := make(map[string]interface{})
		dataMap["_table"] = chunk.TableName
		dataMap["_schema"] = psa.config.Database

		for i, colName := range columns {
			val := values[i]
			if val == nil {
				dataMap[colName] = nil
			} else {
				// Use type mapper for conversion
				colType := columnTypes[i]
				convertedVal, err := psa.convertColumnValue(val, colType)
				if err != nil {
					log.Printf("Warning: failed to convert column %s: %v", colName, err)
					convertedVal = fmt.Sprintf("%v", val)
				}
				dataMap[colName] = convertedVal
			}
		}

		// Marshal to JSON
		jsonData, err := json.Marshal(dataMap)
		if err != nil {
			log.Printf("Failed to marshal row to JSON: %v", err)
			continue
		}

		records = append(records, string(jsonData))
		processedRows++

		// Send chunk when it reaches the batch size
		if len(records) >= 100 { // Batch size
			if err := psa.sendChunk(stream, records, consistencyLSN); err != nil {
				return fmt.Errorf("failed to send chunk: %w", err)
			}
			records = nil
		}
	}

	// Send remaining records
	if len(records) > 0 {
		if err := psa.sendChunk(stream, records, consistencyLSN); err != nil {
			return fmt.Errorf("failed to send final chunk: %w", err)
		}
	}

	log.Printf("Processed chunk %s: %d rows", chunk.ID, processedRows)
	return nil
}

// sendChunk sends a batch of records to the stream
func (psa *PostgreSQLSnapshotAdapter) sendChunk(stream pb.ConnectorService_BeginSnapshotServer, records []string, consistencyLSN string) error {
	if consistencyLSN == "" {
		consistencyLSN = "0/0"
	}
	chunk := &pb.SnapshotChunk{
		Records:            records,
		Cursor:             consistencyLSN,
		SnapshotBinlogFile: consistencyLSN, // Preserve legacy behavior for PostgreSQL snapshot markers
		SnapshotBinlogPos:  0,
	}

	return stream.Send(chunk)
}

// convertColumnValue converts a database value using the type mapper
func (psa *PostgreSQLSnapshotAdapter) convertColumnValue(value interface{}, colType *sql.ColumnType) (interface{}, error) {
	if psa.typeMapper == nil {
		// Fallback to basic conversion
		return psa.basicTypeConversion(value, colType)
	}

	// Try to get PostgreSQL type OID from column type
	// This is a simplified approach - in practice, you'd need to map
	// column type names to OIDs
	typeName := colType.DatabaseTypeName()
	pgType, err := psa.typeMapper.GetTypeByName(strings.ToLower(typeName))
	if err != nil {
		// Fallback to basic conversion
		return psa.basicTypeConversion(value, colType)
	}

	return psa.typeMapper.ConvertAdvancedValue(value, pgType.OID)
}

// basicTypeConversion provides basic type conversion as fallback
func (psa *PostgreSQLSnapshotAdapter) basicTypeConversion(value interface{}, colType *sql.ColumnType) (interface{}, error) {
	typeName := colType.DatabaseTypeName()

	switch v := value.(type) {
	case []byte:
		log.Printf("Converting column type '%s' from bytes to string", typeName)
		return string(v), nil
	case time.Time:
		log.Printf("Converting column type '%s' from time.Time to RFC3339", typeName)
		return v.Format(time.RFC3339Nano), nil
	default:
		return v, nil
	}
}

// quoteName quotes a database identifier (table/column name)
func (psa *PostgreSQLSnapshotAdapter) quoteName(name string) string {
	// Handle schema.table format
	if parts := strings.Split(name, "."); len(parts) == 2 {
		return fmt.Sprintf(`"%s"."%s"`, parts[0], parts[1])
	}
	return fmt.Sprintf(`"%s"`, name)
}

// GetSnapshotConsistencyPoint returns a consistency point for the snapshot
func (psa *PostgreSQLSnapshotAdapter) GetSnapshotConsistencyPoint(ctx context.Context) (string, error) {
	if psa.preferredConsistencyLSN != "" {
		return psa.preferredConsistencyLSN, nil
	}

	// Start a repeatable read transaction to get a consistent snapshot
	tx, err := psa.dbConnection.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return "", fmt.Errorf("failed to begin snapshot transaction: %w", err)
	}
	defer tx.Rollback()

	// Get the LSN for this snapshot
	var lsn string
	if err := tx.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn); err != nil {
		return "", fmt.Errorf("failed to get snapshot LSN: %w", err)
	}

	return lsn, nil
}

// ParallelSnapshotManager wraps the generic parallel manager for PostgreSQL
type ParallelSnapshotManager struct {
	adapter *PostgreSQLSnapshotAdapter
	config  *parallel.SnapshotConfig
}

// NewParallelSnapshotManager creates a new PostgreSQL parallel snapshot manager
func NewParallelSnapshotManager(adapter *PostgreSQLSnapshotAdapter) *ParallelSnapshotManager {
	config := &parallel.SnapshotConfig{
		MaxConcurrentTables: 3,
		MaxConcurrentChunks: 8,
		ChunkSize:           100000,
		WorkerPoolSize:      12,
		AdaptiveScheduling:  true,
		StreamingMode:       true,
		ChunkStrategy:       "id_based",
	}

	return &ParallelSnapshotManager{
		adapter: adapter,
		config:  config,
	}
}

// StartSnapshot starts a parallel snapshot operation
func (psm *ParallelSnapshotManager) StartSnapshot(ctx context.Context, jobID string, tables []string, stream pb.ConnectorService_BeginSnapshotServer) error {
	log.Printf("Starting parallel snapshot for job %s with %d tables", jobID, len(tables))

	// Initialize adapter
	if err := psm.adapter.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize adapter: %w", err)
	}
	defer psm.adapter.Close()

	// Get snapshot consistency point
	consistencyLSN, err := psm.adapter.GetSnapshotConsistencyPoint(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consistency point: %w", err)
	}

	log.Printf("Snapshot consistency point: LSN %s", consistencyLSN)

	// Process each table
	for _, tableName := range tables {
		if err := psm.processTable(ctx, tableName, consistencyLSN, stream); err != nil {
			log.Printf("Error processing table %s: %v", tableName, err)
			continue
		}
	}

	log.Printf("Completed parallel snapshot for job %s", jobID)
	return nil
}

// processTable processes a single table using parallel chunks
func (psm *ParallelSnapshotManager) processTable(ctx context.Context, tableName string, consistencyLSN string, stream pb.ConnectorService_BeginSnapshotServer) error {
	log.Printf("Processing table %s", tableName)

	// Get table information
	tableInfo, err := psm.adapter.GetTableInfo(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	// Create chunks
	chunks, err := psm.adapter.CreateTableChunks(ctx, tableInfo, psm.config.ChunkSize)
	if err != nil {
		return fmt.Errorf("failed to create chunks: %w", err)
	}

	// Process chunks
	for _, chunk := range chunks {
		if err := psm.adapter.ProcessChunk(ctx, chunk, consistencyLSN, stream); err != nil {
			return fmt.Errorf("failed to process chunk %s: %w", chunk.ID, err)
		}
	}

	log.Printf("Completed processing table %s (%d chunks)", tableName, len(chunks))
	return nil
}

// GetStats returns statistics about the parallel snapshot operation
func (psm *ParallelSnapshotManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"config": psm.config,
		"status": "running", // This would be more sophisticated in a real implementation
	}
}
