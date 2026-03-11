package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
	"github.com/yogoosoft/elasticrelay/internal/config"
)

const (
	snapshotChunkSize  = 100
	checkpointFilePath = "postgresql_checkpoints.json"
)

// ReplicationSlot represents a PostgreSQL replication slot
type ReplicationSlot struct {
	SlotName       string
	Plugin         string
	SlotType       string
	Database       string
	Temporary      bool
	Active         bool
	RestartLSN     string
	ConfirmedFlush string
}

// LSNPosition represents a PostgreSQL Log Sequence Number position
type LSNPosition struct {
	LSN        string
	Timeline   uint32
	XLogOffset uint64
}

// EventHandler handles logical replication events
type EventHandler struct {
	stream       pb.ConnectorService_StartCdcServer
	tableFilters []string
	currentLSN   string
}

// Connector manages the PostgreSQL CDC process
type Connector struct {
	config       *pgxpool.Config
	pool         *pgxpool.Pool
	tableFilters []string
	slotName     string
	publication  string
	dbName       string
	jobID        string
}

// NewConnector creates a new PostgreSQL connector
func NewConnector(cfg *config.Config) (*Connector, error) {
	// Build connection string
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure connection pool
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Minute * 30

	return &Connector{
		config:       poolConfig,
		tableFilters: cfg.TableFilters,
		slotName:     fmt.Sprintf("elasticrelay_slot_%d", time.Now().Unix()),
		publication:  "elasticrelay_publication",
		dbName:       cfg.DBName,
	}, nil
}

// SetJobID switches the connector to a stable job-specific replication slot.
func (c *Connector) SetJobID(jobID string) {
	c.jobID = jobID
	if jobID == "" {
		return
	}
	c.slotName = buildJobSlotName(jobID)
}

func buildJobSlotName(jobID string) string {
	const maxSlotNameLen = 63
	const prefix = "elasticrelay_slot_"

	sanitized := strings.ToLower(jobID)
	var b strings.Builder
	for _, r := range sanitized {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}

	name := prefix + strings.Trim(b.String(), "_")
	if name == prefix {
		name = fmt.Sprintf("%s%d", prefix, time.Now().Unix())
	}
	if len(name) > maxSlotNameLen {
		name = name[:maxSlotNameLen]
	}
	return strings.TrimRight(name, "_")
}

// Start runs the CDC process
func (c *Connector) Start(stream pb.ConnectorService_StartCdcServer, startCheckpoint *pb.Checkpoint, jobID string) error {
	var err error

	if jobID != "" {
		c.SetJobID(jobID)
	}

	// Initialize connection pool
	c.pool, err = pgxpool.NewWithConfig(stream.Context(), c.config)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}
	defer c.pool.Close()

	// Test connection
	if err := c.pool.Ping(stream.Context()); err != nil {
		return fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	log.Printf("Connected to PostgreSQL database %s", c.dbName)

	// Get starting LSN position
	var startLSN string
	if startCheckpoint != nil && startCheckpoint.PostgresLsn != "" {
		startLSN = startCheckpoint.PostgresLsn
		log.Printf("Starting CDC from checkpoint LSN: %s", startLSN)
	}

	// Create publication if it doesn't exist
	if err := c.createPublication(stream.Context()); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	// Create replication slot if it doesn't exist. For snapshot-driven jobs this
	// should already exist, and we must reuse the same slot instead of creating a
	// new time-based one during CDC handoff.
	slotLSN, err := c.createReplicationSlot(stream.Context())
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}
	if startLSN == "" {
		startLSN = slotLSN
		log.Printf("No checkpoint provided, starting from replication slot LSN: %s", startLSN)
	}

	// Start logical replication
	handler := &EventHandler{
		stream:       stream,
		tableFilters: c.tableFilters,
		currentLSN:   startLSN,
	}

	return c.startLogicalReplication(stream.Context(), handler, startLSN)
}

// getCurrentLSN gets the current LSN position
func (c *Connector) getCurrentLSN(ctx context.Context) (string, error) {
	var lsn string
	err := c.pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to get current LSN: %w", err)
	}
	return lsn, nil
}

// ResetReplicationSlot drops an inactive job-specific replication slot so force_initial_sync
// can rebuild CDC state from a clean position.
func (c *Connector) ResetReplicationSlot(ctx context.Context, jobID string) error {
	if jobID != "" {
		c.SetJobID(jobID)
	}

	pool, err := pgxpool.NewWithConfig(ctx, c.config)
	if err != nil {
		return fmt.Errorf("failed to create connection pool for slot reset: %w", err)
	}
	defer pool.Close()

	var exists bool
	var active bool
	err = pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1), COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = $1), false)",
		c.slotName).Scan(&exists, &active)
	if err != nil {
		return fmt.Errorf("failed to inspect replication slot %s: %w", c.slotName, err)
	}

	if !exists {
		log.Printf("Replication slot %s does not exist, no reset needed", c.slotName)
		return nil
	}
	if active {
		return fmt.Errorf("replication slot %s is active and cannot be reset", c.slotName)
	}

	if _, err := pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", c.slotName); err != nil {
		return fmt.Errorf("failed to drop replication slot %s: %w", c.slotName, err)
	}

	log.Printf("Dropped replication slot %s due to force_initial_sync", c.slotName)
	return nil
}

// createReplicationSlot creates or reuses a logical replication slot and returns its start LSN.
func (c *Connector) createReplicationSlot(ctx context.Context) (string, error) {
	// First, clean up any old inactive slots with similar names to prevent accumulation
	err := c.cleanupOldReplicationSlots(ctx)
	if err != nil {
		log.Printf("Warning: failed to cleanup old replication slots: %v", err)
	}

	// Check if slot already exists
	var exists bool
	var active bool
	err = c.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1), COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = $1), false)",
		c.slotName).Scan(&exists, &active)
	if err != nil {
		return "", fmt.Errorf("failed to check replication slot existence: %w", err)
	}

	if !exists {
		var lsn string
		err = c.pool.QueryRow(ctx,
			"SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
			c.slotName).Scan(&lsn)
		if err != nil {
			return "", fmt.Errorf("failed to create replication slot: %w", err)
		}
		log.Printf("Created replication slot: %s at LSN %s", c.slotName, lsn)
		return lsn, nil
	} else {
		if active {
			return "", fmt.Errorf("replication slot %s is already active", c.slotName)
		}

		lsn, err := c.getReplicationSlotResumeLSN(ctx)
		if err != nil {
			return "", err
		}
		log.Printf("Replication slot %s already exists (inactive), resuming from LSN %s", c.slotName, lsn)
		return lsn, nil
	}
}

func (c *Connector) getReplicationSlotResumeLSN(ctx context.Context) (string, error) {
	var lsn string
	err := c.pool.QueryRow(ctx,
		`SELECT COALESCE(confirmed_flush_lsn::text, restart_lsn::text, '') 
		 FROM pg_replication_slots WHERE slot_name = $1`,
		c.slotName).Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to load replication slot LSN for %s: %w", c.slotName, err)
	}
	if lsn == "" {
		return "", fmt.Errorf("replication slot %s does not expose a valid resume LSN yet", c.slotName)
	}
	return lsn, nil
}

// cleanupOldReplicationSlots removes old inactive replication slots to prevent accumulation
func (c *Connector) cleanupOldReplicationSlots(ctx context.Context) error {
	// Find old slots with elasticrelay prefix that are not active
	rows, err := c.pool.Query(ctx,
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'elasticrelay_slot_%' AND NOT active AND slot_name != $1",
		c.slotName)
	if err != nil {
		return fmt.Errorf("failed to query old replication slots: %w", err)
	}
	defer rows.Close()

	var slotsToDelete []string
	for rows.Next() {
		var slotName string
		if err := rows.Scan(&slotName); err != nil {
			continue
		}
		slotsToDelete = append(slotsToDelete, slotName)
	}

	// Delete old slots
	for _, slotName := range slotsToDelete {
		_, err := c.pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
		if err != nil {
			log.Printf("Warning: failed to drop old replication slot %s: %v", slotName, err)
		} else {
			log.Printf("Cleaned up old replication slot: %s", slotName)
		}
	}

	return nil
}

// createPublication creates a publication for logical replication
func (c *Connector) createPublication(ctx context.Context) error {
	// Check if publication already exists
	var exists bool
	err := c.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		c.publication).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	if !exists {
		// Create publication for all tables or filtered tables
		var createSQL string
		if len(c.tableFilters) > 0 {
			createSQL = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
				c.publication, c.buildTableList())
		} else {
			createSQL = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", c.publication)
		}

		_, err = c.pool.Exec(ctx, createSQL)
		if err != nil {
			return fmt.Errorf("failed to create publication: %w", err)
		}
		log.Printf("Created publication: %s", c.publication)
	} else {
		log.Printf("Publication already exists: %s", c.publication)

		// Publication exists, verify and ensure configured tables are included
		if len(c.tableFilters) > 0 {
			// Get current tables in publication
			currentTables, err := c.getPublicationTables(ctx)
			if err != nil {
				return fmt.Errorf("failed to get publication tables: %w", err)
			}

			log.Printf("Publication %s currently contains %d tables: %v", c.publication, len(currentTables), currentTables)

			// Build map of current tables for quick lookup
			currentTableMap := make(map[string]bool)
			for _, table := range currentTables {
				currentTableMap[table] = true
			}

			// Find tables that need to be added
			var tablesToAdd []string
			for _, table := range c.tableFilters {
				// Check both with and without schema prefix
				found := currentTableMap[table] || currentTableMap["public."+table]
				if !found {
					tablesToAdd = append(tablesToAdd, table)
				}
			}

			// Add missing tables to publication
			if len(tablesToAdd) > 0 {
				log.Printf("Adding missing tables to publication %s: %v", c.publication, tablesToAdd)
				tableList := ""
				for i, table := range tablesToAdd {
					if i > 0 {
						tableList += ", "
					}
					tableList += table
				}

				alterSQL := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", c.publication, tableList)
				_, err = c.pool.Exec(ctx, alterSQL)
				if err != nil {
					return fmt.Errorf("failed to add tables to publication %s: %w", c.publication, err)
				}
				log.Printf("Successfully added %d tables to publication %s", len(tablesToAdd), c.publication)
			} else {
				log.Printf("All configured tables are already in publication %s", c.publication)
			}
		}
	}

	return nil
}

// buildTableList builds a comma-separated list of tables for the publication
func (c *Connector) buildTableList() string {
	if len(c.tableFilters) == 0 {
		return ""
	}

	tableList := ""
	for i, table := range c.tableFilters {
		if i > 0 {
			tableList += ", "
		}
		tableList += table
	}
	return tableList
}

// getPublicationTables returns the list of tables in the publication
func (c *Connector) getPublicationTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT schemaname || '.' || tablename as full_table_name
		FROM pg_publication_tables 
		WHERE pubname = $1
		ORDER BY schemaname, tablename`

	rows, err := c.pool.Query(ctx, query, c.publication)
	if err != nil {
		return nil, fmt.Errorf("failed to query publication tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating publication tables: %w", err)
	}

	return tables, nil
}

// startLogicalReplication starts the logical replication process
func (c *Connector) startLogicalReplication(ctx context.Context, handler *EventHandler, startLSN string) error {
	log.Printf("Starting logical replication from LSN: %s", startLSN)

	// Create replication connection manager
	replMgr := NewReplicationConnectionManager(c.buildLegacyConfig())

	// Test replication connection first
	if err := replMgr.TestReplicationConnection(ctx); err != nil {
		return fmt.Errorf("failed to test replication connection: %w", err)
	}

	// Verify replication slot is ready before creating connection
	err := c.verifyReplicationSlotReady(ctx)
	if err != nil {
		return fmt.Errorf("replication slot not ready: %w", err)
	}

	// Create replication connection
	replConn, err := replMgr.CreateReplicationConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %w", err)
	}
	defer replConn.Close(ctx)

	// Add a small delay to ensure the replication connection is fully established
	time.Sleep(100 * time.Millisecond)

	// Create WAL parser
	walParser := NewWALParser(replConn, c.slotName, c.publication, startLSN, c.tableFilters)

	// Preload table schema information to avoid "unknown relation ID" errors
	if err := c.preloadTableSchemas(ctx, walParser); err != nil {
		log.Printf("Warning: failed to preload table schemas: %v", err)
		// Continue anyway - schemas will be loaded from RELATION messages
	}

	// Start replication and parse messages
	return walParser.StartReplication(ctx, handler)
}

// preloadTableSchemas queries table schema information and preloads it into the WAL parser
func (c *Connector) preloadTableSchemas(ctx context.Context, walParser *WALParser) error {
	// Get tables from publication
	tables, err := c.getPublicationTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to get publication tables: %w", err)
	}

	log.Printf("Preloading schemas for %d tables from publication %s", len(tables), c.publication)

	for _, fullTableName := range tables {
		// Parse schema.table format
		parts := strings.Split(fullTableName, ".")
		var schemaName, tableName string
		if len(parts) == 2 {
			schemaName = parts[0]
			tableName = parts[1]
		} else {
			schemaName = "public"
			tableName = fullTableName
		}

		// Query table OID and columns
		var tableOID uint32
		err := c.pool.QueryRow(ctx,
			`SELECT c.oid FROM pg_class c
			 JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = $1 AND c.relname = $2`,
			schemaName, tableName).Scan(&tableOID)
		if err != nil {
			log.Printf("Warning: failed to get OID for table %s.%s: %v", schemaName, tableName, err)
			continue
		}

		// Query columns
		rows, err := c.pool.Query(ctx,
			`SELECT a.attname, a.atttypid, a.atttypmod, t.typname
			 FROM pg_attribute a
			 JOIN pg_type t ON t.oid = a.atttypid
			 WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
			 ORDER BY a.attnum`,
			tableOID)
		if err != nil {
			log.Printf("Warning: failed to query columns for table %s.%s: %v", schemaName, tableName, err)
			continue
		}

		var columns []ColumnInfo
		for rows.Next() {
			var colName, typeName string
			var typeID uint32
			var typeMod int32
			if err := rows.Scan(&colName, &typeID, &typeMod, &typeName); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan column info: %w", err)
			}
			columns = append(columns, ColumnInfo{
				Flags:    0, // Default flags
				Name:     colName,
				TypeID:   typeID,
				TypeMod:  typeMod,
				TypeName: typeName,
			})
		}
		rows.Close()

		pkRows, err := c.pool.Query(ctx,
			`SELECT a.attname
			 FROM pg_index i
			 JOIN pg_attribute a
			   ON a.attrelid = i.indrelid
			  AND a.attnum = ANY(i.indkey)
			 WHERE i.indrelid = $1
			   AND i.indisprimary
			 ORDER BY array_position(i.indkey, a.attnum)`,
			tableOID)
		if err != nil {
			return fmt.Errorf("failed to query primary key columns for table %s.%s: %w", schemaName, tableName, err)
		}

		var primaryKeyCols []string
		for pkRows.Next() {
			var colName string
			if err := pkRows.Scan(&colName); err != nil {
				pkRows.Close()
				return fmt.Errorf("failed to scan primary key column info: %w", err)
			}
			primaryKeyCols = append(primaryKeyCols, colName)
		}
		if err := pkRows.Err(); err != nil {
			pkRows.Close()
			return fmt.Errorf("failed to iterate primary key columns: %w", err)
		}
		pkRows.Close()

		// Create RelationInfo and add to parser
		relation := &RelationInfo{
			RelationID:      tableOID,
			Namespace:       schemaName,
			RelationName:    tableName,
			ReplicaIdentity: 'f', // FULL
			Columns:         columns,
			PrimaryKeyCols:  primaryKeyCols,
		}
		walParser.AddRelation(relation)

		log.Printf("Preloaded schema for %s.%s (OID: %d, columns: %d)",
			schemaName, tableName, tableOID, len(columns))
	}

	return nil
}

// verifyReplicationSlotReady ensures the replication slot is ready for use
func (c *Connector) verifyReplicationSlotReady(ctx context.Context) error {
	var slotName, plugin, slotType string
	var active bool
	var restartLSN, confirmedFlushLSN *string

	err := c.pool.QueryRow(ctx,
		`SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn 
		 FROM pg_replication_slots WHERE slot_name = $1`,
		c.slotName).Scan(&slotName, &plugin, &slotType, &active, &restartLSN, &confirmedFlushLSN)
	if err != nil {
		return fmt.Errorf("replication slot %s not found: %w", c.slotName, err)
	}

	log.Printf("Replication slot status: name=%s, plugin=%s, type=%s, active=%t",
		slotName, plugin, slotType, active)

	if plugin != "pgoutput" {
		return fmt.Errorf("replication slot %s has wrong plugin: %s (expected pgoutput)", c.slotName, plugin)
	}

	if slotType != "logical" {
		return fmt.Errorf("replication slot %s has wrong type: %s (expected logical)", c.slotName, slotType)
	}

	return nil
}

// buildLegacyConfig builds a legacy config from the connector configuration
func (c *Connector) buildLegacyConfig() *config.Config {
	return &config.Config{
		DBHost:       c.config.ConnConfig.Host,
		DBPort:       int(c.config.ConnConfig.Port),
		DBUser:       c.config.ConnConfig.User,
		DBPassword:   c.config.ConnConfig.Password,
		DBName:       c.config.ConnConfig.Database,
		TableFilters: c.tableFilters,
	}
}

// Server implements the ConnectorService for PostgreSQL
type Server struct {
	pb.UnimplementedConnectorServiceServer
	config          *config.Config
	checkpointMutex sync.RWMutex
}

// NewServer creates a new PostgreSQL connector server
func NewServer(cfg *config.Config) (*Server, error) {
	log.Printf("PostgreSQL Connector Server created")
	return &Server{
		config: cfg,
	}, nil
}

// BeginSnapshot implements the gRPC service endpoint for taking snapshots
func (s *Server) BeginSnapshot(req *pb.BeginSnapshotRequest, stream pb.ConnectorService_BeginSnapshotServer) error {
	log.Printf("Received BeginSnapshot request for job %s, table %s", req.JobId, req.TableName)

	// Create PostgreSQL config for snapshot adapter
	pgConfig := &PostgreSQLConfig{
		Host:     s.config.DBHost,
		Port:     s.config.DBPort,
		User:     s.config.DBUser,
		Password: s.config.DBPassword,
		Database: s.config.DBName,
		SSLMode:  "disable",
		AppName:  "elasticrelay_snapshot",
		Timeout:  30 * time.Second,
	}

	// Create connection pool manager and get a pool
	poolManager := NewConnectionPoolManager()
	defer poolManager.CloseAll()

	pool, err := poolManager.CreatePool("snapshot", s.config, DefaultConnectionPoolConfig())
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create advanced type mapper
	typeMapper := NewAdvancedTypeMapper(pool.Pool)
	if err := typeMapper.LoadCustomTypes(stream.Context()); err != nil {
		log.Printf("Warning: failed to load custom types: %v", err)
	}

	// Create the replication slot before snapshot so CDC can resume from the same slot.
	conn, err := NewConnector(s.config)
	if err != nil {
		return fmt.Errorf("failed to create connector for snapshot slot setup: %w", err)
	}
	conn.SetJobID(req.JobId)
	conn.pool = pool.Pool
	if err := conn.createPublication(stream.Context()); err != nil {
		return fmt.Errorf("failed to ensure publication before snapshot: %w", err)
	}
	slotLSN, err := conn.createReplicationSlot(stream.Context())
	if err != nil {
		return fmt.Errorf("failed to create replication slot before snapshot: %w", err)
	}

	// Create snapshot adapter
	adapter := NewPostgreSQLSnapshotAdapter(pgConfig, pool, typeMapper)
	adapter.SetPreferredConsistencyLSN(slotLSN)

	// Use parallel snapshot manager for large tables
	parallelManager := NewParallelSnapshotManager(adapter)

	// Determine if we should use parallel processing
	// For now, always use single table processing
	tables := []string{req.TableName}

	return parallelManager.StartSnapshot(stream.Context(), req.JobId, tables, stream)
}

// StartCdc implements the gRPC service endpoint for CDC
func (s *Server) StartCdc(req *pb.StartCdcRequest, stream pb.ConnectorService_StartCdcServer) error {
	log.Printf("Received StartCdc request for job %s", req.JobId)

	startCheckpoint := req.StartCheckpoint
	if startCheckpoint == nil || startCheckpoint.PostgresLsn == "" {
		log.Printf("No checkpoint in request for job %s, attempting to load from file.", req.JobId)
		loadedCheckpoint, err := s.loadCheckpoint(req.JobId)
		if err != nil {
			log.Printf("Could not load checkpoint for job %s: %v. This is normal if it's a new job.", req.JobId, err)
		} else {
			startCheckpoint = loadedCheckpoint
		}
	}

	// Create enhanced connector with all components
	conn, err := NewConnector(s.config)
	if err != nil {
		log.Printf("Failed to create connector: %v", err)
		return err
	}
	conn.SetJobID(req.JobId)

	// Start LSN and checkpoint management
	lsnManager := NewLSNManager(conn.pool, checkpointFilePath)
	replicationMgr := NewReplicationSlotManager(conn.pool)
	checkpointMgr := NewCheckpointManager(lsnManager, replicationMgr)

	// Start checkpoint manager
	if err := checkpointMgr.Start(stream.Context(), req.JobId, conn.slotName); err != nil {
		log.Printf("Warning: failed to start checkpoint manager: %v", err)
	}
	defer checkpointMgr.Stop()

	return conn.Start(stream, startCheckpoint, req.JobId)
}

// CommitCheckpoint implements the gRPC service endpoint for checkpoint commits
func (s *Server) CommitCheckpoint(ctx context.Context, req *pb.CommitCheckpointRequest) (*pb.CommitCheckpointResponse, error) {
	if req.Checkpoint == nil {
		return nil, fmt.Errorf("commit request has no checkpoint")
	}

	// Checkpoints are persisted to the local checkpoint file and do not require a DB pool.
	lsnManager := NewLSNManager(nil, checkpointFilePath)

	// Convert pb.Checkpoint to LSNCheckpoint
	lsnCheckpoint := &LSNCheckpoint{
		JobID:      req.JobId,
		LSN:        req.Checkpoint.PostgresLsn,
		LastUpdate: time.Now(),
	}

	// Save checkpoint using LSN manager
	if err := lsnManager.SaveCheckpoint(lsnCheckpoint); err != nil {
		return nil, fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return &pb.CommitCheckpointResponse{Success: true}, nil
}

// loadCheckpoint loads a checkpoint for a specific job
func (s *Server) loadCheckpoint(jobId string) (*pb.Checkpoint, error) {
	s.checkpointMutex.RLock()
	defer s.checkpointMutex.RUnlock()

	checkpoints, err := s.loadAllCheckpoints()
	if err != nil {
		return nil, err
	}

	cp, ok := checkpoints[jobId]
	if !ok {
		return nil, fmt.Errorf("no checkpoint found for job ID: %s", jobId)
	}
	return cp, nil
}

// loadAllCheckpoints loads all checkpoints from file
func (s *Server) loadAllCheckpoints() (map[string]*pb.Checkpoint, error) {
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
