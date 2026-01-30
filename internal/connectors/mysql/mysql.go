package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
	"github.com/yogoosoft/elasticrelay/internal/config"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

const (
	snapshotChunkSize  = 100
	checkpointFilePath = "checkpoints.json"
)

// tryParseDateTime attempts to parse MySQL datetime strings and convert them to RFC3339 format
func tryParseDateTime(value string) (string, bool) {
	// MySQL datetime formats to try (most specific first)
	formats := []string{
		"2006-01-02 15:04:05.999999999", // with nanoseconds
		"2006-01-02 15:04:05.999999",    // with microseconds  
		"2006-01-02 15:04:05.999",       // with milliseconds
		"2006-01-02 15:04:05",           // standard MySQL DATETIME format
		"2006-01-02",                    // MySQL DATE format
		"15:04:05",                      // MySQL TIME format
		time.RFC3339Nano,                // RFC3339 with nanoseconds
		time.RFC3339,                    // RFC3339
	}
	
	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			// Convert to UTC and format as RFC3339Nano for Elasticsearch compatibility
			return t.UTC().Format(time.RFC3339Nano), true
		}
	}
	
	// If all parsing attempts fail, it's not a datetime string
	return "", false
}

// eventHandler handles the binlog events.
type eventHandler struct {
	stream            pb.ConnectorService_StartCdcServer
	currentBinlogFile string
	tableFilters      []string
	tableMap          map[uint64]*replication.TableMapEvent
	tableMapMutex     sync.RWMutex
}

// Connector manages the MySQL CDC process.
type Connector struct {
	cfg          *replication.BinlogSyncerConfig
	tableFilters []string
}

// NewConnector creates a new MySQL connector.
func NewConnector(cfg *config.Config) (*Connector, error) {
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: cfg.ServerID,
		Flavor:   "mysql",
		Charset:  "utf8mb4",
		Host:     cfg.DBHost,
		Port:     uint16(cfg.DBPort),
		User:     cfg.DBUser,
		Password: cfg.DBPassword,
	}

	return &Connector{
		cfg:          &syncerCfg,
		tableFilters: cfg.TableFilters,
	}, nil
}

// Start runs the CDC process.
func (c *Connector) Start(stream pb.ConnectorService_StartCdcServer, startCheckpoint *pb.Checkpoint) error {
	var pos mysql.Position
	var err error

	if startCheckpoint != nil && startCheckpoint.MysqlBinlogFile != "" {
		log.Printf("Starting CDC from provided checkpoint: %s:%d", startCheckpoint.MysqlBinlogFile, startCheckpoint.MysqlBinlogPos)
		pos = mysql.Position{
			Name: startCheckpoint.MysqlBinlogFile,
			Pos:  startCheckpoint.MysqlBinlogPos,
		}
	} else {
		log.Println("No checkpoint provided, starting from current master position.")
		pos, err = getMasterPosition(stream.Context(), c.cfg)
		if err != nil {
			return fmt.Errorf("failed to get master position: %w", err)
		}
	}

	syncer := replication.NewBinlogSyncer(*c.cfg)

	handler := &eventHandler{
		stream:            stream,
		currentBinlogFile: pos.Name,
		tableFilters:      c.tableFilters,
		tableMap:          make(map[uint64]*replication.TableMapEvent),
	}

	s, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("failed to start sync: %w", err)
	}

	log.Printf("CDC sync started from position %s", pos)

	for {
		ev, err := s.GetEvent(stream.Context())
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				log.Printf("Context cancelled, stopping CDC.")
				return nil
			}
			return fmt.Errorf("error receiving event: %w", err)
		}

		if err := handler.OnEvent(ev); err != nil {
			return fmt.Errorf("error handling event: %w", err)
		}
	}
}

func (h *eventHandler) OnEvent(event *replication.BinlogEvent) error {
	switch ev := event.Event.(type) {
	case *replication.RotateEvent:
		h.currentBinlogFile = string(ev.NextLogName)
		log.Printf("Binlog file rotated to: %s", h.currentBinlogFile)

	case *replication.TableMapEvent:
		h.tableMapMutex.Lock()
		h.tableMap[ev.TableID] = ev
		h.tableMapMutex.Unlock()

	case *replication.RowsEvent:
		h.tableMapMutex.RLock()
		table := h.tableMap[ev.TableID]
		h.tableMapMutex.RUnlock()

		if table == nil {
			log.Printf("Warning: no table map event for table ID %d, skipping event", ev.TableID)
			return nil
		}

		tableName := string(table.Table)
		if len(h.tableFilters) > 0 {
			found := false
			for _, filter := range h.tableFilters {
				if filter == tableName {
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		}
		return h.handleRowsEvent(event.Header, table, ev)
	}
	return nil
}

func (h *eventHandler) handleRowsEvent(header *replication.EventHeader, table *replication.TableMapEvent, rowsEvent *replication.RowsEvent) error {
	var action string
	var step int
	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = "INSERT"
		step = 1
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = "UPDATE"
		step = 2 // Update events have old and new rows
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = "DELETE"
		step = 1
	default:
		log.Printf("Unknown RowsEvent type: %v", header.EventType)
		return nil
	}

	for i := 0; i < len(rowsEvent.Rows); i += step {
		row := rowsEvent.Rows[i]
		if action == "UPDATE" {
			row = rowsEvent.Rows[i+1] // Get the new row for update
		}

		dataMap := make(map[string]interface{})

		// Add table name as metadata to the data
		dataMap["_table"] = string(table.Table)
		dataMap["_schema"] = string(table.Schema)

		for colIdx, colData := range row {
			var colName string
			if colIdx < len(table.ColumnName) {
				colName = string(table.ColumnName[colIdx])
			} else {
				colName = fmt.Sprintf("col_%d", colIdx) // Fallback column name
			}
			if colData == nil {
				dataMap[colName] = nil
			} else {
				switch v := colData.(type) {
				case []byte:
					// Ensure proper handling of UTF-8 encoded byte data
					s := string(v)
					// Try datetime parsing first for potential datetime fields
					if parsed, ok := tryParseDateTime(s); ok {
						dataMap[colName] = parsed
					} else if len(s) <= 10 {
						// Only convert to number if string is short enough to avoid precision issues
						// This prevents phone numbers (11 digits), ID cards (18 digits), bank cards (16-19 digits)
						// from being converted to numbers and losing precision in JSON
						if num, err := strconv.ParseInt(s, 10, 64); err == nil {
							dataMap[colName] = num
						} else if f, err := strconv.ParseFloat(s, 64); err == nil {
							dataMap[colName] = f
						} else if b, err := strconv.ParseBool(s); err == nil {
							dataMap[colName] = b
						} else {
							dataMap[colName] = s
						}
					} else {
						// For long numeric strings (phone, ID card, bank card), keep as string
						// to avoid precision loss in JSON serialization
						// Ensure strings are properly handled as UTF-8
						if !utf8.Valid(v) {
							log.Printf("Invalid UTF-8 data in column %s, attempting to fix", colName)
							// Attempt to fix possible encoding issues
							s = string([]rune(string(v)))
						}
						dataMap[colName] = s
					}
				case string:
					// Handle string datetime values
					if parsed, ok := tryParseDateTime(v); ok {
						dataMap[colName] = parsed
					} else {
						dataMap[colName] = v
					}
				case time.Time:
					dataMap[colName] = v.UTC().Format(time.RFC3339Nano)
				default:
					dataMap[colName] = v
				}
			}
		}
		jsonData, err := json.Marshal(dataMap)
		if err != nil {
			log.Printf("Failed to marshal row to JSON: %v", err)
			continue
		}

		pkValue, err := h.getPrimaryKey(table, row)
		if err != nil {
			log.Printf("Failed to get primary key: %v", err)
			continue
		}

		changeEvent := &pb.ChangeEvent{
			Op: action,
			Checkpoint: &pb.Checkpoint{
				MysqlBinlogFile: h.currentBinlogFile,
				MysqlBinlogPos:  header.LogPos,
			},
			PrimaryKey: pkValue,
			Data:       string(jsonData),
		}

		if err := h.stream.Send(changeEvent); err != nil {
			log.Printf("Failed to send ChangeEvent to stream: %v", err)
			return err
		}
	}

	return nil
}

func (h *eventHandler) getPrimaryKey(table *replication.TableMapEvent, row []interface{}) (string, error) {

	if len(table.PrimaryKey) == 0 {
		log.Printf("Warning: No primary key defined for table %s.%s", string(table.Schema), string(table.Table))
		return "", nil
	}

	var pkParts []string
	for _, pkColIndex := range table.PrimaryKey {
		if int(pkColIndex) < len(row) {
			pkParts = append(pkParts, fmt.Sprintf("%v", row[pkColIndex]))
		} else {
			return "", fmt.Errorf("primary key column index %d out of bounds", pkColIndex)
		}
	}
	return strings.Join(pkParts, ":"), nil
}

func getMasterPosition(ctx context.Context, cfg *replication.BinlogSyncerConfig) (mysql.Position, error) {
	conn, err := client.Connect(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), cfg.User, cfg.Password, "")
	if err != nil {
		return mysql.Position{}, err
	}
	defer conn.Close()

	// Set connection charset
	if _, err := conn.Execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		log.Printf("Warning: failed to set charset for master position connection: %v", err)
	}

	r, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, err
	}

	file, err := r.GetString(0, 0)
	if err != nil {
		return mysql.Position{}, err
	}
	pos64, err := r.GetUint(0, 1)
	if err != nil {
		return mysql.Position{}, err
	}
	pos32 := uint32(pos64)

	return mysql.Position{
		Name: file,
		Pos:  pos32,
	}, nil
}

// Server implements the ConnectorService for MySQL.
type Server struct {
	pb.UnimplementedConnectorServiceServer
	config          *config.Config
	checkpointMutex sync.RWMutex
}

func NewServer(cfg *config.Config) (*Server, error) {
	log.Printf("MySQL Connector Server created")
	return &Server{
		config: cfg,
	}, nil
}

// BeginSnapshot implements the gRPC service endpoint.
func (s *Server) BeginSnapshot(req *pb.BeginSnapshotRequest, stream pb.ConnectorService_BeginSnapshotServer) error {
	log.Printf("Received BeginSnapshot request for job %s, table %s", req.JobId, req.TableName)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&collation=utf8mb4_unicode_ci&interpolateParams=true&loc=Local", s.config.DBUser, s.config.DBPassword, s.config.DBHost, s.config.DBPort, s.config.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(stream.Context()); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Ensure connection uses correct charset
	if _, err := db.ExecContext(stream.Context(), "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		return fmt.Errorf("failed to set connection charset: %w", err)
	}

	masterPos, err := getMasterPositionWithLock(stream.Context(), db)
	if err != nil {
		return fmt.Errorf("failed to get master position with lock: %w", err)
	}
	defer func() {
		if _, unlockErr := db.Exec("UNLOCK TABLES"); unlockErr != nil {
			log.Printf("Failed to unlock tables: %v", unlockErr)
		}
	}()

	log.Printf("Snapshot started for job %s, table %s at binlog position %s:%d", req.JobId, req.TableName, masterPos.Name, masterPos.Pos)

	rows, err := db.QueryContext(stream.Context(), "SELECT * FROM "+req.TableName)
	if err != nil {
		return fmt.Errorf("failed to query table %s: %w", req.TableName, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	var records []string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}

		if err := rows.Scan(valPtrs...); err != nil {
			return fmt.Errorf("failed to scan row from table %s: %w", req.TableName, err)
		}

		dataMap := make(map[string]interface{})

		// Add table name as metadata to the data (same as CDC)
		dataMap["_table"] = req.TableName
		dataMap["_schema"] = s.config.DBName

		for i, colName := range cols {
			val := valPtrs[i].(*interface{})
			if *val == nil {
				dataMap[colName] = nil
			} else {
				switch v := (*val).(type) {
				case []byte:
					// Ensure proper handling of UTF-8 encoded byte data
					s := string(v)
					// Try datetime parsing first for potential datetime fields
					if parsed, ok := tryParseDateTime(s); ok {
						dataMap[colName] = parsed
					} else if len(s) <= 10 {
						// Only convert to number if string is short enough to avoid precision issues
						// This prevents phone numbers (11 digits), ID cards (18 digits), bank cards (16-19 digits)
						// from being converted to numbers and losing precision in JSON
						if num, err := strconv.ParseInt(s, 10, 64); err == nil {
							dataMap[colName] = num
						} else if f, err := strconv.ParseFloat(s, 64); err == nil {
							dataMap[colName] = f
						} else if b, err := strconv.ParseBool(s); err == nil {
							dataMap[colName] = b
						} else {
							dataMap[colName] = s
						}
					} else {
						// For long numeric strings (phone, ID card, bank card), keep as string
						// to avoid precision loss in JSON serialization
						// Ensure strings are properly handled as UTF-8
						if !utf8.Valid(v) {
							log.Printf("Invalid UTF-8 data in column %s, attempting to fix", colName)
							// Attempt to fix possible encoding issues
							s = string([]rune(string(v)))
						}
						dataMap[colName] = s
					}
				case string:
					// Handle string datetime values  
					if parsed, ok := tryParseDateTime(v); ok {
						dataMap[colName] = parsed
					} else {
						dataMap[colName] = v
					}
				case time.Time:
					dataMap[colName] = v.UTC().Format(time.RFC3339Nano)
				default:
					dataMap[colName] = v
				}
			}
		}

		jsonData, err := json.Marshal(dataMap)
		if err != nil {
			log.Printf("Failed to marshal row to JSON: %v", err)
			continue
		}

		records = append(records, string(jsonData))

		if len(records) >= snapshotChunkSize {
			if err := stream.Send(&pb.SnapshotChunk{Records: records, SnapshotBinlogFile: masterPos.Name, SnapshotBinlogPos: masterPos.Pos}); err != nil {
				return fmt.Errorf("failed to send snapshot chunk: %w", err)
			}
			records = nil
		}
	}

	if len(records) > 0 {
		if err := stream.Send(&pb.SnapshotChunk{Records: records, SnapshotBinlogFile: masterPos.Name, SnapshotBinlogPos: masterPos.Pos}); err != nil {
			return fmt.Errorf("failed to send final snapshot chunk: %w", err)
		}
	}

	log.Printf("Finished snapshot for job %s, table %s", req.JobId, req.TableName)
	return nil
}

func getMasterPositionWithLock(ctx context.Context, db *sql.DB) (mysql.Position, error) {
	_, err := db.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK")
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to acquire global read lock: %w", err)
	}

	var file string
	var pos uint32
	var binlogDoDB, binlogIgnoreDB, executedGtidSet interface{}
	err = db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &pos, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet)
	if err != nil {
		db.ExecContext(ctx, "UNLOCK TABLES")
		return mysql.Position{}, fmt.Errorf("failed to get master status: %w", err)
	}

	return mysql.Position{Name: file, Pos: pos}, nil
}

// StartCdc implements the gRPC service endpoint.
func (s *Server) StartCdc(req *pb.StartCdcRequest, stream pb.ConnectorService_StartCdcServer) error {
	log.Printf("Received StartCdc request for job %s", req.JobId)

	startCheckpoint := req.StartCheckpoint
	if startCheckpoint == nil || startCheckpoint.MysqlBinlogFile == "" {
		log.Printf("No checkpoint in request for job %s, attempting to load from file.", req.JobId)
		loadedCheckpoint, err := s.loadCheckpoint(req.JobId)
		if err != nil {
			log.Printf("Could not load checkpoint for job %s: %v. This is normal if it's a new job.", req.JobId, err)
		} else {
			startCheckpoint = loadedCheckpoint
		}
	}

	conn, err := NewConnector(s.config)
	if err != nil {
		log.Printf("Failed to create connector: %v", err)
		return err
	}

	return conn.Start(stream, startCheckpoint)
}

// CommitCheckpoint implements the gRPC service endpoint.
func (s *Server) CommitCheckpoint(ctx context.Context, req *pb.CommitCheckpointRequest) (*pb.CommitCheckpointResponse, error) {
	if req.Checkpoint == nil {
		return nil, fmt.Errorf("commit request has no checkpoint")
	}

	s.checkpointMutex.Lock()
	defer s.checkpointMutex.Unlock()

	checkpoints, err := s.loadAllCheckpoints()
	if err != nil {
		return nil, err
	}

	checkpoints[req.JobId] = req.Checkpoint

	data, err := json.MarshalIndent(checkpoints, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal checkpoints: %w", err)
	}

	if err := os.WriteFile(checkpointFilePath, data, 0644); err != nil {
		return nil, fmt.Errorf("failed to write checkpoints file: %w", err)
	}

	log.Printf("Successfully committed checkpoint for job %s to %s", req.JobId, checkpointFilePath)
	return &pb.CommitCheckpointResponse{Success: true}, nil
}

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
