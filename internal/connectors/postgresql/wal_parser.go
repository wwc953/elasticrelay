package postgresql

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
	"github.com/yogoosoft/elasticrelay/internal/logger"
)

// WALMessage represents a parsed WAL message from PostgreSQL
type WALMessage struct {
	LSN         string
	Type        WALMessageType
	Timestamp   time.Time
	Transaction *TransactionInfo
	Relation    *RelationInfo
	Data        *RowData
}

// WALMessageType represents the type of WAL message
type WALMessageType int

const (
	WALMessageTypeUnknown WALMessageType = iota
	WALMessageTypeBegin
	WALMessageTypeCommit
	WALMessageTypeRelation
	WALMessageTypeInsert
	WALMessageTypeUpdate
	WALMessageTypeDelete
	WALMessageTypeTruncate
)

// String returns string representation of WAL message type
func (wmt WALMessageType) String() string {
	switch wmt {
	case WALMessageTypeBegin:
		return "BEGIN"
	case WALMessageTypeCommit:
		return "COMMIT"
	case WALMessageTypeRelation:
		return "RELATION"
	case WALMessageTypeInsert:
		return "INSERT"
	case WALMessageTypeUpdate:
		return "UPDATE"
	case WALMessageTypeDelete:
		return "DELETE"
	case WALMessageTypeTruncate:
		return "TRUNCATE"
	default:
		return "UNKNOWN"
	}
}

// TransactionInfo contains information about a transaction
type TransactionInfo struct {
	XID        uint32
	CommitTime time.Time
	BeginLSN   string
	CommitLSN  string
	FinalLSN   string
}

// RelationInfo contains information about a table/relation
type RelationInfo struct {
	RelationID      uint32
	Namespace       string
	RelationName    string
	ReplicaIdentity byte
	Columns         []ColumnInfo
	PrimaryKeyCols  []string
}

// ColumnInfo contains information about a column
type ColumnInfo struct {
	Flags    uint8
	Name     string
	TypeID   uint32
	TypeMod  int32
	TypeName string
}

// RowData contains the actual data for INSERT/UPDATE/DELETE operations
type RowData struct {
	RelationID uint32
	TupleType  byte // 'N' for new, 'O' for old, 'K' for key
	Columns    []ColumnData
}

// ColumnData contains data for a single column
type ColumnData struct {
	Name     string
	Value    interface{}
	IsNull   bool
	TypeID   uint32
	TypeName string
}

// WALParser handles parsing of PostgreSQL logical replication messages
type WALParser struct {
	conn         *pgconn.PgConn
	slotName     string
	publication  string
	startLSN     string
	relations    map[uint32]*RelationInfo
	currentTxn   *TransactionInfo
	tableFilters []string
	typeMapper   *TypeMapper
	logicalBuf   []byte
}

var errIncompleteLogicalMessage = errors.New("incomplete logical replication message")
var errUnsupportedLogicalMessage = errors.New("unsupported logical replication message")

// NewWALParser creates a new WAL parser
func NewWALParser(conn *pgconn.PgConn, slotName, publication, startLSN string, tableFilters []string) *WALParser {
	return &WALParser{
		conn:         conn,
		slotName:     slotName,
		publication:  publication,
		startLSN:     startLSN,
		relations:    make(map[uint32]*RelationInfo),
		tableFilters: tableFilters,
		typeMapper:   NewTypeMapper(),
	}
}

// AddRelation adds a preloaded relation/table schema to the parser
func (wp *WALParser) AddRelation(relation *RelationInfo) {
	wp.relations[relation.RelationID] = relation
}

// StartReplication begins logical replication and parses messages
func (wp *WALParser) StartReplication(ctx context.Context, handler *EventHandler) error {
	// Build replication command with correct syntax
	// All options should be in a single parentheses, comma-separated
	cmd := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (\"publication_names\" '%s', \"proto_version\" '1', \"messages\" 'false')",
		wp.slotName, wp.startLSN, wp.publication)

	log.Printf("Starting logical replication with command: %s", cmd)

	// Send replication command using SimpleQuery protocol
	logger.Debug("About to send replication command using SimpleQuery")

	// Create a Query message
	queryMsg := &pgproto3.Query{String: cmd}

	// Encode the message
	buf := make([]byte, 0, 256)
	buf, err := queryMsg.Encode(buf)
	if err != nil {
		return fmt.Errorf("failed to encode replication command: %w", err)
	}

	// Write directly to the connection
	logger.Debug("Writing query message to connection")
	_, err = wp.conn.Conn().Write(buf)
	if err != nil {
		return fmt.Errorf("failed to send replication command: %w", err)
	}

	logger.Debug("Command sent, waiting for CopyBothResponse")

	// Now receive the response - should be CopyBothResponse
	initialMsg, err := wp.conn.ReceiveMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive initial response: %w", err)
	}
	logger.Debug("Received initial message type: %T", initialMsg)

	// Verify it's a CopyBothResponse
	if _, ok := initialMsg.(*pgproto3.CopyBothResponse); !ok {
		return fmt.Errorf("unexpected initial response: %T, expected CopyBothResponse", initialMsg)
	}

	logger.Debug("Successfully entered replication mode (got CopyBothResponse)")

	// Process replication messages
	return wp.processMessages(ctx, handler)
}

// processMessages processes incoming WAL messages
func (wp *WALParser) processMessages(ctx context.Context, handler *EventHandler) error {
	logger.Debug("Entering processMessages function")

	// Track the last received LSN for status updates - initialize with starting LSN
	lastReceivedLSN, err := wp.parseLSNToUint64(wp.startLSN)
	if err != nil {
		log.Printf("Warning: failed to parse starting LSN %s: %v", wp.startLSN, err)
		lastReceivedLSN = 0
	}
	logger.Debug("Parsed starting LSN: %s -> %d", wp.startLSN, lastReceivedLSN)

	// Send initial keepalive to establish the connection properly
	log.Printf("Sending initial keepalive message")
	if err := wp.sendStandbyStatusUpdate(ctx, lastReceivedLSN, lastReceivedLSN, lastReceivedLSN); err != nil {
		log.Printf("Failed to send initial keepalive: %v", err)
		return fmt.Errorf("failed to send initial keepalive: %w", err)
	}
	logger.Debug("Initial keepalive sent successfully")

	nextKeepalive := time.Now().Add(10 * time.Second)
	readTimeout := 1 * time.Second

	for {
		if err := ctx.Err(); err != nil {
			logger.Debug("Context cancelled, exiting processMessages")
			return err
		}

		now := time.Now()
		if !now.Before(nextKeepalive) {
			log.Printf("Sending periodic keepalive message")
			if err := wp.sendStandbyStatusUpdate(ctx, lastReceivedLSN, lastReceivedLSN, lastReceivedLSN); err != nil {
				log.Printf("Failed to send periodic keepalive: %v", err)
			}
			nextKeepalive = time.Now().Add(10 * time.Second)
		}

		if err := wp.conn.Conn().SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return fmt.Errorf("failed to set read deadline: %w", err)
		}
		msg, err := wp.conn.ReceiveMessage(ctx)
		if clearErr := wp.conn.Conn().SetReadDeadline(time.Time{}); clearErr != nil {
			return fmt.Errorf("failed to clear read deadline: %w", clearErr)
		}

		if err != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("failed to receive message: %w", err)
		}

		logger.Debug("Processing message from connection: %T", msg)
		if err := wp.handleMessage(msg, handler); err != nil {
			return fmt.Errorf("error handling replication message: %w", err)
		}

		if copyData, ok := msg.(*pgproto3.CopyData); ok && len(copyData.Data) > 0 {
			if copyData.Data[0] == pglogrepl.XLogDataByteID && len(copyData.Data) >= 25 {
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					log.Printf("Failed to parse XLogData for LSN update: %v", err)
					continue
				}

				if xld.WALStart > 0 {
					lastReceivedLSN = uint64(xld.WALStart)
				} else if xld.ServerWALEnd > 0 {
					lastReceivedLSN = uint64(xld.ServerWALEnd)
				}

				logger.Debug("LSN update: walStart=%s, walEnd=%s, using LSN=%X/%X",
					xld.WALStart.String(), xld.ServerWALEnd.String(),
					uint32(lastReceivedLSN>>32), uint32(lastReceivedLSN))
			}
		}
	}
}

// handleMessage processes a single message from PostgreSQL
func (wp *WALParser) handleMessage(msg pgproto3.BackendMessage, handler *EventHandler) error {
	logger.Debug("handleMessage called with type: %T", msg)
	switch m := msg.(type) {
	case *pgproto3.CopyData:
		logger.Debug("Processing CopyData message, length: %d", len(m.Data))
		return wp.processCopyData(m.Data, handler)
	case *pgproto3.ErrorResponse:
		logger.Debug("Received ErrorResponse: %s", m.Message)
		return fmt.Errorf("PostgreSQL error: %s", m.Message)
	case *pgproto3.NoticeResponse:
		log.Printf("PostgreSQL notice: %s", m.Message)
	default:
		log.Printf("Received unhandled message type: %T", msg)
	}
	return nil
}

// processCopyData processes CopyData messages containing WAL data
func (wp *WALParser) processCopyData(data []byte, handler *EventHandler) error {
	if len(data) == 0 {
		logger.Debug("processCopyData: empty data")
		return nil
	}

	msgType := data[0]
	logger.Debug("processCopyData: message type '%c' (0x%02x), data length: %d", msgType, msgType, len(data))

	switch msgType {
	case 'w': // XLogData message
		logger.Debug("Processing XLogData message")
		return wp.parseXLogData(data[1:], handler)
	case 'k': // Primary keepalive message
		logger.Debug("Processing primary keepalive message")
		return wp.parsePrimaryKeepalive(data[1:])
	default:
		return fmt.Errorf("unknown copy data message type: %c (0x%02x)", msgType, msgType)
	}
}

// parseXLogData parses XLogData messages containing actual WAL records
func (wp *WALParser) parseXLogData(data []byte, handler *EventHandler) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData message: %w", err)
	}

	logger.Debug("XLogData: walStart=%X/%X, walEnd=%X/%X, sendTime=%d",
		uint32(uint64(xld.WALStart)>>32), uint32(uint64(xld.WALStart)),
		uint32(uint64(xld.ServerWALEnd)>>32), uint32(uint64(xld.ServerWALEnd)),
		xld.ServerTime.UnixMicro())

	currentLSN := uint64(xld.WALStart)
	if currentLSN == 0 {
		currentLSN = uint64(xld.ServerWALEnd)
	}
	if currentLSN > 0 {
		handler.currentLSN = formatLSN(currentLSN)
	}

	if len(xld.WALData) == 0 {
		return nil
	}

	wp.logicalBuf = append(wp.logicalBuf, xld.WALData...)
	return wp.processBufferedLogicalMessages(handler)
}

// parseLogicalMessage parses all pgoutput messages from a single XLogData payload.
func (wp *WALParser) parseLogicalMessage(data []byte, handler *EventHandler) error {
	if len(data) == 0 {
		logger.Debug("parseLogicalMessage: empty data")
		return nil
	}

	offset := 0
	for offset < len(data) {
		consumed, err := wp.parseSingleLogicalMessage(data[offset:], handler)
		if err != nil {
			return fmt.Errorf("failed to parse logical replication message at offset %d: %w", offset, err)
		}
		if consumed <= 0 {
			return fmt.Errorf("logical replication parser made no progress at offset %d", offset)
		}
		offset += consumed
	}

	return nil
}

func (wp *WALParser) processBufferedLogicalMessages(handler *EventHandler) error {
	offset := 0
	for offset < len(wp.logicalBuf) {
		consumed, err := wp.parseSingleLogicalMessage(wp.logicalBuf[offset:], handler)
		if err != nil {
			if errors.Is(err, errIncompleteLogicalMessage) {
				break
			}
			return fmt.Errorf("failed to parse logical replication message at offset %d: %w", offset, err)
		}
		if consumed <= 0 {
			return fmt.Errorf("logical replication parser made no progress at offset %d", offset)
		}
		offset += consumed
	}

	if offset == 0 {
		return nil
	}

	if offset >= len(wp.logicalBuf) {
		wp.logicalBuf = nil
		return nil
	}

	wp.logicalBuf = append([]byte(nil), wp.logicalBuf[offset:]...)
	return nil
}

func (wp *WALParser) parseSingleLogicalMessage(data []byte, handler *EventHandler) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	msgType := data[0]
	payload := data[1:]

	switch msgType {
	case 'B':
		consumed, err := wp.parseBegin(payload)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'C':
		consumed, err := wp.parseCommit(payload)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'R':
		consumed, err := wp.parseRelation(payload)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'I':
		consumed, err := wp.parseInsert(payload, handler)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'U':
		consumed, err := wp.parseUpdate(payload, handler)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'D':
		consumed, err := wp.parseDelete(payload, handler)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'T':
		consumed, err := wp.parseTruncate(payload, handler)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'Y':
		consumed, err := parseTypeMessage(payload)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'O':
		consumed, err := parseOriginMessage(payload)
		return 1 + consumed, normalizeLogicalMessageError(err)
	case 'M':
		consumed, err := parseGenericMessage(payload)
		return 1 + consumed, normalizeLogicalMessageError(err)
	default:
		return 0, fmt.Errorf("%w: %q", errUnsupportedLogicalMessage, msgType)
	}
}

func normalizeLogicalMessageError(err error) error {
	if err == nil {
		return nil
	}
	if isIncompleteLogicalMessageError(err) {
		return fmt.Errorf("%w: %v", errIncompleteLogicalMessage, err)
	}
	return err
}

func isIncompleteLogicalMessageError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "too short") ||
		strings.Contains(msg, "not null-terminated") ||
		strings.Contains(msg, "missing new tuple marker")
}

func convertRelationMessage(msg *pglogrepl.RelationMessage) *RelationInfo {
	columns := make([]ColumnInfo, 0, len(msg.Columns))
	for _, col := range msg.Columns {
		columns = append(columns, ColumnInfo{
			Flags:    col.Flags,
			Name:     col.Name,
			TypeID:   col.DataType,
			TypeMod:  col.TypeModifier,
			TypeName: "",
		})
	}

	return &RelationInfo{
		RelationID:      msg.RelationID,
		Namespace:       msg.Namespace,
		RelationName:    msg.RelationName,
		ReplicaIdentity: msg.ReplicaIdentity,
		Columns:         columns,
	}
}

func (wp *WALParser) convertTupleData(relationID uint32, tupleType byte, tuple *pglogrepl.TupleData) (*RowData, error) {
	if tuple == nil {
		return nil, nil
	}

	relation := wp.relations[relationID]
	if relation == nil {
		return nil, fmt.Errorf("unknown relation ID: %d", relationID)
	}

	columns := make([]ColumnData, 0, len(tuple.Columns))
	for idx, col := range tuple.Columns {
		if idx >= len(relation.Columns) {
			break
		}

		columnInfo := relation.Columns[idx]
		var value interface{}
		var isNull bool

		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			isNull = true
		case pglogrepl.TupleDataTypeToast:
			continue
		case pglogrepl.TupleDataTypeText:
			value = string(col.Data)
		case pglogrepl.TupleDataTypeBinary:
			value = col.Data
		default:
			return nil, fmt.Errorf("unknown tuple column type: %c", col.DataType)
		}

		columns = append(columns, ColumnData{
			Name:     columnInfo.Name,
			Value:    value,
			IsNull:   isNull,
			TypeID:   columnInfo.TypeID,
			TypeName: columnInfo.TypeName,
		})
	}

	return &RowData{
		RelationID: relationID,
		TupleType:  tupleType,
		Columns:    columns,
	}, nil
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// parseBegin parses BEGIN transaction message
func (wp *WALParser) parseBegin(data []byte) (int, error) {
	if len(data) < 20 {
		return 0, fmt.Errorf("BEGIN message too short")
	}

	finalLSN := binary.BigEndian.Uint64(data[0:8])
	commitTime := int64(binary.BigEndian.Uint64(data[8:16]))
	xid := binary.BigEndian.Uint32(data[16:20])

	wp.currentTxn = &TransactionInfo{
		XID:        xid,
		CommitTime: time.Unix(commitTime/1000000, (commitTime%1000000)*1000),
		FinalLSN:   fmt.Sprintf("%X/%X", uint32(finalLSN>>32), uint32(finalLSN)),
	}

	return 20, nil
}

// parseCommit parses COMMIT transaction message
func (wp *WALParser) parseCommit(data []byte) (int, error) {
	if len(data) < 25 {
		return 0, fmt.Errorf("COMMIT message too short")
	}

	flags := data[0]
	commitLSN := binary.BigEndian.Uint64(data[1:9])
	endLSN := binary.BigEndian.Uint64(data[9:17])
	commitTime := int64(binary.BigEndian.Uint64(data[17:25]))

	if wp.currentTxn != nil {
		wp.currentTxn.CommitLSN = formatLSN(commitLSN)
		wp.currentTxn.FinalLSN = formatLSN(endLSN)
		wp.currentTxn.CommitTime = time.Unix(commitTime/1000000, (commitTime%1000000)*1000)
		_ = flags
	}

	// Reset current transaction
	wp.currentTxn = nil

	return 25, nil
}

// parseRelation parses RELATION message
func (wp *WALParser) parseRelation(data []byte) (int, error) {
	if len(data) < 7 {
		return 0, fmt.Errorf("RELATION message too short")
	}

	relationID := binary.BigEndian.Uint32(data[0:4])
	offset := 4

	// Parse namespace (null-terminated string)
	namespace, consumed, err := parseCString(data[offset:])
	if err != nil {
		return 0, fmt.Errorf("RELATION message: %w", err)
	}
	offset += consumed

	// Parse relation name (null-terminated string)
	relationName, consumed, err := parseCString(data[offset:])
	if err != nil {
		return 0, fmt.Errorf("RELATION message: %w", err)
	}
	offset += consumed

	// Parse replica identity
	if offset >= len(data) {
		return 0, fmt.Errorf("RELATION message too short for replica identity")
	}
	replicaIdentity := data[offset]
	offset++

	// Parse column count
	if offset+2 > len(data) {
		return 0, fmt.Errorf("RELATION message too short for column count")
	}
	columnCount := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	columns := make([]ColumnInfo, 0, columnCount)
	primaryKeyCols := make([]string, 0)

	for i := uint16(0); i < columnCount; i++ {
		if offset+1 > len(data) {
			return 0, fmt.Errorf("RELATION message too short while parsing column %d", i)
		}

		flags := data[offset]
		offset++

		// Parse column name (null-terminated string)
		columnName, consumed, err := parseCString(data[offset:])
		if err != nil {
			return 0, fmt.Errorf("RELATION message column %d: %w", i, err)
		}
		offset += consumed

		// Parse type ID and type mod
		if offset+8 > len(data) {
			return 0, fmt.Errorf("RELATION message too short for column %d type info", i)
		}
		typeID := binary.BigEndian.Uint32(data[offset : offset+4])
		typeMod := int32(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		offset += 8

		columns = append(columns, ColumnInfo{
			Flags:   flags,
			Name:    columnName,
			TypeID:  typeID,
			TypeMod: typeMod,
		})
		if flags&1 == 1 {
			primaryKeyCols = append(primaryKeyCols, columnName)
		}
	}

	wp.relations[relationID] = &RelationInfo{
		RelationID:      relationID,
		Namespace:       namespace,
		RelationName:    relationName,
		ReplicaIdentity: replicaIdentity,
		Columns:         columns,
		PrimaryKeyCols:  primaryKeyCols,
	}

	logger.Debug("Parsed RELATION: id=%d, schema=%s, table=%s, columns=%d",
		relationID, namespace, relationName, len(columns))

	return offset, nil
}

// parseInsert parses INSERT message
func (wp *WALParser) parseInsert(data []byte, handler *EventHandler) (int, error) {
	if len(data) < 5 {
		return 0, fmt.Errorf("INSERT message too short")
	}

	relationID := binary.BigEndian.Uint32(data[0:4])
	tupleType := data[4]

	if tupleType != 'N' {
		return 0, fmt.Errorf("unexpected tuple type for INSERT: %c", tupleType)
	}

	rowData, consumed, err := wp.parseTupleData(data[5:], relationID, tupleType)
	if err != nil {
		return 0, fmt.Errorf("failed to parse INSERT tuple data: %w", err)
	}

	if err := wp.createChangeEvent("INSERT", handler.currentLSN, relationID, rowData, nil, handler); err != nil {
		return 0, err
	}
	return 5 + consumed, nil
}

// parseUpdate parses UPDATE message
func (wp *WALParser) parseUpdate(data []byte, handler *EventHandler) (int, error) {
	if len(data) < 5 {
		return 0, fmt.Errorf("UPDATE message too short")
	}

	relationID := binary.BigEndian.Uint32(data[0:4])

	var oldData, newData *RowData
	var err error
	offset := 4

	// Parse old tuple (if present)
	if offset < len(data) {
		tupleType := data[offset]
		offset++

		if tupleType == 'O' || tupleType == 'K' {
			var consumed int
			oldData, consumed, err = wp.parseTupleData(data[offset:], relationID, tupleType)
			if err != nil {
				return 0, fmt.Errorf("failed to parse UPDATE old tuple data: %w", err)
			}
			offset += consumed
			if offset >= len(data) || data[offset] != 'N' {
				return 0, fmt.Errorf("UPDATE message missing new tuple marker")
			}
			offset++
		} else if tupleType == 'N' {
			var consumed int
			newData, consumed, err = wp.parseTupleData(data[offset:], relationID, tupleType)
			if err != nil {
				return 0, fmt.Errorf("failed to parse UPDATE new tuple data: %w", err)
			}
			offset += consumed
		} else {
			return 0, fmt.Errorf("unexpected tuple type for UPDATE: %c", tupleType)
		}
	}

	// Parse new tuple
	if newData == nil && offset < len(data) {
		var consumed int
		newData, consumed, err = wp.parseTupleData(data[offset:], relationID, 'N')
		if err != nil {
			return 0, fmt.Errorf("failed to parse UPDATE new tuple data: %w", err)
		}
		offset += consumed
	}

	if err := wp.createChangeEvent("UPDATE", handler.currentLSN, relationID, newData, oldData, handler); err != nil {
		return 0, err
	}
	return offset, nil
}

// parseDelete parses DELETE message
func (wp *WALParser) parseDelete(data []byte, handler *EventHandler) (int, error) {
	if len(data) < 5 {
		return 0, fmt.Errorf("DELETE message too short")
	}

	relationID := binary.BigEndian.Uint32(data[0:4])
	tupleType := data[4]

	if tupleType != 'O' && tupleType != 'K' {
		return 0, fmt.Errorf("unexpected tuple type for DELETE: %c", tupleType)
	}

	rowData, consumed, err := wp.parseTupleData(data[5:], relationID, tupleType)
	if err != nil {
		return 0, fmt.Errorf("failed to parse DELETE tuple data: %w", err)
	}

	if err := wp.createChangeEvent("DELETE", handler.currentLSN, relationID, nil, rowData, handler); err != nil {
		return 0, err
	}
	return 5 + consumed, nil
}

// parseTruncate parses TRUNCATE message
func (wp *WALParser) parseTruncate(data []byte, handler *EventHandler) (int, error) {
	if len(data) < 5 {
		return 0, fmt.Errorf("TRUNCATE message too short")
	}

	relationCount := binary.BigEndian.Uint32(data[0:4])
	offset := 5 // relationCount(4) + options(1)
	if len(data) < offset+int(relationCount)*4 {
		return 0, fmt.Errorf("TRUNCATE message too short for %d relations", relationCount)
	}

	for i := uint32(0); i < relationCount; i++ {
		relationID := binary.BigEndian.Uint32(data[offset : offset+4])
		err := wp.createChangeEvent("TRUNCATE", "", relationID, nil, nil, handler)
		if err != nil {
			log.Printf("Failed to create TRUNCATE event for relation %d: %v", relationID, err)
		}
		offset += 4
	}

	return offset, nil
}

// parseTupleData parses tuple data from WAL messages
func (wp *WALParser) parseTupleData(data []byte, relationID uint32, tupleType byte) (*RowData, int, error) {
	if len(data) < 2 {
		return nil, 0, fmt.Errorf("tuple data too short")
	}

	columnCount := binary.BigEndian.Uint16(data[0:2])
	columns := make([]ColumnData, 0, columnCount)

	relation := wp.relations[relationID]
	if relation == nil {
		return nil, 0, fmt.Errorf("unknown relation ID: %d", relationID)
	}

	offset := 2
	for i := uint16(0); i < columnCount && i < uint16(len(relation.Columns)); i++ {
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("tuple data too short while parsing column %d", i)
		}

		columnInfo := relation.Columns[i]
		columnType := data[offset]
		offset++

		var value interface{}
		var isNull bool

		switch columnType {
		case 'n': // NULL value
			isNull = true
			value = nil
		case 't': // Text value
			if offset+4 > len(data) {
				return nil, 0, fmt.Errorf("tuple data too short for text length")
			}
			length := binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4

			if offset+int(length) > len(data) {
				return nil, 0, fmt.Errorf("tuple data too short for text payload")
			}

			value = string(data[offset : offset+int(length)])
			offset += int(length)
		case 'u': // Unchanged TOAST value
			// Skip unchanged TOAST values
			continue
		default:
			return nil, 0, fmt.Errorf("unknown tuple column type: %c", columnType)
		}

		columns = append(columns, ColumnData{
			Name:     columnInfo.Name,
			Value:    value,
			IsNull:   isNull,
			TypeID:   columnInfo.TypeID,
			TypeName: columnInfo.TypeName,
		})
	}

	if columnCount > uint16(len(relation.Columns)) {
		return nil, 0, fmt.Errorf("tuple data has %d columns but relation %d only has %d columns",
			columnCount, relationID, len(relation.Columns))
	}

	return &RowData{
		RelationID: relationID,
		TupleType:  tupleType,
		Columns:    columns,
	}, offset, nil
}

func parseCString(data []byte) (string, int, error) {
	for i, b := range data {
		if b == 0 {
			return string(data[:i]), i + 1, nil
		}
	}
	return "", 0, fmt.Errorf("cstring not null-terminated")
}

func parseOriginMessage(data []byte) (int, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("ORIGIN message too short")
	}
	_, consumed, err := parseCString(data[8:])
	if err != nil {
		return 0, err
	}
	return 8 + consumed, nil
}

func parseTypeMessage(data []byte) (int, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("TYPE message too short")
	}

	offset := 4
	_, consumed, err := parseCString(data[offset:])
	if err != nil {
		return 0, err
	}
	offset += consumed

	_, consumed, err = parseCString(data[offset:])
	if err != nil {
		return 0, err
	}
	offset += consumed

	return offset, nil
}

func parseGenericMessage(data []byte) (int, error) {
	if len(data) < 13 {
		return 0, fmt.Errorf("MESSAGE message too short")
	}
	prefix, consumed, err := parseCString(data[9:])
	if err != nil {
		return 0, err
	}
	_ = prefix
	lengthOffset := 9 + consumed
	if len(data) < lengthOffset+4 {
		return 0, fmt.Errorf("MESSAGE message too short for content length")
	}
	contentLength := int(binary.BigEndian.Uint32(data[lengthOffset : lengthOffset+4]))
	if len(data) < lengthOffset+4+contentLength {
		return 0, fmt.Errorf("MESSAGE message too short for content")
	}
	return lengthOffset + 4 + contentLength, nil
}

// createChangeEvent creates a change event from parsed WAL data
func (wp *WALParser) createChangeEvent(operation, lsn string, relationID uint32, newData, oldData *RowData, handler *EventHandler) error {
	relation := wp.relations[relationID]
	if relation == nil {
		return fmt.Errorf("unknown relation ID: %d", relationID)
	}

	// Check table filters
	tableName := relation.RelationName
	if len(wp.tableFilters) > 0 {
		found := false
		for _, filter := range wp.tableFilters {
			if filter == tableName {
				found = true
				break
			}
		}
		if !found {
			return nil // Skip filtered table
		}
	}

	// Use new data if available, otherwise old data
	data := newData
	if data == nil {
		data = oldData
	}
	if data == nil {
		return fmt.Errorf("no data available for change event")
	}

	// Convert to JSON
	dataMap := make(map[string]interface{})
	dataMap["_table"] = tableName
	dataMap["_schema"] = relation.Namespace

	for _, col := range data.Columns {
		// Use TypeMapper to convert PostgreSQL values to Elasticsearch-compatible format
		convertedValue, err := wp.typeMapper.ConvertValue(col.Value, col.TypeID)
		if err != nil {
			log.Printf("Warning: failed to convert column '%s' (type OID %d): %v, using raw value",
				col.Name, col.TypeID, err)
			dataMap[col.Name] = col.Value
		} else {
			dataMap[col.Name] = convertedValue
		}
	}

	jsonData, err := json.Marshal(dataMap)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	primaryKey, err := extractPrimaryKey(data, relation)
	if err != nil {
		return fmt.Errorf("failed to extract primary key for %s.%s: %w", relation.Namespace, relation.RelationName, err)
	}

	// Create change event
	changeEvent := &pb.ChangeEvent{
		Op: operation,
		Checkpoint: &pb.Checkpoint{
			Position:    lsn,
			PostgresLsn: lsn,
			Timestamp:   time.Now().Unix(),
		},
		PrimaryKey: primaryKey,
		Data:       string(jsonData),
	}

	// Send to handler
	return handler.stream.Send(changeEvent)
}

func extractPrimaryKey(data *RowData, relation *RelationInfo) (string, error) {
	if data == nil {
		return "", fmt.Errorf("row data is nil")
	}

	if len(relation.PrimaryKeyCols) == 0 {
		if len(data.Columns) == 0 {
			return "", fmt.Errorf("no columns available")
		}
		return primaryKeyValueString(data.Columns[0].Value), nil
	}

	valuesByName := make(map[string]interface{}, len(data.Columns))
	for _, col := range data.Columns {
		valuesByName[col.Name] = col.Value
	}

	pkParts := make([]string, 0, len(relation.PrimaryKeyCols))
	for _, colName := range relation.PrimaryKeyCols {
		value, ok := valuesByName[colName]
		if !ok {
			return "", fmt.Errorf("primary key column %s not found in row data", colName)
		}
		pkParts = append(pkParts, primaryKeyValueString(value))
	}

	return strings.Join(pkParts, ":"), nil
}

func primaryKeyValueString(value interface{}) string {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", value)
	}
}

// parsePrimaryKeepalive parses a primary keepalive message
func (wp *WALParser) parsePrimaryKeepalive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse primary keepalive message: %w", err)
	}

	if pkm.ReplyRequested {
		// Send standby status update
		walEnd := uint64(pkm.ServerWALEnd)
		return wp.sendStandbyStatusUpdate(context.Background(), walEnd, walEnd, walEnd)
	}

	return nil
}

func formatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// sendStandbyStatusUpdate sends a standby status update message
func (wp *WALParser) sendStandbyStatusUpdate(ctx context.Context, received, flushed, applied uint64) error {
	// Send the standby status update to PostgreSQL
	log.Printf("Sending standby status update: received=%X/%X, flushed=%X/%X, applied=%X/%X",
		uint32(received>>32), uint32(received),
		uint32(flushed>>32), uint32(flushed),
		uint32(applied>>32), uint32(applied))

	// Check context before writing
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before sending standby status: %w", err)
	}

	return pglogrepl.SendStandbyStatusUpdate(ctx, wp.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(received),
		WALFlushPosition: pglogrepl.LSN(flushed),
		WALApplyPosition: pglogrepl.LSN(applied),
		ClientTime:       time.Now(),
	})
}

// parseLSNToUint64 converts PostgreSQL LSN string format (e.g., "0/19A6E88") to uint64
func (wp *WALParser) parseLSNToUint64(lsnStr string) (uint64, error) {
	if lsnStr == "" {
		return 0, fmt.Errorf("empty LSN string")
	}

	parts := strings.Split(lsnStr, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN format: %s", lsnStr)
	}

	// Parse high 32 bits
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN high part %s: %w", parts[0], err)
	}

	// Parse low 32 bits
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN low part %s: %w", parts[1], err)
	}

	// Combine into 64-bit LSN
	return (high << 32) | low, nil
}
