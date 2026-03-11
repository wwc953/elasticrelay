package postgresql

import (
	"context"
	"encoding/binary"
	"testing"

	metadata "google.golang.org/grpc/metadata"

	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
)

type mockStartCdcStream struct {
	ctx    context.Context
	events []*pb.ChangeEvent
}

func (m *mockStartCdcStream) Send(event *pb.ChangeEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *mockStartCdcStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockStartCdcStream) SendHeader(metadata.MD) error { return nil }
func (m *mockStartCdcStream) SetTrailer(metadata.MD)       {}
func (m *mockStartCdcStream) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
func (m *mockStartCdcStream) SendMsg(any) error { return nil }
func (m *mockStartCdcStream) RecvMsg(any) error { return nil }

func TestParseLogicalMessage_ProcessesAllMessagesInBuffer(t *testing.T) {
	parser := NewWALParser(nil, "slot", "pub", "0/0", []string{"test_data"})
	parser.relations[42] = &RelationInfo{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "test_data",
		Columns: []ColumnInfo{
			{Name: "id", TypeID: 25},
			{Name: "name", TypeID: 25},
		},
	}

	stream := &mockStartCdcStream{}
	handler := &EventHandler{
		stream:     stream,
		currentLSN: "0/ABCDEF",
	}

	var walData []byte
	walData = append(walData, buildInsertMessage(42, "1", "user_1")...)
	walData = append(walData, buildInsertMessage(42, "2", "user_2")...)

	if err := parser.parseLogicalMessage(walData, handler); err != nil {
		t.Fatalf("parseLogicalMessage returned error: %v", err)
	}

	if len(stream.events) != 2 {
		t.Fatalf("expected 2 change events, got %d", len(stream.events))
	}

	if stream.events[0].PrimaryKey != "1" {
		t.Fatalf("expected first primary key to be 1, got %s", stream.events[0].PrimaryKey)
	}
	if stream.events[1].PrimaryKey != "2" {
		t.Fatalf("expected second primary key to be 2, got %s", stream.events[1].PrimaryKey)
	}
}

func TestParseXLogData_ReassemblesFragmentedLogicalMessages(t *testing.T) {
	parser := NewWALParser(nil, "slot", "pub", "0/0", []string{"test_data"})
	parser.relations[42] = &RelationInfo{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "test_data",
		Columns: []ColumnInfo{
			{Name: "id", TypeID: 25},
			{Name: "name", TypeID: 25},
		},
	}

	stream := &mockStartCdcStream{}
	handler := &EventHandler{
		stream: stream,
	}

	insertMsg := buildInsertMessage(42, "1", "user_1")
	splitAt := len(insertMsg) / 2

	if err := parser.parseXLogData(buildXLogDataPayload(insertMsg[:splitAt]), handler); err != nil {
		t.Fatalf("first parseXLogData returned error: %v", err)
	}
	if len(stream.events) != 0 {
		t.Fatalf("expected no events after first fragmented payload, got %d", len(stream.events))
	}

	if err := parser.parseXLogData(buildXLogDataPayload(insertMsg[splitAt:]), handler); err != nil {
		t.Fatalf("second parseXLogData returned error: %v", err)
	}
	if len(stream.events) != 1 {
		t.Fatalf("expected 1 event after reassembly, got %d", len(stream.events))
	}
	if stream.events[0].PrimaryKey != "1" {
		t.Fatalf("expected reassembled primary key to be 1, got %s", stream.events[0].PrimaryKey)
	}
}

func TestParseXLogData_ReassemblesTupleBoundaryFragments(t *testing.T) {
	parser := NewWALParser(nil, "slot", "pub", "0/0", []string{"test_data"})
	parser.relations[42] = &RelationInfo{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "test_data",
		Columns: []ColumnInfo{
			{Name: "id", TypeID: 25},
			{Name: "name", TypeID: 25},
		},
	}

	stream := &mockStartCdcStream{}
	handler := &EventHandler{stream: stream}

	insertMsg := buildInsertMessage(42, "1", "user_1")
	splitAt := 13 // Split just after the first tuple column is fully read.

	if err := parser.parseXLogData(buildXLogDataPayload(insertMsg[:splitAt]), handler); err != nil {
		t.Fatalf("first parseXLogData returned error: %v", err)
	}
	if len(stream.events) != 0 {
		t.Fatalf("expected no events after tuple-boundary fragment, got %d", len(stream.events))
	}

	if err := parser.parseXLogData(buildXLogDataPayload(insertMsg[splitAt:]), handler); err != nil {
		t.Fatalf("second parseXLogData returned error: %v", err)
	}
	if len(stream.events) != 1 {
		t.Fatalf("expected 1 event after tuple reassembly, got %d", len(stream.events))
	}
	if stream.events[0].PrimaryKey != "1" {
		t.Fatalf("expected reassembled primary key to be 1, got %s", stream.events[0].PrimaryKey)
	}
}

func TestProcessBufferedLogicalMessages_FailsOnUnexpectedByte(t *testing.T) {
	parser := NewWALParser(nil, "slot", "pub", "0/0", []string{"test_data"})
	parser.relations[42] = &RelationInfo{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "test_data",
		Columns: []ColumnInfo{
			{Name: "id", TypeID: 25},
			{Name: "name", TypeID: 25},
		},
	}

	stream := &mockStartCdcStream{}
	handler := &EventHandler{stream: stream}

	parser.logicalBuf = append(parser.logicalBuf, byte('m'))
	parser.logicalBuf = append(parser.logicalBuf, buildInsertMessage(42, "1", "user_1")...)

	err := parser.processBufferedLogicalMessages(handler)
	if err == nil {
		t.Fatal("expected processBufferedLogicalMessages to fail on unexpected byte")
	}
	if len(stream.events) != 0 {
		t.Fatalf("expected 0 events on parser failure, got %d", len(stream.events))
	}
}

func TestCreateChangeEvent_UsesConfiguredPrimaryKeyColumn(t *testing.T) {
	parser := NewWALParser(nil, "slot", "pub", "0/0", []string{"test_data"})
	parser.relations[42] = &RelationInfo{
		RelationID:     42,
		Namespace:      "public",
		RelationName:   "test_data",
		PrimaryKeyCols: []string{"id"},
		Columns:        []ColumnInfo{{Name: "name", TypeID: 25}, {Name: "id", TypeID: 25}},
	}

	stream := &mockStartCdcStream{}
	handler := &EventHandler{
		stream:     stream,
		currentLSN: "0/ABCDEF",
	}

	err := parser.createChangeEvent("INSERT", handler.currentLSN, 42, &RowData{
		RelationID: 42,
		TupleType:  'N',
		Columns: []ColumnData{
			{Name: "name", Value: "user_1", TypeID: 25},
			{Name: "id", Value: "1001", TypeID: 25},
		},
	}, nil, handler)
	if err != nil {
		t.Fatalf("createChangeEvent returned error: %v", err)
	}

	if len(stream.events) != 1 {
		t.Fatalf("expected 1 change event, got %d", len(stream.events))
	}
	if stream.events[0].PrimaryKey != "1001" {
		t.Fatalf("expected primary key 1001, got %s", stream.events[0].PrimaryKey)
	}
}

func buildInsertMessage(relationID uint32, values ...string) []byte {
	msg := []byte{'I'}

	relationBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(relationBuf, relationID)
	msg = append(msg, relationBuf...)
	msg = append(msg, 'N')

	columnCountBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(columnCountBuf, uint16(len(values)))
	msg = append(msg, columnCountBuf...)

	for _, value := range values {
		msg = append(msg, 't')

		lengthBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lengthBuf, uint32(len(value)))
		msg = append(msg, lengthBuf...)
		msg = append(msg, []byte(value)...)
	}

	return msg
}

func buildXLogDataPayload(walData []byte) []byte {
	payload := make([]byte, 24)
	payload = append(payload, walData...)
	return payload
}
