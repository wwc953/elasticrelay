package transform

import (
	"context"
	"encoding/json"
	"testing"

	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
)

// TestFieldMapper tests the field mapping functionality
func TestFieldMapper_Rename(t *testing.T) {
	mapper := NewFieldMapper()

	data := map[string]interface{}{
		"old_name": "value",
		"other":    123,
	}

	mappings := []FieldMapping{
		{SourceField: "old_name", TargetField: "new_name", Action: MappingActionRename},
	}

	result, err := mapper.Apply(mappings, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, exists := result["old_name"]; exists {
		t.Error("old_name should not exist after rename")
	}

	if result["new_name"] != "value" {
		t.Errorf("expected new_name=value, got %v", result["new_name"])
	}

	if result["other"] != 123 {
		t.Errorf("expected other=123, got %v", result["other"])
	}
}

func TestFieldMapper_Copy(t *testing.T) {
	mapper := NewFieldMapper()

	data := map[string]interface{}{
		"source": "original",
	}

	mappings := []FieldMapping{
		{SourceField: "source", TargetField: "copy", Action: MappingActionCopy},
	}

	result, err := mapper.Apply(mappings, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result["source"] != "original" {
		t.Errorf("source should still exist, got %v", result["source"])
	}

	if result["copy"] != "original" {
		t.Errorf("expected copy=original, got %v", result["copy"])
	}
}

func TestFieldMapper_NestedPath(t *testing.T) {
	mapper := NewFieldMapper()

	data := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "John",
			"age":  30,
		},
	}

	mappings := []FieldMapping{
		{SourceField: "user.name", TargetField: "username", Action: MappingActionCopy},
	}

	result, err := mapper.Apply(mappings, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result["username"] != "John" {
		t.Errorf("expected username=John, got %v", result["username"])
	}
}

// TestTypeConverter tests type conversion functionality
func TestTypeConverter_ToInt(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		input    interface{}
		expected int
	}{
		{123, 123},
		{int64(456), 456},
		{float64(789.5), 789},
		{"100", 100},
		{true, 1},
		{false, 0},
	}

	for _, test := range tests {
		result, err := converter.Convert(test.input, DataTypeInt)
		if err != nil {
			t.Errorf("unexpected error for input %v: %v", test.input, err)
			continue
		}
		if result != test.expected {
			t.Errorf("input %v: expected %d, got %v", test.input, test.expected, result)
		}
	}
}

func TestTypeConverter_ToFloat64(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		input    interface{}
		expected float64
	}{
		{123.45, 123.45},
		{100, 100.0},
		{"99.9", 99.9},
	}

	for _, test := range tests {
		result, err := converter.Convert(test.input, DataTypeFloat64)
		if err != nil {
			t.Errorf("unexpected error for input %v: %v", test.input, err)
			continue
		}
		if result != test.expected {
			t.Errorf("input %v: expected %f, got %v", test.input, test.expected, result)
		}
	}
}

func TestTypeConverter_ToBool(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		input    interface{}
		expected bool
	}{
		{true, true},
		{false, false},
		{1, true},
		{0, false},
		{"true", true},
		{"false", false},
		{"yes", true},
		{"no", false},
	}

	for _, test := range tests {
		result, err := converter.Convert(test.input, DataTypeBool)
		if err != nil {
			t.Errorf("unexpected error for input %v: %v", test.input, err)
			continue
		}
		if result != test.expected {
			t.Errorf("input %v: expected %v, got %v", test.input, test.expected, result)
		}
	}
}

func TestTypeConverter_ToDate(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		input    interface{}
		hasError bool
	}{
		{"2024-01-15", false},
		{"2024-01-15 10:30:00", false},
		{"2024-01-15T10:30:00Z", false},
		{int64(1705315800), false}, // Unix timestamp
	}

	for _, test := range tests {
		result, err := converter.Convert(test.input, DataTypeDate)
		if test.hasError {
			if err == nil {
				t.Errorf("expected error for input %v", test.input)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error for input %v: %v", test.input, err)
			}
			if result == nil || result == "" {
				t.Errorf("expected non-empty result for input %v", test.input)
			}
		}
	}
}

// TestEngine tests the main transform engine
func TestEngine_PassThrough(t *testing.T) {
	engine, err := NewEngine([]*TransformConfig{}, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	if !engine.IsPassThrough() {
		t.Error("engine should be in pass-through mode with no configs")
	}

	event := &pb.ChangeEvent{
		Op:         "INSERT",
		PrimaryKey: "1",
		Data:       `{"name":"test","age":25}`,
	}

	result, include, err := engine.Transform(context.Background(), event)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	if !include {
		t.Error("event should be included")
	}

	if result.Data != event.Data {
		t.Errorf("pass-through should not modify data")
	}
}

func TestEngine_FieldMapping(t *testing.T) {
	config := &TransformConfig{
		ID:      "test-rule",
		Enabled: true,
		FieldMappings: []FieldMapping{
			{SourceField: "user_name", TargetField: "username", Action: MappingActionRename},
		},
	}

	engine, err := NewEngine([]*TransformConfig{config}, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	event := &pb.ChangeEvent{
		Op:         "INSERT",
		PrimaryKey: "1",
		Data:       `{"user_name":"john","age":25}`,
	}

	result, include, err := engine.Transform(context.Background(), event)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	if !include {
		t.Error("event should be included")
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(result.Data), &data); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}

	if data["username"] != "john" {
		t.Errorf("expected username=john, got %v", data["username"])
	}

	if _, exists := data["user_name"]; exists {
		t.Error("user_name should be renamed to username")
	}
}

func TestEngine_FieldExclusion(t *testing.T) {
	config := &TransformConfig{
		ID:      "test-rule",
		Enabled: true,
		FieldConfigs: []FieldConfig{
			{Field: "internal_notes", Exclude: true},
			{Field: "debug_info", Exclude: true},
		},
	}

	engine, err := NewEngine([]*TransformConfig{config}, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	event := &pb.ChangeEvent{
		Op:         "INSERT",
		PrimaryKey: "1",
		Data:       `{"name":"test","internal_notes":"secret","debug_info":"debug","visible":"yes"}`,
	}

	result, include, err := engine.Transform(context.Background(), event)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	if !include {
		t.Error("event should be included")
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(result.Data), &data); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}

	if _, exists := data["internal_notes"]; exists {
		t.Error("internal_notes should be excluded")
	}

	if _, exists := data["debug_info"]; exists {
		t.Error("debug_info should be excluded")
	}

	if data["visible"] != "yes" {
		t.Error("visible field should remain")
	}
}

func TestEngine_TypeConversion(t *testing.T) {
	config := &TransformConfig{
		ID:      "test-rule",
		Enabled: true,
		FieldConfigs: []FieldConfig{
			{Field: "age", TargetType: DataTypeInt},
			{Field: "price", TargetType: DataTypeFloat64},
			{Field: "active", TargetType: DataTypeBool},
		},
	}

	engine, err := NewEngine([]*TransformConfig{config}, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	event := &pb.ChangeEvent{
		Op:         "INSERT",
		PrimaryKey: "1",
		Data:       `{"age":"25","price":"99.99","active":"true"}`,
	}

	result, include, err := engine.Transform(context.Background(), event)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	if !include {
		t.Error("event should be included")
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(result.Data), &data); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}

	// Note: JSON unmarshalling converts numbers to float64
	if data["age"] != float64(25) {
		t.Errorf("expected age=25, got %v (%T)", data["age"], data["age"])
	}

	if data["price"] != 99.99 {
		t.Errorf("expected price=99.99, got %v", data["price"])
	}

	if data["active"] != true {
		t.Errorf("expected active=true, got %v", data["active"])
	}
}

func TestEngine_TablePatternMatching(t *testing.T) {
	config := &TransformConfig{
		ID:            "user-rule",
		Enabled:       true,
		TablePatterns: []string{"users", "user_*"},
		FieldMappings: []FieldMapping{
			{SourceField: "name", TargetField: "full_name", Action: MappingActionRename},
		},
	}

	engine, err := NewEngine([]*TransformConfig{config}, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// Test matching table
	event1 := &pb.ChangeEvent{
		Op:         "INSERT",
		PrimaryKey: "1",
		Data:       `{"_table":"users","name":"John"}`,
	}

	result1, include1, err := engine.Transform(context.Background(), event1)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	if !include1 {
		t.Error("event should be included")
	}

	var data1 map[string]interface{}
	json.Unmarshal([]byte(result1.Data), &data1)
	if data1["full_name"] != "John" {
		t.Error("name should be renamed to full_name for users table")
	}

	// Test non-matching table
	event2 := &pb.ChangeEvent{
		Op:         "INSERT",
		PrimaryKey: "1",
		Data:       `{"_table":"orders","name":"Order1"}`,
	}

	result2, include2, err := engine.Transform(context.Background(), event2)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	if !include2 {
		t.Error("event should be included")
	}

	var data2 map[string]interface{}
	json.Unmarshal([]byte(result2.Data), &data2)
	if data2["name"] != "Order1" {
		t.Error("name should not be renamed for orders table")
	}
}

// TestNestedPathHelpers tests the nested path helper functions
func TestGetNestedValue(t *testing.T) {
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"profile": map[string]interface{}{
				"name": "John",
			},
		},
		"simple": "value",
	}

	// Test nested path
	val, exists := getNestedValue(data, "user.profile.name")
	if !exists {
		t.Error("user.profile.name should exist")
	}
	if val != "John" {
		t.Errorf("expected John, got %v", val)
	}

	// Test simple path
	val, exists = getNestedValue(data, "simple")
	if !exists {
		t.Error("simple should exist")
	}
	if val != "value" {
		t.Errorf("expected value, got %v", val)
	}

	// Test non-existent path
	_, exists = getNestedValue(data, "nonexistent.path")
	if exists {
		t.Error("nonexistent.path should not exist")
	}
}

func TestSetNestedValue(t *testing.T) {
	data := make(map[string]interface{})

	setNestedValue(data, "user.profile.name", "John")

	val, exists := getNestedValue(data, "user.profile.name")
	if !exists {
		t.Error("user.profile.name should exist after setting")
	}
	if val != "John" {
		t.Errorf("expected John, got %v", val)
	}
}

func TestDeleteNestedValue(t *testing.T) {
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "John",
			"age":  30,
		},
	}

	deleteNestedValue(data, "user.name")

	_, exists := getNestedValue(data, "user.name")
	if exists {
		t.Error("user.name should be deleted")
	}

	// user.age should still exist
	val, exists := getNestedValue(data, "user.age")
	if !exists {
		t.Error("user.age should still exist")
	}
	if val != 30 {
		t.Errorf("expected 30, got %v", val)
	}
}

// Benchmark tests
func BenchmarkEngine_Transform(b *testing.B) {
	config := &TransformConfig{
		ID:      "bench-rule",
		Enabled: true,
		FieldMappings: []FieldMapping{
			{SourceField: "user_name", TargetField: "username", Action: MappingActionRename},
		},
		FieldConfigs: []FieldConfig{
			{Field: "age", TargetType: DataTypeInt},
		},
	}

	engine, _ := NewEngine([]*TransformConfig{config}, nil)

	event := &pb.ChangeEvent{
		Op:         "INSERT",
		PrimaryKey: "123",
		Data:       `{"user_name":"test","age":"25","email":"test@example.com"}`,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Transform(ctx, event)
	}
}

func BenchmarkFieldMapper_Apply(b *testing.B) {
	mapper := NewFieldMapper()

	data := map[string]interface{}{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}

	mappings := []FieldMapping{
		{SourceField: "field1", TargetField: "new_field1", Action: MappingActionRename},
		{SourceField: "field2", TargetField: "copy_field2", Action: MappingActionCopy},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mapper.Apply(mappings, data)
	}
}

func BenchmarkTypeConverter_Convert(b *testing.B) {
	converter := NewTypeConverter()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		converter.Convert("12345", DataTypeInt)
		converter.Convert("99.99", DataTypeFloat64)
		converter.Convert("true", DataTypeBool)
	}
}
