package filter

import (
	"testing"
)

func TestEngine_Check_Equal(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"status": "active",
		"age":    25,
	}

	// Test include with matching condition
	rules := []Rule{
		{Field: "status", Operator: OpEqual, Value: "active", Action: ActionInclude},
	}

	result := engine.Check(rules, data)
	if !result.Include {
		t.Error("should include when status=active")
	}

	// Test include with non-matching condition
	rules = []Rule{
		{Field: "status", Operator: OpEqual, Value: "inactive", Action: ActionInclude},
	}

	result = engine.Check(rules, data)
	if result.Include {
		t.Error("should not include when status!=inactive")
	}
}

func TestEngine_Check_NotEqual(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"status": "deleted",
	}

	// Test exclude rule matching
	rules := []Rule{
		{Field: "status", Operator: OpNotEqual, Value: "deleted", Action: ActionInclude},
	}

	result := engine.Check(rules, data)
	if result.Include {
		t.Error("should not include when status=deleted")
	}
}

func TestEngine_Check_Comparison(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"age":   25,
		"score": 85.5,
	}

	tests := []struct {
		field    string
		operator string
		value    interface{}
		expected bool
	}{
		{"age", OpGreater, 20, true},
		{"age", OpGreater, 30, false},
		{"age", OpGreaterOrEq, 25, true},
		{"age", OpLess, 30, true},
		{"age", OpLess, 20, false},
		{"age", OpLessOrEq, 25, true},
		{"score", OpGreater, 80.0, true},
	}

	for _, test := range tests {
		rules := []Rule{
			{Field: test.field, Operator: test.operator, Value: test.value, Action: ActionInclude},
		}

		result := engine.Check(rules, data)
		if result.Include != test.expected {
			t.Errorf("%s %s %v: expected %v, got %v",
				test.field, test.operator, test.value, test.expected, result.Include)
		}
	}
}

func TestEngine_Check_In(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"status": "pending",
		"type":   "order",
	}

	// Test in operator
	rules := []Rule{
		{Field: "status", Operator: OpIn, Value: []interface{}{"pending", "processing"}, Action: ActionInclude},
	}

	result := engine.Check(rules, data)
	if !result.Include {
		t.Error("should include when status in [pending, processing]")
	}

	// Test not in operator
	rules = []Rule{
		{Field: "status", Operator: OpNotIn, Value: []interface{}{"deleted", "cancelled"}, Action: ActionInclude},
	}

	result = engine.Check(rules, data)
	if !result.Include {
		t.Error("should include when status not in [deleted, cancelled]")
	}
}

func TestEngine_Check_Exists(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"name": "John",
	}

	// Test exists = true
	rules := []Rule{
		{Field: "name", Operator: OpExists, Value: true, Action: ActionInclude},
	}

	result := engine.Check(rules, data)
	if !result.Include {
		t.Error("should include when name exists")
	}

	// Test exists = false for non-existent field
	rules = []Rule{
		{Field: "email", Operator: OpExists, Value: false, Action: ActionInclude},
	}

	result = engine.Check(rules, data)
	if !result.Include {
		t.Error("should include when email does not exist")
	}
}

func TestEngine_Check_Regex(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"email": "john@example.com",
		"phone": "138-1234-5678",
	}

	// Test regex matching
	rules := []Rule{
		{Field: "email", Operator: OpRegex, Value: `.*@example\.com$`, Action: ActionInclude},
	}

	result := engine.Check(rules, data)
	if !result.Include {
		t.Error("should include when email matches pattern")
	}

	// Test regex not matching
	rules = []Rule{
		{Field: "email", Operator: OpRegex, Value: `.*@gmail\.com$`, Action: ActionInclude},
	}

	result = engine.Check(rules, data)
	if result.Include {
		t.Error("should not include when email doesn't match pattern")
	}
}

func TestEngine_Check_NestedField(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"user": map[string]interface{}{
			"status": "active",
			"profile": map[string]interface{}{
				"verified": true,
			},
		},
	}

	rules := []Rule{
		{Field: "user.status", Operator: OpEqual, Value: "active", Action: ActionInclude},
	}

	result := engine.Check(rules, data)
	if !result.Include {
		t.Error("should include when user.status=active")
	}
}

func TestEngine_Check_Exclude(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"status": "deleted",
	}

	// Test exclude action
	rules := []Rule{
		{Field: "status", Operator: OpEqual, Value: "deleted", Action: ActionExclude},
	}

	result := engine.Check(rules, data)
	if result.Include {
		t.Error("should exclude when status=deleted matches exclude rule")
	}
}

func TestEngine_Check_Route(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"priority": "high",
	}

	rules := []Rule{
		{Field: "priority", Operator: OpEqual, Value: "high", Action: ActionRoute, RouteTarget: "priority_queue"},
	}

	result := engine.Check(rules, data)
	if result.Route != "priority_queue" {
		t.Errorf("expected route=priority_queue, got %s", result.Route)
	}
}

func TestEngine_CompileRules(t *testing.T) {
	engine := NewEngine()

	rules := []Rule{
		{Field: "email", Operator: OpRegex, Value: `.*@example\.com`, Action: ActionInclude},
	}

	compiled, err := engine.CompileRules(rules)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if compiled[0].compiledRegex == nil {
		t.Error("regex should be compiled")
	}
}

func BenchmarkEngine_Check(b *testing.B) {
	engine := NewEngine()

	data := map[string]interface{}{
		"status": "active",
		"age":    25,
		"type":   "user",
	}

	rules := []Rule{
		{Field: "status", Operator: OpEqual, Value: "active", Action: ActionInclude},
		{Field: "age", Operator: OpGreater, Value: 18, Action: ActionInclude},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Check(rules, data)
	}
}
