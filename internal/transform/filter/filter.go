// Package filter provides data filtering functionality for the transform engine.
package filter

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// Operator constants
const (
	OpEqual       = "eq"
	OpNotEqual    = "ne"
	OpGreater     = "gt"
	OpGreaterOrEq = "gte"
	OpLess        = "lt"
	OpLessOrEq    = "lte"
	OpIn          = "in"
	OpNotIn       = "nin"
	OpRegex       = "regex"
	OpExists      = "exists"
)

// Action constants
const (
	ActionInclude = "include"
	ActionExclude = "exclude"
	ActionRoute   = "route"
)

// Rule represents a filter rule.
type Rule struct {
	// Field is the field name to check
	Field string

	// Operator is the comparison operator
	Operator string

	// Value is the value to compare against
	Value interface{}

	// Action is what to do when condition matches
	Action string

	// RouteTarget is the destination when Action is "route"
	RouteTarget string

	// compiledRegex caches compiled regex pattern
	compiledRegex *regexp.Regexp
}

// Engine handles data filtering based on rules.
type Engine struct {
	// compiledRules caches compiled filter rules
	compiledRules map[string]*Rule
}

// NewEngine creates a new filter engine.
func NewEngine() *Engine {
	return &Engine{
		compiledRules: make(map[string]*Rule),
	}
}

// Result represents the result of filter evaluation.
type Result struct {
	// Include indicates whether the record should be included
	Include bool

	// Route is the target route (when action is "route")
	Route string
}

// Check evaluates filter rules against data and returns whether the record should be included.
func (e *Engine) Check(rules []Rule, data map[string]interface{}) Result {
	if len(rules) == 0 {
		return Result{Include: true}
	}

	// Default to include if no rules match
	result := Result{Include: true}

	for _, rule := range rules {
		matches := e.evaluateRule(&rule, data)

		switch rule.Action {
		case ActionInclude:
			// If include rule doesn't match, exclude the record
			if !matches {
				result.Include = false
				return result
			}
		case ActionExclude:
			// If exclude rule matches, exclude the record
			if matches {
				result.Include = false
				return result
			}
		case ActionRoute:
			// If route rule matches, set the route target
			if matches {
				result.Route = rule.RouteTarget
			}
		}
	}

	return result
}

// evaluateRule evaluates a single rule against data.
func (e *Engine) evaluateRule(rule *Rule, data map[string]interface{}) bool {
	// Get field value using nested path support
	value, exists := getNestedValue(data, rule.Field)

	// Handle "exists" operator specially
	if rule.Operator == OpExists {
		expectedExists, ok := rule.Value.(bool)
		if !ok {
			expectedExists = true
		}
		return exists == expectedExists
	}

	// If field doesn't exist, rule doesn't match (except for "exists" handled above)
	if !exists {
		return false
	}

	// Evaluate based on operator
	switch rule.Operator {
	case OpEqual:
		return isEqual(value, rule.Value)
	case OpNotEqual:
		return !isEqual(value, rule.Value)
	case OpGreater:
		return compare(value, rule.Value) > 0
	case OpGreaterOrEq:
		return compare(value, rule.Value) >= 0
	case OpLess:
		return compare(value, rule.Value) < 0
	case OpLessOrEq:
		return compare(value, rule.Value) <= 0
	case OpIn:
		return isIn(value, rule.Value)
	case OpNotIn:
		return !isIn(value, rule.Value)
	case OpRegex:
		return matchesRegex(value, rule.Value, rule.compiledRegex)
	default:
		return false
	}
}

// getNestedValue retrieves a value from a nested map using dot notation path.
func getNestedValue(data map[string]interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}

	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			val, exists := v[part]
			if !exists {
				return nil, false
			}
			current = val
		default:
			return nil, false
		}
	}

	return current, true
}

// isEqual compares two values for equality.
func isEqual(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try to normalize numeric types for comparison
	aFloat, aIsFloat := toFloat64(a)
	bFloat, bIsFloat := toFloat64(b)
	if aIsFloat && bIsFloat {
		return aFloat == bFloat
	}

	// String comparison
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr && bIsStr {
		return aStr == bStr
	}

	// Boolean comparison
	aBool, aIsBool := a.(bool)
	bBool, bIsBool := b.(bool)
	if aIsBool && bIsBool {
		return aBool == bBool
	}

	// Fallback to reflect.DeepEqual
	return reflect.DeepEqual(a, b)
}

// compare compares two values and returns -1, 0, or 1.
func compare(a, b interface{}) int {
	// Try numeric comparison
	aFloat, aIsFloat := toFloat64(a)
	bFloat, bIsFloat := toFloat64(b)
	if aIsFloat && bIsFloat {
		if aFloat < bFloat {
			return -1
		}
		if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// String comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

// isIn checks if value is in the list.
func isIn(value, list interface{}) bool {
	// Handle slice/array
	switch l := list.(type) {
	case []interface{}:
		for _, item := range l {
			if isEqual(value, item) {
				return true
			}
		}
	case []string:
		strValue := fmt.Sprintf("%v", value)
		for _, item := range l {
			if strValue == item {
				return true
			}
		}
	case []int:
		intValue, ok := toInt(value)
		if ok {
			for _, item := range l {
				if intValue == item {
					return true
				}
			}
		}
	case []float64:
		floatValue, ok := toFloat64(value)
		if ok {
			for _, item := range l {
				if floatValue == item {
					return true
				}
			}
		}
	}
	return false
}

// matchesRegex checks if value matches the regex pattern.
func matchesRegex(value, pattern interface{}, compiled *regexp.Regexp) bool {
	strValue := fmt.Sprintf("%v", value)
	strPattern, ok := pattern.(string)
	if !ok {
		return false
	}

	// Use cached compiled regex if available
	if compiled != nil {
		return compiled.MatchString(strValue)
	}

	// Compile and match
	re, err := regexp.Compile(strPattern)
	if err != nil {
		return false
	}

	return re.MatchString(strValue)
}

// toFloat64 attempts to convert value to float64.
func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

// toInt attempts to convert value to int.
func toInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case uint:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	case float32:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

// CompileRules pre-compiles regex patterns for better performance.
func (e *Engine) CompileRules(rules []Rule) ([]Rule, error) {
	compiled := make([]Rule, len(rules))
	for i, rule := range rules {
		compiled[i] = rule
		if rule.Operator == OpRegex {
			pattern, ok := rule.Value.(string)
			if !ok {
				return nil, fmt.Errorf("regex pattern must be a string for field %s", rule.Field)
			}
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("invalid regex pattern for field %s: %w", rule.Field, err)
			}
			compiled[i].compiledRegex = re
		}
	}
	return compiled, nil
}
