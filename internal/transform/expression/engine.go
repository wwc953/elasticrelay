// Package expression provides expression evaluation functionality for the transform engine.
// Currently implements a basic expression engine with built-in functions.
// Future versions will integrate with goja JavaScript engine for full expression support.
package expression

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ComputedField represents a field to be computed.
type ComputedField struct {
	// Field is the target field name
	Field string

	// Expression is the expression to evaluate
	Expression string

	// Dependencies lists the fields this computation depends on
	Dependencies []string
}

// Engine handles expression evaluation.
type Engine struct {
	// functions is the registry of built-in functions
	functions map[string]Function
}

// Function represents a built-in function.
type Function func(args []interface{}) (interface{}, error)

// Context provides data context for expression evaluation.
type Context struct {
	Data      map[string]interface{}
	Operation string
	PrimaryKey string
}

// NewEngine creates a new expression engine with built-in functions.
func NewEngine() *Engine {
	e := &Engine{
		functions: make(map[string]Function),
	}

	// Register built-in functions
	e.registerBuiltins()

	return e
}

// registerBuiltins registers all built-in functions.
func (e *Engine) registerBuiltins() {
	// String functions
	e.Register("concat", funcConcat)
	e.Register("substr", funcSubstr)
	e.Register("upper", funcUpper)
	e.Register("lower", funcLower)
	e.Register("trim", funcTrim)
	e.Register("replace", funcReplace)
	e.Register("length", funcLength)

	// Math functions
	e.Register("round", funcRound)
	e.Register("abs", funcAbs)
	e.Register("floor", funcFloor)
	e.Register("ceil", funcCeil)
	e.Register("min", funcMin)
	e.Register("max", funcMax)

	// Date/Time functions
	e.Register("now", funcNow)
	e.Register("formatDate", funcFormatDate)
	e.Register("parseDate", funcParseDate)

	// Conditional functions
	e.Register("ifNull", funcIfNull)
	e.Register("ifEmpty", funcIfEmpty)
	e.Register("coalesce", funcCoalesce)
}

// Register registers a custom function.
func (e *Engine) Register(name string, fn Function) {
	e.functions[name] = fn
}

// Compute evaluates computed fields and adds results to data.
func (e *Engine) Compute(fields []ComputedField, data map[string]interface{}, ctx *Context) (map[string]interface{}, error) {
	for _, field := range fields {
		result, err := e.Evaluate(field.Expression, data)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate expression for field %s: %w", field.Field, err)
		}
		data[field.Field] = result
	}
	return data, nil
}

// Evaluate evaluates an expression with the given data context.
// Supports basic expressions like:
// - Field access: $.field_name
// - Function calls: concat($.a, $.b)
// - Ternary: $.age < 18 ? 'minor' : 'adult'
// - Arithmetic: $.price * $.quantity
func (e *Engine) Evaluate(expression string, data map[string]interface{}) (interface{}, error) {
	expr := strings.TrimSpace(expression)

	// Handle special "now()" function
	if expr == "now()" {
		return time.Now().Unix(), nil
	}

	// Handle function calls
	if fnResult, handled, err := e.tryFunctionCall(expr, data); handled {
		return fnResult, err
	}

	// Handle ternary expressions
	if result, handled, err := e.tryTernary(expr, data); handled {
		return result, err
	}

	// Handle arithmetic expressions
	if result, handled, err := e.tryArithmetic(expr, data); handled {
		return result, err
	}

	// Handle comparison expressions
	if result, handled, err := e.tryComparison(expr, data); handled {
		return result, err
	}

	// Handle field access
	if strings.HasPrefix(expr, "$.") {
		fieldPath := expr[2:]
		return getNestedValue(data, fieldPath)
	}

	// Handle string literals
	if (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) ||
		(strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) {
		return expr[1 : len(expr)-1], nil
	}

	// Handle numeric literals
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f, nil
	}

	// Handle boolean literals
	if expr == "true" {
		return true, nil
	}
	if expr == "false" {
		return false, nil
	}

	// Return expression as string if no pattern matches
	return expr, nil
}

// tryFunctionCall attempts to parse and execute a function call.
func (e *Engine) tryFunctionCall(expr string, data map[string]interface{}) (interface{}, bool, error) {
	// Match function call pattern: funcName(args...)
	re := regexp.MustCompile(`^(\w+)\((.+)\)$`)
	matches := re.FindStringSubmatch(expr)
	if matches == nil {
		return nil, false, nil
	}

	funcName := matches[1]
	argsStr := matches[2]

	fn, exists := e.functions[funcName]
	if !exists {
		return nil, false, nil
	}

	// Parse arguments
	args, err := e.parseArgs(argsStr, data)
	if err != nil {
		return nil, true, err
	}

	result, err := fn(args)
	return result, true, err
}

// parseArgs parses function arguments.
func (e *Engine) parseArgs(argsStr string, data map[string]interface{}) ([]interface{}, error) {
	// Simple argument parsing (handles basic cases)
	args := splitArgs(argsStr)
	result := make([]interface{}, len(args))

	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		val, err := e.Evaluate(arg, data)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}

	return result, nil
}

// splitArgs splits function arguments, handling nested parentheses.
func splitArgs(argsStr string) []string {
	var args []string
	var current strings.Builder
	depth := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(argsStr); i++ {
		c := argsStr[i]

		if inString {
			current.WriteByte(c)
			if c == stringChar && (i == 0 || argsStr[i-1] != '\\') {
				inString = false
			}
			continue
		}

		switch c {
		case '"', '\'':
			inString = true
			stringChar = c
			current.WriteByte(c)
		case '(':
			depth++
			current.WriteByte(c)
		case ')':
			depth--
			current.WriteByte(c)
		case ',':
			if depth == 0 {
				args = append(args, current.String())
				current.Reset()
			} else {
				current.WriteByte(c)
			}
		default:
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

// tryTernary attempts to parse and evaluate a ternary expression.
func (e *Engine) tryTernary(expr string, data map[string]interface{}) (interface{}, bool, error) {
	// Match ternary pattern: condition ? trueVal : falseVal
	parts := strings.SplitN(expr, "?", 2)
	if len(parts) != 2 {
		return nil, false, nil
	}

	resultParts := strings.SplitN(parts[1], ":", 2)
	if len(resultParts) != 2 {
		return nil, false, nil
	}

	condition := strings.TrimSpace(parts[0])
	trueVal := strings.TrimSpace(resultParts[0])
	falseVal := strings.TrimSpace(resultParts[1])

	condResult, err := e.Evaluate(condition, data)
	if err != nil {
		return nil, true, err
	}

	if toBool(condResult) {
		result, err := e.Evaluate(trueVal, data)
		return result, true, err
	}
	result, err := e.Evaluate(falseVal, data)
	return result, true, err
}

// tryArithmetic attempts to parse and evaluate arithmetic expressions.
func (e *Engine) tryArithmetic(expr string, data map[string]interface{}) (interface{}, bool, error) {
	// Handle basic arithmetic: +, -, *, /
	operators := []string{" + ", " - ", " * ", " / "}
	for _, op := range operators {
		if idx := strings.Index(expr, op); idx != -1 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(op):])

			leftVal, err := e.Evaluate(left, data)
			if err != nil {
				return nil, true, err
			}
			rightVal, err := e.Evaluate(right, data)
			if err != nil {
				return nil, true, err
			}

			leftNum := toFloat64(leftVal)
			rightNum := toFloat64(rightVal)

			var result float64
			switch strings.TrimSpace(op) {
			case "+":
				result = leftNum + rightNum
			case "-":
				result = leftNum - rightNum
			case "*":
				result = leftNum * rightNum
			case "/":
				if rightNum == 0 {
					return nil, true, fmt.Errorf("division by zero")
				}
				result = leftNum / rightNum
			}

			return floatToJSONNumber(result), true, nil
		}
	}

	return nil, false, nil
}

// tryComparison attempts to parse and evaluate comparison expressions.
func (e *Engine) tryComparison(expr string, data map[string]interface{}) (interface{}, bool, error) {
	// Handle comparisons: <, >, <=, >=, ==, !=
	operators := []struct {
		op   string
		cmp  func(a, b float64) bool
	}{
		{"<=", func(a, b float64) bool { return a <= b }},
		{">=", func(a, b float64) bool { return a >= b }},
		{"!=", func(a, b float64) bool { return a != b }},
		{"==", func(a, b float64) bool { return a == b }},
		{"<", func(a, b float64) bool { return a < b }},
		{">", func(a, b float64) bool { return a > b }},
	}

	for _, op := range operators {
		if idx := strings.Index(expr, " "+op.op+" "); idx != -1 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(op.op)+2:])

			leftVal, err := e.Evaluate(left, data)
			if err != nil {
				return nil, true, err
			}
			rightVal, err := e.Evaluate(right, data)
			if err != nil {
				return nil, true, err
			}

			return op.cmp(toFloat64(leftVal), toFloat64(rightVal)), true, nil
		}
	}

	return nil, false, nil
}

// getNestedValue retrieves a value from a nested map.
func getNestedValue(data map[string]interface{}, path string) (interface{}, error) {
	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			val, exists := v[part]
			if !exists {
				return nil, nil
			}
			current = val
		default:
			return nil, nil
		}
	}

	return current, nil
}

// Helper functions

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

func toBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case int:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != "" && val != "false" && val != "0"
	default:
		return v != nil
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Built-in function implementations

func funcConcat(args []interface{}) (interface{}, error) {
	var result strings.Builder
	for _, arg := range args {
		result.WriteString(toString(arg))
	}
	return result.String(), nil
}

func funcSubstr(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("substr requires at least 2 arguments")
	}
	s := toString(args[0])
	start := int(toFloat64(args[1]))
	length := len(s) - start
	if len(args) >= 3 {
		length = int(toFloat64(args[2]))
	}
	if start < 0 || start >= len(s) {
		return "", nil
	}
	end := start + length
	if end > len(s) {
		end = len(s)
	}
	return s[start:end], nil
}

func funcUpper(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", nil
	}
	return strings.ToUpper(toString(args[0])), nil
}

func funcLower(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", nil
	}
	return strings.ToLower(toString(args[0])), nil
}

func funcTrim(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return "", nil
	}
	return strings.TrimSpace(toString(args[0])), nil
}

func funcReplace(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("replace requires 3 arguments")
	}
	s := toString(args[0])
	old := toString(args[1])
	new := toString(args[2])
	return strings.ReplaceAll(s, old, new), nil
}

func funcLength(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return 0, nil
	}
	return len(toString(args[0])), nil
}

func funcRound(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return json.Number("0"), nil
	}
	val := toFloat64(args[0])
	precision := 0
	if len(args) >= 2 {
		precision = int(toFloat64(args[1]))
	}
	pow := math.Pow(10, float64(precision))
	result := math.Round(val*pow) / pow
	return json.Number(strconv.FormatFloat(result, 'f', precision, 64)), nil
}

func funcAbs(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return json.Number("0"), nil
	}
	return floatToJSONNumber(math.Abs(toFloat64(args[0]))), nil
}

func funcFloor(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return json.Number("0"), nil
	}
	return floatToJSONNumber(math.Floor(toFloat64(args[0]))), nil
}

func funcCeil(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return json.Number("0"), nil
	}
	return floatToJSONNumber(math.Ceil(toFloat64(args[0]))), nil
}

func funcMin(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}
	min := toFloat64(args[0])
	for _, arg := range args[1:] {
		val := toFloat64(arg)
		if val < min {
			min = val
		}
	}
	return floatToJSONNumber(min), nil
}

func funcMax(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}
	max := toFloat64(args[0])
	for _, arg := range args[1:] {
		val := toFloat64(arg)
		if val > max {
			max = val
		}
	}
	return floatToJSONNumber(max), nil
}

// floatToJSONNumber formats a float64 as json.Number, ensuring a decimal point
// is always present so ES consistently maps the field as float, not long.
func floatToJSONNumber(f float64) json.Number {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return json.Number(s)
}

func funcNow(args []interface{}) (interface{}, error) {
	return time.Now().Unix(), nil
}

func funcFormatDate(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("formatDate requires 2 arguments")
	}
	timestamp := int64(toFloat64(args[0]))
	format := toString(args[1])
	t := time.Unix(timestamp, 0)
	// Convert Go format to standard
	goFormat := format
	goFormat = strings.ReplaceAll(goFormat, "YYYY", "2006")
	goFormat = strings.ReplaceAll(goFormat, "MM", "01")
	goFormat = strings.ReplaceAll(goFormat, "DD", "02")
	goFormat = strings.ReplaceAll(goFormat, "HH", "15")
	goFormat = strings.ReplaceAll(goFormat, "mm", "04")
	goFormat = strings.ReplaceAll(goFormat, "ss", "05")
	return t.Format(goFormat), nil
}

func funcParseDate(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, nil
	}
	dateStr := toString(args[0])
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t.Unix(), nil
		}
	}
	return nil, fmt.Errorf("cannot parse date: %s", dateStr)
}

func funcIfNull(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("ifNull requires 2 arguments")
	}
	if args[0] == nil {
		return args[1], nil
	}
	return args[0], nil
}

func funcIfEmpty(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("ifEmpty requires 2 arguments")
	}
	if toString(args[0]) == "" {
		return args[1], nil
	}
	return args[0], nil
}

func funcCoalesce(args []interface{}) (interface{}, error) {
	for _, arg := range args {
		if arg != nil && toString(arg) != "" {
			return arg, nil
		}
	}
	return nil, nil
}
