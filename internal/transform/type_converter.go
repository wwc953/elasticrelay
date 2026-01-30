package transform

import (
	"fmt"
	"strconv"
	"time"
)

// TypeConverter handles data type conversions.
type TypeConverter struct {
	converters map[string]ConvertFunc
}

// ConvertFunc is a function that converts a value to a specific type.
type ConvertFunc func(value interface{}) (interface{}, error)

// NewTypeConverter creates a new type converter with built-in converters.
func NewTypeConverter() *TypeConverter {
	tc := &TypeConverter{
		converters: make(map[string]ConvertFunc),
	}

	// Register built-in converters
	tc.Register(DataTypeString, tc.toString)
	tc.Register(DataTypeInt, tc.toInt)
	tc.Register(DataTypeInt64, tc.toInt64)
	tc.Register(DataTypeFloat, tc.toFloat64)
	tc.Register(DataTypeFloat64, tc.toFloat64)
	tc.Register(DataTypeBool, tc.toBool)
	tc.Register(DataTypeDate, tc.toDate)
	tc.Register(DataTypeTimestamp, tc.toTimestamp)
	tc.Register(DataTypeKeyword, tc.toString) // ES keyword type
	tc.Register(DataTypeText, tc.toString)    // ES text type
	tc.Register(DataTypeObject, tc.toObject)  // JSON object type (pass-through)

	return tc
}

// Register registers a custom type converter.
func (tc *TypeConverter) Register(typeName string, fn ConvertFunc) {
	tc.converters[typeName] = fn
}

// Convert converts a value to the specified target type.
func (tc *TypeConverter) Convert(value interface{}, targetType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	converter, exists := tc.converters[targetType]
	if !exists {
		return nil, fmt.Errorf("unsupported target type: %s", targetType)
	}

	return converter(value)
}

// ApplyFieldConfigs applies type conversions based on field configurations.
func (tc *TypeConverter) ApplyFieldConfigs(configs []FieldConfig, data map[string]interface{}) (map[string]interface{}, error) {
	for _, config := range configs {
		if config.TargetType == "" || config.Exclude {
			continue
		}

		value, exists := getNestedValue(data, config.Field)
		if !exists || value == nil {
			continue
		}

		converted, err := tc.Convert(value, config.TargetType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s to %s: %w", config.Field, config.TargetType, err)
		}

		setNestedValue(data, config.Field, converted)
	}

	return data, nil
}

// toString converts any value to string.
func (tc *TypeConverter) toString(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case nil:
		return "", nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

// toObject passes through the value as-is (for JSON object/array fields).
func (tc *TypeConverter) toObject(value interface{}) (interface{}, error) {
	// Pass through the value unchanged - it's already a map or slice
	return value, nil
}

// toInt converts a value to int.
func (tc *TypeConverter) toInt(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	case string:
		// Try to parse as integer first
		if i, err := strconv.Atoi(v); err == nil {
			return i, nil
		}
		// Try to parse as float and convert
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int(f), nil
		}
		return nil, fmt.Errorf("cannot convert string '%s' to int", v)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to int", value)
	}
}

// toInt64 converts a value to int64.
func (tc *TypeConverter) toInt64(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		// Try to parse as integer first
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
		// Try to parse as float and convert
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f), nil
		}
		return nil, fmt.Errorf("cannot convert string '%s' to int64", v)
	case bool:
		if v {
			return int64(1), nil
		}
		return int64(0), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// toFloat64 converts a value to float64.
func (tc *TypeConverter) toFloat64(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to float64: %w", v, err)
		}
		return f, nil
	case bool:
		if v {
			return float64(1), nil
		}
		return float64(0), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// toBool converts a value to bool.
func (tc *TypeConverter) toBool(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case string:
		// Handle common boolean string representations
		switch v {
		case "true", "True", "TRUE", "1", "yes", "Yes", "YES", "on", "On", "ON":
			return true, nil
		case "false", "False", "FALSE", "0", "no", "No", "NO", "off", "Off", "OFF", "":
			return false, nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to bool: %w", v, err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// toDate converts a value to RFC3339 date string.
func (tc *TypeConverter) toDate(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case time.Time:
		return v.UTC().Format(time.RFC3339), nil
	case string:
		// Try multiple date formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05.999",
			"2006-01-02 15:04:05",
			"2006-01-02",
			"02/01/2006 15:04:05",
			"02/01/2006",
			"01-02-2006 15:04:05",
			"01-02-2006",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t.UTC().Format(time.RFC3339), nil
			}
		}
		// Return original string if already in valid format
		return v, nil
	case int64:
		// Assume Unix timestamp
		t := time.Unix(v, 0)
		return t.UTC().Format(time.RFC3339), nil
	case float64:
		// Assume Unix timestamp
		t := time.Unix(int64(v), 0)
		return t.UTC().Format(time.RFC3339), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to date", value)
	}
}

// toTimestamp converts a value to Unix timestamp (int64).
func (tc *TypeConverter) toTimestamp(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case time.Time:
		return v.Unix(), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		// Try to parse as date first
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t.Unix(), nil
			}
		}
		// Try to parse as number
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			return ts, nil
		}
		return nil, fmt.Errorf("cannot convert string '%s' to timestamp", v)
	default:
		return nil, fmt.Errorf("cannot convert %T to timestamp", value)
	}
}

// IsNumericType checks if the type name represents a numeric type.
func IsNumericType(typeName string) bool {
	switch typeName {
	case DataTypeInt, DataTypeInt64, DataTypeFloat, DataTypeFloat64, DataTypeTimestamp:
		return true
	default:
		return false
	}
}

// IsStringType checks if the type name represents a string type.
func IsStringType(typeName string) bool {
	switch typeName {
	case DataTypeString, DataTypeKeyword, DataTypeText, DataTypeDate:
		return true
	default:
		return false
	}
}
