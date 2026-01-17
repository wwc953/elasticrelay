package transform

import (
	"fmt"
	"strings"
)

// FieldMapper handles field rename/copy/move operations.
type FieldMapper struct{}

// NewFieldMapper creates a new field mapper instance.
func NewFieldMapper() *FieldMapper {
	return &FieldMapper{}
}

// Apply applies field mapping rules to the data.
func (fm *FieldMapper) Apply(mappings []FieldMapping, data map[string]interface{}) (map[string]interface{}, error) {
	if len(mappings) == 0 {
		return data, nil
	}

	// Create a copy of the data to avoid modifying the original
	result := make(map[string]interface{})
	for k, v := range data {
		result[k] = deepCopy(v)
	}

	// Apply each mapping rule
	for _, mapping := range mappings {
		value, exists := getNestedValue(result, mapping.SourceField)
		if !exists {
			continue
		}

		switch mapping.Action {
		case MappingActionRename, MappingActionMove:
			// Remove source field and add to target
			deleteNestedValue(result, mapping.SourceField)
			setNestedValue(result, mapping.TargetField, value)

		case MappingActionCopy:
			// Keep source field and add to target
			setNestedValue(result, mapping.TargetField, value)

		default:
			return nil, fmt.Errorf("unknown mapping action: %s", mapping.Action)
		}
	}

	return result, nil
}

// getNestedValue retrieves a value from a nested map using dot notation path.
// Supports paths like "a.b.c" to access nested fields.
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

// setNestedValue sets a value in a nested map using dot notation path.
// Creates intermediate maps as needed.
func setNestedValue(data map[string]interface{}, path string, value interface{}) {
	if path == "" {
		return
	}

	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		if i == len(parts)-1 {
			// Last part - set the value
			current[part] = value
		} else {
			// Intermediate part - create or traverse nested map
			if next, ok := current[part].(map[string]interface{}); ok {
				current = next
			} else {
				// Create new nested map
				newMap := make(map[string]interface{})
				current[part] = newMap
				current = newMap
			}
		}
	}
}

// deleteNestedValue removes a value from a nested map using dot notation path.
func deleteNestedValue(data map[string]interface{}, path string) {
	if path == "" {
		return
	}

	parts := strings.Split(path, ".")

	if len(parts) == 1 {
		// Simple case - delete from top level
		delete(data, path)
		return
	}

	// Navigate to parent of the target
	current := data
	for i := 0; i < len(parts)-1; i++ {
		if next, ok := current[parts[i]].(map[string]interface{}); ok {
			current = next
		} else {
			// Path doesn't exist, nothing to delete
			return
		}
	}

	// Delete the final key
	delete(current, parts[len(parts)-1])
}

// deepCopy creates a deep copy of the value.
func deepCopy(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = deepCopy(val)
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = deepCopy(val)
		}
		return result

	default:
		// Primitive types are immutable, return as-is
		return value
	}
}

// ProcessFieldConfigs applies field configuration rules (exclude, default values, null handling).
func (fm *FieldMapper) ProcessFieldConfigs(configs []FieldConfig, data map[string]interface{}) (map[string]interface{}, error) {
	if len(configs) == 0 {
		return data, nil
	}

	for _, config := range configs {
		// Handle field exclusion
		if config.Exclude {
			deleteNestedValue(data, config.Field)
			continue
		}

		// Get current value
		value, exists := getNestedValue(data, config.Field)

		// Handle missing or null values
		if !exists || value == nil {
			switch config.NullStrategy {
			case NullStrategyDefault:
				if config.DefaultValue != nil {
					setNestedValue(data, config.Field, config.DefaultValue)
				}
			case NullStrategyRemove:
				deleteNestedValue(data, config.Field)
			case NullStrategyError:
				if config.Required {
					return nil, fmt.Errorf("required field %s is missing or null", config.Field)
				}
			case NullStrategyIgnore, "":
				// Do nothing
			}
		}
	}

	return data, nil
}

// HasNestedPath checks if a path contains nested notation.
func HasNestedPath(path string) bool {
	return strings.Contains(path, ".")
}

// SplitPath splits a dot-notation path into parts.
func SplitPath(path string) []string {
	if path == "" {
		return nil
	}
	return strings.Split(path, ".")
}

// JoinPath joins path parts with dot notation.
func JoinPath(parts ...string) string {
	return strings.Join(parts, ".")
}
