// Package transform provides data transformation engine for ElasticRelay.
// It supports field mapping, type conversion, data masking, expression evaluation, and filtering.
package transform

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

// TransformConfig represents a complete transformation rule configuration.
type TransformConfig struct {
	// ID is the unique identifier for this rule
	ID string `json:"id"`

	// Name is the human-readable name of this rule
	Name string `json:"name"`

	// Description provides additional details about the rule
	Description string `json:"description,omitempty"`

	// SourceID is the data source this rule applies to (empty means global)
	SourceID string `json:"source_id,omitempty"`

	// TablePatterns are the table/collection names this rule applies to (supports wildcards)
	TablePatterns []string `json:"table_patterns,omitempty"`

	// FieldMappings defines field rename/copy/move operations
	FieldMappings []FieldMapping `json:"field_mappings,omitempty"`

	// FieldConfigs defines per-field type conversion and validation
	FieldConfigs []FieldConfig `json:"field_configs,omitempty"`

	// Filters defines conditions for including/excluding records
	Filters []FilterRule `json:"filters,omitempty"`

	// MaskingRules defines data masking/anonymization rules
	MaskingRules []MaskingRule `json:"masking_rules,omitempty"`

	// ComputedFields defines dynamically calculated fields
	ComputedFields []ComputedField `json:"computed_fields,omitempty"`

	// Enabled indicates whether this rule is active
	Enabled bool `json:"enabled"`

	// Priority determines the order of rule application (lower = higher priority)
	Priority int `json:"priority"`
}

// FieldMapping defines a field rename/copy/move operation.
type FieldMapping struct {
	// SourceField is the original field name (supports nested paths like "a.b.c")
	SourceField string `json:"source_field"`

	// TargetField is the new field name (supports nested paths)
	TargetField string `json:"target_field"`

	// Action is the operation type: "rename", "copy", or "move"
	Action string `json:"action"`
}

// FieldConfig defines per-field configuration for type conversion and validation.
type FieldConfig struct {
	// Field is the field name (supports nested paths like "a.b.c")
	Field string `json:"field"`

	// TargetType is the desired data type: "string", "int", "int64", "float", "float64", "bool", "date", "timestamp", "keyword", "text"
	TargetType string `json:"target_type,omitempty"`

	// Required indicates whether this field must be present
	Required bool `json:"required,omitempty"`

	// DefaultValue is the value to use when field is missing or null
	DefaultValue interface{} `json:"default_value,omitempty"`

	// NullStrategy defines how to handle null values: "ignore", "default", "error", "remove"
	NullStrategy string `json:"null_strategy,omitempty"`

	// Validation defines validation rules for this field
	Validation *ValidationRule `json:"validation,omitempty"`

	// Exclude indicates whether to remove this field from output
	Exclude bool `json:"exclude,omitempty"`
}

// ValidationRule defines validation constraints for a field.
type ValidationRule struct {
	// Pattern is a regex pattern for string validation
	Pattern string `json:"pattern,omitempty"`

	// Min is the minimum value for numeric fields
	Min *float64 `json:"min,omitempty"`

	// Max is the maximum value for numeric fields
	Max *float64 `json:"max,omitempty"`

	// MinLength is the minimum length for string fields
	MinLength *int `json:"min_length,omitempty"`

	// MaxLength is the maximum length for string fields
	MaxLength *int `json:"max_length,omitempty"`

	// ErrorMessage is the custom error message when validation fails
	ErrorMessage string `json:"error_message,omitempty"`
}

// FilterRule defines a condition for including/excluding records.
type FilterRule struct {
	// Field is the field name to check
	Field string `json:"field"`

	// Operator is the comparison operator: "eq", "ne", "gt", "gte", "lt", "lte", "in", "nin", "regex", "exists"
	Operator string `json:"operator"`

	// Value is the value to compare against
	Value interface{} `json:"value"`

	// Action is what to do when condition matches: "include", "exclude", "route"
	Action string `json:"action"`

	// RouteTarget is the destination when Action is "route"
	RouteTarget string `json:"route_target,omitempty"`

	// Description provides additional details about the filter
	Description string `json:"description,omitempty"`
}

// MaskingRule defines a data masking/anonymization rule.
type MaskingRule struct {
	// Field is the field name to mask
	Field string `json:"field"`

	// Strategy is the masking strategy: "mask", "hash", "token", "regex"
	Strategy string `json:"strategy,omitempty"`

	// Params contains strategy-specific parameters
	Params map[string]interface{} `json:"params,omitempty"`

	// Template is the preset template name: "phone", "id_card", "email", "bank_card", "name"
	Template string `json:"template,omitempty"`

	// Description provides additional details about the masking rule
	Description string `json:"description,omitempty"`
}

// ComputedField defines a dynamically calculated field.
type ComputedField struct {
	// Field is the target field name for the computed value
	Field string `json:"field"`

	// Expression is the JavaScript expression to evaluate
	Expression string `json:"expression"`

	// Dependencies lists the fields that this computation depends on
	Dependencies []string `json:"dependencies,omitempty"`

	// Description provides additional details about the computation
	Description string `json:"description,omitempty"`
}

// GlobalTransformSettings defines global settings for the transform engine.
type GlobalTransformSettings struct {
	// DefaultNullStrategy is the default strategy for handling null values
	DefaultNullStrategy string `json:"default_null_strategy,omitempty"`

	// EnableValidation enables/disables field validation
	EnableValidation bool `json:"enable_validation"`

	// EnableComputedFields enables/disables computed field evaluation
	EnableComputedFields bool `json:"enable_computed_fields"`

	// EnableMasking enables/disables data masking
	EnableMasking bool `json:"enable_masking"`

	// MaxExpressionTimeoutMs is the maximum time for expression evaluation
	MaxExpressionTimeoutMs int `json:"max_expression_timeout_ms,omitempty"`

	// CacheCompiledRules enables caching of compiled rules for better performance
	CacheCompiledRules bool `json:"cache_compiled_rules"`
}

// TransformRulesConfig is the root configuration containing all transform rules.
type TransformRulesConfig struct {
	// TransformRules is the list of transformation rules
	TransformRules []*TransformConfig `json:"transform_rules"`

	// GlobalSettings contains global configuration options
	GlobalSettings *GlobalTransformSettings `json:"global_settings,omitempty"`

	// MaskingTemplates contains predefined masking templates
	MaskingTemplates map[string]*MaskingTemplateConfig `json:"masking_templates,omitempty"`
}

// MaskingTemplateConfig defines a reusable masking template.
type MaskingTemplateConfig struct {
	// Strategy is the masking strategy
	Strategy string `json:"strategy"`

	// Params contains strategy-specific parameters
	Params map[string]interface{} `json:"params,omitempty"`
}

// NullStrategy constants
const (
	NullStrategyIgnore  = "ignore"
	NullStrategyDefault = "default"
	NullStrategyError   = "error"
	NullStrategyRemove  = "remove"
)

// MappingAction constants
const (
	MappingActionRename = "rename"
	MappingActionCopy   = "copy"
	MappingActionMove   = "move"
)

// FilterOperator constants
const (
	FilterOpEqual       = "eq"
	FilterOpNotEqual    = "ne"
	FilterOpGreater     = "gt"
	FilterOpGreaterOrEq = "gte"
	FilterOpLess        = "lt"
	FilterOpLessOrEq    = "lte"
	FilterOpIn          = "in"
	FilterOpNotIn       = "nin"
	FilterOpRegex       = "regex"
	FilterOpExists      = "exists"
)

// FilterAction constants
const (
	FilterActionInclude = "include"
	FilterActionExclude = "exclude"
	FilterActionRoute   = "route"
)

// MaskingStrategy constants
const (
	MaskingStrategyMask  = "mask"
	MaskingStrategyHash  = "hash"
	MaskingStrategyToken = "token"
	MaskingStrategyRegex = "regex"
)

// DataType constants for type conversion
const (
	DataTypeString    = "string"
	DataTypeInt       = "int"
	DataTypeInt64     = "int64"
	DataTypeFloat     = "float"
	DataTypeFloat64   = "float64"
	DataTypeBool      = "bool"
	DataTypeDate      = "date"
	DataTypeTimestamp = "timestamp"
	DataTypeKeyword   = "keyword" // ES keyword type
	DataTypeText      = "text"    // ES text type
	DataTypeObject    = "object"  // JSON object type (pass-through)
)

// LoadTransformConfig loads transform configuration from a JSON file.
func LoadTransformConfig(path string) ([]*TransformConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read transform config file %s: %w", path, err)
	}

	var config TransformRulesConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse transform config file %s: %w", path, err)
	}

	// Filter enabled rules only
	var enabledRules []*TransformConfig
	for _, rule := range config.TransformRules {
		if rule.Enabled {
			enabledRules = append(enabledRules, rule)
		} else {
			log.Printf("Transform: Skipping disabled rule '%s'", rule.ID)
		}
	}

	log.Printf("Transform: Loaded %d enabled rules from %s", len(enabledRules), path)
	return enabledRules, nil
}

// LoadTransformConfigFromBytes loads transform configuration from JSON bytes.
func LoadTransformConfigFromBytes(data []byte) ([]*TransformConfig, error) {
	var config TransformRulesConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse transform config: %w", err)
	}

	// Filter enabled rules only
	var enabledRules []*TransformConfig
	for _, rule := range config.TransformRules {
		if rule.Enabled {
			enabledRules = append(enabledRules, rule)
		}
	}

	return enabledRules, nil
}
