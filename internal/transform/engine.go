package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yogoosoft/elasticrelay/internal/transform/expression"
	"github.com/yogoosoft/elasticrelay/internal/transform/filter"
	"github.com/yogoosoft/elasticrelay/internal/transform/masking"

	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
)

// Engine is the main transform engine that coordinates all transformation operations.
type Engine struct {
	mu sync.RWMutex

	// Configuration
	configs        []*TransformConfig
	globalSettings *GlobalTransformSettings

	// Sub-engines
	fieldMapper   *FieldMapper
	typeConverter *TypeConverter
	filterEngine  *filter.Engine
	maskingEngine *masking.Engine
	exprEngine    *expression.Engine

	// Statistics
	stats *EngineStats
}

// EngineStats tracks transformation statistics.
type EngineStats struct {
	ProcessedCount uint64
	ErrorCount     uint64
	FilteredCount  uint64
	LastProcessed  time.Time
}

// IncrementProcessed increments the processed counter.
func (s *EngineStats) IncrementProcessed() {
	atomic.AddUint64(&s.ProcessedCount, 1)
	s.LastProcessed = time.Now()
}

// IncrementError increments the error counter.
func (s *EngineStats) IncrementError() {
	atomic.AddUint64(&s.ErrorCount, 1)
}

// IncrementFiltered increments the filtered counter.
func (s *EngineStats) IncrementFiltered() {
	atomic.AddUint64(&s.FilteredCount, 1)
}

// NewEngine creates a new Transform engine with the given configurations.
func NewEngine(configs []*TransformConfig, settings *GlobalTransformSettings) (*Engine, error) {
	if settings == nil {
		settings = &GlobalTransformSettings{
			DefaultNullStrategy:  NullStrategyIgnore,
			EnableValidation:     true,
			EnableComputedFields: true,
			EnableMasking:        true,
		}
	}

	engine := &Engine{
		configs:        configs,
		globalSettings: settings,
		fieldMapper:    NewFieldMapper(),
		typeConverter:  NewTypeConverter(),
		filterEngine:   filter.NewEngine(),
		maskingEngine:  masking.NewEngine(),
		exprEngine:     expression.NewEngine(),
		stats:          &EngineStats{},
	}

	// Sort configs by priority
	engine.sortConfigsByPriority()

	log.Printf("Transform Engine created with %d rules", len(configs))
	return engine, nil
}

// sortConfigsByPriority sorts configurations by priority (lower = higher priority).
func (e *Engine) sortConfigsByPriority() {
	sort.Slice(e.configs, func(i, j int) bool {
		return e.configs[i].Priority < e.configs[j].Priority
	})
}

// Transform processes a single change event through the transformation pipeline.
func (e *Engine) Transform(ctx context.Context, event *pb.ChangeEvent) (*pb.ChangeEvent, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Parse event data using UseNumber() to preserve numeric representations
	// (e.g. "3200.00" stays as json.Number instead of becoming float64(3200))
	var data map[string]interface{}
	dec := json.NewDecoder(strings.NewReader(event.Data))
	dec.UseNumber()
	if err := dec.Decode(&data); err != nil {
		e.stats.IncrementError()
		return nil, false, fmt.Errorf("failed to parse event data: %w", err)
	}

	// Extract table name from data
	tableName := e.extractTableName(data)
	// Extract source_id from data first (more reliable), fallback to Checkpoint
	sourceType := e.extractSourceID(data)
	if sourceType == "" && event.Checkpoint != nil {
		sourceType = event.Checkpoint.SourceType
	}

	// Debug: Log extracted table name (first few events only to avoid log spam)
	if e.stats.ProcessedCount < 5 {
		log.Printf("Transform: Extracted table='%s', sourceType='%s', PK='%s'", tableName, sourceType, event.PrimaryKey)
	}

	// Match and apply rules
	matchedRules := e.matchRules(tableName, sourceType)

	// Debug: Log matched rules count
	if e.stats.ProcessedCount < 5 {
		ruleIDs := make([]string, len(matchedRules))
		for i, r := range matchedRules {
			ruleIDs[i] = r.ID
		}
		log.Printf("Transform: Table='%s' matched %d rules: %v", tableName, len(matchedRules), ruleIDs)
	}

	// If no rules match, pass through unchanged
	if len(matchedRules) == 0 {
		e.stats.IncrementProcessed()
		return event, true, nil
	}

	// Apply each matching rule
	var shouldInclude = true
	for _, rule := range matchedRules {
		var err error
		data, shouldInclude, err = e.applyRule(ctx, rule, data, event)
		if err != nil {
			e.stats.IncrementError()
			return nil, false, fmt.Errorf("failed to apply rule %s: %w", rule.ID, err)
		}

		// If filtered out, stop processing
		if !shouldInclude {
			e.stats.IncrementFiltered()
			return nil, false, nil
		}
	}

	// Serialize result
	resultData, err := json.Marshal(data)
	if err != nil {
		e.stats.IncrementError()
		return nil, false, fmt.Errorf("failed to marshal result: %w", err)
	}

	// Build result event
	result := &pb.ChangeEvent{
		Op:         event.Op,
		Checkpoint: event.Checkpoint,
		PrimaryKey: event.PrimaryKey,
		Data:       string(resultData),
	}

	e.stats.IncrementProcessed()
	return result, true, nil
}

// extractTableName extracts the table/collection name from event data.
func (e *Engine) extractTableName(data map[string]interface{}) string {
	// Check common table name fields
	for _, field := range []string{"_table", "_collection", "_tableName", "table"} {
		if val, ok := data[field].(string); ok && val != "" {
			return val
		}
	}
	return ""
}

// extractSourceID extracts the source_id from event data.
func (e *Engine) extractSourceID(data map[string]interface{}) string {
	if val, ok := data["_source_id"].(string); ok && val != "" {
		return val
	}
	return ""
}

// matchRules finds all rules that apply to the given table and source.
func (e *Engine) matchRules(tableName, sourceType string) []*TransformConfig {
	var matched []*TransformConfig

	for _, config := range e.configs {
		if !config.Enabled {
			continue
		}

		// Check source ID match (empty means global)
		if config.SourceID != "" && config.SourceID != sourceType {
			continue
		}

		// Check table pattern match
		if len(config.TablePatterns) > 0 {
			if !e.matchesTablePattern(tableName, config.TablePatterns) {
				continue
			}
		}

		matched = append(matched, config)
	}

	return matched
}

// matchesTablePattern checks if table name matches any of the patterns.
func (e *Engine) matchesTablePattern(tableName string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchPattern(pattern, tableName) {
			return true
		}
	}
	return false
}

// matchPattern matches a pattern against a string.
// Supports * wildcard.
func matchPattern(pattern, str string) bool {
	// Exact match
	if pattern == str {
		return true
	}

	// Wildcard patterns
	if strings.Contains(pattern, "*") {
		// Use filepath.Match for glob-style matching
		matched, _ := filepath.Match(pattern, str)
		return matched
	}

	return false
}

// applyRule applies a single transformation rule to the data.
func (e *Engine) applyRule(ctx context.Context, rule *TransformConfig, data map[string]interface{}, event *pb.ChangeEvent) (map[string]interface{}, bool, error) {
	var err error

	// 1. Apply filters first
	if len(rule.Filters) > 0 {
		filterRules := e.convertFilterRules(rule.Filters)
		result := e.filterEngine.Check(filterRules, data)
		if !result.Include {
			return data, false, nil
		}
	}

	// 2. Apply field mappings
	if len(rule.FieldMappings) > 0 {
		data, err = e.fieldMapper.Apply(rule.FieldMappings, data)
		if err != nil {
			return nil, false, err
		}
	}

	// 3. Process field configs (exclusions, defaults, null handling)
	if len(rule.FieldConfigs) > 0 {
		data, err = e.fieldMapper.ProcessFieldConfigs(rule.FieldConfigs, data)
		if err != nil {
			return nil, false, err
		}

		// Apply type conversions
		data, err = e.typeConverter.ApplyFieldConfigs(rule.FieldConfigs, data)
		if err != nil {
			return nil, false, err
		}
	}

	// 4. Apply masking rules
	if e.globalSettings.EnableMasking && len(rule.MaskingRules) > 0 {
		maskingRules := e.convertMaskingRules(rule.MaskingRules)
		data, err = e.maskingEngine.Apply(maskingRules, data)
		if err != nil {
			return nil, false, err
		}
	}

	// 5. Compute calculated fields
	if e.globalSettings.EnableComputedFields && len(rule.ComputedFields) > 0 {
		computedFields := e.convertComputedFields(rule.ComputedFields)
		exprCtx := &expression.Context{
			Data:       data,
			Operation:  event.Op,
			PrimaryKey: event.PrimaryKey,
		}
		data, err = e.exprEngine.Compute(computedFields, data, exprCtx)
		if err != nil {
			return nil, false, err
		}
	}

	return data, true, nil
}

// convertFilterRules converts config FilterRules to filter.Rule.
func (e *Engine) convertFilterRules(rules []FilterRule) []filter.Rule {
	result := make([]filter.Rule, len(rules))
	for i, r := range rules {
		result[i] = filter.Rule{
			Field:       r.Field,
			Operator:    r.Operator,
			Value:       r.Value,
			Action:      r.Action,
			RouteTarget: r.RouteTarget,
		}
	}
	return result
}

// convertMaskingRules converts config MaskingRules to masking.Rule.
func (e *Engine) convertMaskingRules(rules []MaskingRule) []masking.Rule {
	result := make([]masking.Rule, len(rules))
	for i, r := range rules {
		result[i] = masking.Rule{
			Field:    r.Field,
			Strategy: r.Strategy,
			Params:   r.Params,
			Template: r.Template,
		}
	}
	return result
}

// convertComputedFields converts config ComputedFields to expression.ComputedField.
func (e *Engine) convertComputedFields(fields []ComputedField) []expression.ComputedField {
	result := make([]expression.ComputedField, len(fields))
	for i, f := range fields {
		result[i] = expression.ComputedField{
			Field:        f.Field,
			Expression:   f.Expression,
			Dependencies: f.Dependencies,
		}
	}
	return result
}

// LoadConfig loads new configuration into the engine.
func (e *Engine) LoadConfig(configs []*TransformConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.configs = configs
	e.sortConfigsByPriority()

	log.Printf("Transform Engine reloaded with %d rules", len(configs))
	return nil
}

// GetStats returns current engine statistics.
func (e *Engine) GetStats() EngineStats {
	return EngineStats{
		ProcessedCount: atomic.LoadUint64(&e.stats.ProcessedCount),
		ErrorCount:     atomic.LoadUint64(&e.stats.ErrorCount),
		FilteredCount:  atomic.LoadUint64(&e.stats.FilteredCount),
		LastProcessed:  e.stats.LastProcessed,
	}
}

// GetConfigs returns the current configurations.
func (e *Engine) GetConfigs() []*TransformConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.configs
}

// IsPassThrough returns true if no transformation rules are configured.
func (e *Engine) IsPassThrough() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.configs) == 0
}
