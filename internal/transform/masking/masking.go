// Package masking provides data masking/anonymization functionality for the transform engine.
package masking

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// Strategy constants
const (
	StrategyMask  = "mask"
	StrategyHash  = "hash"
	StrategyToken = "token"
	StrategyRegex = "regex"
)

// Template constants
const (
	TemplatePhone    = "phone"
	TemplateIDCard   = "id_card"
	TemplateEmail    = "email"
	TemplateBankCard = "bank_card"
	TemplateName     = "name"
)

// Rule represents a masking rule.
type Rule struct {
	// Field is the field name to mask
	Field string

	// Strategy is the masking strategy
	Strategy string

	// Params contains strategy-specific parameters
	Params map[string]interface{}

	// Template is the preset template name
	Template string
}

// Strategy interface for masking strategies.
type Strategy interface {
	Mask(value string, params map[string]interface{}) (string, error)
}

// Template represents a preset masking template.
type Template struct {
	Name     string
	Strategy string
	Params   map[string]interface{}
}

// Engine handles data masking.
type Engine struct {
	strategies map[string]Strategy
	templates  map[string]*Template
}

// NewEngine creates a new masking engine with built-in strategies and templates.
func NewEngine() *Engine {
	e := &Engine{
		strategies: make(map[string]Strategy),
		templates:  make(map[string]*Template),
	}

	// Register built-in strategies
	e.RegisterStrategy(StrategyMask, &MaskStrategy{})
	e.RegisterStrategy(StrategyHash, &HashStrategy{})
	e.RegisterStrategy(StrategyToken, &TokenStrategy{})
	e.RegisterStrategy(StrategyRegex, &RegexStrategy{})

	// Register built-in templates
	e.RegisterTemplate(TemplatePhone, &Template{
		Name:     TemplatePhone,
		Strategy: StrategyMask,
		Params:   map[string]interface{}{"prefix": 3, "suffix": 4, "char": "*"},
	})
	e.RegisterTemplate(TemplateIDCard, &Template{
		Name:     TemplateIDCard,
		Strategy: StrategyMask,
		Params:   map[string]interface{}{"prefix": 4, "suffix": 4, "char": "*"},
	})
	e.RegisterTemplate(TemplateEmail, &Template{
		Name:     TemplateEmail,
		Strategy: StrategyRegex,
		Params:   map[string]interface{}{"pattern": `(.{2}).*(@.*)`, "replace": "$1***$2"},
	})
	e.RegisterTemplate(TemplateBankCard, &Template{
		Name:     TemplateBankCard,
		Strategy: StrategyMask,
		Params:   map[string]interface{}{"prefix": 4, "suffix": 4, "char": "*"},
	})
	e.RegisterTemplate(TemplateName, &Template{
		Name:     TemplateName,
		Strategy: StrategyMask,
		Params:   map[string]interface{}{"prefix": 1, "suffix": 0, "char": "*"},
	})

	return e
}

// RegisterStrategy registers a custom masking strategy.
func (e *Engine) RegisterStrategy(name string, strategy Strategy) {
	e.strategies[name] = strategy
}

// RegisterTemplate registers a masking template.
func (e *Engine) RegisterTemplate(name string, template *Template) {
	e.templates[name] = template
}

// Apply applies masking rules to data.
func (e *Engine) Apply(rules []Rule, data map[string]interface{}) (map[string]interface{}, error) {
	for _, rule := range rules {
		value, exists := data[rule.Field]
		if !exists {
			continue
		}

		// Convert value to string
		strValue, ok := value.(string)
		if !ok {
			strValue = fmt.Sprintf("%v", value)
		}

		// Skip empty values
		if strValue == "" {
			continue
		}

		var maskedValue string
		var err error

		if rule.Template != "" {
			// Use preset template
			template, templateExists := e.templates[rule.Template]
			if !templateExists {
				return nil, fmt.Errorf("unknown template: %s", rule.Template)
			}
			maskedValue, err = e.applyStrategy(template.Strategy, strValue, template.Params)
		} else {
			// Use custom strategy
			maskedValue, err = e.applyStrategy(rule.Strategy, strValue, rule.Params)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to mask field %s: %w", rule.Field, err)
		}

		data[rule.Field] = maskedValue
	}

	return data, nil
}

// applyStrategy applies a specific masking strategy.
func (e *Engine) applyStrategy(strategyName string, value string, params map[string]interface{}) (string, error) {
	strategy, exists := e.strategies[strategyName]
	if !exists {
		return "", fmt.Errorf("unknown strategy: %s", strategyName)
	}
	return strategy.Mask(value, params)
}

// MaskStrategy implements character masking (e.g., 138****5678).
type MaskStrategy struct{}

// Mask masks the value by replacing middle characters.
func (s *MaskStrategy) Mask(value string, params map[string]interface{}) (string, error) {
	prefix := getIntParam(params, "prefix", 3)
	suffix := getIntParam(params, "suffix", 4)
	char := getStringParam(params, "char", "*")

	runes := []rune(value)
	length := len(runes)

	if length <= prefix+suffix {
		// Value too short, mask everything
		return strings.Repeat(char, length), nil
	}

	maskLen := length - prefix - suffix
	masked := string(runes[:prefix]) + strings.Repeat(char, maskLen) + string(runes[length-suffix:])
	return masked, nil
}

// HashStrategy implements hash-based masking (SHA256, MD5).
type HashStrategy struct{}

// Mask hashes the value using the specified algorithm.
func (s *HashStrategy) Mask(value string, params map[string]interface{}) (string, error) {
	algorithm := getStringParam(params, "algorithm", "sha256")

	switch algorithm {
	case "sha256":
		hash := sha256.Sum256([]byte(value))
		return hex.EncodeToString(hash[:]), nil
	case "md5":
		hash := md5.Sum([]byte(value))
		return hex.EncodeToString(hash[:]), nil
	default:
		return "", fmt.Errorf("unsupported hash algorithm: %s", algorithm)
	}
}

// TokenStrategy implements tokenization (replacing with a token).
type TokenStrategy struct{}

// Mask replaces the value with a token.
func (s *TokenStrategy) Mask(value string, params map[string]interface{}) (string, error) {
	prefix := getStringParam(params, "prefix", "TOKEN_")
	// Generate a deterministic token based on the value
	hash := sha256.Sum256([]byte(value))
	return prefix + hex.EncodeToString(hash[:8]), nil
}

// RegexStrategy implements regex-based masking.
type RegexStrategy struct{}

// Mask applies regex replacement to mask the value.
func (s *RegexStrategy) Mask(value string, params map[string]interface{}) (string, error) {
	pattern := getStringParam(params, "pattern", "")
	replace := getStringParam(params, "replace", "***")

	if pattern == "" {
		return "***", nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", fmt.Errorf("invalid regex pattern: %w", err)
	}

	return re.ReplaceAllString(value, replace), nil
}

// Helper functions for extracting parameters.

func getIntParam(params map[string]interface{}, key string, defaultValue int) int {
	if params == nil {
		return defaultValue
	}
	if v, ok := params[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case int64:
			return int(val)
		case float64:
			return int(val)
		}
	}
	return defaultValue
}

func getStringParam(params map[string]interface{}, key string, defaultValue string) string {
	if params == nil {
		return defaultValue
	}
	if v, ok := params[key].(string); ok {
		return v
	}
	return defaultValue
}

// MaskPhone masks a phone number (e.g., 13812345678 -> 138****5678).
func MaskPhone(phone string) string {
	s := &MaskStrategy{}
	result, _ := s.Mask(phone, map[string]interface{}{"prefix": 3, "suffix": 4, "char": "*"})
	return result
}

// MaskIDCard masks an ID card number (e.g., 110101199001011234 -> 1101**********1234).
func MaskIDCard(idCard string) string {
	s := &MaskStrategy{}
	result, _ := s.Mask(idCard, map[string]interface{}{"prefix": 4, "suffix": 4, "char": "*"})
	return result
}

// MaskEmail masks an email address (e.g., john.doe@example.com -> jo***@example.com).
func MaskEmail(email string) string {
	s := &RegexStrategy{}
	result, _ := s.Mask(email, map[string]interface{}{"pattern": `(.{2}).*(@.*)`, "replace": "$1***$2"})
	return result
}

// MaskBankCard masks a bank card number (e.g., 6222021234567890 -> 6222********7890).
func MaskBankCard(bankCard string) string {
	s := &MaskStrategy{}
	result, _ := s.Mask(bankCard, map[string]interface{}{"prefix": 4, "suffix": 4, "char": "*"})
	return result
}

// HashSHA256 hashes a value using SHA256.
func HashSHA256(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

// HashMD5 hashes a value using MD5.
func HashMD5(value string) string {
	hash := md5.Sum([]byte(value))
	return hex.EncodeToString(hash[:])
}
