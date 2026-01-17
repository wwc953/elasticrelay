package masking

import (
	"strings"
	"testing"
)

func TestMaskStrategy_Phone(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"phone": "13812345678",
	}

	rules := []Rule{
		{Field: "phone", Template: TemplatePhone},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "138****5678"
	if result["phone"] != expected {
		t.Errorf("expected %s, got %v", expected, result["phone"])
	}
}

func TestMaskStrategy_IDCard(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"id_card": "110101199001011234",
	}

	rules := []Rule{
		{Field: "id_card", Template: TemplateIDCard},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "1101**********1234"
	if result["id_card"] != expected {
		t.Errorf("expected %s, got %v", expected, result["id_card"])
	}
}

func TestMaskStrategy_Email(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"email": "john.doe@example.com",
	}

	rules := []Rule{
		{Field: "email", Template: TemplateEmail},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	maskedEmail := result["email"].(string)
	if !strings.Contains(maskedEmail, "***") {
		t.Errorf("email should be masked, got %s", maskedEmail)
	}
	if !strings.Contains(maskedEmail, "@example.com") {
		t.Errorf("email domain should be preserved, got %s", maskedEmail)
	}
}

func TestMaskStrategy_BankCard(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"bank_card": "6222021234567890",
	}

	rules := []Rule{
		{Field: "bank_card", Template: TemplateBankCard},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "6222********7890"
	if result["bank_card"] != expected {
		t.Errorf("expected %s, got %v", expected, result["bank_card"])
	}
}

func TestHashStrategy_SHA256(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"password": "secret123",
	}

	rules := []Rule{
		{Field: "password", Strategy: StrategyHash, Params: map[string]interface{}{"algorithm": "sha256"}},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hashedPassword := result["password"].(string)
	if len(hashedPassword) != 64 { // SHA256 hex = 64 chars
		t.Errorf("expected 64 char hash, got %d chars: %s", len(hashedPassword), hashedPassword)
	}

	// Verify deterministic hashing
	data2 := map[string]interface{}{
		"password": "secret123",
	}
	result2, _ := engine.Apply(rules, data2)
	if result["password"] != result2["password"] {
		t.Error("same input should produce same hash")
	}
}

func TestHashStrategy_MD5(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"password": "secret123",
	}

	rules := []Rule{
		{Field: "password", Strategy: StrategyHash, Params: map[string]interface{}{"algorithm": "md5"}},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hashedPassword := result["password"].(string)
	if len(hashedPassword) != 32 { // MD5 hex = 32 chars
		t.Errorf("expected 32 char hash, got %d chars: %s", len(hashedPassword), hashedPassword)
	}
}

func TestTokenStrategy(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"ssn": "123-45-6789",
	}

	rules := []Rule{
		{Field: "ssn", Strategy: StrategyToken, Params: map[string]interface{}{"prefix": "SSN_"}},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tokenized := result["ssn"].(string)
	if !strings.HasPrefix(tokenized, "SSN_") {
		t.Errorf("expected SSN_ prefix, got %s", tokenized)
	}
}

func TestRegexStrategy(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"ip": "192.168.1.100",
	}

	rules := []Rule{
		{Field: "ip", Strategy: StrategyRegex, Params: map[string]interface{}{
			"pattern": `(\d+)\.(\d+)\.(\d+)\.(\d+)`,
			"replace": "$1.$2.xxx.xxx",
		}},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "192.168.xxx.xxx"
	if result["ip"] != expected {
		t.Errorf("expected %s, got %v", expected, result["ip"])
	}
}

func TestMaskStrategy_CustomParams(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"custom": "ABCDEFGHIJ",
	}

	rules := []Rule{
		{Field: "custom", Strategy: StrategyMask, Params: map[string]interface{}{
			"prefix": 2,
			"suffix": 2,
			"char":   "#",
		}},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "AB######IJ"
	if result["custom"] != expected {
		t.Errorf("expected %s, got %v", expected, result["custom"])
	}
}

func TestMaskStrategy_ShortValue(t *testing.T) {
	strategy := &MaskStrategy{}

	// Value shorter than prefix + suffix
	result, err := strategy.Mask("AB", map[string]interface{}{"prefix": 3, "suffix": 4})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != "**" {
		t.Errorf("expected **, got %s", result)
	}
}

func TestEngine_MissingField(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"name": "John",
	}

	rules := []Rule{
		{Field: "nonexistent", Template: TemplatePhone},
	}

	result, err := engine.Apply(rules, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not add the field or error
	if _, exists := result["nonexistent"]; exists {
		t.Error("should not add nonexistent field")
	}
}

func TestEngine_UnknownTemplate(t *testing.T) {
	engine := NewEngine()

	data := map[string]interface{}{
		"field": "value",
	}

	rules := []Rule{
		{Field: "field", Template: "unknown_template"},
	}

	_, err := engine.Apply(rules, data)
	if err == nil {
		t.Error("expected error for unknown template")
	}
}

func TestHelperFunctions(t *testing.T) {
	// Test MaskPhone
	result := MaskPhone("13812345678")
	if result != "138****5678" {
		t.Errorf("MaskPhone: expected 138****5678, got %s", result)
	}

	// Test MaskIDCard
	result = MaskIDCard("110101199001011234")
	if result != "1101**********1234" {
		t.Errorf("MaskIDCard: expected 1101**********1234, got %s", result)
	}

	// Test MaskBankCard
	result = MaskBankCard("6222021234567890")
	if result != "6222********7890" {
		t.Errorf("MaskBankCard: expected 6222********7890, got %s", result)
	}

	// Test HashSHA256
	hash := HashSHA256("test")
	if len(hash) != 64 {
		t.Errorf("HashSHA256: expected 64 chars, got %d", len(hash))
	}

	// Test HashMD5
	hash = HashMD5("test")
	if len(hash) != 32 {
		t.Errorf("HashMD5: expected 32 chars, got %d", len(hash))
	}
}

func BenchmarkMaskStrategy(b *testing.B) {
	strategy := &MaskStrategy{}
	params := map[string]interface{}{"prefix": 3, "suffix": 4, "char": "*"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.Mask("13812345678", params)
	}
}

func BenchmarkHashStrategy_SHA256(b *testing.B) {
	strategy := &HashStrategy{}
	params := map[string]interface{}{"algorithm": "sha256"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.Mask("secret_password_123", params)
	}
}

func BenchmarkEngine_Apply(b *testing.B) {
	engine := NewEngine()

	data := map[string]interface{}{
		"phone":     "13812345678",
		"id_card":   "110101199001011234",
		"email":     "test@example.com",
		"bank_card": "6222021234567890",
	}

	rules := []Rule{
		{Field: "phone", Template: TemplatePhone},
		{Field: "id_card", Template: TemplateIDCard},
		{Field: "email", Template: TemplateEmail},
		{Field: "bank_card", Template: TemplateBankCard},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Need to copy data since Apply modifies in place
		dataCopy := make(map[string]interface{})
		for k, v := range data {
			dataCopy[k] = v
		}
		engine.Apply(rules, dataCopy)
	}
}
