package internal

import (
	"encoding/json"
	"regexp"
	"strings"
)

var (
	reNoneLiteral = regexp.MustCompile(`:\s*None([\s,}])`)
)

// NormalizeJSON returns a cleaned version of a malformed JSON-like string.
// If it's already valid JSON, it's returned untouched.
func NormalizeJSON(input string) string {
	if isValid(input) {
		return input
	}

	cleaned := strings.ReplaceAll(input, "'", `"`)
	if isValid(cleaned) {
		return cleaned
	}

	cleaned = reNoneLiteral.ReplaceAllString(cleaned, `: null$1`)

	return cleaned
}

// isValid checks if a string is a valid JSON object.
func isValid(s string) bool {
	var js map[string]interface{}
	if err := json.Unmarshal([]byte(s), &js); err != nil {
		return false
	}
	return true
}
