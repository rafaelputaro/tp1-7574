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
	var test interface{}
	if json.Unmarshal([]byte(input), &test) == nil {
		return input
	}

	cleaned := strings.ReplaceAll(input, "'", `"`)
	cleaned = reNoneLiteral.ReplaceAllString(cleaned, `: null$1`)

	return cleaned
}
