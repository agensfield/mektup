package cli

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

const (
	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiDim    = "\x1b[2m"
	ansiRed    = "\x1b[31m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiCyan   = "\x1b[36m"
)

var (
	humanIDPattern = regexp.MustCompile(`\b(?:msg|op|rcpt|evt|ep|store)_[0-9a-f-]+\b`)
	humanWords     = []struct {
		pattern *regexp.Regexp
		color   string
	}{
		{regexp.MustCompile(`(?i)\b(?:healthy|reachable|accepted|applied|success)\b`), ansiGreen},
		{regexp.MustCompile(`(?i)\b(?:attention required|warning|busy)\b`), ansiYellow},
		{regexp.MustCompile(`(?i)\b(?:error|failed|rejected)\b`), ansiRed},
	}
)

// StyleHuman adds terminal decoration after an executor has produced semantic
// text. Machine JSON and portable JSON bypass this function entirely.
func StyleHuman(value string, enabled bool) string {
	value = sanitizeTerminalText(value)
	if !enabled || value == "" {
		return value
	}
	lines := strings.Split(value, "\n")
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		switch {
		case strings.HasPrefix(trimmed, "mektup:"):
			line = ansiBold + ansiRed + line + ansiReset
		case strings.HasPrefix(upper, "OK "):
			line = ansiGreen + line + ansiReset
		case strings.HasPrefix(upper, "NOTICE "):
			line = ansiCyan + line + ansiReset
		case strings.HasPrefix(upper, "WARNING "):
			line = ansiYellow + line + ansiReset
		case strings.HasPrefix(upper, "ERROR ") || strings.HasPrefix(upper, "FAILED "):
			line = ansiRed + line + ansiReset
		case index == 0 || isTableHeading(trimmed):
			line = ansiBold + ansiCyan + line + ansiReset
		}
		for _, item := range humanWords {
			line = item.pattern.ReplaceAllStringFunc(line, func(word string) string {
				return item.color + word + ansiReset
			})
		}
		line = humanIDPattern.ReplaceAllStringFunc(line, func(id string) string {
			return ansiDim + id + ansiReset
		})
		lines[index] = line
	}
	return strings.Join(lines, "\n")
}

func sanitizeTerminalText(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	for _, char := range value {
		switch char {
		case '\n', '\t':
			result.WriteRune(char)
		default:
			if unicode.IsControl(char) || unsafeDirectionalFormat(char) {
				if char <= 0xff {
					_, _ = fmt.Fprintf(&result, `\x%02x`, char)
				} else {
					_, _ = fmt.Fprintf(&result, `\u%04x`, char)
				}
				continue
			}
			result.WriteRune(char)
		}
	}
	return result.String()
}

func unsafeDirectionalFormat(char rune) bool {
	return char == '\u061c' || char == '\u200e' || char == '\u200f' ||
		(char >= '\u202a' && char <= '\u202e') ||
		(char >= '\u2066' && char <= '\u2069')
}

func isTableHeading(value string) bool {
	return value != "" && value == strings.ToUpper(value) && strings.Contains(value, "  ")
}

func humanWarnings(result ExecutionResult) []string {
	seen := make(map[string]bool)
	warnings := make([]string, 0)
	add := func(value any) {
		for _, message := range warningMessages(value) {
			if !seen[message] {
				seen[message] = true
				warnings = append(warnings, message)
			}
		}
	}
	for _, event := range result.Events {
		add(event.Machine)
	}
	add(result.Receipt)
	return warnings
}

func warningMessages(value any) []string {
	if value == nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var object map[string]any
	if json.Unmarshal(encoded, &object) != nil {
		return nil
	}
	raw, _ := object["warnings"].([]any)
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		switch warning := item.(type) {
		case string:
			if warning != "" {
				result = append(result, warning)
			}
		case map[string]any:
			message, _ := warning["message"].(string)
			code, _ := warning["code"].(string)
			if message == "" {
				message = code
			} else if code != "" && !strings.Contains(message, code) {
				message = code + ": " + message
			}
			if message != "" {
				result = append(result, message)
			}
		}
	}
	return result
}

func commandContractHuman(document commandContractDocument) string {
	var output strings.Builder
	_, _ = fmt.Fprintf(&output, "Mektup commands (contract %s)\n", document.Contract)
	for _, command := range document.Commands {
		_, _ = fmt.Fprintf(&output, "\n%s\n  usage: %s\n  result: %s\n  effects: %s\n  availability: %s\n",
			command.Name, command.Usage, command.Result, strings.Join(command.Effects, ", "), command.Availability)
	}
	return strings.TrimRight(output.String(), "\n")
}
