package cli

import (
	"regexp"
	"strings"
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

func isTableHeading(value string) bool {
	return value != "" && value == strings.ToUpper(value) && strings.Contains(value, "  ")
}
