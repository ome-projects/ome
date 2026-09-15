// Package safetext bounds display strings and prevents credential-shaped values
// from entering stable CLI reports.
package safetext

import (
	"regexp"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/printers"
)

const maxInspectionBytes = 8192

var credentialShapes = []*regexp.Regexp{
	regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})`),
	regexp.MustCompile(`(?:^|[^A-Za-z0-9])sk-(?:[A-Za-z0-9_-]{20,})`),
	regexp.MustCompile(`(?:^|[^A-Za-z0-9])(?:AKIA|ASIA)[A-Z0-9]{16}(?:$|[^A-Z0-9])`),
	regexp.MustCompile(`eyJ[A-Za-z0-9_-]{3,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}`),
	regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9])bearer[[:space:]]+[A-Za-z0-9._~+/-]{4,}`),
	regexp.MustCompile(`(?i)(?:^|[^A-Za-z0-9])(?:token|password|secret|api[_-]?key|authorization|credential)[[:space:]]*[:=][[:space:]]*[^[:space:]]{4,}`),
	regexp.MustCompile(`-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----`),
}

// Sanitize returns a display-width-bounded value, or a fixed marker when the
// portion that could be emitted contains a recognized credential shape.
func Sanitize(value string, width int) string {
	inspected := value
	if len(inspected) > maxInspectionBytes {
		inspected = inspected[:maxInspectionBytes]
	}
	for _, shape := range credentialShapes {
		if shape.FindStringIndex(inspected) != nil {
			return "[REDACTED]"
		}
	}
	return printers.BoundedCell(strings.ToValidUTF8(inspected, "�"), width)
}
