package safetext_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/safetext"
)

func TestSanitizeRecognizesCredentialShapesAndPreservesOrdinaryReasons(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"ghp_0123456789abcdefghijklmnopqrstuvwxyz",
		"sk-proj-0123456789abcdefghijklmnopqrstuvwxyz",
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJl",
		"AKIAIOSFODNN7EXAMPLE",
		"Bearer secret",
		"password=hunter2",
		"failure_ghp_0123456789abcdefghijklmnopqrstuvwxyz",
		"context_password=hunter2",
		"context_Bearer secret-token",
		"-----BEGIN PRIVATE KEY-----",
	} {
		assert.Equal(t, "[REDACTED]", safetext.Sanitize("prefix "+value, 256))
	}
	for _, value := range []string{"TokenExpired", "SecretNotFound", "CredentialRotationCompleted", "ordinary reason"} {
		assert.Equal(t, value, safetext.Sanitize(value, 256))
	}
}

func TestSanitizeBoundsHostileUnicodeWithoutControls(t *testing.T) {
	t.Parallel()
	value := "safe\x00\x1b[31m\u202e" + strings.Repeat("界", 10000)
	got := safetext.Sanitize(value, 32)
	assert.True(t, utf8.ValidString(got))
	assert.LessOrEqual(t, printers.CellDisplayWidth(got), 32)
	assert.NotContains(t, got, "\x1b")
	assert.NotContains(t, got, "\u202e")
}

func TestSanitizePreservesIdentifiersWithEmbeddedSK(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"task-0123456789abcdefghijklmnopqrstuvwxyz",
		"disk-0123456789abcdefghijklmnopqrstuvwxyz",
		"mask-0123456789abcdefghijklmnopqrstuvwxyz",
	} {
		t.Run(value, func(t *testing.T) {
			assert.Equal(t, value, safetext.Sanitize(value, 256))
		})
	}
}

func TestSanitizeRecognizesSKWithNonAlphanumericLeftBoundary(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"sk-proj-0123456789abcdefghijklmnopqrstuvwxyz",
		"Bearer sk-proj-0123456789abcdefghijklmnopqrstuvwxyz",
		"prefix sk-proj-0123456789abcdefghijklmnopqrstuvwxyz",
		"context_sk-proj-0123456789abcdefghijklmnopqrstuvwxyz",
		"failure-sk-proj-0123456789abcdefghijklmnopqrstuvwxyz",
		"(sk-proj-0123456789abcdefghijklmnopqrstuvwxyz)",
	} {
		t.Run(value, func(t *testing.T) {
			assert.Equal(t, "[REDACTED]", safetext.Sanitize(value, 256))
		})
	}
}

func TestSanitizeDoesNotRecognizeSKConcatenatedAfterAlphanumericPrefix(t *testing.T) {
	t.Parallel()
	// Distinguishing embedded sk- identifiers also leaves directly concatenated
	// token-shaped strings undetected by the sk- heuristic.
	value := "contextsk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	assert.Equal(t, value, safetext.Sanitize(value, 256))
}

func TestSanitizeRecognizesSlackCredentialShapes(t *testing.T) {
	t.Parallel()
	botToken := strings.Join([]string{"xoxb", "123456789012", "123456789012", "abcdefghijklmnopqrstuvwxyz"}, "-")
	appToken := strings.Join([]string{"xapp", "1", "A1234567890", "1234567890123", "abcdefghijklmnopqrstuvwxyz"}, "-")
	webhook := strings.Join([]string{"https://hooks.slack.com", "services", "T12345678", "B12345678", "abcdefghijklmnopqrstuvwx"}, "/")

	for _, value := range []string{botToken, appToken, webhook} {
		assert.Equal(t, "[REDACTED]", safetext.Sanitize("prefix "+value, 256))
	}
}

func TestSanitizeRecognizesSlackWebhookPrefixAnywhereCaseInsensitively(t *testing.T) {
	t.Parallel()
	prefix := strings.Join([]string{"https:", "", "hooks.slack.com", "services", ""}, "/")
	fullWebhook := prefix + strings.Join([]string{"T12345678", "B12345678", "abcdefghijklmnopqrstuvwx"}, "/")

	tests := []struct {
		name  string
		value string
	}{
		{name: "embedded", value: "audit=" + fullWebhook + "&source=operator"},
		{name: "uppercase scheme and host", value: "before " + strings.ToUpper(fullWebhook) + " after"},
		{name: "adversarial prefix and suffix", value: "javascript:" + prefix + "?redirect=after"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, "[REDACTED]", safetext.Sanitize(tt.value, 256))
		})
	}
}

func TestSanitizeRecognizesGovSlackWebhookPrefixAnywhereCaseInsensitively(t *testing.T) {
	t.Parallel()
	prefix := strings.Join([]string{"https:", "", "hooks.slack-gov.com", "services", ""}, "/")
	fullWebhook := prefix + strings.Join([]string{"T12345678", "B12345678", "abcdefghijklmnopqrstuvwx"}, "/")

	tests := []struct {
		name  string
		value string
	}{
		{name: "exact", value: fullWebhook},
		{name: "embedded and wrapped", value: "audit=<" + fullWebhook + ">&source=operator"},
		{name: "uppercase scheme and host", value: "before " + strings.ToUpper(fullWebhook) + " after"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, "[REDACTED]", safetext.Sanitize(tt.value, 256))
		})
	}
}

func TestSanitizePreservesOrdinaryNonWebhookSlackText(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		strings.Join([]string{"https:", "", "hooks.slack.com", "services"}, "/"),
		strings.Join([]string{"http:", "", "hooks.slack.com", "services", "public"}, "/"),
		strings.Join([]string{"https:", "", "hooks.slack.com", "archives", "public"}, "/"),
		"hooks.slack.com/services/public",
		strings.Join([]string{"https:", "", "hooks.slack-gov.com", "services"}, "/"),
		strings.Join([]string{"http:", "", "hooks.slack-gov.com", "services", "public"}, "/"),
		strings.Join([]string{"https:", "", "hooks.slack-gov.com", "archives", "public"}, "/"),
		strings.Join([]string{"https:", "", "hooks.slack-gov.com.evil.example", "services", "T1", "B1", "secret"}, "/"),
		strings.Join([]string{"https:", "", "hooks.slack.gov.com", "services", "T1", "B1", "secret"}, "/"),
		"hooks.slack-gov.com/services/public",
	} {
		assert.Equal(t, value, safetext.Sanitize(value, 256))
	}
}

func TestSanitizeRecognizesEmbeddedAndWorkflowSlackCredentials(t *testing.T) {
	t.Parallel()
	botPrefix := strings.Join([]string{"xox", "b"}, "")
	workflowPrefix := strings.Join([]string{"xw", "fp"}, "")
	embeddedBot := "tenant" + strings.Join([]string{botPrefix, strings.Repeat("a", 24)}, "-")
	workflow := strings.Join([]string{workflowPrefix, strings.Repeat("a", 24)}, "-")

	for _, value := range []string{embeddedBot, workflow} {
		assert.Equal(t, "[REDACTED]", safetext.Sanitize(value, 256))
	}
}

func TestSanitizeRecognizesSlackRefreshTokens(t *testing.T) {
	t.Parallel()
	refreshToken := strings.Join([]string{"xoxe", "1", strings.Repeat("a", 24)}, "-")

	for _, value := range []string{
		refreshToken,
		"audit=<" + refreshToken + ">&source=operator",
	} {
		assert.Equal(t, "[REDACTED]", safetext.Sanitize(value, 256))
	}
}

func TestSanitizePreservesSlackRefreshTokenNearMisses(t *testing.T) {
	t.Parallel()
	refreshToken := strings.Join([]string{"xoxe", "1", strings.Repeat("a", 24)}, "-")

	for _, value := range []string{
		strings.ToUpper(refreshToken),
		"Xoxe" + strings.TrimPrefix(refreshToken, "xoxe"),
		strings.Join([]string{"xoxe", "1", strings.Repeat("a", 24)}, "_"),
		strings.Join([]string{"xoxee", "1", strings.Repeat("a", 24)}, "-"),
		"xoxe-short",
	} {
		assert.Equal(t, value, safetext.Sanitize(value, 256))
	}
}
