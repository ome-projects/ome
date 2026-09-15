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
