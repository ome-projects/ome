package apierror

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var gr = schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}

func testSecret() string {
	// Keep a realistic credential shape out of repository source while still
	// exercising the scanner that guards operator-visible output.
	return strings.Join([]string{"xoxb", "123456789012", "123456789012", "abcdefghijklmnopqrstuvwxyz"}, "-")
}

func testEmbeddedSlackSecret() string {
	prefix := strings.Join([]string{"xox", "b"}, "")
	return "tenant" + strings.Join([]string{prefix, strings.Repeat("a", 24)}, "-")
}

func testWorkflowSecret() string {
	prefix := strings.Join([]string{"xw", "fp"}, "")
	return strings.Join([]string{prefix, strings.Repeat("a", 24)}, "-")
}

func TestSafeReadClassifiesWithoutRenderingRawErrors(t *testing.T) {
	t.Parallel()
	hostile := "upstream https://user:" + testSecret() + "@api.invalid failed"
	tests := []struct {
		name   string
		raw    error
		reason string
	}{
		{name: "canceled", raw: fmt.Errorf("%s: %w", hostile, context.Canceled), reason: "Canceled"},
		{name: "deadline", raw: fmt.Errorf("%s: %w", hostile, context.DeadlineExceeded), reason: "TimedOut"},
		{name: "forbidden", raw: kerrors.NewForbidden(gr, "chat", errors.New(hostile)), reason: "Forbidden"},
		{name: "unauthorized", raw: kerrors.NewUnauthorized(hostile), reason: "Unauthorized"},
		{name: "not found", raw: kerrors.NewNotFound(gr, "chat"), reason: "NotFound"},
		{name: "conflict", raw: kerrors.NewConflict(gr, "chat", errors.New(hostile)), reason: "Conflict"},
		{name: "invalid", raw: kerrors.NewGenericServerResponse(http.StatusUnprocessableEntity, http.MethodGet, gr, "chat", hostile, 0, true), reason: "Invalid"},
		{name: "bad request", raw: kerrors.NewBadRequest(hostile), reason: "Invalid"},
		{name: "too many requests", raw: kerrors.NewTooManyRequests(hostile, 3), reason: "TooManyRequests"},
		{name: "service unavailable", raw: kerrors.NewServiceUnavailable(hostile), reason: "ServiceUnavailable"},
		{name: "gateway timeout", raw: kerrors.NewGenericServerResponse(http.StatusGatewayTimeout, http.MethodGet, gr, "chat", hostile, 0, true), reason: "TimedOut"},
		{name: "internal", raw: kerrors.NewInternalError(errors.New(hostile)), reason: "ServerError"},
		{name: "unsupported", raw: kerrors.NewGenericServerResponse(http.StatusUnsupportedMediaType, http.MethodGet, gr, "chat", hostile, 0, true), reason: "UnsupportedAPI"},
		{name: "unknown transport", raw: errors.New(hostile), reason: "Unavailable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := SafeRead(tt.raw, ReadTarget{
				Operation: ReadOperationGet,
				Resource:  "inferenceservices",
				Namespace: "team-a",
				Name:      "chat",
			})
			require.Error(t, err)
			assert.Equal(t, "get inferenceservices team-a/chat: "+tt.reason, err.Error())
			assert.True(t, errors.Is(err, tt.raw), "safe wrapper must retain the original cause")
			assert.NotContains(t, err.Error(), hostile)
			assert.NotContains(t, err.Error(), testSecret())
		})
	}
}

func TestSafeReadPreservesErrorPredicates(t *testing.T) {
	t.Parallel()
	forbidden := kerrors.NewForbidden(gr, "chat", errors.New("sensitive policy text"))
	wrappedForbidden := SafeRead(forbidden, ReadTarget{Operation: ReadOperationGet, Resource: "inferenceservices", Name: "chat"})
	assert.True(t, kerrors.IsForbidden(wrappedForbidden))
	assert.True(t, errors.Is(wrappedForbidden, forbidden))

	canceled := SafeRead(fmt.Errorf("transport: %w", context.Canceled), ReadTarget{Operation: ReadOperationList, Resource: "inferenceservices"})
	assert.True(t, errors.Is(canceled, context.Canceled))

	deadline := SafeRead(fmt.Errorf("transport: %w", context.DeadlineExceeded), ReadTarget{Operation: ReadOperationList, Resource: "inferenceservices"})
	assert.True(t, errors.Is(deadline, context.DeadlineExceeded))
}

func TestSafeReadMissingOMEAPIIsActionableWithoutInspectingMessage(t *testing.T) {
	t.Parallel()
	raw := kerrors.NewGenericServerResponse(http.StatusNotFound, http.MethodGet, gr, "", "hostile "+testSecret(), 0, true)
	err := SafeRead(raw, ReadTarget{
		Operation: ReadOperationGet,
		Resource:  "inferenceservices",
		Namespace: "team-a",
		Name:      "chat",
	})

	require.Error(t, err)
	assert.Equal(t, "get inferenceservices team-a/chat: OMEAPIMissing (install OME first)", err.Error())
	assert.LessOrEqual(t, len("error: "+err.Error()), maxRootErrorDisplayWidth)
	assert.True(t, errors.Is(err, raw))
	assert.True(t, kerrors.IsNotFound(err))
	assert.NotContains(t, err.Error(), testSecret())
}

func TestSafeReadRecognizesNamedGenericMissingOMEAPI(t *testing.T) {
	t.Parallel()
	raw := kerrors.NewGenericServerResponse(
		http.StatusNotFound,
		http.MethodGet,
		gr,
		"chat",
		"proxy "+testSecret(),
		0,
		true,
	)
	err := SafeRead(raw, ReadTarget{
		Operation: ReadOperationGet,
		Resource:  "inferenceservices",
		Namespace: "team-a",
		Name:      "chat",
	})

	require.Error(t, err)
	assert.Equal(t, "get inferenceservices team-a/chat: OMEAPIMissing (install OME first)", err.Error())
	assert.LessOrEqual(t, len("error: "+err.Error()), maxRootErrorDisplayWidth)
	assert.True(t, errors.Is(err, raw))
	assert.True(t, kerrors.IsNotFound(err))
	assert.NotContains(t, err.Error(), testSecret())
	assert.NotContains(t, err.Error(), raw.Error())
}

func TestSafeReadOmitsUnsafeOrInvalidTargetIdentity(t *testing.T) {
	t.Parallel()
	raw := kerrors.NewForbidden(gr, "chat", errors.New("policy"))
	tests := []struct {
		name   string
		target ReadTarget
		want   string
	}{
		{
			name:   "safe identity",
			target: ReadTarget{Operation: ReadOperationGet, Resource: "inferenceservices", Namespace: "team-a", Name: "chat-v2"},
			want:   "get inferenceservices team-a/chat-v2: Forbidden",
		},
		{
			name:   "safe cluster scoped identity",
			target: ReadTarget{Operation: ReadOperationGet, Resource: "clusterbasemodels", Name: "llama-v2"},
			want:   "get clusterbasemodels llama-v2: Forbidden",
		},
		{
			name:   "credential shaped name",
			target: ReadTarget{Operation: ReadOperationGet, Resource: "inferenceservices", Namespace: "team-a", Name: testSecret()},
			want:   "get inferenceservices: Forbidden",
		},
		{
			name:   "embedded slack credential shaped name",
			target: ReadTarget{Operation: ReadOperationGet, Resource: "models", Namespace: "team-a", Name: testEmbeddedSlackSecret()},
			want:   "get models: Forbidden",
		},
		{
			name:   "workflow credential shaped name",
			target: ReadTarget{Operation: ReadOperationGet, Resource: "models", Namespace: "team-a", Name: testWorkflowSecret()},
			want:   "get models: Forbidden",
		},
		{
			name:   "valid but overlong target",
			target: ReadTarget{Operation: ReadOperationGet, Resource: "inferenceservices", Namespace: "team-a", Name: strings.Repeat("a", 63)},
			want:   "get inferenceservices: Forbidden",
		},
		{
			name:   "control in namespace",
			target: ReadTarget{Operation: ReadOperationList, Resource: "inferenceservices", Namespace: "team-a\nsecret"},
			want:   "list inferenceservices: Forbidden",
		},
		{
			name:   "unknown operation and resource",
			target: ReadTarget{Operation: ReadOperation("get\nleak"), Resource: "../../" + testSecret(), Namespace: "team-a", Name: "chat"},
			want:   "read OME resource: Forbidden",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := SafeRead(raw, tt.target)
			assert.Equal(t, tt.want, err.Error())
			assert.LessOrEqual(t, len("error: "+err.Error()), maxRootErrorDisplayWidth)
			assert.NotContains(t, err.Error(), testSecret())
			assert.NotContains(t, err.Error(), testEmbeddedSlackSecret())
			assert.NotContains(t, err.Error(), testWorkflowSecret())
			assert.NotContains(t, err.Error(), "\n")
		})
	}
}

func TestFriendlyRecognizesNamedGenericMissingAPIWithoutRenderingCause(t *testing.T) {
	t.Parallel()
	raw := kerrors.NewGenericServerResponse(
		http.StatusNotFound,
		http.MethodGet,
		gr,
		"chat",
		"proxy "+testSecret(),
		0,
		true,
	)

	err := Friendly(raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OME does not appear to be installed")
	assert.NotContains(t, err.Error(), testSecret())
	assert.NotContains(t, err.Error(), raw.Error())
	assert.True(t, errors.Is(err, raw))
	assert.True(t, kerrors.IsNotFound(err))
}

func TestFriendlyRetainsCompatibilityAndSafelyExplainsMissingAPI(t *testing.T) {
	t.Parallel()
	assert.NoError(t, Friendly(nil))
	other := errors.New("transport " + testSecret())
	assert.Same(t, other, Friendly(other))

	objectMissing := kerrors.NewNotFound(gr, "chat")
	assert.Same(t, error(objectMissing), Friendly(objectMissing))

	raw := &kerrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonNotFound,
		Message: "hostile " + testSecret(),
	}}
	err := Friendly(raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OME does not appear to be installed")
	assert.NotContains(t, err.Error(), testSecret())
	assert.True(t, errors.Is(err, raw))
	assert.True(t, kerrors.IsNotFound(err))
}
