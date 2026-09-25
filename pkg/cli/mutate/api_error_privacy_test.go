package mutate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func TestSafeAPIErrorNormalizesWrappedCancellationWithoutPrivateProse(t *testing.T) {
	for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, err := range []error{sentinel, fmt.Errorf("PRIVATE_URL_TOKEN: %w", sentinel), errors.Join(errors.New("PRIVATE_API_MESSAGE"), sentinel)} {
			got := SafeAPIError(err)
			require.True(t, got == sentinel, "preserve the context singleton, not arbitrary upstream prose")
			require.ErrorIs(t, got, sentinel)
			require.NotContains(t, got.Error(), "PRIVATE_")
		}
	}
}

func TestSafeAPIErrorPreservesResourceExpiredWithoutPrivateProse(t *testing.T) {
	expired := apierrors.NewResourceExpired("PRIVATE_CONTINUE_TOKEN")

	got := SafeAPIError(expired)

	require.True(t, apierrors.IsResourceExpired(got))
	require.ErrorIs(t, got, expired)
	require.Equal(t,
		"required Kubernetes API request failed; check access and connectivity",
		got.Error(),
	)
	require.NotContains(t, got.Error(), "PRIVATE_")
	require.Same(t, got, SafeAPIError(got), "safe wrappers must not nest")
	require.Same(t, got, SafeAPIError(fmt.Errorf("PRIVATE_OUTER: %w", got)))
	for _, rendered := range []string{
		fmt.Sprintf("%v", got),
		fmt.Sprintf("%+v", got),
		fmt.Sprintf("%#v", got),
		fmt.Sprintf("%q", got),
		fmt.Sprintf("%d", got),
		fmt.Sprintf("%c", got),
		fmt.Sprintf("%U", got),
		fmt.Sprintf("%#x", got),
		fmt.Sprintf("%20.5v", got),
	} {
		require.Equal(t,
			"required Kubernetes API request failed; check access and connectivity",
			rendered,
		)
		require.NotContains(t, rendered, "PRIVATE_CONTINUE_TOKEN")
		require.NotContains(t, rendered, "PRIVATE_OUTER")
	}
}
