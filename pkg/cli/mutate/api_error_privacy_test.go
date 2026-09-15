package mutate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
