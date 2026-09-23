package constants

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

func TestArtifactReadyLabelKey(t *testing.T) {
	key, err := ArtifactReadyLabelKey(types.UID("11111111-2222-4333-8444-555555555555"))
	require.NoError(t, err)
	require.Equal(t, "models.ome.io/ready-11111111222243338444555555555555", key)
	for _, uid := range []string{"", "---", "bad/uid", "bad uid", strings.Repeat("a", 58)} {
		t.Run(uid, func(t *testing.T) {
			_, err := ArtifactReadyLabelKey(types.UID(uid))
			require.Error(t, err)
		})
	}
}
