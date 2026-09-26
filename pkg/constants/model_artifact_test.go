package constants

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

func TestModelArtifactRequestLabelUsesOnlyModelUID(t *testing.T) {
	seen := map[string]bool{}
	for _, uid := range []types.UID{"model-uid", "550e8400-e29b-41d4-a716-446655440000", "namespace/name", types.UID(strings.Repeat("long UID ", 30))} {
		key := GetModelArtifactRequestLabel(uid)
		require.Empty(t, validation.IsQualifiedName(key))
		require.Equal(t, key, GetModelArtifactRequestLabel(uid))
		require.False(t, seen[key])
		seen[key] = true
	}
}
