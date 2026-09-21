package openapi

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

func TestModelDownloadPriorityEnum(t *testing.T) {
	want := []interface{}{"Background", "Standard", "High"}
	t.Run("generated Go schema", func(t *testing.T) {
		definitions := GetOpenAPIDefinitions(func(string) spec.Ref { return spec.Ref{} })
		storage, ok := definitions["sigs.k8s.io/ome/pkg/apis/ome/v1beta1.StorageSpec"]
		require.True(t, ok)
		priority, ok := storage.Schema.Properties["downloadPriority"]
		require.True(t, ok)
		assert.ElementsMatch(t, want, priority.Enum)
	})
	t.Run("published Swagger schema", func(t *testing.T) {
		data, err := os.ReadFile("swagger.json")
		require.NoError(t, err)
		var swagger spec.Swagger
		require.NoError(t, json.Unmarshal(data, &swagger))
		storage, ok := swagger.Definitions["v1beta1.StorageSpec"]
		require.True(t, ok)
		priority, ok := storage.Properties["downloadPriority"]
		require.True(t, ok)
		assert.ElementsMatch(t, want, priority.Enum)
	})
}
