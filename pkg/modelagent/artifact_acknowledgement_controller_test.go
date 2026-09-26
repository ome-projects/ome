package modelagent

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	controllershared "sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func TestArtifactAcknowledgementControllerJSON(t *testing.T) {
	raw := `{"name":"model","status":"Ready","modelUID":"model-uid","artifactRehydrationID":"request","nodeUID":"node-uid"}`
	var entry controllershared.ModelEntry
	require.NoError(t, json.Unmarshal([]byte(raw), &entry))
	roundtrip, err := json.Marshal(entry)
	require.NoError(t, err)
	require.JSONEq(t, raw, string(roundtrip))
}
