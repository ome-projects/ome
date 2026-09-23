package v1beta1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelObservedArtifactStatusJSON(t *testing.T) {
	var status ModelStatusSpec
	require.NoError(t, json.Unmarshal([]byte(`{"state":"Ready","rehydration":{"requestID":"r2","completedRequestID":"r1"}}`), &status))
	data, err := json.Marshal(status)
	require.NoError(t, err)
	require.Contains(t, string(data), `"requestID":"r2"`)
	require.Contains(t, string(data), `"completedRequestID":"r1"`)
}
