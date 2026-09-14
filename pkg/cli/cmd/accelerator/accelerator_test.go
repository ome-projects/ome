package accelerator

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

func TestAcceleratorCommandContract(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
	})
	assert.Equal(t, "accelerator", cmd.Use)
	assert.Equal(t, "Inspect accelerator selection evidence", cmd.Short)

	explain, _, err := cmd.Find([]string{"explain"})
	require.NoError(t, err)
	require.NotSame(t, cmd, explain)
	assert.Equal(t, "explain INFERENCESERVICE", explain.Use)
	assert.Contains(t, explain.Long, "never reruns accelerator selection")
	assert.Contains(t, explain.Long, "free-form selection reason is represented by a digest")
	output := explain.Flags().Lookup("output")
	require.NotNil(t, output)
	assert.Equal(t, "o", output.Shorthand)
	assert.Equal(t, "table", output.DefValue)
	require.NotNil(t, explain.Flags().Lookup("ome-namespace"))
}
