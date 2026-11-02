package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func commandNamed(t *testing.T, name string) command {
	t.Helper()
	for _, c := range commands {
		if c.name == name {
			return c
		}
	}
	t.Fatalf("no command named %q", name)
	return command{}
}

func TestCommandsAreTheFiveAgents(t *testing.T) {
	var names []string
	for _, c := range commands {
		names = append(names, c.name)
		assert.NotEmpty(t, c.short, c.name)
		assert.NotEmpty(t, c.long, c.name)
		assert.NotEmpty(t, c.modules, c.name)
		assert.NotNil(t, c.runs.populate, c.name)
		assert.NotNil(t, c.runs.start, c.name)
	}
	assert.Equal(t, []string{"hf-download", "replica", "serving-agent", "fine-tuned-adapter", "model-metadata"}, names)
}

func TestRootCommandCarriesEveryRow(t *testing.T) {
	var registered []string
	for _, cmd := range rootCmd.Commands() {
		registered = append(registered, cmd.Name())
	}
	for _, c := range commands {
		assert.Contains(t, registered, c.name)
	}
}

func TestModelMetadataRowWords(t *testing.T) {
	row := commandNamed(t, "model-metadata")

	assert.Equal(t, "Extract model metadata from PVC-mounted models", row.short)
	expected := "Model metadata agent parses a PVC-mounted model directory and writes a per-model status ConfigMap " +
		"that the BaseModel/ClusterBaseModel controller consumes to update the CR spec and lifecycle state."
	assert.Equal(t, expected, row.long)
}

func TestModelMetadataFlags(t *testing.T) {
	cmd := &cobra.Command{}

	modelMetadataFlags(cmd)

	for _, name := range []string{"model-path", "basemodel-name", "basemodel-namespace", "cluster-scoped"} {
		flag := cmd.Flags().Lookup(name)
		require.NotNil(t, flag, name)
		assert.Equal(t, name, flag.Name)
	}
	for _, name := range []string{"model-path", "basemodel-name"} {
		assert.Equal(t, []string{"true"}, cmd.Flags().Lookup(name).Annotations[cobra.BashCompOneRequiredFlag], name)
	}
	assert.Nil(t, cmd.Flags().Lookup("basemodel-namespace").Annotations[cobra.BashCompOneRequiredFlag])
}
