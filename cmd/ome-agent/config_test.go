package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/internal/ome-agent/replica"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestConfigProviderPassesArtifactReuseEnvironmentOverrideToReplicaConfig(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	configPath := filepath.Join(t.TempDir(), "ome-agent.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("local_path: /tmp/models\n"), 0600))

	previousConfigFilePath := configFilePath
	configFilePath = configPath
	t.Cleanup(func() { configFilePath = previousConfigFilePath })
	t.Setenv(constants.AgentTargetArtifactReuseAllowedEnvVarKey, "true")

	command := &cobra.Command{}
	command.Flags().Bool("debug", false, "")

	var configuredViper *viper.Viper
	app := fx.New(
		configProvider(command),
		fx.Populate(&configuredViper),
	)
	require.NoError(t, app.Err())

	config, err := replica.NewReplicaConfig(replica.WithViper(configuredViper))
	require.NoError(t, err)
	assert.True(t, config.TargetArtifactReuseAllowed)
}

func TestConfigProviderLeavesArtifactReuseDisabledWithoutEnvironmentOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	previousValue, wasSet := os.LookupEnv(constants.AgentTargetArtifactReuseAllowedEnvVarKey)
	require.NoError(t, os.Unsetenv(constants.AgentTargetArtifactReuseAllowedEnvVarKey))
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv(constants.AgentTargetArtifactReuseAllowedEnvVarKey, previousValue)
			return
		}
		_ = os.Unsetenv(constants.AgentTargetArtifactReuseAllowedEnvVarKey)
	})

	configPath := filepath.Join(t.TempDir(), "ome-agent.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("local_path: /tmp/models\n"), 0600))

	previousConfigFilePath := configFilePath
	configFilePath = configPath
	t.Cleanup(func() { configFilePath = previousConfigFilePath })

	command := &cobra.Command{}
	command.Flags().Bool("debug", false, "")

	var configuredViper *viper.Viper
	app := fx.New(
		configProvider(command),
		fx.Populate(&configuredViper),
	)
	require.NoError(t, app.Err())

	config, err := replica.NewReplicaConfig(replica.WithViper(configuredViper))
	require.NoError(t, err)
	assert.False(t, config.TargetArtifactReuseAllowed)
}
