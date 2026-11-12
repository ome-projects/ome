package fine_tuned_adapter

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/constants"
)

// agentViper reads settings the way the agent binary does: every key may
// come from an OME_AGENT_ environment variable, dots written as underscores.
func agentViper() *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix(constants.AgentAppName)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	return v
}

// The webhook hands the adapter its weight location through the environment
// alone: the bucket, namespace and object name of the nested `model` block.
// Every start must read all three.
func TestNewConfig_ModelFromTheEnvironmentAlone_ReadOnEveryStart(t *testing.T) {
	t.Setenv(constants.AgentModelBucketNameEnvVarKey, "example-bucket")
	t.Setenv(constants.AgentModelNamespaceEnvVarKey, "example-namespace")
	t.Setenv(constants.AgentModelObjectName, "weights/adapter.zip")
	for start := 1; start <= 64; start++ {
		c, err := newConfig(agentViper(), fineTunedAdapterParams{})
		require.NoError(t, err)
		require.NotNil(t, c.FineTunedWeightURI, "start %d", start)
		require.Equal(t, "example-bucket", c.FineTunedWeightURI.BucketName, "start %d read no bucket name", start)
		require.Equal(t, "example-namespace", c.FineTunedWeightURI.Namespace, "start %d read no namespace", start)
		require.Equal(t, "weights/adapter.zip", c.FineTunedWeightURI.ObjectName, "start %d read no object name", start)
	}
}
