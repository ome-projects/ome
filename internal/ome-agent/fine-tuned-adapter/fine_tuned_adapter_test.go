package fine_tuned_adapter

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestWithViperReadsModelFromEnvironment(t *testing.T) {
	t.Setenv("OME_AGENT_MODEL", "")
	t.Setenv("OME_AGENT_MODEL_NAMESPACE", "test-namespace")
	t.Setenv("OME_AGENT_MODEL_BUCKET_NAME", "test-bucket")
	t.Setenv("OME_AGENT_MODEL_OBJECT_NAME", "models/weights.zip-merged-weight")

	// Match the agent's configuration file: model fields exist only in the environment.
	const configYAML = `zipped_fine_tuned_weight_directory: /mnt/zipped-ft-models
unzipped_fine_tuned_weight_directory: /mnt/unzipped-ft-models
`
	v := viper.New()
	v.SetEnvPrefix("OME_AGENT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(configYAML)))

	config, err := NewFineTunedAdapterConfig(WithViper(v))

	require.NoError(t, err)
	require.NotNil(t, config.FineTunedWeightURI)
	require.Equal(t, "test-namespace", config.FineTunedWeightURI.Namespace)
	require.Equal(t, "test-bucket", config.FineTunedWeightURI.BucketName)
	require.Equal(t, "models/weights.zip-merged-weight", config.FineTunedWeightURI.ObjectName)
}
