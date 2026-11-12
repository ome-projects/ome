package agentconfig

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type targetSettings struct {
	Name string `mapstructure:"name"`
}

type nestedSettings struct {
	LocalPath string `mapstructure:"local_path"`
	Source    struct {
		StorageURI string `mapstructure:"storage_uri"`
	} `mapstructure:"source"`
	Target   *targetSettings `mapstructure:"target"`
	Untagged string
}

type clockSettings struct {
	At time.Time `mapstructure:"at"`
}

// agentViper reads settings the way the agent binary does: every key may
// come from an OME_AGENT_ environment variable, dots written as underscores.
func agentViper() *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix("OME_AGENT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	return v
}

func TestBindEnvs_NestedSettingFromTheEnvironmentAlone_ReadOnEveryStart(t *testing.T) {
	t.Setenv("OME_AGENT_SOURCE_STORAGE_URI", "pvc://weights/example-model")
	for start := 1; start <= 64; start++ {
		v := agentViper()
		c := &nestedSettings{}
		require.NoError(t, BindEnvs(v, c))
		require.NoError(t, v.Unmarshal(c))
		require.Equal(t, "pvc://weights/example-model", c.Source.StorageURI, "start %d read no storage URI", start)
	}
}

func TestBindEnvs_FileAndEnvironment_EnvironmentWinsPerSetting(t *testing.T) {
	t.Setenv("OME_AGENT_LOCAL_PATH", "/env/models")
	t.Setenv("OME_AGENT_TARGET_NAME", "from-the-environment")
	v := agentViper()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(
		"local_path: /file/models\nsource:\n  storage_uri: oci://n/example/b/models/o/example-model\ntarget:\n  name: from-the-file\n")))
	c := &nestedSettings{}
	require.NoError(t, BindEnvs(v, c))
	require.NoError(t, v.Unmarshal(c))
	assert.Equal(t, "/env/models", c.LocalPath)
	assert.Equal(t, "oci://n/example/b/models/o/example-model", c.Source.StorageURI)
	assert.Equal(t, "from-the-environment", c.Target.Name)
}

func TestBindEnvs_NilPointerToAStruct_IsAllocated(t *testing.T) {
	c := &nestedSettings{}
	require.NoError(t, BindEnvs(agentViper(), c))
	assert.NotNil(t, c.Target)
}

func TestBindEnvs_StructWithoutSettings_IsOneSetting(t *testing.T) {
	v := agentViper()
	require.NoError(t, BindEnvs(v, &clockSettings{}))
	assert.ElementsMatch(t, []string{"at"}, v.AllKeys())
}

type loadSettings struct {
	LocalPath string `mapstructure:"local_path"`
	Count     int    `mapstructure:"count"`
	Nested    struct {
		URI string `mapstructure:"uri"`
	} `mapstructure:"nested"`
}

func TestLoad(t *testing.T) {
	t.Run("an empty file keeps the defaults", func(t *testing.T) {
		c, err := Load(viper.New(), loadSettings{Count: 10})
		require.NoError(t, err)
		assert.Equal(t, 10, c.Count)
		assert.Empty(t, c.LocalPath)
		assert.Empty(t, c.Nested.URI)
	})

	t.Run("the file overrides the defaults", func(t *testing.T) {
		v := viper.New()
		v.Set("count", 5)
		v.Set("nested.uri", "pvc://models/example-model")
		c, err := Load(v, loadSettings{Count: 10})
		require.NoError(t, err)
		assert.Equal(t, 5, c.Count)
		assert.Equal(t, "pvc://models/example-model", c.Nested.URI)
	})

	t.Run("the environment fills a setting the file leaves out", func(t *testing.T) {
		t.Setenv("OME_AGENT_LOCAL_PATH", "/models/example-model")
		c, err := Load(agentViper(), loadSettings{Count: 10})
		require.NoError(t, err)
		assert.Equal(t, "/models/example-model", c.LocalPath)
		assert.Equal(t, 10, c.Count)
	})

	t.Run("a value of the wrong type is an unmarshal error", func(t *testing.T) {
		v := viper.New()
		v.Set("count", "many")
		c, err := Load(v, loadSettings{})
		require.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), "error occurred when unmarshalling config")
	})
}
