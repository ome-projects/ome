package configutils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testutils "sigs.k8s.io/ome/pkg/testing"
)

const leafConfig = `imports:
  - intermediate.yaml

a:
  b: 1
`

const intermediateConfig = `imports:
  - root.yaml
  -

a:
  c: 2
`

const rootConfig = `
a:
  b: 2
  d: 3
`

const expectedConfig = `a:
    b: 1
    c: 2
    d: 3
imports:
    - intermediate.yaml
`

func TestBindEnvsRecursiveNestedConfig(t *testing.T) {
	type checksum struct {
		Algorithm string `mapstructure:"algorithm"`
	}
	type location struct {
		Bucket   string   `mapstructure:"bucket"`
		Object   string   `mapstructure:"object"`
		Checksum checksum `mapstructure:"checksum"`
	}
	type config struct {
		Source *location `mapstructure:"source"`
		Target location  `mapstructure:"target"`
	}

	for _, initialized := range []bool{false, true} {
		t.Run(fmt.Sprintf("pointer_initialized_%t", initialized), func(t *testing.T) {
			cfg := config{}
			if initialized {
				cfg.Source = &location{}
			}
			v := viper.New()
			v.SetEnvPrefix("BIND_TEST")
			v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
			v.AutomaticEnv()
			t.Setenv("BIND_TEST_SOURCE_BUCKET", "source-bucket")
			t.Setenv("BIND_TEST_SOURCE_OBJECT", "weights/model.bin")
			t.Setenv("BIND_TEST_SOURCE_CHECKSUM_ALGORITHM", "sha256")
			t.Setenv("BIND_TEST_TARGET_BUCKET", "target-bucket")
			t.Setenv("BIND_TEST_TARGET_OBJECT", "copy/model.bin")
			v.SetDefault("target.checksum.algorithm", "md5")

			require.NoError(t, BindEnvsRecursive(v, &cfg, ""))
			// Parent environment keys shadow nested keys in Viper's AllKeys,
			// making environment-only fields disappear during Unmarshal.
			require.ElementsMatch(t, []string{
				"source.bucket", "source.object", "source.checksum.algorithm",
				"target.bucket", "target.object", "target.checksum.algorithm",
			}, v.AllKeys())
			require.NoError(t, v.Unmarshal(&cfg))
			assert.Equal(t, config{
				Source: &location{Bucket: "source-bucket", Object: "weights/model.bin", Checksum: checksum{Algorithm: "sha256"}},
				Target: location{Bucket: "target-bucket", Object: "copy/model.bin", Checksum: checksum{Algorithm: "md5"}},
			}, cfg)
		})
	}
}

func TestBindEnvsRecursiveLeafTypes(t *testing.T) {
	type config struct {
		Name     *string           `mapstructure:"name"`
		Enabled  bool              `mapstructure:"enabled"`
		Workers  int               `mapstructure:"workers"`
		Timeout  time.Duration     `mapstructure:"timeout"`
		Labels   []string          `mapstructure:"labels"`
		Metadata map[string]string `mapstructure:"metadata"`
	}
	cfg := config{}
	v := viper.New()
	v.SetEnvPrefix("BIND_TEST")
	v.AutomaticEnv()
	t.Setenv("BIND_TEST_NAME", "adapter")
	t.Setenv("BIND_TEST_ENABLED", "true")
	t.Setenv("BIND_TEST_WORKERS", "2")
	t.Setenv("BIND_TEST_TIMEOUT", "30m")
	t.Setenv("BIND_TEST_LABELS", "first,second")

	require.NoError(t, BindEnvsRecursive(v, &cfg, ""))
	assert.ElementsMatch(t, []string{"name", "enabled", "workers", "timeout", "labels", "metadata"}, v.AllKeys())
	v.Set("metadata", map[string]string{"owner": "test"})
	require.NoError(t, v.Unmarshal(&cfg))
	name := "adapter"
	assert.Equal(t, config{
		Name: &name, Enabled: true, Workers: 2, Timeout: 30 * time.Minute,
		Labels: []string{"first", "second"}, Metadata: map[string]string{"owner": "test"},
	}, cfg)
}

func TestConfigFileImports(t *testing.T) {
	// TODO: Would be ideal to use afero or similar in the future. Creating
	// files on the actual file system is not the worst thing ever (the Go
	// standard library does this in its own tests), but it's also not the
	// cleanest thing.

	t.Run("should import config files correctly", func(t *testing.T) {
		v := viper.New()

		tempDir, closer, err := testutils.TempDir()
		assert.NoError(t, err, "should not error creating temporary directory")
		defer closer()

		leafConfigPath := filepath.Join(tempDir, "leaf.yaml")
		err = os.WriteFile(leafConfigPath, []byte(leafConfig), 0666)
		assert.NoError(t, err, "should not error writing leaf config")

		intermediateConfigPath := filepath.Join(tempDir, "intermediate.yaml")
		err = os.WriteFile(intermediateConfigPath, []byte(intermediateConfig), 0666)
		assert.NoError(t, err, "should not error writing intermediate config")

		rootConfigPath := filepath.Join(tempDir, "root.yaml")
		err = os.WriteFile(rootConfigPath, []byte(rootConfig), 0666)
		assert.NoError(t, err, "should not error writing root config")

		err = ResolveAndMergeFile(v, leafConfigPath)
		assert.NoError(t, err, "should not error creating config")

		outputConfigPath := filepath.Join(tempDir, "assert.yaml")
		require.NoError(t, v.WriteConfigAs(outputConfigPath))

		writtenConfig, err := os.ReadFile(outputConfigPath)
		assert.NoError(t, err, "should not error reading config file")
		assert.Equal(t, expectedConfig, string(writtenConfig))
	})

	t.Run("should error when importing nonexistent configs", func(t *testing.T) {
		v := viper.New()

		tempDir, closer, err := testutils.TempDir()
		assert.NoError(t, err, "should not error creating temporary directory")
		defer closer()

		// create a nonexistent absolute path and a config referencing it
		nonexistentConfigPath := filepath.Join(tempDir, "nonexistent.yaml")
		badConfig := fmt.Sprintf("imports:\n- \"%s\"", nonexistentConfigPath)

		// write the config
		configPath := filepath.Join(tempDir, "test.yaml")
		err = os.WriteFile(configPath, []byte(badConfig), 0666)
		assert.NoError(t, err, "should not error writing config")

		err = ResolveAndMergeFile(v, configPath)
		assert.Error(t, err, "should error creating config")
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("should error when importing malformed configs", func(t *testing.T) {
		v := viper.New()

		tempDir, closer, err := testutils.TempDir()
		assert.NoError(t, err, "should not error creating temporary directory")
		defer closer()

		leafConfigPath := filepath.Join(tempDir, "leaf.yaml")
		err = os.WriteFile(leafConfigPath, []byte(leafConfig), 0666)
		assert.NoError(t, err, "should not error writing leaf config")

		// ensure the intermediate config is malformed
		intermediateConfigPath := filepath.Join(tempDir, "intermediate.yaml")
		err = os.WriteFile(intermediateConfigPath, []byte("malformed"), 0666)
		assert.NoError(t, err, "should not error writing intermediate config")

		err = ResolveAndMergeFile(v, leafConfigPath)
		assert.Error(t, err, "should error creating config")
		assert.Contains(t, err.Error(), "could not resolve configuration imports")
	})

	t.Run("should surface error when it occurs in child config", func(t *testing.T) {
		v := viper.New()

		tempDir, closer, err := testutils.TempDir()
		assert.NoError(t, err, "should not error creating temporary directory")
		defer closer()

		leafConfigPath := filepath.Join(tempDir, "leaf.yaml")
		err = os.WriteFile(leafConfigPath, []byte(leafConfig), 0666)
		assert.NoError(t, err, "should not error writing leaf config")

		intermediateConfigPath := filepath.Join(tempDir, "intermediate.yaml")
		err = os.WriteFile(intermediateConfigPath, []byte(intermediateConfig), 0666)
		assert.NoError(t, err, "should not error writing intermediate config")

		// the root config (referenced by the intermediate config) does not
		// exist, so the error should surface up
		err = ResolveAndMergeFile(v, leafConfigPath)
		assert.Error(t, err, "should error creating config")
		assert.Contains(t, err.Error(), "no such file or directory")
	})
}
