package fine_tuned_adapter

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/utils/testing"
)

// buildThroughModule builds the adapter the way the agent binary does: the
// settings in v, the named logger and an object store with a client wired through fx.
func buildThroughModule(t *testing.T, v *viper.Viper) error {
	t.Helper()
	app := fx.New(
		fx.NopLogger,
		fx.Supply(v, &ociobjectstore.OCIOSDataStore{Client: &objectstorage.ObjectStorageClient{}}),
		fx.Provide(fx.Annotated{Name: "another_log", Target: func() logging.Interface { return testingPkg.SetupMockLogger() }}),
		Module,
		fx.Invoke(func(*FineTunedAdapter) {}),
	)
	return app.Err()
}

func completeSettings() *viper.Viper {
	v := viper.New()
	v.Set("model.namespace", "example-namespace")
	v.Set("model.bucket_name", "example-bucket")
	v.Set("model.object_name", "weights/adapter.zip")
	v.Set("unzipped_fine_tuned_weight_directory", "/mnt/unzipped-ft-models")
	v.Set("zipped_fine_tuned_weight_directory", "/mnt/zipped-ft-models")
	return v
}

func TestModule_EveryRequiredSettingPresent_Builds(t *testing.T) {
	require.NoError(t, buildThroughModule(t, completeSettings()))
}

// A required setting left empty is refused when the agent starts, naming the
// setting, instead of failing inside the download later.
func TestModule_RequiredSettingMissing_RefusedAtStart(t *testing.T) {
	v := completeSettings()
	v.Set("zipped_fine_tuned_weight_directory", "")
	err := buildThroughModule(t, v)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ZippedFineTunedWeightDirectory")
}
