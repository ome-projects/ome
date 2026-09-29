package effective

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/specdefaults"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

const testDeployBlock = `{
	"defaultDeploymentMode": "RawDeployment",
	"terminationGracePeriodSeconds": 45,
	"minReadySeconds": 10,
	"replicas": {
		"defaultMinReplicas": 2,
		"defaultMaxReplicas": {"engine": 6, "decoder": 4, "router": 3}
	}
}`

func deployConfigMap(namespace, deploy string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: namespace},
		Data:       map[string]string{controllerconfig.DeployConfigName: deploy},
	}
}

func testDeployConfig(t *testing.T) *controllerconfig.DeployConfig {
	t.Helper()
	cfg, err := controllerconfig.ParseDeployConfig(deployConfigMap("ome", testDeployBlock))
	require.NoError(t, err)
	return cfg
}

func TestLoadDeployConfig(t *testing.T) {
	t.Run("parses the live ConfigMap", func(t *testing.T) {
		client := fake.NewSimpleClientset(deployConfigMap("ome", testDeployBlock))
		cfg, err := LoadDeployConfig(context.Background(), client.CoreV1(), "ome")
		require.NoError(t, err)
		require.NotNil(t, cfg.TerminationGracePeriodSeconds)
		assert.Equal(t, int64(45), *cfg.TerminationGracePeriodSeconds)
	})
	t.Run("missing deploy block yields empty defaults", func(t *testing.T) {
		configMap := deployConfigMap("ome", "")
		configMap.Data = nil
		client := fake.NewSimpleClientset(configMap)
		cfg, err := LoadDeployConfig(context.Background(), client.CoreV1(), "ome")
		require.NoError(t, err)
		assert.Equal(t, &controllerconfig.DeployConfig{}, cfg)
	})

	failures := map[string]struct {
		objects  []runtime.Object
		reactor  clienttesting.ReactionFunc
		contains string
	}{
		"not found": {contains: "ome/inferenceservice-config not found"},
		"wrong namespace": {
			objects: []runtime.Object{deployConfigMap("other", testDeployBlock)}, contains: "not found",
		},
		"forbidden": {
			reactor: func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, constants.InferenceServiceConfigMapName, nil)
			},
			contains: "forbidden",
		},
		"invalid": {
			objects:  []runtime.Object{deployConfigMap("ome", `{"replicas": {"defaultMinReplicas": 0}}`)},
			contains: "deploy defaults: ConfigMap ome/inferenceservice-config",
		},
	}
	for name, test := range failures {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset(test.objects...)
			if test.reactor != nil {
				client.PrependReactor("get", "configmaps", test.reactor)
			}
			cfg, err := LoadDeployConfig(context.Background(), client.CoreV1(), "ome")
			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Contains(t, err.Error(), test.contains)
		})
	}
}

func TestDecodeDeployConfig(t *testing.T) {
	valid := `apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  deploy: |
    {"defaultDeploymentMode": "RawDeployment", "terminationGracePeriodSeconds": 45}
`
	cfg, err := DecodeDeployConfig([]byte("---\n" + valid))
	require.NoError(t, err)
	require.NotNil(t, cfg.TerminationGracePeriodSeconds)
	assert.Equal(t, int64(45), *cfg.TerminationGracePeriodSeconds)

	jsonManifest := `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"inferenceservice-config"},"data":{"deploy":"{\"defaultDeploymentMode\": \"RawDeployment\", \"minReadySeconds\": 5}"}}`
	cfg, err = DecodeDeployConfig([]byte(jsonManifest))
	require.NoError(t, err)
	require.NotNil(t, cfg.MinReadySeconds)

	failures := map[string]struct {
		manifest string
		contains string
	}{
		"empty":         {manifest: "", contains: "manifest is empty"},
		"two objects":   {manifest: valid + "---\n" + valid, contains: "exactly one object"},
		"wrong kind":    {manifest: "apiVersion: v1\nkind: Secret\nmetadata:\n  name: inferenceservice-config\n", contains: "want v1 ConfigMap"},
		"wrong name":    {manifest: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n", contains: `named "other"`},
		"not a mapping": {manifest: "- a\n- b\n", contains: "decode manifest"},
		"invalid block": {
			manifest: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: inferenceservice-config\ndata:\n  deploy: 'not json'\n",
			contains: "deploy defaults:",
		},
	}
	for name, test := range failures {
		t.Run(name, func(t *testing.T) {
			cfg, err := DecodeDeployConfig([]byte(test.manifest))
			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Contains(t, err.Error(), test.contains)
		})
	}
}

// TestMergeEffectiveComponentsMatchesController guards against drift: with a
// deploy config, the effective specs equal the controller's own merge, mode
// resolution, and specdefaults calls.
func TestMergeEffectiveComponentsMatchesController(t *testing.T) {
	one := 1
	tests := map[string]struct {
		isvc    v1beta1.InferenceServiceSpec
		runtime v1beta1.ServingRuntimeSpec
	}{
		"raw deployment": {
			isvc:    v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
			runtime: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
		},
		"omenative leader and worker": {
			isvc: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{},
			}},
			runtime: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{Size: &one},
			}},
		},
		"pd disaggregated with router": {
			isvc: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{}, Router: &v1beta1.RouterSpec{},
			},
			runtime: v1beta1.ServingRuntimeSpec{
				EngineConfig: &v1beta1.EngineSpec{}, DecoderConfig: &v1beta1.DecoderSpec{}, RouterConfig: &v1beta1.RouterSpec{},
			},
		},
	}
	cfg := testDeployConfig(t)
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod"}, Spec: test.isvc}
			runtimeSpec := test.runtime.DeepCopy()

			engine, decoder, router, err := isvcutils.MergeRuntimeSpecs(isvc.DeepCopy(), runtimeSpec.DeepCopy(), logr.Discard())
			require.NoError(t, err)
			engineMode, decoderMode, routerMode, err := isvcutils.DetermineDeploymentModes(engine, decoder, router, runtimeSpec, isvc.Spec.DeploymentMode)
			require.NoError(t, err)
			specdefaults.Engine(engine, engineMode, cfg)
			specdefaults.Decoder(decoder, decoderMode, cfg)
			specdefaults.Router(router, routerMode, cfg)

			got, err := MergeEffectiveComponents(isvc, runtimeSpec, cfg)
			require.NoError(t, err)
			for _, component := range got {
				switch component.Type {
				case v1beta1.EngineComponent:
					assert.Equal(t, engineMode, component.DeploymentMode)
					assert.Equal(t, engine, component.engine)
				case v1beta1.DecoderComponent:
					assert.Equal(t, decoderMode, component.DeploymentMode)
					assert.Equal(t, decoder, component.decoder)
				case v1beta1.RouterComponent:
					assert.Equal(t, routerMode, component.DeploymentMode)
					assert.Equal(t, router, component.router)
				}
			}
			require.NotNil(t, got[0].engine.MinReplicas)
			assert.Equal(t, 2, *got[0].engine.MinReplicas)
		})
	}
}

func TestMergeEffectiveComponentsServiceVirtualSkipsDefaults(t *testing.T) {
	virtual := constants.VirtualDeployment
	tests := map[string]struct {
		annotations map[string]string
		specMode    *constants.DeploymentModeType
		wantSource  ComponentDeploymentModeSource
	}{
		"annotation": {
			annotations: map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)},
			wantSource:  DeploymentModeServiceAnnotation,
		},
		"spec field": {specMode: &virtual, wantSource: DeploymentModeServiceSpec},
	}
	cfg := testDeployConfig(t)
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Annotations: test.annotations},
				Spec: v1beta1.InferenceServiceSpec{
					DeploymentMode: test.specMode,
					Engine:         &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}},
					Router:         &v1beta1.RouterSpec{},
				},
			}
			source, ok := ServiceVirtualDeployment(isvc, cfg)
			assert.True(t, ok)
			assert.Equal(t, test.wantSource, source)

			got, err := MergeEffectiveComponents(isvc, &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}, cfg)
			require.NoError(t, err)
			require.Len(t, got, 2)
			for _, component := range got {
				assert.Equal(t, constants.VirtualDeployment, component.DeploymentMode)
				assert.Equal(t, test.wantSource, component.DeploymentModeSource)
			}
			assert.Nil(t, got[0].engine.MinReplicas)
			assert.Nil(t, got[0].engine.Worker.Size)
			assert.Nil(t, got[0].engine.TerminationGracePeriodSeconds)
			assert.Nil(t, got[1].router.MinReplicas)
		})
	}
}

func TestMergeEffectiveComponentsNilDeployConfigFillsNothing(t *testing.T) {
	isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}},
	}}
	got, err := MergeEffectiveComponents(isvc, &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Nil(t, got[0].engine.MinReplicas)
	assert.Nil(t, got[0].engine.Worker.Size, "specdefaults must not run without a deploy config")

	_, ok := ServiceVirtualDeployment(isvc, nil)
	assert.False(t, ok)
}
