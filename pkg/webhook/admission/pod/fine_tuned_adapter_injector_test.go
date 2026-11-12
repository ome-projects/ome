package pod

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestNewFineTunedAdapterInjector_ValidMissingOrBadJSON_OnlyBadJSONErrors(t *testing.T) {
	tests := []struct {
		name        string
		configData  map[string]string
		expectError bool
	}{
		{
			name: "valid config",
			configData: map[string]string{
				fineTunedAdapterConfigMapKeyName: `{"image":"ome/agent:1","compartmentId":"c1","authType":"InstancePrincipal"}`,
			},
			expectError: false,
		},
		{
			name:        "missing key yields zero-value injector",
			configData:  map[string]string{},
			expectError: false,
		},
		{
			name: "malformed JSON returns error instead of panicking",
			configData: map[string]string{
				fineTunedAdapterConfigMapKeyName: `{"image": not-json`,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configMap := &v1.ConfigMap{Data: tt.configData}
			var injector *FineTunedAdapterInjector
			var err error
			assert.NotPanics(t, func() {
				injector, err = newFineTunedAdapterInjector(configMap, nil)
			})
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, injector)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, injector)
			}
		})
	}
}

// TestInjectFineTunedAdapter_MissingWeightReturnsError verifies that a
// FineTunedWeight lookup failure surfaces as an error from the injector
// instead of a nil-pointer panic in getModelInitEnvs.
func TestInjectFineTunedAdapter_MissingWeightReturnsError(t *testing.T) {
	fakeClient := fakeclient.New(t)

	injector, err := newFineTunedAdapterInjector(&v1.ConfigMap{Data: map[string]string{
		fineTunedAdapterConfigMapKeyName: `{"image":"ome/agent:1","compartmentId":"c1","authType":"InstancePrincipal","memoryRequest":"100Mi","memoryLimit":"1Gi","cpuRequest":"100m","cpuLimit":"1"}`,
	}}, fakeClient)
	assert.NoError(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			Annotations: map[string]string{
				constants.FineTunedAdapterInjectionKey: "does-not-exist",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Name: constants.MainContainerName}},
		},
	}

	assert.NotPanics(t, func() {
		err = injector.InjectFineTunedAdapter(pod)
	})
	assert.Error(t, err)
	assert.Empty(t, pod.Spec.InitContainers, "no init container should be injected on lookup failure")
}

// TestInjectFineTunedAdapter_MissingStorageURIReturnsError covers the
// FineTunedWeight-exists-but-has-no-storage-URI shape, which must return
// an error rather than dereference a nil pointer.
func TestInjectFineTunedAdapter_MissingStorageURIReturnsError(t *testing.T) {
	ftw := &v1beta1.FineTunedWeight{
		ObjectMeta: metav1.ObjectMeta{Name: "no-storage"},
	}
	fakeClient := fakeclient.New(t, fakeclient.WithObjects(ftw))

	injector, err := newFineTunedAdapterInjector(&v1.ConfigMap{Data: map[string]string{
		fineTunedAdapterConfigMapKeyName: `{"image":"ome/agent:1","compartmentId":"c1","authType":"InstancePrincipal","memoryRequest":"100Mi","memoryLimit":"1Gi","cpuRequest":"100m","cpuLimit":"1"}`,
	}}, fakeClient)
	assert.NoError(t, err)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			Annotations: map[string]string{
				constants.FineTunedAdapterInjectionKey: "no-storage",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Name: constants.MainContainerName}},
		},
	}

	assert.NotPanics(t, func() {
		err = injector.InjectFineTunedAdapter(pod)
	})
	assert.Error(t, err)
	assert.Empty(t, pod.Spec.InitContainers)
}

// newTestFineTunedAdapterInjector builds an injector from a
// fully-populated fineTunedAdapter config block backed by the given
// client for FineTunedWeight lookups.
func newTestFineTunedAdapterInjector(t *testing.T, cl client.Client, authType string) *FineTunedAdapterInjector {
	t.Helper()
	injector, err := newFineTunedAdapterInjector(&v1.ConfigMap{Data: map[string]string{
		fineTunedAdapterConfigMapKeyName: `{
  "image": "ghcr.io/test/fine-tuned-adapter:test",
  "compartmentId": "compartment-1",
  "authType": "` + authType + `",
  "region": "region-1",
  "memoryRequest": "100Mi",
  "memoryLimit": "1Gi",
  "cpuRequest": "100m",
  "cpuLimit": "1"
}`,
	}}, cl)
	require.NoError(t, err)
	return injector
}

// fakeClientWithWeight returns a client pre-loaded with one
// cluster-scoped FineTunedWeight whose storage points at an OCI URI.
func fakeClientWithWeight(t *testing.T, name, uri string) client.Client {
	t.Helper()
	ftw := &v1beta1.FineTunedWeight{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.FineTunedWeightSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: &uri},
		},
	}
	return fakeclient.New(t, fakeclient.WithObjects(ftw))
}

// adapterTestPod returns a pod carrying the adapter trigger annotation
// and a main container.
func adapterTestPod(weightName string, extraAnnotations map[string]string) *v1.Pod {
	annos := map[string]string{
		constants.FineTunedAdapterInjectionKey: weightName,
	}
	for k, v := range extraAnnotations {
		annos[k] = v
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "adapter-pod",
			Annotations: annos,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: constants.MainContainerName,
				SecurityContext: &v1.SecurityContext{
					RunAsUser: ptrTo(int64(1001)),
				},
			}},
		},
	}
}

func TestInjectFineTunedAdapter_HappyPath(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "oci://n/example-ns/b/example-bucket/o/adapters/test-adapter")
	injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")
	pod := adapterTestPod("test-weight", nil)

	require.NoError(t, injector.InjectFineTunedAdapter(pod))
	require.Len(t, pod.Spec.InitContainers, 1)

	ic := pod.Spec.InitContainers[0]
	assert.Equal(t, constants.FineTunedAdapterContainerName, ic.Name)
	assert.Equal(t, "ghcr.io/test/fine-tuned-adapter:test", ic.Image)
	assert.Equal(t, []string{"fine-tuned-adapter", "--config", "/ome-agent.yaml", "--debug"}, ic.Args)
	assert.Equal(t, v1.TerminationMessageFallbackToLogsOnError, ic.TerminationMessagePolicy)

	assert.Equal(t, resource.MustParse("1"), ic.Resources.Limits[v1.ResourceCPU])
	assert.Equal(t, resource.MustParse("1Gi"), ic.Resources.Limits[v1.ResourceMemory])
	assert.Equal(t, resource.MustParse("100m"), ic.Resources.Requests[v1.ResourceCPU])
	assert.Equal(t, resource.MustParse("100Mi"), ic.Resources.Requests[v1.ResourceMemory])

	env := map[string]string{}
	for _, e := range ic.Env {
		env[e.Name] = e.Value
	}
	assert.Equal(t, "InstancePrincipal", env[constants.AgentAuthTypeEnvVarKey])
	assert.Equal(t, "compartment-1", env[constants.AgentCompartmentIDEnvVarKey])
	assert.Equal(t, "region-1", env[constants.AgentRegionEnvVarKey])
	assert.Equal(t, "example-bucket", env[constants.AgentModelBucketNameEnvVarKey])
	assert.Equal(t, "example-ns", env[constants.AgentModelNamespaceEnvVarKey])
	assert.Equal(t, "adapters/test-adapter", env[constants.AgentModelObjectName])
	assert.Equal(t, constants.ModelDefaultMountPath, env[constants.AgentUnzippedFineTunedWeightDirectory])
	assert.Equal(t, constants.FineTunedWeightDownloadMountPath, env[constants.AgentZippedFineTunedWeightDirectory])

	// Mount order: zipped download dir first, then the unzipped weight
	// destination on the shared model emptyDir with no subpath by default.
	require.Len(t, ic.VolumeMounts, 2)
	assert.Equal(t, constants.ModelEmptyDirVolumeName, ic.VolumeMounts[0].Name)
	assert.Equal(t, constants.FineTunedWeightDownloadMountPath, ic.VolumeMounts[0].MountPath)
	assert.Equal(t, constants.ModelEmptyDirVolumeName, ic.VolumeMounts[1].Name)
	assert.Equal(t, constants.ModelDefaultMountPath, ic.VolumeMounts[1].MountPath)
	assert.Empty(t, ic.VolumeMounts[1].SubPath)

	// Security context is inherited from the main container as a deep copy.
	require.NotNil(t, ic.SecurityContext)
	assert.Equal(t, pod.Spec.Containers[0].SecurityContext, ic.SecurityContext)
	assert.NotSame(t, pod.Spec.Containers[0].SecurityContext, ic.SecurityContext)
}

func TestInjectFineTunedAdapter_AnnotationGating(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "oci://n/example-ns/b/example-bucket/o/adapters/test-adapter")
	injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")

	t.Run("absent annotation is a no-op", func(t *testing.T) {
		pod := adapterTestPod("test-weight", nil)
		delete(pod.Annotations, constants.FineTunedAdapterInjectionKey)
		assert.NoError(t, injector.InjectFineTunedAdapter(pod))
		assert.Empty(t, pod.Spec.InitContainers)
	})

	t.Run("empty annotation value is a no-op", func(t *testing.T) {
		pod := adapterTestPod("", nil)
		assert.NoError(t, injector.InjectFineTunedAdapter(pod))
		assert.Empty(t, pod.Spec.InitContainers)
	})
}

// TestInjectFineTunedAdapter_MergedWeights pins the merged-weight
// download shape: the object name env gains the merged-weight archive
// suffix so the agent fetches the single merged archive instead of the
// adapter layout.
func TestInjectFineTunedAdapter_MergedWeights(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "oci://n/example-ns/b/example-bucket/o/adapters/test-adapter")
	injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")
	pod := adapterTestPod("test-weight", map[string]string{
		constants.FTServingWithMergedWeightsAnnotationKey: "true",
	})

	require.NoError(t, injector.InjectFineTunedAdapter(pod))
	require.Len(t, pod.Spec.InitContainers, 1)

	env := map[string]string{}
	for _, e := range pod.Spec.InitContainers[0].Env {
		env[e.Name] = e.Value
	}
	assert.Equal(t, "adapters/test-adapter"+constants.MergedModelWeightZippedFileSuffix,
		env[constants.AgentModelObjectName])
}

// TestInjectFineTunedAdapter_TensorRTSubPath pins the TensorRT-LLM
// weight destination: the unzipped mount lands in the tensorrt_llm
// subdirectory of the shared model volume.
func TestInjectFineTunedAdapter_TensorRTSubPath(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "oci://n/example-ns/b/example-bucket/o/adapters/test-adapter")
	injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")
	pod := adapterTestPod("test-weight", map[string]string{
		constants.BaseModelFormat: constants.TensorRTLLM,
	})

	require.NoError(t, injector.InjectFineTunedAdapter(pod))
	require.Len(t, pod.Spec.InitContainers, 1)
	mounts := pod.Spec.InitContainers[0].VolumeMounts
	require.Len(t, mounts, 2)
	assert.Equal(t, constants.TensorRTModelVolumeMountSubPath, mounts[1].SubPath)
	assert.Equal(t, constants.ModelDefaultMountPath, mounts[1].MountPath)
}

// TestInjectFineTunedAdapter_CohereTFew pins the TFew stacked-serving
// destination: vendor cohere + tfew strategy without merged weights
// mounts the weight under the dedicated tfew path.
func TestInjectFineTunedAdapter_CohereTFew(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "oci://n/example-ns/b/example-bucket/o/adapters/test-adapter")
	injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")
	pod := adapterTestPod("test-weight", map[string]string{
		constants.BaseModelVendorAnnotationKey: string(constants.Cohere),
		constants.FineTunedWeightFTStrategyKey: string(constants.TFewTrainingStrategy),
	})

	require.NoError(t, injector.InjectFineTunedAdapter(pod))
	require.Len(t, pod.Spec.InitContainers, 1)
	ic := pod.Spec.InitContainers[0]

	require.Len(t, ic.VolumeMounts, 2)
	assert.Equal(t, constants.CohereTFewFineTunedWeightVolumeMountPath, ic.VolumeMounts[1].MountPath)
	assert.Equal(t, constants.FineTunedWeightVolumeMountSubPath, ic.VolumeMounts[1].SubPath)

	env := map[string]string{}
	for _, e := range ic.Env {
		env[e.Name] = e.Value
	}
	assert.Equal(t, constants.CohereTFewFineTunedWeightVolumeMountPath,
		env[constants.AgentUnzippedFineTunedWeightDirectory])
}

// TestInjectFineTunedAdapter_Reinvocation pins webhook-reinvocation
// safety: the presence check runs before the weight lookup, so a second
// pass neither duplicates the init container nor re-resolves the weight.
func TestInjectFineTunedAdapter_Reinvocation(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "oci://n/example-ns/b/example-bucket/o/adapters/test-adapter")
	injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")
	pod := adapterTestPod("test-weight", nil)

	require.NoError(t, injector.InjectFineTunedAdapter(pod))
	require.Len(t, pod.Spec.InitContainers, 1)
	first := pod.DeepCopy()

	require.NoError(t, injector.InjectFineTunedAdapter(pod))
	assert.Equal(t, first.Spec, pod.Spec, "re-invocation must leave the pod unchanged")
}

func TestInjectFineTunedAdapter_NonOCIStorageURI_Errors(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "hf://example-org/example-repo")
	injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")
	pod := adapterTestPod("test-weight", nil)

	assert.Error(t, injector.InjectFineTunedAdapter(pod))
	assert.Empty(t, pod.Spec.InitContainers)
}

func TestInjectFineTunedAdapter_OKEWorkloadIdentity(t *testing.T) {
	cl := fakeClientWithWeight(t, "test-weight", "oci://n/example-ns/b/example-bucket/o/adapters/test-adapter")
	injector := newTestFineTunedAdapterInjector(t, cl, constants.AuthtypeOKEWorkloadIdentity)

	t.Run("without service account is rejected", func(t *testing.T) {
		pod := adapterTestPod("test-weight", nil)
		assert.Error(t, injector.InjectFineTunedAdapter(pod))
		assert.Empty(t, pod.Spec.InitContainers)
	})

	t.Run("with service account injects and enables token automount", func(t *testing.T) {
		pod := adapterTestPod("test-weight", nil)
		pod.Spec.ServiceAccountName = "workload-identity-sa"
		require.NoError(t, injector.InjectFineTunedAdapter(pod))
		assert.Len(t, pod.Spec.InitContainers, 1)
		require.NotNil(t, pod.Spec.AutomountServiceAccountToken)
		assert.True(t, *pod.Spec.AutomountServiceAccountToken)
	})
}

func TestInjectorConstructors_TruncatedJSONConfig_ReturnError(t *testing.T) {
	tests := map[string]struct {
		configKey string
		new       func(*v1.ConfigMap) error
	}{
		"fine-tuned adapter": {
			configKey: fineTunedAdapterConfigMapKeyName,
			new: func(configMap *v1.ConfigMap) error {
				_, err := newFineTunedAdapterInjector(configMap, nil)
				return err
			},
		},
		"serving sidecar": {
			configKey: servingSidecarConfigMapKeyName,
			new: func(configMap *v1.ConfigMap) error {
				_, err := newServingSidecarInjector(configMap)
				return err
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			configMap := &v1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "test-configmap"},
				Data: map[string]string{
					tt.configKey: `{"image":`,
				},
			}
			if err := tt.new(configMap); err == nil {
				t.Fatalf("expected invalid JSON error")
			}
		})
	}
}

func TestInjectFineTunedAdapter_WeightNotFound_ReturnsLookupError(t *testing.T) {
	injector := newErrPathFineTunedAdapterInjector(t, nil)
	pod := newErrPathFineTunedAdapterPod("missing-weight")

	err := injector.InjectFineTunedAdapter(pod)
	if err == nil {
		t.Fatalf("expected fine-tuned weight lookup error")
	}
}

func TestInjectFineTunedAdapter_WeightWithoutStorageURI_ReturnsStorageURIError(t *testing.T) {
	tests := map[string]*v1beta1.StorageSpec{
		"missing storage": nil,
		"missing uri":     {},
		"empty uri":       {StorageUri: errPathStringPtr("")},
	}

	for name, storageSpec := range tests {
		t.Run(name, func(t *testing.T) {
			injector := newErrPathFineTunedAdapterInjector(t, &v1beta1.FineTunedWeight{
				ObjectMeta: metav1.ObjectMeta{Name: "ft-weight"},
				Spec: v1beta1.FineTunedWeightSpec{
					Storage: storageSpec,
				},
			})
			pod := newErrPathFineTunedAdapterPod("ft-weight")

			err := injector.InjectFineTunedAdapter(pod)
			if err == nil {
				t.Fatalf("expected storage URI validation error")
			}
			if !strings.Contains(err.Error(), "storage URI") {
				t.Fatalf("expected storage URI validation error, got %v", err)
			}
		})
	}
}

func newErrPathFineTunedAdapterInjector(t *testing.T, objects ...*v1beta1.FineTunedWeight) *FineTunedAdapterInjector {
	t.Helper()

	var weights []client.Object
	for _, obj := range objects {
		if obj != nil {
			weights = append(weights, obj)
		}
	}

	return &FineTunedAdapterInjector{
		Image:         "fine-tuned-adapter:latest",
		CompartmentId: "ocid1.compartment.oc1..test",
		AuthType:      "InstancePrincipal",
		client:        fakeclient.New(t, fakeclient.WithObjects(weights...)),
	}
}

func newErrPathFineTunedAdapterPod(weightName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				constants.FineTunedAdapterInjectionKey: weightName,
			},
		},
	}
}

func errPathStringPtr(value string) *string {
	return &value
}

// A FineTunedWeight whose URI names no bucket, or no object behind the
// trailing slash, is refused at admission; the adapter container could not
// download anything from it.
func TestInjectFineTunedAdapter_EmptyBucketOrObjectPathIsRefused(t *testing.T) {
	for name, uri := range map[string]string{
		"empty bucket":      "oci://n/example-ns/b//o/adapters/test-adapter",
		"empty object path": "oci://n/example-ns/b/example-bucket/o/",
	} {
		t.Run(name, func(t *testing.T) {
			cl := fakeClientWithWeight(t, "test-weight", uri)
			injector := newTestFineTunedAdapterInjector(t, cl, "InstancePrincipal")
			pod := adapterTestPod("test-weight", nil)

			err := injector.InjectFineTunedAdapter(pod)
			require.Error(t, err)
			assert.Contains(t, err.Error(), uri)
			assert.Empty(t, pod.Spec.InitContainers)
		})
	}
}
