package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestFilterUtil(t *testing.T) {
	scenarios := map[string]struct {
		input     map[string]string
		predicate func(string) bool
		expected  map[string]string
	}{
		"TruthyFilter": {
			input:     map[string]string{"key1": "val1", "key2": "val2"},
			predicate: func(key string) bool { return true },
			expected:  map[string]string{"key1": "val1", "key2": "val2"},
		},
		"FalsyFilter": {
			input:     map[string]string{"key1": "val1", "key2": "val2"},
			predicate: func(key string) bool { return false },
			expected:  map[string]string{},
		},
	}
	for name, scenario := range scenarios {
		result := Filter(scenario.input, scenario.predicate)

		if diff := cmp.Diff(scenario.expected, result); diff != "" {
			t.Errorf("Test %q unexpected result (-want +got): %v", name, diff)
		}
	}
}

func TestUnionUtil(t *testing.T) {
	scenarios := map[string]struct {
		input1   map[string]string
		input2   map[string]string
		expected map[string]string
	}{
		"UnionTwoMaps": {
			input1: map[string]string{"ome.io/service": "mnist",
				"label1": "value1"},
			input2: map[string]string{"service.knative.dev/service": "mnist",
				"label2": "value2"},
			expected: map[string]string{"ome.io/service": "mnist",
				"label1": "value1", "service.knative.dev/service": "mnist", "label2": "value2"},
		},
		"UnionTwoMapsOverwritten": {
			input1: map[string]string{"ome.io/service": "mnist",
				"label1": "value1", "label3": "value1"},
			input2: map[string]string{"service.knative.dev/service": "mnist",
				"label2": "value2", "label3": "value3"},
			expected: map[string]string{"ome.io/service": "mnist",
				"label1": "value1", "service.knative.dev/service": "mnist", "label2": "value2", "label3": "value3"},
		},
		"UnionWithEmptyMap": {
			input1: map[string]string{},
			input2: map[string]string{"service.knative.dev/service": "mnist",
				"label2": "value2"},
			expected: map[string]string{"service.knative.dev/service": "mnist", "label2": "value2"},
		},
		"UnionWithNilMap": {
			input1: nil,
			input2: map[string]string{"service.knative.dev/service": "mnist",
				"label2": "value2"},
			expected: map[string]string{"service.knative.dev/service": "mnist", "label2": "value2"},
		},
		"UnionNilMaps": {
			input1:   nil,
			input2:   nil,
			expected: map[string]string{},
		},
	}
	for name, scenario := range scenarios {
		result := Union(scenario.input1, scenario.input2)

		if diff := cmp.Diff(scenario.expected, result); diff != "" {
			t.Errorf("Test %q unexpected result (-want +got): %v", name, diff)
		}
	}
}

func TestContainsUtil(t *testing.T) {
	scenarios := map[string]struct {
		input1   []string
		input2   string
		expected bool
	}{
		"SliceContainsString": {
			input1:   []string{"hey", "hello"},
			input2:   "hey",
			expected: true,
		},
		"SliceDoesNotContainString": {
			input1:   []string{"hey", "hello"},
			input2:   "he",
			expected: false,
		},
		"SliceIsEmpty": {
			input1:   []string{},
			input2:   "hey",
			expected: false,
		},
	}
	for name, scenario := range scenarios {
		result := Includes(scenario.input1, scenario.input2)
		if diff := cmp.Diff(scenario.expected, result); diff != "" {
			t.Errorf("Test %q unexpected result (-want +got): %v", name, diff)
		}
	}
}

func TestAppendVolumeIfNotExists(t *testing.T) {

	scenarios := map[string]struct {
		volumes         []v1.Volume
		volume          v1.Volume
		expectedVolumes []v1.Volume
	}{
		"DuplicateVolume": {
			volumes: []v1.Volume{
				{
					Name: "oci",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-oci-sa",
						},
					},
				},
				{
					Name: "blue",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-gcp-sa",
						},
					},
				},
			},
			volume: v1.Volume{
				Name: "oci",
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: "user-oci-sa",
					},
				},
			},
			expectedVolumes: []v1.Volume{
				{
					Name: "oci",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-oci-sa",
						},
					},
				},
				{
					Name: "blue",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-gcp-sa",
						},
					},
				},
			},
		},
		"NotDuplicateVolume": {
			volumes: []v1.Volume{
				{
					Name: "azure",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-azure-sa",
						},
					},
				},
				{
					Name: "blue",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-gcp-sa",
						},
					},
				},
			},
			volume: v1.Volume{
				Name: "green",
				VolumeSource: v1.VolumeSource{
					Secret: &v1.SecretVolumeSource{
						SecretName: "user-gcp-sa",
					},
				},
			},
			expectedVolumes: []v1.Volume{
				{
					Name: "azure",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-azure-sa",
						},
					},
				},
				{
					Name: "blue",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-gcp-sa",
						},
					},
				},
				{
					Name: "green",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "user-gcp-sa",
						},
					},
				},
			},
		},
	}

	for name, scenario := range scenarios {
		volumes := AppendVolumeIfNotExists(scenario.volumes, scenario.volume)

		if diff := cmp.Diff(scenario.expectedVolumes, volumes); diff != "" {
			t.Errorf("Test %q unexpected volume (-want +got): %v", name, diff)
		}
	}
}

func TestMergeEnvs(t *testing.T) {

	scenarios := map[string]struct {
		baseEnvs     []v1.EnvVar
		overrideEnvs []v1.EnvVar
		expectedEnvs []v1.EnvVar
	}{
		"EmptyOverrides": {
			baseEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
			},
			overrideEnvs: []v1.EnvVar{},
			expectedEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
			},
		},
		"EmptyBase": {
			baseEnvs: []v1.EnvVar{},
			overrideEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
			},
			expectedEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
			},
		},
		"NoOverlap": {
			baseEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
			},
			overrideEnvs: []v1.EnvVar{
				{
					Name:  "name2",
					Value: "value2",
				},
			},
			expectedEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
				{
					Name:  "name2",
					Value: "value2",
				},
			},
		},
		"SingleOverlap": {
			baseEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
			},
			overrideEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value2",
				},
			},
			expectedEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value2",
				},
			},
		},
		"MultiOverlap": {
			baseEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value1",
				},
				{
					Name:  "name2",
					Value: "value2",
				},
				{
					Name:  "name3",
					Value: "value3",
				},
			},
			overrideEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value3",
				},
				{
					Name:  "name3",
					Value: "value1",
				},
				{
					Name:  "name4",
					Value: "value4",
				},
			},
			expectedEnvs: []v1.EnvVar{
				{
					Name:  "name1",
					Value: "value3",
				},
				{
					Name:  "name2",
					Value: "value2",
				},
				{
					Name:  "name3",
					Value: "value1",
				},
				{
					Name:  "name4",
					Value: "value4",
				},
			},
		},
	}

	for name, scenario := range scenarios {
		envs := MergeEnvs(scenario.baseEnvs, scenario.overrideEnvs)

		if diff := cmp.Diff(scenario.expectedEnvs, envs); diff != "" {
			t.Errorf("Test %q unexpected envs (-want +got): %v", name, diff)
		}
	}
}

func TestIsGpuEnabled(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	multiVendor := []string{constants.NvidiaGPUResourceType, "amd.com/gpu", "google.com/tpu"}
	scenarios := map[string]struct {
		resource             v1.ResourceRequirements
		acceleratorResources []string
		expected             bool
	}{
		"NvidiaGpuEnabled_WithConfiguredList": {
			resource: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":                           resource.Quantity{Format: "100"},
					constants.NvidiaGPUResourceType: resource.MustParse("1"),
				},
				Requests: v1.ResourceList{
					"cpu":                           resource.Quantity{Format: "90"},
					constants.NvidiaGPUResourceType: resource.MustParse("1"),
				},
			},
			acceleratorResources: multiVendor,
			expected:             true,
		},
		"AmdGpuEnabled_WithConfiguredList": {
			resource: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu":         resource.Quantity{Format: "100"},
					"amd.com/gpu": resource.MustParse("1"),
				},
			},
			acceleratorResources: multiVendor,
			expected:             true,
		},
		"TpuEnabled_WithConfiguredList": {
			resource: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"google.com/tpu": resource.MustParse("4"),
				},
			},
			acceleratorResources: multiVendor,
			expected:             true,
		},
		"GPUDisabled_EmptyLimits": {
			resource: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"cpu": resource.Quantity{Format: "100"},
				},
				Requests: v1.ResourceList{
					"cpu": resource.Quantity{Format: "90"},
				},
			},
			acceleratorResources: multiVendor,
			expected:             false,
		},
		"AbsentConfig_PreservesNvidiaOnlyBehavior": {
			resource: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					constants.NvidiaGPUResourceType: resource.MustParse("1"),
				},
			},
			acceleratorResources: nil,
			expected:             true,
		},
		"AbsentConfig_DoesNotRecognizeAmd": {
			resource: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					"amd.com/gpu": resource.MustParse("1"),
				},
			},
			acceleratorResources: nil,
			expected:             false,
		},
		"EmptyConfiguredList_AlsoFallsBackToNvidiaOnly": {
			resource: v1.ResourceRequirements{
				Limits: v1.ResourceList{
					constants.NvidiaGPUResourceType: resource.MustParse("1"),
				},
			},
			acceleratorResources: []string{},
			expected:             true,
		},
		"RequestsOnlyNeverCounts_EvenWithMatchingConfig": {
			resource: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					constants.NvidiaGPUResourceType: resource.MustParse("1"),
				},
			},
			acceleratorResources: multiVendor,
			expected:             false,
		},
	}
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			res := IsGPUEnabled(scenario.resource, scenario.acceleratorResources)
			g.Expect(res).To(gomega.Equal(scenario.expected))
		})
	}
}

func TestPodRequestsAccelerator(t *testing.T) {
	multiVendor := []string{
		constants.NvidiaGPUResourceType,
		"amd.com/gpu",
		constants.GoogleTPUResourceType,
	}
	container := func(name string, req, lim v1.ResourceList) v1.Container {
		return v1.Container{Name: name, Resources: v1.ResourceRequirements{Requests: req, Limits: lim}}
	}

	tests := []struct {
		name                 string
		spec                 *v1.PodSpec
		acceleratorResources []string
		expected             bool
	}{
		{
			name:                 "nil spec",
			spec:                 nil,
			acceleratorResources: multiVendor,
			expected:             false,
		},
		{
			name: "cpu and memory only",
			spec: &v1.PodSpec{Containers: []v1.Container{
				container("router", v1.ResourceList{"cpu": resource.MustParse("2")}, v1.ResourceList{"memory": resource.MustParse("4Gi")}),
			}},
			acceleratorResources: multiVendor,
			expected:             false,
		},
		{
			name: "accelerator in limits",
			spec: &v1.PodSpec{Containers: []v1.Container{
				container("engine", nil, v1.ResourceList{constants.GoogleTPUResourceType: resource.MustParse("4")}),
			}},
			acceleratorResources: multiVendor,
			expected:             true,
		},
		{
			// Unlike IsGPUEnabled, which reads Limits alone, this predicate
			// also reads Requests: a pod holding a chip through requests is
			// under accelerator budget just the same.
			name: "accelerator in requests only",
			spec: &v1.PodSpec{Containers: []v1.Container{
				container("engine", v1.ResourceList{constants.NvidiaGPUResourceType: resource.MustParse("8")}, nil),
			}},
			acceleratorResources: multiVendor,
			expected:             true,
		},
		{
			name: "accelerator in a container other than the first",
			spec: &v1.PodSpec{Containers: []v1.Container{
				container("proxy", v1.ResourceList{"cpu": resource.MustParse("1")}, nil),
				container("engine", nil, v1.ResourceList{"amd.com/gpu": resource.MustParse("1")}),
			}},
			acceleratorResources: multiVendor,
			expected:             true,
		},
		{
			name: "accelerator in an init container",
			spec: &v1.PodSpec{
				InitContainers: []v1.Container{
					container("warmup", nil, v1.ResourceList{constants.NvidiaGPUResourceType: resource.MustParse("1")}),
				},
				Containers: []v1.Container{
					container("router", v1.ResourceList{"cpu": resource.MustParse("2")}, nil),
				},
			},
			acceleratorResources: multiVendor,
			expected:             true,
		},
		{
			name: "vendor name outside the configured list",
			spec: &v1.PodSpec{Containers: []v1.Container{
				container("engine", nil, v1.ResourceList{"amd.com/gpu": resource.MustParse("1")}),
			}},
			acceleratorResources: []string{constants.NvidiaGPUResourceType},
			expected:             false,
		},
		{
			// No fallback list: this predicate reports only what the caller
			// asked about, so an empty list matches nothing. Callers that must
			// not silently ungate real accelerator work guard on the empty
			// list themselves rather than relying on a default here.
			name: "empty resource list matches nothing",
			spec: &v1.PodSpec{Containers: []v1.Container{
				container("engine", nil, v1.ResourceList{constants.NvidiaGPUResourceType: resource.MustParse("8")}),
			}},
			acceleratorResources: nil,
			expected:             false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			res := PodRequestsAccelerator(tt.spec, tt.acceleratorResources)
			g.Expect(res).To(gomega.Equal(tt.expected))
		})
	}
}

func TestFirstNonNilError(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	scenarios := map[string]struct {
		errors  []error
		matcher types.GomegaMatcher
	}{
		"NoNonNilError": {
			errors: []error{
				nil,
				nil,
			},
			matcher: gomega.BeNil(),
		},
		"ContainsError": {
			errors: []error{
				nil,
				errors.New("First non nil error"),
				errors.New("Second non nil error"),
			},
			matcher: gomega.Equal(errors.New("First non nil error")),
		},
	}
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			err := FirstNonNilError(scenario.errors)
			g.Expect(err).Should(scenario.matcher)
		})
	}
}

func TestRemoveString(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	testStrings := []string{
		"Model Tensorflow",
		"SKLearn Model",
		"Model",
		"ModelPytorch",
	}
	expected := []string{
		"Model Tensorflow",
		"SKLearn Model",
		"ModelPytorch",
	}
	res := RemoveString(testStrings, "Model")
	g.Expect(res).Should(gomega.Equal(expected))
}

func TestIsPrefixSupported(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	prefixes := []string{
		"S3://",
		"GCS://",
		"HTTP://",
		"HTTPS://",
	}
	scenarios := map[string]struct {
		input    string
		expected bool
	}{
		"SupportedPrefix": {
			input:    "GCS://test/model",
			expected: true,
		},
		"UnSupportedPreifx": {
			input:    "PVC://test/model",
			expected: false,
		},
	}
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			res := IsPrefixSupported(scenario.input, prefixes)
			g.Expect(res).Should(gomega.Equal(scenario.expected))
		})
	}
}

func TestFilterPodOnlyAnnotations(t *testing.T) {
	scenarios := map[string]struct {
		annotations map[string]string
		expected    map[string]string
	}{
		"NilAnnotations": {
			annotations: nil,
			expected:    nil,
		},
		"EmptyAnnotations": {
			annotations: map[string]string{},
			expected:    map[string]string{},
		},
		"FilterGrafanaAnnotations": {
			annotations: map[string]string{
				"k8s.grafana.com/scrape": "true",
				"k8s.grafana.com/port":   "8080",
				"ome.io/base-model-name": "test-model",
			},
			expected: map[string]string{
				"ome.io/base-model-name": "test-model",
			},
		},
		"FilterLokiAnnotations": {
			annotations: map[string]string{
				"loki.grafana.com/scrape":     "true",
				"loki.grafana.com/log-format": "json",
				"meta.helm.sh/release-name":   "test",
			},
			expected: map[string]string{
				"meta.helm.sh/release-name": "test",
			},
		},
		"FilterPrometheusAnnotations": {
			annotations: map[string]string{
				"prometheus.io/scrape":      "true",
				"prometheus.io/port":        "8080",
				"prometheus.io/path":        "/metrics",
				"meta.helm.sh/release-name": "test",
			},
			expected: map[string]string{
				"meta.helm.sh/release-name": "test",
			},
		},
		"FilterNetworkingGKEAnnotations": {
			annotations: map[string]string{
				"networking.gke.io/default-interface": "eth0",
				"networking.gke.io/interfaces":        "[{\"interfaceName\":\"eth0\"}]",
				"meta.helm.sh/release-name":           "test",
			},
			expected: map[string]string{
				"meta.helm.sh/release-name": "test",
			},
		},
		"FilterRDMAAnnotations": {
			annotations: map[string]string{
				"rdma.ome.io/auto-inject": "true",
				"rdma.ome.io/profile":     "default",
				"ome.io/base-model-name":  "test-model",
			},
			expected: map[string]string{
				"ome.io/base-model-name": "test-model",
			},
		},
		"FilterInjectionAnnotations": {
			annotations: map[string]string{
				constants.FineTunedAdapterInjectionKey: "weight-name",
				constants.ServingSidecarInjectionKey:   "true",
				"ome.io/base-model-name":               "test-model",
				"ome.io/serving-runtime":               "test-runtime",
			},
			expected: map[string]string{
				"ome.io/base-model-name": "test-model",
				"ome.io/serving-runtime": "test-runtime",
			},
		},
		"PreserveNonPodOnlyAnnotations": {
			annotations: map[string]string{
				"ome.io/deploymentMode":     "RawDeployment",
				"ome.io/service-type":       "ClusterIP",
				"custom.annotation/key":     "value",
				"meta.helm.sh/release-name": "test",
			},
			expected: map[string]string{
				"ome.io/deploymentMode":     "RawDeployment",
				"ome.io/service-type":       "ClusterIP",
				"custom.annotation/key":     "value",
				"meta.helm.sh/release-name": "test",
			},
		},
		"MixedAnnotations": {
			annotations: map[string]string{
				"k8s.grafana.com/scrape":               "true",
				"networking.gke.io/interfaces":         "[...]",
				constants.FineTunedAdapterInjectionKey: "weight-name",
				"ome.io/base-model-name":               "test-model",
				"ome.io/service-type":                  "ClusterIP",
				"meta.helm.sh/release-name":            "test",
			},
			expected: map[string]string{
				"ome.io/base-model-name":    "test-model",
				"ome.io/service-type":       "ClusterIP",
				"meta.helm.sh/release-name": "test",
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			result := FilterPodOnlyAnnotations(scenario.annotations)
			if diff := cmp.Diff(scenario.expected, result); diff != "" {
				t.Errorf("Test %q unexpected result (-want +got): %v", name, diff)
			}
		})
	}
}

// Helper to assert a path is a symlink with expected relative target and resolves to the absolute target.
func assertSymlink(t *testing.T, linkPath, expectedRelTarget, absoluteTarget string) {
	t.Helper()

	info, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("lstat on %s failed: %v", linkPath, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected %s to be a symlink", linkPath)
	}

	gotTarget, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("readlink failed: %v", err)
	}
	if gotTarget != expectedRelTarget {
		t.Fatalf("unexpected symlink target: got %q want %q", gotTarget, expectedRelTarget)
	}

	resolved, err := filepath.EvalSymlinks(linkPath)
	if err != nil {
		t.Fatalf("EvalSymlinks failed: %v", err)
	}
	expectedResolved, err := filepath.EvalSymlinks(absoluteTarget)
	if err != nil {
		t.Fatalf("EvalSymlinks of expected target failed: %v", err)
	}
	if filepath.Clean(resolved) != filepath.Clean(expectedResolved) {
		t.Fatalf("resolved path mismatch: got %q want %q", resolved, expectedResolved)
	}
}

func TestCreateSymbolicLink_HappyPath(t *testing.T) {
	tmp := t.TempDir()

	parentPath := filepath.Join(tmp, "parentDir")
	if err := os.MkdirAll(parentPath, 0o755); err != nil {
		t.Fatalf("failed to create parent dir: %v", err)
	}

	childPath := filepath.Join(tmp, "child", "link")

	if err := CreateSymbolicLink(childPath, parentPath); err != nil {
		t.Fatalf("CreateSymbolicLink returned error: %v", err)
	}

	expectedRel, err := filepath.Rel(filepath.Dir(childPath), parentPath)
	if err != nil {
		t.Fatalf("failed to compute expected relative path: %v", err)
	}

	assertSymlink(t, childPath, expectedRel, parentPath)
}

func TestCreateSymbolicLink_IdempotentNoOp(t *testing.T) {
	tmp := t.TempDir()

	parentPath := filepath.Join(tmp, "parent")
	if err := os.MkdirAll(parentPath, 0o755); err != nil {
		t.Fatalf("failed to create parent dir: %v", err)
	}

	childPath := filepath.Join(tmp, "child", "link")

	// First creation
	if err := CreateSymbolicLink(childPath, parentPath); err != nil {
		t.Fatalf("CreateSymbolicLink returned error: %v", err)
	}

	target1, err := os.Readlink(childPath)
	if err != nil {
		t.Fatalf("readlink failed: %v", err)
	}

	// Second creation should be a no-op
	if err := CreateSymbolicLink(childPath, parentPath); err != nil {
		t.Fatalf("CreateSymbolicLink returned error on idempotent call: %v", err)
	}

	target2, err := os.Readlink(childPath)
	if err != nil {
		t.Fatalf("readlink failed: %v", err)
	}

	if target1 != target2 {
		t.Fatalf("idempotent call should keep same symlink target, got %q want %q", target2, target1)
	}

	resolved, err := filepath.EvalSymlinks(childPath)
	if err != nil {
		t.Fatalf("EvalSymlinks failed: %v", err)
	}
	expectedResolved, err := filepath.EvalSymlinks(parentPath)
	if err != nil {
		t.Fatalf("EvalSymlinks of expected target failed: %v", err)
	}
	if filepath.Clean(resolved) != filepath.Clean(expectedResolved) {
		t.Fatalf("resolved mismatch: got %q want %q", resolved, expectedResolved)
	}
}

func TestCreateSymbolicLink_ReplacesExistingSymlinkWithDifferentTarget(t *testing.T) {
	tmp := t.TempDir()

	parentPath1 := filepath.Join(tmp, "parent1")
	parentPath2 := filepath.Join(tmp, "parent2")
	if err := os.MkdirAll(parentPath1, 0o755); err != nil {
		t.Fatalf("failed to create parent1: %v", err)
	}
	if err := os.MkdirAll(parentPath2, 0o755); err != nil {
		t.Fatalf("failed to create parent2: %v", err)
	}

	childPath := filepath.Join(tmp, "child", "link")

	// Create symlink to parent1
	if err := CreateSymbolicLink(childPath, parentPath1); err != nil {
		t.Fatalf("CreateSymbolicLink returned error: %v", err)
	}
	oldTarget, err := os.Readlink(childPath)
	if err != nil {
		t.Fatalf("readlink failed: %v", err)
	}

	// Retarget to parent2
	if err := CreateSymbolicLink(childPath, parentPath2); err != nil {
		t.Fatalf("CreateSymbolicLink returned error on retarget: %v", err)
	}

	newTarget, err := os.Readlink(childPath)
	if err != nil {
		t.Fatalf("readlink failed: %v", err)
	}

	if oldTarget == newTarget {
		t.Fatalf("symlink target should have changed, still %q", newTarget)
	}

	expectedRel2, err := filepath.Rel(filepath.Dir(childPath), parentPath2)
	if err != nil {
		t.Fatalf("failed to compute expected relative path: %v", err)
	}
	assertSymlink(t, childPath, expectedRel2, parentPath2)
}

func TestCreateSymbolicLink_WhenNonSymlinkAlreadyExists_RemoveAndCreateSymbolicLink(t *testing.T) {
	tmp := t.TempDir()

	parentPath := filepath.Join(tmp, "parent")
	if err := os.MkdirAll(parentPath, 0o755); err != nil {
		t.Fatalf("failed to create parent: %v", err)
	}

	childPath := filepath.Join(tmp, "child", "link")

	// Create a regular file at childPath
	if err := os.MkdirAll(filepath.Dir(childPath), 0o755); err != nil {
		t.Fatalf("failed to create child dir: %v", err)
	}
	if err := os.WriteFile(childPath, []byte("regular file"), 0o644); err != nil {
		t.Fatalf("failed to create regular file: %v", err)
	}

	if err := CreateSymbolicLink(childPath, parentPath); err != nil {
		t.Fatalf("not expect err")
	}

	info, err := os.Lstat(childPath)
	if err != nil {
		t.Fatalf("lstat failed: %v", err)
	}
	result := info.Mode()&os.ModeSymlink != 0
	assert.True(t, result)

	expectedRel, err := filepath.Rel(filepath.Dir(childPath), parentPath)
	if err != nil {
		t.Fatalf("failed to compute expected relative path: %v", err)
	}

	assertSymlink(t, childPath, expectedRel, parentPath)
}

func TestCreateSymbolicLink_CreatesParentDirsForChildPath(t *testing.T) {
	tmp := t.TempDir()

	parentPath := filepath.Join(tmp, "targetDir")
	if err := os.MkdirAll(parentPath, 0o755); err != nil {
		t.Fatalf("failed to create parent dir: %v", err)
	}

	// Intentionally nested childPath parent that doesn't exist
	childPath := filepath.Join(tmp, "a", "very", "deep", "dir", "link")

	// Precondition: parent of childPath should not exist
	if _, err := os.Stat(filepath.Dir(childPath)); !os.IsNotExist(err) {
		t.Fatalf("expected parent dir to not exist before call")
	}

	if err := CreateSymbolicLink(childPath, parentPath); err != nil {
		t.Fatalf("CreateSymbolicLink returned error: %v", err)
	}

	// Parent dir should now exist and link should be correct
	if _, err := os.Stat(filepath.Dir(childPath)); err != nil {
		t.Fatalf("expected parent dir to exist after call, err: %v", err)
	}

	expectedRel, err := filepath.Rel(filepath.Dir(childPath), parentPath)
	if err != nil {
		t.Fatalf("failed to compute expected relative path: %v", err)
	}
	assertSymlink(t, childPath, expectedRel, parentPath)
}

func TestContainsString(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scenarios := map[string]struct {
		values          []interface{}
		target          string
		isCaseSensitive bool
		expected        bool
	}{
		"Sensitive_Match": {
			values:          []interface{}{"hey", "Hello"},
			target:          "Hello",
			isCaseSensitive: true,
			expected:        true,
		},
		"Sensitive_NoMatch_DifferentCase": {
			values:          []interface{}{"hey", "Hello"},
			target:          "hello",
			isCaseSensitive: true,
			expected:        false,
		},
		"Insensitive_Match_DifferentCase": {
			values:          []interface{}{"hey", "Hello"},
			target:          "hello",
			isCaseSensitive: false,
			expected:        true,
		},
		"NotFoundInAllStrings": {
			values:          []interface{}{"hey", "hello"},
			target:          "he",
			isCaseSensitive: true,
			expected:        false,
		},
		"FoundWithMixedTypes": {
			values:          []interface{}{"a", 123, "b", 4.56},
			target:          "b",
			isCaseSensitive: true,
			expected:        true,
		},
		"NotFoundWithMixedTypesOnlyNonStringMatch": {
			values:          []interface{}{123, 456},
			target:          "123",
			isCaseSensitive: false,
			expected:        false,
		},
		"EmptySlice": {
			values:          []interface{}{},
			target:          "a",
			isCaseSensitive: false,
			expected:        false,
		},
		"NilSlice": {
			values:          nil,
			target:          "a",
			isCaseSensitive: true,
			expected:        false,
		},
		"ContainsNilElements": {
			values:          []interface{}{"a", nil, "c"},
			target:          "a",
			isCaseSensitive: true,
			expected:        true,
		},
		"Duplicates": {
			values:          []interface{}{"x", "x", "y"},
			target:          "x",
			isCaseSensitive: true,
			expected:        true,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			got := ContainsString(scenario.values, scenario.target, scenario.isCaseSensitive)
			g.Expect(got).To(gomega.Equal(scenario.expected))
		})
	}
}

func TestHasSymlinkPointingToDir(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(root string) (targetDir string, cleanup func())
		wantFound bool
		wantErr   bool
	}{
		{
			name: "symlink exists in child directory",
			/*
				Directory structure:
				/parent
				├── childA
				│   └── targetDir           (real directory)
				└── childB
				    └── grandchild1
				        └── link -> ../../childA/targetDir   (symlink)
			*/
			setupFunc: func(root string) (string, func()) {
				parent := filepath.Join(root, "parent")
				childA := filepath.Join(parent, "childA")
				targetDir := filepath.Join(childA, "targetDir")
				childB := filepath.Join(parent, "childB")
				grandchild := filepath.Join(childB, "grandchild1")

				dirs := []string{parent, childA, targetDir, childB, grandchild}
				for _, dir := range dirs {
					if err := os.MkdirAll(dir, 0o755); err != nil {
						t.Fatalf("failed to mkdir %s: %v", dir, err)
					}
				}

				linkPath := filepath.Join(grandchild, "link")
				relTarget, _ := filepath.Rel(grandchild, targetDir)
				if err := os.Symlink(relTarget, linkPath); err != nil {
					t.Fatalf("failed to create symlink: %v", err)
				}

				return targetDir, func() {}
			},
			wantFound: true,
			wantErr:   false,
		},
		{
			name: "symlink exists in sibling directory",
			/*
				Directory structure:
				/parent
				├── childA
				│   └── link -> ../childB   (symlink)
				└── childB                 (real directory)
			*/
			setupFunc: func(root string) (string, func()) {
				parent := filepath.Join(root, "parent")
				childA := filepath.Join(parent, "childA")
				childB := filepath.Join(parent, "childB")

				dirs := []string{parent, childA, childB}
				for _, dir := range dirs {
					if err := os.MkdirAll(dir, 0o755); err != nil {
						t.Fatalf("failed to mkdir %s: %v", dir, err)
					}
				}

				targetDir := childB

				linkPath := filepath.Join(childA, "link")
				relTarget, _ := filepath.Rel(childA, targetDir)
				if err := os.Symlink(relTarget, linkPath); err != nil {
					t.Fatalf("failed to create symlink: %v", err)
				}

				return targetDir, func() {}
			},
			wantFound: true,
			wantErr:   false,
		},
		{
			name: "no symlink exists",
			/*
				Directory structure:
				/parent
				├── childA   (real directory)
				└── childB   (real directory)
				// No symlinks anywhere
			*/
			setupFunc: func(root string) (string, func()) {
				parent := filepath.Join(root, "parent")
				childA := filepath.Join(parent, "childA")
				childB := filepath.Join(parent, "childB")

				dirs := []string{parent, childA, childB}
				for _, dir := range dirs {
					if err := os.MkdirAll(dir, 0o755); err != nil {
						t.Fatalf("failed to mkdir %s: %v", dir, err)
					}
				}

				targetDir := childA
				return targetDir, func() {}
			},
			wantFound: false,
			wantErr:   false,
		},
		{
			name: "symlink inside targetDir itself",
			/*
				Directory structure:
				/parent
				└── childA  (targetDir)
				    └── grandchild
				        └── link -> ../childA  (symlink inside targetDir)
			*/
			setupFunc: func(root string) (string, func()) {
				parent := filepath.Join(root, "parent")
				targetDir := filepath.Join(parent, "childA")
				child := filepath.Join(targetDir, "grandchild")

				if err := os.MkdirAll(child, 0o755); err != nil {
					t.Fatalf("failed to mkdir %s: %v", child, err)
				}

				linkPath := filepath.Join(child, "link")
				relTarget, _ := filepath.Rel(child, targetDir)
				if err := os.Symlink(relTarget, linkPath); err != nil {
					t.Fatalf("failed to create symlink: %v", err)
				}

				return targetDir, func() {}
			},
			wantFound: true,
			wantErr:   false,
		},
		{
			name: "a possible case from production",
			/*
				Directory structure:
				/models
				└── meta-llama
				    ├── llama-3.2-1b-instruct-parent   (real directory)
				    └── llama-3.2-1b-instruct-kid3 -> ../llama-3.2-1b-instruct-parent  (symlink)
			*/
			setupFunc: func(root string) (string, func()) {
				modelsDir := filepath.Join(root, "models")
				metaLlama := filepath.Join(modelsDir, "meta-llama")

				// Create directories
				targetDir := filepath.Join(metaLlama, "llama-3.2-1b-instruct-parent")
				kidDir := filepath.Join(metaLlama, "llama-3.2-1b-instruct-kid3")

				dirs := []string{modelsDir, metaLlama, targetDir}
				for _, d := range dirs {
					if err := os.MkdirAll(d, 0o755); err != nil {
						t.Fatalf("failed to create directory %s: %v", d, err)
					}
				}

				// Create symlink: kid3 -> parent
				relTarget, _ := filepath.Rel(metaLlama, targetDir) // relative path
				if err := os.Symlink(relTarget, kidDir); err != nil {
					t.Fatalf("failed to create symlink %s -> %s: %v", kidDir, targetDir, err)
				}
				return targetDir, func() {}
			},
			wantFound: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			targetDir, cleanup := tt.setupFunc(tmpDir)
			defer cleanup()

			found, err := HasSymlinkPointingToDir(tmpDir, targetDir)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantFound, found)
			}
		})
	}
}

func TestRemoveSymbolicLink(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(t *testing.T, dir string) string
		expectError bool
	}{
		{
			name: "symlink exists and is removed",
			setup: func(t *testing.T, dir string) string {
				targetDir := filepath.Join(dir, "target")
				linkPath := filepath.Join(dir, "link")

				assert.NoError(t, os.Mkdir(targetDir, 0755))
				assert.NoError(t, os.Symlink(targetDir, linkPath))

				return linkPath
			},
			expectError: false,
		},
		{
			name: "path does not exist (no-op)",
			setup: func(t *testing.T, dir string) string {
				return filepath.Join(dir, "nonexistent")
			},
			expectError: true,
		},
		{
			name: "path exists but is a directory (error)",
			setup: func(t *testing.T, dir string) string {
				dirPath := filepath.Join(dir, "real-dir")
				assert.NoError(t, os.Mkdir(dirPath, 0755))
				return dirPath
			},
			expectError: true,
		},
		{
			name: "path exists but is a regular file (error)",
			setup: func(t *testing.T, dir string) string {
				filePath := filepath.Join(dir, "file")
				assert.NoError(t, os.WriteFile(filePath, []byte("data"), 0644))
				return filePath
			},
			expectError: true,
		},
		{
			name: "broken symlink is removed",
			setup: func(t *testing.T, dir string) string {
				linkPath := filepath.Join(dir, "broken-link")
				assert.NoError(t, os.Symlink(filepath.Join(dir, "missing"), linkPath))
				return linkPath
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			path := tt.setup(t, tempDir)

			err := RemoveSymbolicLink(path)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			_, statErr := os.Lstat(path)
			assert.True(t, os.IsNotExist(statErr), "path should not exist after removal")
		})
	}
}

// TestGvResourcesCacheConcurrentAccess exercises the gvResourcesCache through its
// exported accessors from many goroutines at once. Before the cache was guarded by
// gvResourcesCacheMu this reliably produced a fatal "concurrent map writes" /
// "concurrent map read and map write" crash; run with -race to also catch the
// data race directly. The reads use the cache-hit branch of
// GetAvailableResourcesForApi (config is never dereferenced on a hit) so the test
// needs no live cluster.
func TestGvResourcesCacheConcurrentAccess(t *testing.T) {
	const (
		seededKeys = 8
		goroutines = 64
		opsPerG    = 200
	)

	// Pre-seed a set of keys so concurrent readers stay on the cache-hit path.
	seeded := make([]string, seededKeys)
	for i := 0; i < seededKeys; i++ {
		gv := fmt.Sprintf("seeded.ome.io/v%d", i)
		seeded[i] = gv
		SetAvailableResourcesForApi(gv, &metav1.APIResourceList{GroupVersion: gv})
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for op := 0; op < opsPerG; op++ {
				if id%2 == 0 {
					// Writer: hit both fresh per-goroutine keys and the shared
					// seeded keys to force overlapping writes.
					gv := fmt.Sprintf("writer.ome.io/g%d-op%d", id, op)
					SetAvailableResourcesForApi(gv, &metav1.APIResourceList{GroupVersion: gv})
					SetAvailableResourcesForApi(seeded[op%seededKeys], &metav1.APIResourceList{GroupVersion: seeded[op%seededKeys]})
				} else {
					// Reader: cache-hit lookups (nil config is safe on a hit).
					got, err := GetAvailableResourcesForApi(nil, seeded[op%seededKeys])
					if err != nil {
						t.Errorf("unexpected error reading seeded key: %v", err)
						return
					}
					if got == nil || got.GroupVersion != seeded[op%seededKeys] {
						t.Errorf("cache returned unexpected value for %s: %+v", seeded[op%seededKeys], got)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()

	// Sanity: every seeded key is still retrievable after the storm.
	for _, gv := range seeded {
		got, err := GetAvailableResourcesForApi(nil, gv)
		assert.NoError(t, err)
		if assert.NotNil(t, got) {
			assert.Equal(t, gv, got.GroupVersion)
		}
	}
}
