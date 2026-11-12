package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestFilter_PredicateAlwaysTrueOrFalse_KeepsAllOrNoEntries(t *testing.T) {
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

func TestUnion_OverlappingEmptyOrUnsetMaps_LaterValueWinsAndUnsetIsEmpty(t *testing.T) {
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

func TestIncludes_PresentPrefixOnlyOrEmptySlice_TrueOnlyForTheWholeString(t *testing.T) {
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

func TestAppendVolumeIfNotExists_SameOrNewName_AppendsOnlyTheNewName(t *testing.T) {

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

func TestMergeEnvs_OverlappingOrNewNames_OverridesReplaceAndNewNamesAppend(t *testing.T) {
	env := func(name, value string) v1.EnvVar { return v1.EnvVar{Name: name, Value: value} }
	for _, row := range []struct {
		name      string
		base      []v1.EnvVar
		overrides []v1.EnvVar
		want      []v1.EnvVar
	}{
		{"no overrides", []v1.EnvVar{env("name1", "value1")}, []v1.EnvVar{}, []v1.EnvVar{env("name1", "value1")}},
		{"no base", []v1.EnvVar{}, []v1.EnvVar{env("name1", "value1")}, []v1.EnvVar{env("name1", "value1")}},
		{"no overlap", []v1.EnvVar{env("name1", "value1")}, []v1.EnvVar{env("name2", "value2")},
			[]v1.EnvVar{env("name1", "value1"), env("name2", "value2")}},
		{"one overlap", []v1.EnvVar{env("name1", "value1")}, []v1.EnvVar{env("name1", "value2")}, []v1.EnvVar{env("name1", "value2")}},
		{"two overlaps and one addition",
			[]v1.EnvVar{env("name1", "value1"), env("name2", "value2"), env("name3", "value3")},
			[]v1.EnvVar{env("name1", "value3"), env("name3", "value1"), env("name4", "value4")},
			[]v1.EnvVar{env("name1", "value3"), env("name2", "value2"), env("name3", "value1"), env("name4", "value4")}},
	} {
		t.Run(row.name, func(t *testing.T) {
			if diff := cmp.Diff(row.want, MergeEnvs(row.base, row.overrides)); diff != "" {
				t.Errorf("MergeEnvs mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPodRequestsAccelerator_AnyContainerRequestOrLimit_TrueForListedVendorsOnly(t *testing.T) {
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
			// The predicate reads Requests as well as Limits: a pod holding a
			// chip through requests is under accelerator budget just the same.
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

func TestIsPrefixSupported_ListedOrUnlistedScheme_TrueOnlyForListed(t *testing.T) {
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

func TestFilterPodOnlyAnnotations_MixedOrNoKeys_DropsOnlyThePodOnlyKeys(t *testing.T) {
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

// Many goroutines read and write gvResourcesCache through its accessors at once.
// The reads hit seeded keys, so no cluster is needed and -race catches any unguarded access.
func TestGvResourcesCache_ConcurrentReadsAndWrites_SeededKeysStayReadable(t *testing.T) {
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
