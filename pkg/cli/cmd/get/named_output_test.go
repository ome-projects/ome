package get

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8stesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

// Named GETs decode into typed objects whose TypeMeta may be empty. The
// structured output must still identify the resource, including merged views
// where the concrete selected kind cannot be inferred from the command name.
func TestGetNamedStructuredOutputIdentifiesEveryResource(t *testing.T) {
	namespaced := metav1.ObjectMeta{Name: "selected", Namespace: "team-a"}
	cluster := metav1.ObjectMeta{Name: "selected"}
	for _, tc := range []struct {
		resource string
		kind     string
		object   ctrlclient.Object
		shadow   ctrlclient.Object
	}{
		{resource: "inferenceservices", kind: "InferenceService", object: &v1beta1.InferenceService{ObjectMeta: namespaced}},
		{resource: "basemodels", kind: "BaseModel", object: &v1beta1.BaseModel{ObjectMeta: namespaced}},
		{resource: "clusterbasemodels", kind: "ClusterBaseModel", object: &v1beta1.ClusterBaseModel{ObjectMeta: cluster}},
		{resource: "servingruntimes", kind: "ServingRuntime", object: &v1beta1.ServingRuntime{ObjectMeta: namespaced}},
		{resource: "clusterservingruntimes", kind: "ClusterServingRuntime", object: &v1beta1.ClusterServingRuntime{ObjectMeta: cluster}},
		{resource: "acceleratorclasses", kind: "AcceleratorClass", object: &v1beta1.AcceleratorClass{ObjectMeta: cluster}},
		{resource: "benchmarkjobs", kind: "BenchmarkJob", object: &v1beta1.BenchmarkJob{ObjectMeta: namespaced}},
		{resource: "finetunedweights", kind: "FineTunedWeight", object: &v1beta1.FineTunedWeight{ObjectMeta: cluster}},
		{resource: "inferencereplicas", kind: "InferenceReplica", object: &v1beta1.InferenceReplica{ObjectMeta: namespaced}},
		{resource: "workloadclusters", kind: "WorkloadCluster", object: &v1beta1.WorkloadCluster{ObjectMeta: cluster}},
		{resource: "acceleratorquotas", kind: "AcceleratorQuota", object: &v1beta1.AcceleratorQuota{ObjectMeta: cluster}},
		{resource: "rolloutpolicies", kind: "RolloutPolicy", object: &v1beta1.RolloutPolicy{ObjectMeta: namespaced}},
		{resource: "trafficmaps", kind: "TrafficMap", object: &v1beta1.TrafficMap{ObjectMeta: namespaced}},
		{resource: "autoscalerpolicies", kind: "AutoscalerPolicy", object: &v1beta1.AutoscalerPolicy{ObjectMeta: namespaced}},
		{resource: "models", kind: "BaseModel", object: &v1beta1.BaseModel{ObjectMeta: namespaced}, shadow: &v1beta1.ClusterBaseModel{ObjectMeta: cluster}},
		{resource: "models", kind: "ClusterBaseModel", object: &v1beta1.ClusterBaseModel{ObjectMeta: cluster}},
		{resource: "runtimes", kind: "ServingRuntime", object: &v1beta1.ServingRuntime{ObjectMeta: namespaced}, shadow: &v1beta1.ClusterServingRuntime{ObjectMeta: cluster}},
		{resource: "runtimes", kind: "ClusterServingRuntime", object: &v1beta1.ClusterServingRuntime{ObjectMeta: cluster}},
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(tc.resource+"/"+tc.kind+"/"+format, func(t *testing.T) {
				objects := []runtime.Object{tc.object.DeepCopyObject()}
				if tc.shadow != nil {
					objects = append(objects, tc.shadow.DeepCopyObject())
				}
				f := factory.Static{
					OME:     omefake.NewSimpleClientset(objects...),
					Runtime: ctrlfake.NewClientBuilder().WithScheme(getScheme(t)).WithRuntimeObjects(objects...).Build(),
					NS:      "team-a",
				}
				out, stderr, err := executeSeparate(t, f, tc.resource, "selected", "-o", format)
				require.NoError(t, err)
				assert.Empty(t, stderr)
				var got struct {
					metav1.TypeMeta   `json:",inline"`
					metav1.ObjectMeta `json:"metadata"`
				}
				if format == "json" {
					require.True(t, json.Valid([]byte(out)), "must emit one valid JSON object")
				}
				require.NoError(t, yaml.Unmarshal([]byte(out), &got))
				assert.Equal(t, "ome.io/v1beta1", got.APIVersion)
				assert.Equal(t, tc.kind, got.Kind)
				assert.Equal(t, "selected", got.Name)
				assert.Equal(t, tc.object.GetNamespace(), got.Namespace)
			})
		}
	}
}

func TestGetNamedStructuredOutputDoesNotTrustOrMutateSourceTypeMeta(t *testing.T) {
	for _, gvk := range []schema.GroupVersionKind{
		{},
		{Group: "wrong.example", Version: "v99", Kind: "ConfigMap"},
		{Group: "ome.io", Version: "v1beta1", Kind: "InferenceService"},
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(gvk.String()+"/"+format, func(t *testing.T) {
				source := fixtureISVC("selected", "team-a")
				source.SetGroupVersionKind(gvk)
				before := source.DeepCopy()
				client := omefake.NewSimpleClientset()
				// Return the same typed response to detect accidental mutation of
				// shared client/cache objects during serialization.
				client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, source, nil
				})
				out, stderr, err := executeSeparate(t, factory.Static{OME: client, NS: "team-a"}, "isvc", "selected", "-o", format)
				require.NoError(t, err)
				assert.Empty(t, stderr)
				var got v1beta1.InferenceService
				require.NoError(t, yaml.Unmarshal([]byte(out), &got))
				assert.Equal(t, "ome.io/v1beta1", got.APIVersion)
				assert.Equal(t, "InferenceService", got.Kind)
				assert.Equal(t, before.Spec, got.Spec)
				assert.Equal(t, before.ObjectMeta, got.ObjectMeta)
				assert.Equal(t, before, source, "structured output must not rewrite the fetched object")
			})
		}
	}
}

func TestGetStructuredListIdentifiesMixedKinds(t *testing.T) {
	namespaced := metav1.ObjectMeta{Name: "selected", Namespace: "team-a"}
	cluster := metav1.ObjectMeta{Name: "selected"}
	wrong := metav1.TypeMeta{APIVersion: "wrong.example/v99", Kind: "ConfigMap"}
	for _, tc := range []struct {
		resource string
		objects  []runtime.Object
		kinds    []string
	}{
		{
			resource: "models",
			objects: []runtime.Object{
				&v1beta1.BaseModel{ObjectMeta: namespaced},
				&v1beta1.ClusterBaseModel{ObjectMeta: cluster, TypeMeta: wrong},
			},
			kinds: []string{"BaseModel", "ClusterBaseModel"},
		},
		{
			resource: "runtimes",
			objects: []runtime.Object{
				&v1beta1.ServingRuntime{ObjectMeta: namespaced, TypeMeta: wrong},
				&v1beta1.ClusterServingRuntime{ObjectMeta: cluster},
			},
			kinds: []string{"ServingRuntime", "ClusterServingRuntime"},
		},
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(tc.resource+"/"+format, func(t *testing.T) {
				f := factory.Static{OME: omefake.NewSimpleClientset(tc.objects...), NS: "team-a"}
				out, stderr, err := executeSeparate(t, f, tc.resource, "-o", format)
				require.NoError(t, err)
				assert.Empty(t, stderr)
				var got struct {
					metav1.TypeMeta `json:",inline"`
					Items           []struct {
						metav1.TypeMeta   `json:",inline"`
						metav1.ObjectMeta `json:"metadata"`
					} `json:"items"`
				}
				if format == "json" {
					require.True(t, json.Valid([]byte(out)))
				}
				require.NoError(t, yaml.Unmarshal([]byte(out), &got))
				assert.Equal(t, "v1", got.APIVersion)
				assert.Equal(t, "List", got.Kind)
				require.Len(t, got.Items, 2)
				for i, item := range got.Items {
					assert.Equal(t, "ome.io/v1beta1", item.APIVersion)
					assert.Equal(t, tc.kinds[i], item.Kind)
					assert.Equal(t, "selected", item.Name)
				}
				assert.Equal(t, "team-a", got.Items[0].Namespace)
				assert.Empty(t, got.Items[1].Namespace)
			})
		}
	}
}

func TestGetStructuredListDoesNotMutateSourceObjects(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			source := fixtureISVC("selected", "team-a")
			source.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
			before := source.DeepCopy()
			var out bytes.Buffer
			options := Options{
				IOStreams: genericiooptions.IOStreams{Out: &out},
				Output:    format,
				entry: &entry{List: func(context.Context, factory.Factory, string, metav1.ListOptions) ([]runtime.Object, error) {
					return []runtime.Object{source}, nil
				}},
			}
			require.NoError(t, options.Run(context.Background(), factory.Static{}))
			var got struct {
				Items []v1beta1.InferenceService `json:"items"`
			}
			require.NoError(t, yaml.Unmarshal(out.Bytes(), &got))
			require.Len(t, got.Items, 1)
			assert.Equal(t, "ome.io/v1beta1", got.Items[0].APIVersion)
			assert.Equal(t, "InferenceService", got.Items[0].Kind)
			assert.Equal(t, before.Spec, got.Items[0].Spec)
			assert.Equal(t, before.ObjectMeta, got.Items[0].ObjectMeta)
			assert.Equal(t, before, source)
		})
	}
}
