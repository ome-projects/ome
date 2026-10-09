package artifactinventory

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const testParentKey = "artifact.huggingface.example"

// Reads are recorded by kind and key. Any Node/Pod access or List fails the
// test, so these tests also enforce the inventory's limited API footprint.
type recordingReader struct {
	client.Reader
	reads map[string]int
	fail  string
}

func (r *recordingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	var kind string
	switch obj.(type) {
	case *corev1.ConfigMap:
		kind = "ConfigMap"
	case *v1beta1.BaseModel:
		kind = "BaseModel"
	case *unstructured.Unstructured:
		kind = obj.GetObjectKind().GroupVersionKind().Kind
		if kind != "ReplicationJob" {
			return fmt.Errorf("unexpected resource %s", kind)
		}
	default:
		return fmt.Errorf("unexpected GET type %T", obj)
	}
	id := kind + ":" + key.String()
	r.reads[id]++
	if id == r.fail {
		return apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: kind}, key.Name, fmt.Errorf("test denial"))
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *recordingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return fmt.Errorf("inventory must not List")
}

func newTestReader(t *testing.T, objects ...client.Object) *recordingReader {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return &recordingReader{
		Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		reads:  make(map[string]int),
	}
}

func testModel(namespace, name, path string) *v1beta1.BaseModel {
	return &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: name, UID: types.UID(namespace + "-" + name), ResourceVersion: "1",
			Labels: map[string]string{"tenancy-id": "tenant-" + namespace},
		},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			Path: ptr.To(path), StorageUri: ptr.To("oci://n/store/b/models/o/" + name),
			Parameters: &map[string]string{"region": "test-region", "revision": "commit"},
		}},
	}
}

func testJob(name string, size int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "ome.io/v1beta1", "kind": "ReplicationJob",
		"metadata": map[string]interface{}{"namespace": "default", "name": name, "uid": "job-" + name},
		"spec": map[string]interface{}{"destination": map[string]interface{}{
			"storageUri": "oci://n/store/b/models/o/" + name,
			"parameters": map[string]interface{}{"region": "test-region", "revision": "commit"},
		}},
		"status": map[string]interface{}{"status": "Completed", "observed": map[string]interface{}{"sourceArtifactSizeBytes": size}},
	}}
}

func modelKey(m *v1beta1.BaseModel) string {
	return constants.GetModelConfigMapKey(m.Namespace, m.Name, false)
}

func putJSON(t *testing.T, cm *corev1.ConfigMap, key string, value interface{}) {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	cm.Data[key] = string(raw)
}

func testCM(t *testing.T, models ...*v1beta1.BaseModel) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ome", Name: "node-a", UID: "cm-a"}, Data: map[string]string{},
	}
	for _, m := range models {
		putJSON(t, cm, modelKey(m), modelEntry{Name: m.Name, Status: "Ready", ModelUID: m.UID})
	}
	return cm
}

func shareModels(t *testing.T, cm *corev1.ConfigMap, models ...*v1beta1.BaseModel) sharedParent {
	t.Helper()
	parent := sharedParent{Key: testParentKey, Status: "Ready", LocalPath: "/models/_artifacts/shared", Children: map[string]string{}}
	for _, m := range models {
		key := modelKey(m)
		putJSON(t, cm, key, modelEntry{Name: m.Name, Status: "Ready", ModelUID: m.UID, HfArtifactKey: testParentKey})
		parent.Children[key] = storagePath(m.Spec.Storage)
	}
	putJSON(t, cm, testParentKey, parent)
	return parent
}

func TestBuildSharedAndOrdinaryArtifacts(t *testing.T) {
	a := testModel("default", "shared", "/models/a")
	b := testModel("customer", "shared", "/models/b")
	a.Status.NodesReady = []string{"node-a", "node-b"}
	b.Status.NodesReady = []string{"node-b"}
	c := testModel("default", "direct", "/models/direct//")
	d := testModel("customer", "direct", "/models/direct")
	cm := testCM(t, a, b, c, d)
	shareModels(t, cm, a, b)
	r := newTestReader(t, cm, a, b, c, d, testJob("shared", 200), testJob("direct", 80))
	builder := NewBuilder(r, "")

	// The CM supplies readiness even when nodesReady omits node-a (b) or is
	// absent entirely (the direct models).
	result, err := builder.Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Len(t, result.Artifacts, 2)
	require.Equal(t, "node-a", result.NodeName)
	shared, direct := result.Artifacts[0], result.Artifacts[1]
	require.Equal(t, "/models/_artifacts/shared", shared.Path)
	require.EqualValues(t, 200, *shared.EstimatedArtifactBytes)
	require.EqualValues(t, 80, *direct.EstimatedArtifactBytes)
	for _, artifact := range result.Artifacts {
		require.True(t, artifact.EvictionEligible)
		require.Empty(t, artifact.Issues)
		require.Len(t, artifact.Models, 2)
		require.Equal(t, "customer", artifact.Models[0].Namespace)
		require.Equal(t, "tenant-customer", artifact.Models[0].TenancyID)
		require.Equal(t, types.UID("customer-"+artifact.Models[0].Name), artifact.Models[0].UID)
	}
	require.Equal(t, "/models/b", shared.Models[0].Path)
	require.Equal(t, []string{"node-b"}, shared.Models[0].NodesReady)
	require.Equal(t, []string{"node-a", "node-b"}, shared.Models[1].NodesReady)
	for _, model := range direct.Models {
		require.Equal(t, []string{}, model.NodesReady)
	}
	require.Equal(t, map[string]int{
		"ConfigMap:ome/node-a": 1, "BaseModel:default/shared": 1, "BaseModel:customer/shared": 1,
		"BaseModel:default/direct": 1, "BaseModel:customer/direct": 1,
		"ReplicationJob:default/shared": 1, "ReplicationJob:default/direct": 1,
	}, r.reads)

	// Results and GET caches belong to one call, never a previous build.
	again, err := builder.Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Equal(t, result, again)
	for _, count := range r.reads {
		require.Equal(t, 2, count)
	}
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"issues":[]`)
	require.Contains(t, string(encoded), `"nodesReady":[]`)
	require.Contains(t, string(encoded), `"nodesReady":["node-a","node-b"]`)
	require.NotContains(t, string(encoded), "sizeEvidence")
}

func TestBuildFiltersBeforePathAndJobLookup(t *testing.T) {
	vendor := testModel("default", "vendor", "")
	vendor.Spec.Storage.StorageUri = ptr.To("vendor://example")
	evicted := testModel("default", "evicted", "")
	evicted.Annotations = map[string]string{"ome.io/artifact-residency": "Evicted"}
	cm := testCM(t, vendor, evicted)
	cm.Data["clusterbasemodel.excluded"] = "not decoded"
	cm.Data["unrelated"] = "not decoded"
	cm.Data["artifact.huggingface.unused"] = "not decoded"
	cm.Data["default.basemodel.downloading"] = `{"name":"downloading","status":"Updating","hfArtifactKey":"artifact.huggingface.unused"}`
	r := newTestReader(t, cm, vendor, evicted)
	result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Equal(t, []Artifact{}, result.Artifacts)
	require.Len(t, r.reads, 3)
}

func TestBuildAssociationIssuesBlockWholeSharedGroup(t *testing.T) {
	for _, scenario := range []string{"childPathMismatch", "parentNotReady", "parentKeyMismatch", "modelUIDMismatch", "modelNotFound", "pendingDeletion"} {
		t.Run(scenario, func(t *testing.T) {
			a := testModel("default", "shared", "/models/a")
			b := testModel("customer", "shared", "/models/b")
			d := testModel("default", "direct", "/models/d")
			cm := testCM(t, a, b, d)
			parent := shareModels(t, cm, a, b)
			entry := modelEntry{Name: b.Name, Status: "Ready", ModelUID: b.UID, HfArtifactKey: testParentKey}
			objects := []client.Object{a, d, testJob("direct", 80)}
			switch scenario {
			case "childPathMismatch":
				parent.Children[modelKey(b)] = "/models/other"
			case "parentNotReady":
				parent.Status = "Updating"
			case "parentKeyMismatch":
				parent.Key = "other"
			case "modelUIDMismatch":
				entry.ModelUID = "old-model"
			case "pendingDeletion":
				raw := json.RawMessage(`{"parentPath":"/models/_artifacts/shared"}`)
				entry.HfArtifactPendingDeletion = &raw
			}
			putJSON(t, cm, modelKey(b), entry)
			putJSON(t, cm, testParentKey, parent)
			if scenario != "modelNotFound" {
				objects = append(objects, b)
			}
			objects = append(objects, cm)
			r := newTestReader(t, objects...)
			result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
			require.NoError(t, err)
			require.Len(t, result.Artifacts, 2)
			shared := result.Artifacts[0]
			require.False(t, shared.EvictionEligible)
			require.Nil(t, shared.EstimatedArtifactBytes)
			require.Equal(t, scenario, shared.Issues[0].Code)
			if scenario != "modelNotFound" {
				require.Len(t, shared.Models, 2)
			}
			require.True(t, result.Artifacts[1].EvictionEligible)
			require.Zero(t, r.reads["ReplicationJob:default/shared"])
		})
	}
}

func TestBuildFailsWithoutPartialInventory(t *testing.T) {
	for _, scenario := range []string{"missingCM", "invalidJSON", "missingParent", "invalidParentPath", "missingDirectCR", "invalidDirectPath", "cmDenied", "modelDenied", "jobDenied"} {
		t.Run(scenario, func(t *testing.T) {
			m := testModel("default", "m", "/models/m")
			cm := testCM(t, m)
			objects := []client.Object{testJob("m", 100)}
			switch scenario {
			case "invalidJSON":
				cm.Data[modelKey(m)] = "{"
			case "missingParent":
				shareModels(t, cm, m)
				delete(cm.Data, testParentKey)
			case "invalidParentPath":
				parent := shareModels(t, cm, m)
				parent.LocalPath = "relative"
				putJSON(t, cm, testParentKey, parent)
			case "invalidDirectPath":
				m.Spec.Storage.Path = ptr.To("/models/../other")
			}
			if scenario != "missingCM" {
				objects = append(objects, cm)
			}
			if scenario != "missingDirectCR" {
				objects = append(objects, m)
			}
			r := newTestReader(t, objects...)
			r.fail = map[string]string{"cmDenied": "ConfigMap:ome/node-a", "modelDenied": "BaseModel:default/m", "jobDenied": "ReplicationJob:default/m"}[scenario]
			result, err := NewBuilder(r, "").Build(context.Background(), "node-a")
			require.Error(t, err)
			require.Nil(t, result)
		})
	}
}

func TestBuildContextAndNamespace(t *testing.T) {
	cm := testCM(t)
	cm.Namespace = "agents"
	r := newTestReader(t, cm)
	b := NewBuilder(r, "agents")
	result, err := b.Build(context.Background(), "node-a")
	require.NoError(t, err)
	require.Empty(t, result.Artifacts)
	require.Equal(t, 1, r.reads["ConfigMap:agents/node-a"])

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = b.Build(ctx, "node-a")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.Equal(t, 1, r.reads["ConfigMap:agents/node-a"])
}
