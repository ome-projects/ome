package pernode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func servingTestModel(cluster bool) client.Object {
	meta := metav1.ObjectMeta{Name: "model", UID: "model-uid"}
	spec := v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: ptr.To("/models/model")}}
	status := v1beta1.ModelStatusSpec{State: v1beta1.LifeCycleStateInTransit}
	if cluster {
		return &v1beta1.ClusterBaseModel{ObjectMeta: meta, Spec: spec, Status: status}
	}
	meta.Namespace = "ns"
	return &v1beta1.BaseModel{ObjectMeta: meta, Spec: spec, Status: status}
}

func servingTestClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.BaseModel{}, &v1beta1.ClusterBaseModel{}).
		WithObjects(objects...).Build()
}

func reconcileServing(t *testing.T, c client.Client, reader client.Reader, model client.Object) *v1beta1.ModelServingStatus {
	t.Helper()
	_, cluster := model.(*v1beta1.ClusterBaseModel)
	require.NoError(t, ReconcileStatusFromConfigMaps(context.Background(), c, reader, logr.Discard(), model, cluster, "Model"))
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
	_, status, err := shared.ModelSpecAndStatus(model)
	require.NoError(t, err)
	require.NotNil(t, status.Serving)
	return status.Serving
}

func TestServingServiceScopeAndDemand(t *testing.T) {
	for _, tc := range []struct {
		name, namespace, kind, group        string
		cluster, shadow, want               bool
		overlay, legacy, terminating, gated bool
	}{
		{name: "pending legacy direct", namespace: "ns", want: true},
		{name: "zero replicas", namespace: "ns", want: true},
		{name: "explicit namespaced", namespace: "ns", kind: "BaseModel", gated: true, want: true},
		{name: "different namespace", namespace: "other", kind: "BaseModel", gated: true},
		{name: "explicit cluster excludes namespaced", namespace: "ns", kind: "ClusterBaseModel", gated: true},
		{name: "cluster cross namespace", namespace: "other", kind: "ClusterBaseModel", cluster: true, gated: true, want: true},
		{name: "cluster legacy fallback", namespace: "ns", cluster: true, want: true},
		{name: "cluster shadowed", namespace: "ns", cluster: true, shadow: true},
		{name: "explicit cluster ignores shadow", namespace: "ns", kind: "ClusterBaseModel", cluster: true, shadow: true, gated: true, want: true},
		{name: "namespaced excludes cluster", namespace: "ns", kind: "BaseModel", cluster: true, gated: true},
		{name: "foreign group", namespace: "ns", kind: "BaseModel", group: "example.com", gated: true},
		{name: "overlay demand", namespace: "ns", kind: "BaseModel", overlay: true, want: true},
		{name: "ungated cluster overlay ignores shadow", namespace: "ns", kind: "ClusterBaseModel", cluster: true, shadow: true, overlay: true, want: true},
		{name: "ungated cluster overlay excludes namespaced", namespace: "ns", kind: "ClusterBaseModel", overlay: true},
		{name: "legacy annotation", namespace: "ns", legacy: true, want: true},
		{name: "terminating demand", namespace: "ns", terminating: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := servingTestModel(tc.cluster)
			ref := &v1beta1.ModelRef{Name: "model"}
			if tc.kind != "" {
				ref.Kind = ptr.To(tc.kind)
			}
			if tc.group != "" {
				ref.APIGroup = ptr.To(tc.group)
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: tc.namespace}, Spec: v1beta1.InferenceServiceSpec{Model: ref}}
			if tc.gated {
				service.Annotations = map[string]string{constants.ArtifactStartupGateAnnotation: constants.ArtifactStartupGatePending}
			}
			if tc.name == "zero replicas" {
				service.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0), MaxReplicas: 0}}
			}
			if tc.overlay {
				service.Spec.Model = &v1beta1.ModelRef{Name: "other", Overlays: []v1beta1.ModelOverlayRef{{Name: "model", Kind: ref.Kind}}}
			}
			if tc.legacy {
				service.Spec.Model = nil
				service.Annotations = map[string]string{constants.BaseModelName: "model"}
			}
			if tc.terminating {
				service.DeletionTimestamp = ptr.To(metav1.Now())
				service.Finalizers = []string{"test"}
			}
			objects := []client.Object{model.DeepCopyObject().(client.Object), service}
			if tc.shadow {
				objects = append(objects, servingTestModel(false))
			}
			// The cached client deliberately has no services: demand must use the API reader.
			c := servingTestClient(t, model)
			reader := servingTestClient(t, objects...)
			got := reconcileServing(t, c, reader, model)
			require.Equal(t, tc.want, got.InUse)
			require.Nil(t, got.LastUsedTime, "never-used and in-use models have no last-use timestamp")
		})
	}
}

func TestServingPrimaryReferenceCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, kind, group             string
		gate                          *string
		local, wantLocal, wantCluster bool
	}{
		{name: "legacy defaulted collision", kind: "ClusterBaseModel", group: "ome.io", local: true, wantLocal: true},
		{name: "legacy ignores group", kind: "ClusterBaseModel", group: "other.io", local: true, wantLocal: true},
		{name: "legacy defaulted fallback", kind: "ClusterBaseModel", wantCluster: true},
		{name: "pending cluster collision", kind: "ClusterBaseModel", gate: ptr.To("pending"), local: true, wantCluster: true},
		{name: "admitted cluster collision", kind: "ClusterBaseModel", gate: ptr.To("admitted"), local: true, wantCluster: true},
		{name: "present empty gate is scoped", kind: "ClusterBaseModel", gate: ptr.To(""), local: true, wantCluster: true},
		{name: "gated namespaced collision", kind: "BaseModel", gate: ptr.To("pending"), local: true, wantLocal: true},
		{name: "gated namespaced never falls back", kind: "BaseModel", gate: ptr.To("pending")},
		{name: "gated foreign group", kind: "ClusterBaseModel", group: "other.io", gate: ptr.To("pending"), local: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local, cluster := servingTestModel(false), servingTestModel(true)
			service := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "ns", Annotations: map[string]string{}},
				Spec: v1beta1.InferenceServiceSpec{
					Model:  &v1beta1.ModelRef{Name: "model", Kind: ptr.To(tc.kind), APIGroup: ptr.To(tc.group)},
					Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(0), MaxReplicas: 0}},
				},
			}
			if tc.gate != nil {
				service.Annotations[constants.ArtifactStartupGateAnnotation] = *tc.gate
			}
			objects := []client.Object{cluster, service}
			if tc.local {
				objects = append(objects, local)
			}
			// No Pods: pending or zero-replica demand must resolve on the ISVC alone.
			c := servingTestClient(t, objects...)
			if tc.local {
				require.Equal(t, tc.wantLocal, reconcileServing(t, c, c, local).InUse)
			}
			require.Equal(t, tc.wantCluster, reconcileServing(t, c, c, cluster).InUse)
		})
	}
}

func TestServingSurvivingPods(t *testing.T) {
	for _, tc := range []struct {
		name          string
		phase         corev1.PodPhase
		cluster, want bool
	}{
		{name: "namespaced selector", want: true},
		{name: "cluster selector", cluster: true, want: true},
		{name: "request selector", want: true},
		{name: "terminating", phase: corev1.PodRunning, want: true},
		{name: "legacy model label", want: true},
		{name: "host path", want: true},
		{name: "host subpath", want: true},
		{name: "host subpath ancestor", want: true},
		{name: "broad agent mount"},
		{name: "different namespace"},
		{name: "succeeded", phase: corev1.PodSucceeded},
		{name: "failed", phase: corev1.PodFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := servingTestModel(tc.cluster)
			key := constants.GetBaseModelLabel("ns", "model")
			if tc.cluster {
				key = constants.GetClusterBaseModelLabel("model")
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "survivor", Namespace: "ns"}, Spec: corev1.PodSpec{NodeSelector: map[string]string{key: "Ready"}}, Status: corev1.PodStatus{Phase: tc.phase}}
			switch tc.name {
			case "request selector":
				key, err := constants.ArtifactReadyLabelKey(model.GetUID())
				require.NoError(t, err)
				pod.Spec.NodeSelector = map[string]string{key: "old-request"}
			case "terminating":
				pod.DeletionTimestamp = ptr.To(metav1.Now())
				pod.Finalizers = []string{"test"}
			case "legacy model label", "different namespace":
				pod.Spec.NodeSelector = nil
				pod.Labels = map[string]string{constants.InferenceServiceBaseModelNameLabelKey: "model"}
				if tc.name == "different namespace" {
					pod.Namespace = "other"
				}
			case "host path", "host subpath", "host subpath ancestor", "broad agent mount":
				pod.Spec.NodeSelector = nil
				path := "/models/model"
				if tc.name != "host path" {
					path = "/models"
				}
				if tc.name == "host subpath ancestor" {
					path = "/"
				}
				pod.Spec.Volumes = []corev1.Volume{{Name: "weights", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: path}}}}
				mount := corev1.VolumeMount{Name: "weights", MountPath: "/weights"}
				if tc.name == "host subpath" {
					mount.SubPath = "model"
				}
				if tc.name == "host subpath ancestor" {
					mount.SubPath = "models"
				}
				pod.Spec.Containers = []corev1.Container{{Name: "runner", VolumeMounts: []corev1.VolumeMount{mount}}}
			}
			c := servingTestClient(t, model, pod)
			require.Equal(t, tc.want, reconcileServing(t, c, c, model).InUse)
		})
	}
}

func TestServingPodPrimaryBindingOverridesLegacyNames(t *testing.T) {
	for _, binding := range []string{"UID and selectors", "UID only", "scoped selector only"} {
		for _, legacy := range []string{"annotation", "label"} {
			t.Run(binding+"/"+legacy, func(t *testing.T) {
				ctx := context.Background()
				local, cluster := servingTestModel(false), servingTestModel(true)
				local.SetUID("local-uid")
				cluster.SetUID("cluster-uid")
				localSpec, localStatus, err := shared.ModelSpecAndStatus(local)
				require.NoError(t, err)
				localSpec.Storage.Path = ptr.To("/models/local")
				lastUsed := metav1.NewTime(time.Unix(100, 0))
				localStatus.Serving = &v1beta1.ModelServingStatus{LastUsedTime: lastUsed.DeepCopy()}
				clusterSpec, _, err := shared.ModelSpecAndStatus(cluster)
				require.NoError(t, err)
				clusterSpec.Storage.Path = ptr.To("/models/cluster")
				service := &v1beta1.InferenceService{
					ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "ns", Annotations: map[string]string{
						constants.ArtifactStartupGateAnnotation: constants.ArtifactStartupGateAdmitted,
						constants.ArtifactModelUIDAnnotation:    string(cluster.GetUID()),
					}},
					Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model", Kind: ptr.To("ClusterBaseModel")}},
				}
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "survivor", Namespace: "ns", Annotations: map[string]string{}, Labels: map[string]string{}},
					Spec: corev1.PodSpec{NodeSelector: map[string]string{}, Volumes: []corev1.Volume{{
						Name: "model", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: *clusterSpec.Storage.Path}},
					}}},
					Status: corev1.PodStatus{Phase: corev1.PodRunning},
				}
				if binding != "scoped selector only" {
					pod.Annotations[constants.ArtifactModelUIDAnnotation] = string(cluster.GetUID())
				}
				if binding != "UID only" {
					pod.Spec.NodeSelector[constants.GetClusterBaseModelLabel(cluster.GetName())] = "Ready"
				}
				if binding == "UID and selectors" {
					key, err := constants.ArtifactReadyLabelKey(cluster.GetUID())
					require.NoError(t, err)
					pod.Spec.NodeSelector[key] = "r2"
				}
				if legacy == "annotation" {
					pod.Annotations[constants.BaseModelName] = "model"
				} else {
					pod.Labels[constants.InferenceServiceBaseModelNameLabelKey] = "model"
				}
				c := servingTestClient(t, local, cluster, service, pod)
				for _, surviving := range []bool{false, true} {
					if surviving {
						require.NoError(t, c.Delete(ctx, service))
					}
					got := reconcileServing(t, c, c, local)
					require.False(t, got.InUse, "a cluster-model pod must not consume the same-named local model")
					require.Equal(t, &lastUsed, got.LastUsedTime, "unrelated demand must not erase usage history")
					got = reconcileServing(t, c, c, cluster)
					require.True(t, got.InUse)
					require.Nil(t, got.LastUsedTime)
				}
			})
		}
	}
}

func TestServingPodPrimaryBindingPreservesOtherModelUsage(t *testing.T) {
	for _, usage := range []string{"scoped selector", "request selector", "host path", "host subpath"} {
		t.Run(usage, func(t *testing.T) {
			model := servingTestModel(false)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "survivor", Namespace: "ns", Annotations: map[string]string{
					constants.ArtifactModelUIDAnnotation: "other-uid",
					constants.BaseModelName:              "other-model",
				}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			switch usage {
			case "scoped selector":
				pod.Spec.NodeSelector = map[string]string{constants.GetBaseModelLabel("ns", "model"): "Ready"}
			case "request selector":
				key, err := constants.ArtifactReadyLabelKey(model.GetUID())
				require.NoError(t, err)
				pod.Spec.NodeSelector = map[string]string{key: "r2"}
			case "host path", "host subpath":
				root := "/models/model"
				mount := corev1.VolumeMount{Name: "overlay", MountPath: "/overlay"}
				if usage == "host subpath" {
					root, mount.SubPath = "/models", "model"
				}
				pod.Spec.Volumes = []corev1.Volume{{Name: "overlay", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: root}}}}
				pod.Spec.Containers = []corev1.Container{{Name: "runner", VolumeMounts: []corev1.VolumeMount{mount}}}
			}
			c := servingTestClient(t, model, pod)
			require.True(t, reconcileServing(t, c, c, model).InUse, "primary identity must not hide independent model usage")
		})
	}
}

func TestServingLastUsedTransitionAndStableTimestamp(t *testing.T) {
	model := servingTestModel(false)
	_, status, _ := shared.ModelSpecAndStatus(model)
	status.Serving = &v1beta1.ModelServingStatus{InUse: true}
	c := servingTestClient(t, model)
	first := reconcileServing(t, c, c, model)
	require.False(t, first.InUse)
	require.NotNil(t, first.LastUsedTime)
	stamp := first.LastUsedTime.DeepCopy()
	rv := model.GetResourceVersion()
	second := reconcileServing(t, c, c, model)
	require.Equal(t, stamp, second.LastUsedTime)
	require.Equal(t, rv, model.GetResourceVersion(), "unchanged usage must not write status again")
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "ns"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
	require.NoError(t, c.Create(context.Background(), service))
	again := reconcileServing(t, c, c, model)
	require.True(t, again.InUse)
	require.Nil(t, again.LastUsedTime)
}

type failingServingReader struct {
	client.Reader
	failPods, failGet bool
}

func (r failingServingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if r.failGet {
		return errors.New("consumer scope lookup failed")
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r failingServingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	_, pods := list.(*corev1.PodList)
	_, services := list.(*v1beta1.InferenceServiceList)
	if !r.failGet && ((r.failPods && pods) || (!r.failPods && services)) {
		return errors.New("consumer list failed")
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestServingMissingReaderAndFailedScopePreserveUsage(t *testing.T) {
	for _, missingReader := range []bool{false, true} {
		model := servingTestModel(true)
		_, status, _ := shared.ModelSpecAndStatus(model)
		status.Serving = &v1beta1.ModelServingStatus{InUse: true}
		service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "ns"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}}}
		c := servingTestClient(t, model, service)
		var reader client.Reader = failingServingReader{Reader: c, failGet: true}
		if missingReader {
			reader = nil
		}
		err := ReconcileStatusFromConfigMaps(context.Background(), c, reader, logr.Discard(), model, true, "Model")
		require.Error(t, err)
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
		_, status, _ = shared.ModelSpecAndStatus(model)
		require.Equal(t, &v1beta1.ModelServingStatus{InUse: true}, status.Serving)
	}
}

func TestServingFailedListsPreserveUsage(t *testing.T) {
	for _, failPods := range []bool{false, true} {
		for _, inUse := range []bool{false, true} {
			model := servingTestModel(false)
			_, status, _ := shared.ModelSpecAndStatus(model)
			status.Serving = &v1beta1.ModelServingStatus{InUse: inUse}
			if !inUse {
				status.Serving.LastUsedTime = ptr.To(metav1.NewTime(time.Unix(100, 0)))
			}
			want := status.Serving.DeepCopy()
			c := servingTestClient(t, model)
			err := ReconcileStatusFromConfigMaps(context.Background(), c, failingServingReader{Reader: c, failPods: failPods}, logr.Discard(), model, false, "Model")
			require.ErrorContains(t, err, "consumer list failed")
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
			_, status, _ = shared.ModelSpecAndStatus(model)
			require.Equal(t, want, status.Serving)
		}
	}
}
