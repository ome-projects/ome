package input

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var captureTime = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

func captureReader(t *testing.T, objects ...client.Object) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, v1beta1.AddToScheme} {
		if err := register(scheme); err != nil {
			t.Fatal(err)
		}
	}
	gvk := schema.GroupVersionKind{Group: "scheduling.x-k8s.io", Version: "v1alpha1", Kind: "PodGroup"}
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind("PodGroupList"), &unstructured.UnstructuredList{})
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func captureMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: "team", UID: types.UID(name + "-uid"), ResourceVersion: "12"}
}

func TestCapturePreservesLosslessPodAndTransientOccupancy(t *testing.T) {
	deletion := metav1.NewTime(captureTime.Add(-time.Second))
	pod := &corev1.Pod{
		ObjectMeta: captureMeta("bound"),
		Spec: corev1.PodSpec{
			NodeName:    "node-a",
			Containers:  []corev1.Container{{Name: "main", Image: "image", Ports: []corev1.ContainerPort{{ContainerPort: 80, HostPort: 8080}}, Env: []corev1.EnvVar{{Name: "KEEP", Value: "value"}}}},
			Tolerations: []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists}},
			Affinity:    &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}}}}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pod.Annotations = map[string]string{"keep": "annotation"}
	terminating := pod.DeepCopy()
	terminating.Name, terminating.UID = "terminating", "terminating-uid"
	terminating.DeletionTimestamp, terminating.Finalizers = &deletion, []string{"test/finalizer"}
	pending := pod.DeepCopy()
	pending.Name, pending.UID, pending.Spec.NodeName, pending.Status.Phase = "pending", "pending-uid", "", corev1.PodPending
	terminal := pod.DeepCopy()
	terminal.Name, terminal.UID, terminal.Status.Phase = "terminal", "terminal-uid", corev1.PodSucceeded
	reader := captureReader(t, pod, terminating, pending, terminal)
	snap, err := Capture(context.Background(), reader, func() time.Time { return captureTime })
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]corev1.Pod{}
	for _, raw := range snap.Objects {
		var p corev1.Pod
		if err := json.Unmarshal(raw.Raw, &p); err != nil {
			t.Fatal(err)
		}
		if p.Kind == "Pod" {
			got[p.Name] = p
		}
	}
	if len(got) != 3 || got["terminating"].DeletionTimestamp == nil || got["pending"].Status.Phase != corev1.PodPending {
		t.Fatalf("lost transient occupancy or retained terminal pod: %v", got)
	}
	var live corev1.Pod
	if err := reader.Get(context.Background(), client.ObjectKeyFromObject(pod), &live); err != nil {
		t.Fatal(err)
	}
	live.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	if !reflect.DeepEqual(got["bound"], live) {
		t.Fatalf("capture altered full pod: got %#v; want %#v", got["bound"], live)
	}
	if err := snap.Validate(captureTime, time.Minute); err != nil {
		t.Fatal(err)
	}
}

type failingCaptureReader struct {
	client.Reader
	failAt int
	calls  int
}

func (r *failingCaptureReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.calls++
	if r.calls == r.failAt {
		return errors.New("list denied")
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestCaptureRejectsAnyIncompleteList(t *testing.T) {
	for failAt := 1; failAt <= 10; failAt++ {
		r := &failingCaptureReader{Reader: captureReader(t), failAt: failAt}
		snap, err := Capture(context.Background(), r, func() time.Time { return captureTime })
		if err == nil || snap != nil {
			t.Fatalf("list %d failure returned usable snapshot: %v, %v", failAt, snap, err)
		}
	}
}

func TestCaptureDetectsMutationAndAgeFromStart(t *testing.T) {
	call := 0
	snap, err := Capture(context.Background(), captureReader(t), func() time.Time {
		call++
		if call == 1 {
			return captureTime
		}
		return captureTime.Add(10 * time.Second)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Validate(captureTime.Add(12*time.Second), 15*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		now time.Time
		age time.Duration
	}{
		{captureTime.Add(16 * time.Second), 15 * time.Second},
		{captureTime.Add(-time.Second), time.Minute},
		{captureTime.Add(time.Second), time.Minute},
		{captureTime.Add(12 * time.Second), 0},
	} {
		if err := snap.Validate(tc.now, tc.age); err == nil {
			t.Fatalf("accepted stale/future capture: %#v", tc)
		}
	}
	snap.InferenceServices = append(snap.InferenceServices, v1beta1.InferenceService{ObjectMeta: captureMeta("injected")})
	if err := snap.Validate(captureTime.Add(12*time.Second), time.Minute); err == nil {
		t.Fatal("accepted modified snapshot contents")
	}
}

func TestCaptureContentIDAndNoAliasing(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: captureMeta("pod"), Spec: corev1.PodSpec{NodeName: "a"}}
	reader := captureReader(t, pod)
	a, err := Capture(context.Background(), reader, func() time.Time { return captureTime })
	if err != nil {
		t.Fatal(err)
	}
	b, err := Capture(context.Background(), reader, func() time.Time { return captureTime })
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == "" || a.ID != b.ID {
		t.Fatalf("unstable snapshot identity: %q %q", a.ID, b.ID)
	}
	a.Objects[0].Raw[0] = '!'
	if err := a.Validate(captureTime, time.Minute); err == nil {
		t.Fatal("accepted modified raw object")
	}
	if err := b.Validate(captureTime, time.Minute); err != nil {
		t.Fatalf("snapshot aliases another capture: %v", err)
	}
}

type orderedCaptureReader struct {
	client.Reader
	reverse bool
	version string
	partial bool
}

func (r orderedCaptureReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	objects, err := apiMeta.ExtractList(list)
	if err != nil {
		return err
	}
	if r.reverse {
		for i, j := 0, len(objects)-1; i < j; i, j = i+1, j-1 {
			objects[i], objects[j] = objects[j], objects[i]
		}
	}
	if err := apiMeta.SetList(list, objects); err != nil {
		return err
	}
	list.SetResourceVersion(r.version)
	if r.partial {
		list.SetContinue("more")
	}
	return nil
}

func TestCaptureOrderingAndListRevision(t *testing.T) {
	reader := captureReader(t, &corev1.Pod{ObjectMeta: captureMeta("a")}, &corev1.Pod{ObjectMeta: captureMeta("b")})
	clock := func() time.Time { return captureTime }
	a, err := Capture(context.Background(), orderedCaptureReader{Reader: reader, version: "10"}, clock)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Capture(context.Background(), orderedCaptureReader{Reader: reader, reverse: true, version: "10"}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Fatal("list order changed snapshot identity")
	}
	c, err := Capture(context.Background(), orderedCaptureReader{Reader: reader, version: "11"}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == c.ID {
		t.Fatal("different list revisions reused snapshot identity")
	}
	partial, err := Capture(context.Background(), orderedCaptureReader{Reader: reader, partial: true}, clock)
	if err == nil || partial != nil {
		t.Fatal("accepted paginated partial list")
	}
}

func TestCaptureIncludesSupportedObjectsAndSeparatePublicOwners(t *testing.T) {
	pg := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroup",
		"metadata": map[string]any{"name": "gang", "namespace": "team", "uid": "pg-uid"},
		"spec":     map[string]any{"minMember": int64(2)},
	}}
	objects := []client.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid"}},
		&corev1.Pod{ObjectMeta: captureMeta("pod")},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team", UID: "namespace-uid"}},
		&corev1.Service{ObjectMeta: captureMeta("service")},
		&corev1.ReplicationController{ObjectMeta: captureMeta("rc")},
		&appsv1.ReplicaSet{ObjectMeta: captureMeta("rs")},
		&appsv1.StatefulSet{ObjectMeta: captureMeta("sts")}, pg,
		&v1beta1.InferenceService{ObjectMeta: captureMeta("isvc")},
		&v1beta1.InferenceReplica{ObjectMeta: captureMeta("ir")},
	}
	snap, err := Capture(context.Background(), captureReader(t, objects...), func() time.Time { return captureTime })
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Objects) != 8 || len(snap.InferenceServices) != 1 || len(snap.InferenceReplicas) != 1 {
		t.Fatalf("incomplete object capture: %d scheduling, %d ISVC, %d IR", len(snap.Objects), len(snap.InferenceServices), len(snap.InferenceReplicas))
	}
	kinds := map[string]bool{}
	for _, raw := range snap.Objects {
		var meta metav1.TypeMeta
		if err := json.Unmarshal(raw.Raw, &meta); err != nil {
			t.Fatal(err)
		}
		if meta.APIVersion == "" {
			t.Fatal("captured object has no API version")
		}
		kinds[meta.Kind] = true
	}
	for _, kind := range []string{"Node", "Pod", "Namespace", "Service", "ReplicationController", "ReplicaSet", "StatefulSet", "PodGroup"} {
		if !kinds[kind] {
			t.Fatalf("missing %s", kind)
		}
	}
	objects[8].SetLabels(map[string]string{"mutated": "true"})
	if snap.InferenceServices[0].Labels["mutated"] != "" {
		t.Fatal("owner aliases reader input")
	}
}
