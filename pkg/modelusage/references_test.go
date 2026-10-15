package modelusage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var testTime = time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
var modelKey = types.NamespacedName{Namespace: "customer", Name: "model"}

func service(name string, models ...string) *v1beta1.InferenceService {
	s := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Namespace: modelKey.Namespace,
		Name:      name,
		UID:       types.UID(name),
	}}
	if len(models) > 0 {
		s.Spec.Model = &v1beta1.ModelRef{Name: models[0]}
		for _, name := range models[1:] {
			s.Spec.Model.Overlays = append(s.Spec.Model.Overlays, v1beta1.ModelOverlayRef{Name: name})
		}
	}
	return s
}

func workload(name, model string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Namespace: modelKey.Namespace,
		Name:      name,
		UID:       types.UID(name),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey:           "service",
			constants.InferenceServiceBaseModelNameLabelKey: model,
		},
		Annotations: map[string]string{constants.BaseModelName: model},
	}
}

// The fake cache uses the same index registration as the real informer cache.
type fakeIndexer struct {
	builder       *fake.ClientBuilder
	registrations int
	err           error
}

func (i *fakeIndexer) IndexField(_ context.Context, obj client.Object, field string, extract client.IndexerFunc) error {
	if i.err != nil {
		return i.err
	}
	i.registrations++
	i.builder = i.builder.WithIndex(obj, field, extract)
	return nil
}

func queryClient(t *testing.T, objects ...client.Object) (*testClient, *ReferenceQuery) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, corev1.AddToScheme, appsv1.AddToScheme, lws.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	q := NewReferenceQuery(nil, func() bool { return true }, SourceOptions{LeaderWorkerSets: true})
	indexer := &fakeIndexer{builder: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...)}
	if err := q.RegisterIndexes(context.Background(), indexer); err != nil {
		t.Fatal(err)
	}
	if indexer.registrations != 7 {
		t.Fatalf("registered %d kinds, want 7", indexer.registrations)
	}
	c := &testClient{Client: indexer.builder.Build()}
	q.reader = c
	return c, q
}

func TestCurrentReferencesAreIndexedAndNamespaceScoped(t *testing.T) {
	s := service("use", "model", "overlay", "model")
	kind := "ClusterBaseModel"
	s.Spec.Model.Kind = &kind
	s.DeletionTimestamp = &metav1.Time{Time: testTime}
	s.Finalizers = []string{"keep"}
	c, q := queryClient(t, s)
	for _, name := range []string{"model", "overlay"} {
		active, err := q.HasReferences(context.Background(), types.NamespacedName{Namespace: modelKey.Namespace, Name: name})
		if err != nil || !active {
			t.Fatalf("%s: active=%v err=%v", name, active, err)
		}
	}
	for _, key := range []types.NamespacedName{{Namespace: "other", Name: "model"}, {Namespace: modelKey.Namespace, Name: "unused"}} {
		active, err := q.HasReferences(context.Background(), key)
		if err != nil || active {
			t.Fatalf("unrelated %s: %v %v", key, active, err)
		}
	}
	if c.unindexedLists != 0 {
		t.Fatalf("query performed %d unindexed lists", c.unindexedLists)
	}
}

func TestCurrentReferencesIncludeAllResidualWorkloadKinds(t *testing.T) {
	template := corev1.PodTemplateSpec{
		ObjectMeta: workload("template", "model"),
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name:         "model-overlay-overlay",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/models/overlay"}},
		}}},
	}
	template.Namespace = "" // Real templates inherit their owner's namespace.
	objects := []client.Object{
		&corev1.Pod{
			ObjectMeta: workload("pod", "model"),
			Spec:       template.Spec,
			Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
		},
		&appsv1.Deployment{
			ObjectMeta: workload("deploy", "model"),
			Spec:       appsv1.DeploymentSpec{Template: template},
		},
		&appsv1.ReplicaSet{
			ObjectMeta: workload("rs", "model"),
			Spec:       appsv1.ReplicaSetSpec{Template: template},
		},
		&appsv1.StatefulSet{
			ObjectMeta: workload("sts", "model"),
			Spec:       appsv1.StatefulSetSpec{Template: template},
		},
		&lws.LeaderWorkerSet{
			ObjectMeta: workload("lws", "model"),
			Spec: lws.LeaderWorkerSetSpec{LeaderWorkerTemplate: lws.LeaderWorkerTemplate{
				LeaderTemplate: &template,
				WorkerTemplate: template,
			}},
		},
		&v1beta1.InferenceReplica{
			ObjectMeta: workload("irep", "model"),
			Spec:       v1beta1.InferenceReplicaSpec{Runners: []v1beta1.Runner{{Template: template}}},
		},
	}
	for _, obj := range objects {
		t.Run(reflect.TypeOf(obj).String(), func(t *testing.T) {
			obj.SetDeletionTimestamp(&metav1.Time{Time: testTime})
			obj.SetFinalizers([]string{"keep"})
			_, q := queryClient(t, obj)
			for _, name := range []string{"model", "overlay"} {
				active, err := q.HasReferences(context.Background(), types.NamespacedName{Namespace: modelKey.Namespace, Name: name})
				if err != nil || !active {
					t.Fatalf("residual %s: %v %v", name, active, err)
				}
			}
		})
	}
}

func TestInferenceReplicaReferencesBeforeRendering(t *testing.T) {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: modelKey.Namespace, Name: "replica"},
		Spec: v1beta1.InferenceReplicaSpec{ModelRef: &v1beta1.ModelRef{
			Name:     "model",
			Overlays: []v1beta1.ModelOverlayRef{{Name: "overlay"}},
		}},
	}
	_, q := queryClient(t, ir)
	for _, name := range []string{"model", "overlay"} {
		active, err := q.HasReferences(context.Background(), types.NamespacedName{Namespace: modelKey.Namespace, Name: name})
		if err != nil || !active {
			t.Fatalf("unrendered replica %s: %v %v", name, active, err)
		}
	}
}

func TestWorkloadExtractionUsesRendererMetadataAndOnlyLocalOverlayVolumes(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: workload("pod", "model"), Spec: corev1.PodSpec{Volumes: []corev1.Volume{
		{Name: "model-overlay-local", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{}}},
		{Name: "model-overlay-remote", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "unrelated", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/models/guessed"}}},
	}}}
	refs, err := References(pod)
	if err != nil || refs.Unresolved || !reflect.DeepEqual(refs.Models, []types.NamespacedName{{Namespace: modelKey.Namespace, Name: "local"}, modelKey}) {
		t.Fatalf("refs=%+v err=%v", refs, err)
	}
	delete(pod.Labels, constants.InferenceServicePodLabelKey)
	refs, err = References(pod)
	if err != nil || refs.Unresolved || len(refs.Models) != 0 {
		t.Fatalf("non-OME pod: %+v %v", refs, err)
	}
}

func TestUnresolvedSourcesAndQueryFailuresProtect(t *testing.T) {
	for _, mode := range []string{"missing", "conflicting", "empty overlay", "unsynced", "list error"} {
		t.Run(mode, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: workload("pod", "model")}
			switch mode {
			case "missing":
				pod.Annotations = nil
				delete(pod.Labels, constants.InferenceServiceBaseModelNameLabelKey)
			case "conflicting":
				pod.Annotations[constants.BaseModelName] = "another"
			case "empty overlay":
				pod.Spec.Volumes = []corev1.Volume{{Name: "model-overlay-", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{}}}}
			}
			c, q := queryClient(t, pod)
			if mode == "unsynced" {
				q.hasSynced = func() bool { return false }
			}
			if mode == "list error" {
				c.listError = errors.New("cache unavailable")
			}
			_, err := q.HasReferences(context.Background(), types.NamespacedName{Namespace: modelKey.Namespace, Name: "unrelated"})
			if err == nil {
				t.Fatal("uncertain references accepted")
			}
			if mode != "unsynced" && mode != "list error" {
				active, err := q.HasReferences(context.Background(), types.NamespacedName{Namespace: "other", Name: "unrelated"})
				if active || err != nil {
					t.Fatalf("unresolved source leaked namespace: %v %v", active, err)
				}
			}
		})
	}
}

func TestIndexRegistrationFailureAndOptionalSource(t *testing.T) {
	q := NewReferenceQuery(nil, nil, SourceOptions{})
	c, _ := queryClient(t)
	indexer := &fakeIndexer{builder: fake.NewClientBuilder().WithScheme(c.Scheme()), err: errors.New("index failure")}
	if err := q.RegisterIndexes(context.Background(), indexer); err == nil {
		t.Fatal("registration failure hidden")
	}
	indexer.err = nil
	if err := q.RegisterIndexes(context.Background(), indexer); err != nil {
		t.Fatal(err)
	}
	if indexer.registrations != 6 {
		t.Fatalf("default kinds=%d", indexer.registrations)
	}
	if _, err := References(&corev1.Service{}); err == nil {
		t.Fatal("unsupported kind accepted")
	}
	if _, err := q.HasReferences(context.Background(), modelKey); err == nil {
		t.Fatal("missing sync predicate accepted")
	}
}

func TestRemovedReferencesOnlyEnqueuesRemovedNames(t *testing.T) {
	old := service("use", "model", "overlay", "overlay")
	updated := service("use", "new", "overlay")
	requests, err := RemovedReferences(old, updated, testTime)
	want := []Request{{Model: modelKey, ReferenceRemovedAt: testTime}}
	if err != nil || !reflect.DeepEqual(requests, want) {
		t.Fatalf("removed=%+v err=%v", requests, err)
	}
	for _, next := range []*v1beta1.InferenceService{old.DeepCopy(), service("use", "overlay", "model"), service("use", "model", "overlay", "additional")} {
		next.Generation = 10
		next.ResourceVersion = "99"
		requests, err := RemovedReferences(old, next, testTime)
		if err != nil || len(requests) != 0 {
			t.Fatalf("non-removal enqueued: %+v %v", requests, err)
		}
	}
	requests, err = RemovedReferences(nil, old, testTime)
	if err != nil || len(requests) != 0 {
		t.Fatalf("startup invented release: %+v %v", requests, err)
	}
	requests, err = RemovedReferences(old, nil, testTime)
	if err != nil || len(requests) != 2 || !requests[0].ReferenceRemovedAt.Equal(testTime) {
		t.Fatalf("delete: %+v %v", requests, err)
	}
	if _, err := RemovedReferences(old, nil, time.Time{}); err == nil {
		t.Fatal("event without timestamp accepted")
	}
}

func TestWorkloadUpdateAndDeleteDeriveReleases(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: workload("pod", "model")}
	next := pod.DeepCopy()
	next.Status.Phase = corev1.PodSucceeded
	next.DeletionTimestamp = &metav1.Time{Time: testTime}
	requests, err := RemovedReferences(pod, next, testTime)
	if err != nil || len(requests) != 0 {
		t.Fatalf("status/deletion intent released model: %+v %v", requests, err)
	}
	next.Labels[constants.InferenceServiceBaseModelNameLabelKey] = "new"
	next.Annotations[constants.BaseModelName] = "new"
	requests, err = RemovedReferences(pod, next, testTime)
	if err != nil || len(requests) != 1 || requests[0].Model != modelKey {
		t.Fatalf("reference change: %+v %v", requests, err)
	}
	requests, err = RemovedReferences(pod, nil, testTime)
	if err != nil || len(requests) != 1 {
		t.Fatalf("deletion: %+v %v", requests, err)
	}
}

func TestMultiTemplateSourcesUnionReferences(t *testing.T) {
	leader := corev1.PodTemplateSpec{ObjectMeta: workload("leader", "leader-model")}
	worker := corev1.PodTemplateSpec{ObjectMeta: workload("worker", "worker-model")}
	leader.Namespace = ""
	worker.Namespace = ""
	// InferenceReplica injects the OME label when it emits these templates.
	unlabeled := *worker.DeepCopy()
	delete(unlabeled.Labels, constants.InferenceServicePodLabelKey)
	objects := []client.Object{
		&lws.LeaderWorkerSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: modelKey.Namespace, Name: "lws"},
			Spec: lws.LeaderWorkerSetSpec{LeaderWorkerTemplate: lws.LeaderWorkerTemplate{
				LeaderTemplate: &leader,
				WorkerTemplate: worker,
			}},
		},
		&v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Namespace: modelKey.Namespace, Name: "replica"},
			Spec: v1beta1.InferenceReplicaSpec{Runners: []v1beta1.Runner{
				{Template: leader},
				{Template: unlabeled},
			}},
		},
	}
	for _, object := range objects {
		t.Run(reflect.TypeOf(object).String(), func(t *testing.T) {
			_, q := queryClient(t, object)
			for _, name := range []string{"leader-model", "worker-model"} {
				active, err := q.HasReferences(context.Background(), types.NamespacedName{
					Namespace: modelKey.Namespace,
					Name:      name,
				})
				if err != nil || !active {
					t.Fatalf("template %s missing: %v %v", name, active, err)
				}
			}
		})
	}
}

func TestMissingCacheIndexProtectsWithoutFullList(t *testing.T) {
	c, q := queryClient(t)
	c.Client = fake.NewClientBuilder().WithScheme(c.Scheme()).Build()
	if _, err := q.HasReferences(context.Background(), modelKey); err == nil {
		t.Fatal("missing cache index accepted")
	}
	if c.unindexedLists != 0 {
		t.Fatal("query fell back to unindexed list")
	}
}

func TestOnlyFinalClaimReleaseProducesHistoryEvent(t *testing.T) {
	for _, tc := range []struct {
		name, old, next string
		want            int
	}{
		{"release one of two", `{"a":true,"b":true}`, `{"b":true}`, 0},
		{"release final", `{"b":true}`, `{}`, 1},
		{"remove annotation", `{"b":true}`, "", 1},
		{"add", `{}`, `{"a":true}`, 0},
		{"no claims", `{}`, `{}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := baseModel("")
			old.Annotations[constants.ModelArtifactUseClaimsAnnotation] = tc.old
			next := old.DeepCopy()
			if tc.next == "" {
				delete(next.Annotations, constants.ModelArtifactUseClaimsAnnotation)
			} else {
				next.Annotations[constants.ModelArtifactUseClaimsAnnotation] = tc.next
			}
			requests, err := RemovedReferences(old, next, testTime)
			if err != nil || len(requests) != tc.want {
				t.Fatalf("requests=%+v err=%v", requests, err)
			}
			if tc.want == 1 && (!requests[0].ReferenceRemovedAt.Equal(testTime) || requests[0].Model != modelKey) {
				t.Fatalf("wrong claim release event: %+v", requests)
			}
			next.UID = "replacement"
			requests, err = RemovedReferences(old, next, testTime)
			if err != nil || len(requests) != 0 {
				t.Fatalf("replacement generated history: %+v %v", requests, err)
			}
		})
	}
	old := baseModel("")
	old.Annotations[constants.ModelArtifactUseClaimsAnnotation] = `{"a":true}`
	requests, err := RemovedReferences(old, nil, testTime)
	if err != nil || len(requests) != 0 {
		t.Fatalf("deleted model generated history: %+v %v", requests, err)
	}
}
