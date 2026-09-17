package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// testScheme returns a runtime.Scheme with the types the IR reconciler
// exercises registered: v1beta1 (with status subresource for IR),
// corev1 (Pods, EndpointSlices), appsv1 (ControllerRevisions),
// discoveryv1 (EndpointSlices), and schedulingv1alpha1 (PodGroups).
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("v1beta1.AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("appsv1.AddToScheme: %v", err)
	}
	if err := discoveryv1.AddToScheme(s); err != nil {
		t.Fatalf("discoveryv1.AddToScheme: %v", err)
	}
	if err := schedulingv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("schedulingv1alpha1.AddToScheme: %v", err)
	}
	return s
}

// baselineIR returns a controller-write-stamped, single-pod
// InferenceReplica matching the shape the ISVC controller will project.
// Single Runner named "default" with size 1, one container, no
// lifecycle defaults (workload.BuildPlan applies them).
func baselineIR(name, namespace string, replicas int32) *v1beta1.InferenceReplica {
	r := replicas
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			Generation: 1,
			// Controller-owner = parent ISVC. The IR reconciler reads
			// this to derive scopeUID for revision partitioning (parent
			// ISVC UID). The projector stamps the live ISVC; here we
			// stamp a synthetic UID matching the well-known parent name.
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       "llama",
				UID:        types.UID("llama-isvc-uid"),
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{
				Name: "llama",
			},
			Component: v1beta1.EngineComponent,
			Replicas:  &r,
			Runners: []v1beta1.Runner{
				{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
						},
					},
				},
			},
		},
	}
}

// newReconciler builds a fake-client-backed Reconciler with the IR
// status subresource wired and a fresh Expectations cache so per-test
// create attempts aren't blocked by entries from a previous test.
const testScaleDownRequeueInterval = 37 * time.Second

func newReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithIndex(&schedulingv1alpha1.PodGroup{}, workloadgang.PodGroupControllerUIDIndexField, workloadgang.PodGroupControllerUIDIndexExtractor).
		Build()
	return &Reconciler{
		Client:                   c,
		APIReader:                c,
		Log:                      logf.Log.WithName("test"),
		InstanceStatusTarget:     irstatus.EncodingDenseV1,
		Expectations:             workload.NewExpectations(),
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
	}, c
}

// TestReconcile_NotFound_NoError pins the early-return contract: an IR
// deleted between enqueue and reconcile must be a clean no-op (owner-ref
// cascade GC handles children).
func TestReconcile_NotFound_NoError(t *testing.T) {
	g := gomega.NewWithT(t)
	r, _ := newReconciler(t)
	key := types.NamespacedName{Name: "missing", Namespace: "default"}
	r.scaleDownSeriesCache = map[types.NamespacedName]scaleDownSeriesIdentity{
		key: {uid: "deleted-uid", namespace: key.Namespace, isvc: "deleted-isvc", component: "engine"},
	}
	obsmetrics.SetScaleDownActivePods(key.Namespace, "deleted-isvc", "engine", 5)
	obsmetrics.SetScaleDownDeferredInstances(key.Namespace, "deleted-isvc", "engine", 3)
	assertScaleDownGaugeSeries(t, true, key.Namespace, "deleted-isvc", "engine")
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: key,
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"deleted IR between enqueue and reconcile should be a no-op")
	g.Expect(result.Requeue).To(gomega.BeFalse())
	g.Expect(result.RequeueAfter).To(gomega.BeZero())
	assertScaleDownGaugeSeries(t, false, key.Namespace, "deleted-isvc", "engine")
	g.Expect(r.scaleDownSeriesCache).NotTo(gomega.HaveKey(key))
}

func TestScaleDownSeriesCacheReplacesIdentityAndSupportsConcurrentKeys(t *testing.T) {
	r := &Reconciler{}
	old := baselineIR("shared-name", "metric-identity", 1)
	old.Spec.ParentRef.Name = "old-parent"
	r.rememberScaleDownSeries(old)
	obsmetrics.SetScaleDownActivePods(old.Namespace, old.Spec.ParentRef.Name, string(old.Spec.Component), 7)
	obsmetrics.SetScaleDownDeferredInstances(old.Namespace, old.Spec.ParentRef.Name, string(old.Spec.Component), 3)
	assertScaleDownGaugeSeries(t, true, old.Namespace, old.Spec.ParentRef.Name, string(old.Spec.Component))

	replacement := old.DeepCopy()
	replacement.UID = "replacement-uid"
	replacement.Spec.ParentRef.Name = "new-parent"
	replacement.Spec.Component = v1beta1.DecoderComponent
	r.rememberScaleDownSeries(replacement)
	assertScaleDownGaugeSeries(t, false, old.Namespace, old.Spec.ParentRef.Name, string(old.Spec.Component))
	if got := r.scaleDownSeriesCache[client.ObjectKeyFromObject(replacement)]; got.uid != replacement.UID || got.isvc != replacement.Spec.ParentRef.Name || got.component != string(replacement.Spec.Component) {
		t.Fatalf("replacement metric identity = %+v", got)
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		ir := baselineIR(fmt.Sprintf("parallel-%d", i), "metric-parallel", 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.rememberScaleDownSeries(ir)
			r.deleteRememberedScaleDownSeries(client.ObjectKeyFromObject(ir))
		}()
	}
	wg.Wait()
	if len(r.scaleDownSeriesCache) != 1 {
		t.Fatalf("metric identity cache entries = %d, want only replacement", len(r.scaleDownSeriesCache))
	}
	r.deleteScaleDownSeries(replacement)
}

func TestApplyRollbackPayload_RecordedTopologyIsAuthoritative(t *testing.T) {
	stableTopology := "topology.example.com/stable"
	desired := workload.WorkloadDesiredSpec{TopologyKey: "topology.example.com/canary"}
	payload := &revision.DataPayload{TopologyKey: &stableTopology}

	r := &Reconciler{APIReader: podListFailingReader{}}
	if err := r.applyRollbackPayload(context.Background(), nil, &desired, payload, nil); err != nil {
		t.Fatalf("applyRollbackPayload: %v", err)
	}

	if desired.TopologyKey != stableTopology {
		t.Fatalf("recorded rollback topology: got %q want %q", desired.TopologyKey, stableTopology)
	}
}

func TestApplyRollbackPayload_LegacyTopologyRecovery(t *testing.T) {
	const stableTopology = "topology.example.com/stable"
	ir := baselineIR("llama-engine", "prod", 1)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-stablehash"}}
	payload := &revision.DataPayload{
		PodSpec:       &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader"}}},
		WorkerPodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}
	worker := rollbackTopologyWorker(ir, "worker-0", "0", "stablehash", stableTopology)
	otherRevision := rollbackTopologyWorker(ir, "worker-canary", "1", "canaryhash", "topology.example.com/canary")
	r, _ := newReconciler(t, worker, otherRevision)
	// Recovery must still inspect the stable revision when the canary removed
	// topology; live stable workers prove the rollback target used this key.
	desired := workload.WorkloadDesiredSpec{}

	if err := r.applyRollbackPayload(context.Background(), ir, &desired, payload, target); err != nil {
		t.Fatalf("applyRollbackPayload: %v", err)
	}
	if desired.TopologyKey != stableTopology {
		t.Fatalf("recovered rollback topology: got %q want %q", desired.TopologyKey, stableTopology)
	}
}

func TestApplyRollbackPayload_LegacyTopologyWithoutEvidenceFailsClosed(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-stablehash"}}
	payload := &revision.DataPayload{
		PodSpec:       &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader"}}},
		WorkerPodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}
	r, _ := newReconciler(t)
	desired := workload.WorkloadDesiredSpec{TopologyKey: "topology.example.com/current"}

	err := r.applyRollbackPayload(context.Background(), ir, &desired, payload, target)
	if err == nil || !strings.Contains(err.Error(), "no unambiguous OME-generated topology") {
		t.Fatalf("legacy rollback without live topology evidence: got %v", err)
	}
}

func TestApplyRollbackPayload_LegacyTopologyAmbiguityFailsClosed(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-stablehash"}}
	payload := &revision.DataPayload{
		PodSpec:       &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader"}}},
		WorkerPodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}
	podA := rollbackTopologyWorker(ir, "worker-a", "0", "stablehash", "topology.example.com/a")
	podB := rollbackTopologyWorker(ir, "worker-b", "1", "stablehash", "topology.example.com/b")
	r, _ := newReconciler(t, podA, podB)
	desired := workload.WorkloadDesiredSpec{TopologyKey: "topology.example.com/current"}

	err := r.applyRollbackPayload(context.Background(), ir, &desired, payload, target)
	if err == nil || !strings.Contains(err.Error(), "conflicting OME-generated topology") {
		t.Fatalf("ambiguous legacy rollback topology: got %v", err)
	}
}

func TestApplyRollbackPayload_LegacyTopologyFreeDoesNotBlock(t *testing.T) {
	desired := workload.WorkloadDesiredSpec{}
	payload := &revision.DataPayload{
		PodSpec:       &corev1.PodSpec{Containers: []corev1.Container{{Name: "leader"}}},
		WorkerPodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}
	r := &Reconciler{APIReader: podListFailingReader{}}

	if err := r.applyRollbackPayload(context.Background(), nil, &desired, payload, nil); err != nil {
		t.Fatalf("topology-free legacy rollback must not read pods or fail: %v", err)
	}
	if desired.TopologyKey != "" {
		t.Fatalf("TopologyKey: got %q want empty", desired.TopologyKey)
	}
}

func rollbackTopologyWorker(ir *v1beta1.InferenceReplica, name, index, revisionHash, topologyKey string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ir.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
				constants.OMEComponentLabel:           string(ir.Spec.Component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRunner:                     string(v1beta1.RunnerNameWorker),
				query.LabelInstanceIdx:                index,
				query.LabelRevisionHash:               revisionHash,
			},
		},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey: topologyKey,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
					constants.OMEComponentLabel:           string(ir.Spec.Component),
					query.LabelInstanceIdx:                index,
					query.LabelRunner:                     string(v1beta1.RunnerNameLeader),
				}},
			}},
		}}},
	}
}

type podListFailingReader struct {
	client.Reader
}

func (r podListFailingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return errors.New("injected live pod list failure")
	}
	return r.Reader.List(ctx, list, opts...)
}

type podGroupListCountingReader struct {
	client.Reader
	lists int
}

func (r *podGroupListCountingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*schedulingv1alpha1.PodGroupList); ok {
		r.lists++
	}
	return r.Reader.List(ctx, list, opts...)
}

type firstStaleInferenceReplicaReader struct {
	client.Reader
	stale  *v1beta1.InferenceReplica
	served bool
}

func (r *firstStaleInferenceReplicaReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if target, ok := obj.(*v1beta1.InferenceReplica); ok && !r.served && key == client.ObjectKeyFromObject(r.stale) {
		r.served = true
		r.stale.DeepCopyInto(target)
		return nil
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestReconcile_TerminatingPodGroupUsesConfiguredPoll(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "test:v1"}}}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "test:v1"}}}}},
	}
	now := metav1.Now()
	pg := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:              query.PodGroupName(ir.Spec.ParentRef.Name, workload.ComponentEngine, 0),
		Namespace:         ir.Namespace,
		UID:               "terminating-pg",
		DeletionTimestamp: &now,
		Finalizers:        []string{"example.com/hold"},
		OwnerReferences:   []metav1.OwnerReference{*metav1.NewControllerRef(ir, IRGVK())},
	}}
	r, c := newReconciler(t, ir, pg)
	r.APIReader = c
	r.GangSchedulingAvailable = true

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)})
	if err != nil {
		t.Fatalf("reconcile terminating PodGroup: %v", err)
	}
	if result.Requeue || result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("terminating PodGroup must use the configured poll interval, got %+v", result)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("created %d pods while their PodGroup name was terminating", len(pods.Items))
	}
}

func TestReconcile_GangCleanupRetainsStatusUntilPodGroupAbsent(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "test:v2"}}}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "test:v2"}}}}},
	}
	surgeIndex := int32(2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceFailed,
			RunningRevision: "llama-engine-old", TargetRevision: "llama-engine-retired",
			Operation: &v1beta1.InstanceOperation{
				ID: "source-0", Type: v1beta1.InstanceOperationUpdate, Step: "Surge",
				TargetRevision: "llama-engine-retired", SurgeIndex: &surgeIndex,
			},
		},
		{
			Index: surgeIndex, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			TargetRevision: "llama-engine-retired",
			Operation: &v1beta1.InstanceOperation{
				ID: "target-2", Type: v1beta1.InstanceOperationUpdate,
				Step: workload.UpdateStepGangSurgeTarget, TargetRevision: "llama-engine-retired",
			},
		},
	}
	targetPG := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:            query.PodGroupName(ir.Spec.ParentRef.Name, workload.ComponentEngine, surgeIndex),
		Namespace:       ir.Namespace,
		UID:             "target-pg",
		Finalizers:      []string{"example.com/hold"},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ir, IRGVK())},
	}}
	r, c := newReconciler(t, ir, targetPG)
	r.APIReader = c
	r.GangSchedulingAvailable = true
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("delete PodGroup pass: %v", err)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get IR after delete request: %v", err)
	}
	if statusByIndex(stored.Status.InstanceStatuses, surgeIndex) == nil {
		t.Fatal("cleanup marker was removed before PodGroup absence was observed")
	}
	terminating := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(targetPG), terminating); err != nil {
		t.Fatalf("get terminating target PodGroup: %v", err)
	}
	if terminating.DeletionTimestamp == nil {
		t.Fatal("target PodGroup delete was not issued")
	}

	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("terminating PodGroup pass: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get IR while PodGroup is terminating: %v", err)
	}
	if statusByIndex(stored.Status.InstanceStatuses, surgeIndex) == nil {
		t.Fatal("cleanup marker was removed while the PodGroup was terminating")
	}

	terminating.Finalizers = nil
	if err := c.Update(context.Background(), terminating); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("release target PodGroup finalizer: %v", err)
	}
	remaining := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(targetPG), remaining); err == nil {
		if err := c.Delete(context.Background(), remaining); err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("complete target PodGroup deletion: %v", err)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("get target PodGroup after finalizer release: %v", err)
	}

	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("absence completion pass: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get completed IR: %v", err)
	}
	if statusByIndex(stored.Status.InstanceStatuses, surgeIndex) != nil {
		t.Fatal("cleanup marker remained after authoritative PodGroup absence")
	}
	source := statusByIndex(stored.Status.InstanceStatuses, 0)
	if source == nil || source.Phase != v1beta1.OMENativeInstanceReady || source.Operation != nil {
		t.Fatalf("source was not reset atomically with marker removal: %+v", source)
	}
}

func TestReconcile_GangScaleDownWaitsForPodGroupAbsence(t *testing.T) {
	ir := baselineIR("llama-engine", "gang-scale-down", 1)
	ir.Finalizers = []string{TeardownFinalizer}
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "test:v1"}}}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "test:v1"}}}}},
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, PodCount: 2, ReadyPodCount: 2, ServingPodCount: 2},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, PodCount: 2, ReadyPodCount: 2, ServingPodCount: 2},
	}
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	objects := []client.Object{ir}
	for index := int32(0); index <= 1; index++ {
		groupName := query.PodGroupName(ir.Spec.ParentRef.Name, component, index)
		for _, runner := range []string{string(v1beta1.RunnerNameLeader), string(v1beta1.RunnerNameWorker)} {
			pod := podForIR(ir, index, runner, 0, true, true)
			pod.Labels[query.LabelPodGroup] = groupName
			objects = append(objects, pod)
		}
		pg := ownedPodGroupForIR(ir, groupName, index)
		pg.Spec.MinMember = 2
		objects = append(objects, pg)
	}

	r, base := newReconciler(t, objects...)
	podDeletes, podGroupDeletes := 0, 0
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			switch obj.(type) {
			case *corev1.Pod:
				podDeletes++
			case *schedulingv1alpha1.PodGroup:
				podGroupDeletes++
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r.APIReader = base
	r.GangSchedulingAvailable = true
	budget := int32(2)
	r.ScaleDownPodBatchSize = &budget
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	result, err := r.Reconcile(context.Background(), req)
	if err != nil || !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("admission pass result/error = %+v/%v", result, err)
	}
	result, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Pod effect pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval || podDeletes != 2 || podGroupDeletes != 0 {
		t.Fatalf("Pod effect result/deletes = %+v/%d/%d, want poll/2/0", result, podDeletes, podGroupDeletes)
	}

	result, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("PodGroup delete pass: %v", err)
	}
	if result.RequeueAfter != testScaleDownRequeueInterval || podGroupDeletes != 1 {
		t.Fatalf("PodGroup delete result/count = %+v/%d, want poll/1", result, podGroupDeletes)
	}
	extraGroup := types.NamespacedName{
		Namespace: ir.Namespace,
		Name:      query.PodGroupName(ir.Spec.ParentRef.Name, component, 1),
	}
	if err := base.Get(context.Background(), extraGroup, &schedulingv1alpha1.PodGroup{}); !apierrors.IsNotFound(err) {
		t.Fatalf("extra PodGroup still present after accepted delete: %v", err)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get IR after PodGroup delete: %v", err)
	}
	if statusByIndex(stored.Status.InstanceStatuses, 1) == nil {
		t.Fatal("InstanceStatus was removed before a fresh PodGroup absence observation")
	}

	result, err = r.Reconcile(context.Background(), req)
	if err != nil || !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("status completion pass result/error = %+v/%v", result, err)
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get completed IR: %v", err)
	}
	if statusByIndex(stored.Status.InstanceStatuses, 1) != nil {
		t.Fatalf("extra InstanceStatus remained after PodGroup absence: %+v", stored.Status.InstanceStatuses)
	}
	retainedGroup := types.NamespacedName{
		Namespace: ir.Namespace,
		Name:      query.PodGroupName(ir.Spec.ParentRef.Name, component, 0),
	}
	if err := base.Get(context.Background(), retainedGroup, &schedulingv1alpha1.PodGroup{}); err != nil {
		t.Fatalf("retained Instance PodGroup changed: %v", err)
	}
}

func statusByIndex(statuses []v1beta1.OMENativeInstanceStatus, index int32) *v1beta1.OMENativeInstanceStatus {
	for i := range statuses {
		if statuses[i].Index == index {
			return &statuses[i]
		}
	}
	return nil
}

func TestReconcile_PausedDeadlineParksWhenTopologyReadFails(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Paused = true
	ir.Spec.Runners = []v1beta1.Runner{
		{
			Name: v1beta1.RunnerNameLeader,
			Size: 1,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "leader", Image: "test:v1"}},
			}},
		},
		{
			Name: v1beta1.RunnerNameWorker,
			Size: 1,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "worker", Image: "test:v1"}},
			}},
		},
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationCreate,
			Step:     "CreatePods",
			Deadline: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
	}}
	r, c := newReconciler(t, ir)
	r.APIReader = podListFailingReader{Reader: c}
	r.GangSchedulingAvailable = true
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err == nil || !strings.Contains(err.Error(), "injected live pod list failure") {
		t.Fatalf("expected topology live-read error, got %v", err)
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get paused IR: %v", err)
	}
	if got.Status.InstanceStatuses[0].Operation == nil ||
		!got.Status.InstanceStatuses[0].Operation.Deadline.IsZero() {
		t.Fatalf("paused deadline was not parked after earlier topology error: %+v",
			got.Status.InstanceStatuses[0].Operation)
	}
	if got.Status.InstanceStatuses[0].Phase == v1beta1.OMENativeInstanceFailed {
		t.Fatal("paused Instance expired despite topology error")
	}
}

func TestReconcile_ParentPauseParksDeadlineBeforeRevisionError(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	// Model a stale projection caused by an ISVC component render failure: the
	// parent is paused, but the IR spec was never patched.
	ir.Spec.Paused = false
	ir.OwnerReferences = nil // force revision creation to fail before the defer
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			Type:     v1beta1.InstanceOperationCreate,
			Step:     "CreatePods",
			Deadline: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
	}}
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name:      ir.Spec.ParentRef.Name,
		Namespace: ir.Namespace,
		Annotations: map[string]string{
			constants.PausedRolloutAnnotation: "true",
		},
	}}
	r, c := newReconciler(t, ir, parent)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err == nil || !strings.Contains(err.Error(), "missing controller OwnerReference") {
		t.Fatalf("expected pre-defer revision error, got %v", err)
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get paused IR: %v", err)
	}
	if got.Spec.Paused {
		t.Fatal("test requires stale IR spec pause=false; parent must be authoritative at reconcile time")
	}
	if got.Status.InstanceStatuses[0].Operation == nil ||
		!got.Status.InstanceStatuses[0].Operation.Deadline.IsZero() {
		t.Fatalf("parent pause did not park deadline before revision error: %+v",
			got.Status.InstanceStatuses[0].Operation)
	}
}

// TestReconcile_DeletionTimestamp_NoOp pins the pre-upgrade /
// hand-stripped shape: an IR Terminating WITHOUT the teardown finalizer
// is a clean no-op — background GC owns the children via their owner
// references and the controller must not interfere. This is the
// upgrade story for IRs already Terminating before the finalizer
// existed. Deeper assertions (pods untouched) live in teardown_test.go.
func TestReconcile_DeletionTimestamp_NoOp(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	now := metav1.Now()
	ir.DeletionTimestamp = &now
	// A DeletionTimestamp without a finalizer makes the fake client
	// drop the object immediately. Stamp a dummy (non-teardown)
	// finalizer so the IR remains observable for the test.
	ir.Finalizers = []string{"keep-for-test"}
	r, _ := newReconciler(t, ir)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.Requeue).To(gomega.BeFalse())
	g.Expect(result.RequeueAfter).To(gomega.BeZero())
}

// TestReconcile_Create_MaterializesPods pins the create-from-scratch
// path: new IR with replicas=2 dispatches into workload.Reconcile,
// which reaches the Create pass and creates pods. Status aggregator
// then stamps Replicas / LabelSelector / UpdateRevision.
func TestReconcile_Create_MaterializesPods(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	r, c := newReconciler(t, ir)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"Reconcile should not error on the first Create pass")

	// Pods should now exist for both desired Instances.
	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	g.Expect(pods.Items).To(gomega.HaveLen(2),
		"expected one pod per desired Instance; got %d", len(pods.Items))

	// Pod names must follow the legacy shape <isvc>-<component>-<idx>-default-0
	// so existing selectors keep matching.
	names := podNames(pods.Items)
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 0)))
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 1, "default", 0)))

	// Every pod must be owner-ref'd to the IR (Kind=InferenceReplica),
	// NOT the legacy ISVC owner.
	for _, pod := range pods.Items {
		g.Expect(pod.OwnerReferences).To(gomega.HaveLen(1))
		ref := pod.OwnerReferences[0]
		g.Expect(ref.Kind).To(gomega.Equal("InferenceReplica"),
			"pod %s owner ref should point at the IR, not the ISVC", pod.Name)
		g.Expect(ref.Name).To(gomega.Equal(ir.Name))
		g.Expect(ref.Controller).NotTo(gomega.BeNil())
		g.Expect(*ref.Controller).To(gomega.BeTrue())
	}

	// IR.Status should reflect the desired Replicas + LabelSelector +
	// ObservedGeneration. The status writer runs in defer so it always
	// fires before Reconcile returns.
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.Replicas).To(gomega.Equal(int32(2)),
		"Replicas counter should match desired Instance count")
	g.Expect(got.Status.ObservedGeneration).To(gomega.Equal(int64(1)))
	g.Expect(got.Status.LabelSelector).NotTo(gomega.BeEmpty(),
		"LabelSelector must be set for HPA scale subresource")
	// LabelSelector must encode the legacy OMENative pod-selector trio so
	// existing HPAs continue to resolve.
	g.Expect(got.Status.LabelSelector).To(gomega.ContainSubstring("component=engine"))
	g.Expect(got.Status.LabelSelector).To(gomega.ContainSubstring("ome.io/inferenceservice=llama"))
	g.Expect(got.Status.LabelSelector).To(gomega.ContainSubstring("ome.io/managed-by=OMENative"))
	// UpdateRevision should be stamped off the freshly-ensured CR.
	g.Expect(got.Status.UpdateRevision).NotTo(gomega.BeEmpty(),
		"UpdateRevision must point at the target ControllerRevision")
}

func TestReconcile_Create_ConfiguredBatchSizeCapsPods(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 5)
	r, c := newReconciler(t, ir)
	batchSize := int32(2)
	r.ScaleUpPodBatchSize = &batchSize

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.RequeueAfter).NotTo(gomega.BeZero(),
		"deferred Instances must cause a follow-up reconcile")

	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	g.Expect(pods.Items).To(gomega.HaveLen(2),
		"configured scale-up Pod batch size must cap a reconcile to two missing Pods")
	g.Expect(podNames(pods.Items)).To(gomega.ConsistOf(
		query.PodName("llama", workload.ComponentEngine, 0, "default", 0),
		query.PodName("llama", workload.ComponentEngine, 1, "default", 0),
	))

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses).To(gomega.HaveLen(2))
	for _, status := range got.Status.InstanceStatuses {
		g.Expect(status.Phase).To(gomega.Equal(v1beta1.OMENativeInstanceCreating))
		g.Expect(status.Operation).NotTo(gomega.BeNil())
		g.Expect(status.Operation.Type).To(gomega.Equal(v1beta1.InstanceOperationCreate))
	}
}

// TestReconcile_Create_AlsoCreatesHeadlessService pins the headless
// Service wire-in: a fresh IR reconcile must call
// workload.ReconcileHeadlessService alongside workload.Reconcile so a
// per-Component headless Service appears in the same pass that creates
// the pods. The Service rendering itself is unit-tested in
// workload/services_test.go — this test only verifies the wire-in.
//
// Asserts on the canonical shape:
//   - Name == query.HeadlessServiceName(parent, component) so any
//     tooling that looks for `<isvc>-<component>-headless` keeps working
//   - ClusterIP == None so DNS returns per-pod A records (no
//     kube-proxy load-balancing) — required for gang-init peer discovery
//   - PublishNotReadyAddresses == true so peer DNS resolves before
//     pods flip Ready (workers must discover each other during init)
//   - Selector carries the legacy OMENative pod-selector trio
//     (ome.io/inferenceservice + component + managed-by=OMENative) so
//     the Service matches the same pods the workload renderer stamps
//   - Owner ref points at the IR (Kind=InferenceReplica), NOT the
//     parent ISVC — the IR is the per-Component lifecycle owner; on
//     IR deletion the Service must cascade with it
func TestReconcile_Create_AlsoCreatesHeadlessService(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	r, c := newReconciler(t, ir)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"Reconcile should not error on the first Create pass with headless Service wire-in")

	// The Service name is derived from the parent ISVC name (ParentRef.Name)
	// — pods, services, and selectors all key off the ISVC name, not the
	// IR name.
	wantName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
	svc := &corev1.Service{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: wantName, Namespace: ir.Namespace},
		svc)).To(gomega.Succeed(),
		"headless Service %s/%s should be created by the IR reconciler", ir.Namespace, wantName)

	// Headless contract: ClusterIP=None + PublishNotReadyAddresses=true
	// so peer DNS works during gang init before pods flip Ready.
	g.Expect(svc.Spec.ClusterIP).To(gomega.Equal(corev1.ClusterIPNone),
		"headless Service must have ClusterIP=None (DNS returns per-pod A records)")
	g.Expect(svc.Spec.PublishNotReadyAddresses).To(gomega.BeTrue(),
		"headless Service must publish not-ready addresses for gang init")

	// Selector must match the OMENative pod-selector trio so the
	// Service selects the same pods the workload renderer stamps.
	g.Expect(svc.Spec.Selector).To(gomega.Equal(map[string]string{
		constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
		constants.OMEComponentLabel:           string(ir.Spec.Component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}))

	// Owner ref points at the IR — not the parent ISVC — so deletion of
	// the IR cascades to the Service. The ISVC controller manages IR
	// lifecycle at the per-Component layer; inside its lifetime,
	// the IR controller owns the per-Component supporting resources.
	g.Expect(svc.OwnerReferences).To(gomega.HaveLen(1))
	ref := svc.OwnerReferences[0]
	g.Expect(ref.Kind).To(gomega.Equal("InferenceReplica"),
		"headless Service owner ref should point at the IR, not the ISVC")
	g.Expect(ref.Name).To(gomega.Equal(ir.Name))
	g.Expect(ref.UID).To(gomega.Equal(ir.UID))
	g.Expect(ref.Controller).NotTo(gomega.BeNil())
	g.Expect(*ref.Controller).To(gomega.BeTrue())
}

// TestReconcile_ScaleUp_AddsPods pins the scale-up path: a steady
// IR at replicas=1 with a Ready Instance grows to replicas=2; the next
// reconcile must reach the Create pass and add the second pod.
func TestReconcile_ScaleUp_AddsPods(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 2)
	// Pretend the controller already ran once: instance 0 is Ready with
	// its pod alive and serving. Workload Create's idempotency keys on
	// the per-Instance pod-name lookup so the pre-existing pod won't be
	// re-created.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index:           0,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceReady,
			PodCount:        1,
			ReadyPodCount:   1,
			ServingPodCount: 1,
			ActiveOrdinal:   0,
		},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)

	r, c := newReconciler(t, ir, pod0)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	g.Expect(len(pods.Items)).To(gomega.BeNumerically(">=", 2),
		"scale-up should add at least one more pod alongside the existing one")
	names := podNames(pods.Items)
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 0, "default", 0)))
	g.Expect(names).To(gomega.ContainElement(query.PodName("llama", v1beta1convert.ComponentTypeToWorkload(v1beta1.EngineComponent), 1, "default", 0)))
}

// TestReconcile_ScaleDown_DeletesExcessInstances pins the scale-down
// path: an IR at replicas=1 with two Instance entries (one extra)
// triggers workload.Reconcile's Delete pass on the excess Instance.
// The dispatcher returns a non-zero requeue while Delete progresses.
func TestReconcile_ScaleDown_DeletesExcessInstances(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	// Pre-existing status with Instance 1 as the scale-down victim.
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0,
		},
		{
			Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0,
		},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)

	r, c := newReconciler(t, ir, pod0, pod1)
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.Requeue).To(gomega.BeTrue(), "admission commit must requeue immediately before effects")
	g.Expect(result.RequeueAfter).To(gomega.BeZero())

	result, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.RequeueAfter).NotTo(gomega.BeZero(), "the effect pass must poll while Pod deletion converges")

	// Instance 1's pod should be gone (or marked for deletion).
	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	for _, p := range pods.Items {
		if p.Name == pod1.Name && p.DeletionTimestamp == nil {
			t.Errorf("expected pod %s to be deleted, but it still exists with no DeletionTimestamp", p.Name)
		}
	}
}

func TestReconcile_ScaleDownTwoThousandUsesOneAuthoritativePodList(t *testing.T) {
	const replicas = int32(2000)
	ir := baselineIR("llama-engine", "scale-down-two-thousand", 1)
	ir.Finalizers = []string{TeardownFinalizer}
	ir.Status.InstanceStatuses = make([]v1beta1.OMENativeInstanceStatus, 0, replicas)
	objects := make([]client.Object, 0, replicas+1)
	objects = append(objects, ir)
	for index := int32(0); index < replicas; index++ {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index:           index,
			Incarnation:     1,
			Phase:           v1beta1.OMENativeInstanceReady,
			PodCount:        1,
			ReadyPodCount:   1,
			ServingPodCount: 1,
			ActiveOrdinal:   0,
		})
		objects = append(objects, podForIR(ir, index, "default", 0, true, true))
	}

	r, base := newReconciler(t, objects...)
	deletedPods := 0
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				deletedPods++
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	reader := &teardownListReader{Reader: base}
	r.APIReader = reader
	budget := int32(100)
	r.ScaleDownPodBatchSize = &budget
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("fresh bounded admission must requeue immediately, got %+v", result)
	}
	if reader.podLists != 1 {
		t.Fatalf("authoritative Pod LISTs = %d, want 1", reader.podLists)
	}
	if deletedPods != 0 {
		t.Fatalf("admission pass deleted %d Pods, want 0", deletedPods)
	}

	stored := &v1beta1.InferenceReplica{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get admitted IR: %v", err)
	}
	deleteOwned := make(map[int32]bool, budget)
	for _, status := range stored.Status.InstanceStatuses {
		if status.Phase == v1beta1.OMENativeInstanceDeleting && status.Operation != nil &&
			status.Operation.Type == v1beta1.InstanceOperationDelete {
			deleteOwned[status.Index] = true
		}
	}
	if len(deleteOwned) != int(budget) {
		t.Fatalf("Delete-owned instances = %d, want %d", len(deleteOwned), budget)
	}
	for index := replicas - budget; index < replicas; index++ {
		if !deleteOwned[index] {
			t.Errorf("highest-index victim %d was not admitted", index)
		}
	}
	if deleteOwned[replicas-budget-1] {
		t.Fatalf("deferred instance %d was admitted", replicas-budget-1)
	}

	result, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("effect pass: %v", err)
	}
	if result.Requeue || result.RequeueAfter != testScaleDownRequeueInterval {
		t.Fatalf("effect pass must poll for Pod absence, got %+v", result)
	}
	if reader.podLists != 2 {
		t.Fatalf("effect-pass authoritative Pod LISTs = %d total, want 2", reader.podLists)
	}
	if deletedPods != int(budget) {
		t.Fatalf("effect pass deleted %d Pods, want %d", deletedPods, budget)
	}

	result, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("completion pass: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("completion commit must requeue immediately, got %+v", result)
	}
	if reader.podLists != 3 {
		t.Fatalf("completion-pass authoritative Pod LISTs = %d total, want 3", reader.podLists)
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get completed wave IR: %v", err)
	}
	if len(stored.Status.InstanceStatuses) != int(replicas-budget) {
		t.Fatalf("statuses after one completed wave = %d, want %d",
			len(stored.Status.InstanceStatuses), replicas-budget)
	}
	for _, status := range stored.Status.InstanceStatuses {
		if status.Phase == v1beta1.OMENativeInstanceDeleting && status.Operation != nil &&
			status.Operation.Type == v1beta1.InstanceOperationDelete {
			t.Fatalf("completion pass admitted another victim %d", status.Index)
		}
	}
}

func TestReconcile_ScaleDownMalformedOwnedPodFailsBeforeLifecycleEffects(t *testing.T) {
	ir := baselineIR("llama-engine", "scale-down-malformed-index", 1)
	ir.Finalizers = []string{TeardownFinalizer}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{
			Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0,
		},
		{
			Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0,
		},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)
	orphan := podForIR(ir, 7, "default", 0, true, true)
	orphan.Labels[query.LabelInstanceIdx] = "not-an-index"
	pgName := query.PodGroupName(ir.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), 1)
	pg := ownedPodGroupForIR(ir, pgName, 1)
	before := ir.Status.DeepCopy()

	r, base := newReconciler(t, ir, pod0, pod1, orphan, pg)
	effects := 0
	counting := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			effects++
			return c.Delete(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			effects++
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			effects++
			return c.Patch(ctx, obj, patch, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			effects++
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	r.Client = counting
	r.APIReader = counting
	r.GangSchedulingAvailable = true

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)})
	if err == nil {
		t.Fatal("malformed UID-owned Pod must fail closed")
	}
	if !strings.Contains(err.Error(), "1 UID-owned component pod(s)") ||
		!strings.Contains(err.Error(), query.LabelInstanceIdx) {
		t.Fatalf("error lacks bounded malformed-pod summary: %v", err)
	}
	if strings.Contains(err.Error(), orphan.Name) {
		t.Fatalf("error must not grow with Pod names: %v", err)
	}
	if effects != 0 {
		t.Fatalf("malformed snapshot allowed %d lifecycle write(s), want 0", effects)
	}

	stored := &v1beta1.InferenceReplica{}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if !reflect.DeepEqual(&stored.Status, before) {
		t.Fatalf("status changed despite malformed snapshot:\n got: %+v\nwant: %+v", stored.Status, *before)
	}
	for _, pod := range []*corev1.Pod{pod0, pod1, orphan} {
		storedPod := &corev1.Pod{}
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), storedPod); err != nil {
			t.Fatalf("owned Pod %s was removed: %v", pod.Name, err)
		}
		if storedPod.DeletionTimestamp != nil {
			t.Fatalf("owned Pod %s started deletion", pod.Name)
		}
	}
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(pg), &schedulingv1alpha1.PodGroup{}); err != nil {
		t.Fatalf("owned PodGroup was removed: %v", err)
	}
	service := &corev1.Service{}
	serviceKey := client.ObjectKey{Namespace: ir.Namespace, Name: query.HeadlessServiceName(
		ir.Spec.ParentRef.Name, v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))}
	if err := base.Get(context.Background(), serviceKey, service); !apierrors.IsNotFound(err) {
		t.Fatalf("downstream headless Service effect occurred: %v", err)
	}
}

func TestInvalidInstanceIndexPodCount(t *testing.T) {
	validZero := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "0"}}}
	validMax := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "2147483647"}}}
	missing := &corev1.Pod{}
	malformed := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "not-an-index"}}}
	negative := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "-1"}}}
	overflow := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{query.LabelInstanceIdx: "2147483648"}}}

	if got := invalidInstanceIndexPodCount([]*corev1.Pod{validZero, validMax, missing, malformed, negative, overflow, nil}); got != 5 {
		t.Fatalf("invalidInstanceIndexPodCount = %d, want 5", got)
	}
}

func TestReconcile_SteadySinglePodSkipsAuthoritativePodGroupInventory(t *testing.T) {
	ir := baselineIR("llama-engine", "podgroup-steady-single", 1)
	r, c := newReconciler(t, ir)
	reader := &podGroupListCountingReader{Reader: c}
	r.APIReader = reader
	r.GangSchedulingAvailable = true
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	for pass := 1; pass <= 2; pass++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("steady single-pod pass %d: %v", pass, err)
		}
	}
	if reader.lists != 0 {
		t.Fatalf("steady single-pod reconciliation issued %d authoritative PodGroup LIST(s), want 0", reader.lists)
	}
}

func TestRequiresAuthoritativePodGroupInventoryLifecycleTriggers(t *testing.T) {
	ir := baselineIR("llama-engine", "podgroup-inventory-triggers", 1)
	r, _ := newReconciler(t, ir)
	singlePlan := workload.ComponentPlan{
		Component: workload.ComponentEngine,
		Instances: []workload.InstancePlan{{
			Index:   0,
			Runners: []workload.RunnerPlan{{Name: "default", Size: 1}},
		}},
	}
	multiPlan := singlePlan
	multiPlan.Instances = []workload.InstancePlan{{
		Index: 0,
		Runners: []workload.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 1},
		},
	}}
	steadyInput := workload.ReconcileInput{
		OwnerObject: ir,
		ObservedState: workload.WorkloadObservedState{InstanceStatuses: []workload.InstanceStatus{{
			Index: 0,
			Phase: workload.InstancePhaseReady,
		}}},
	}
	scaleDownInput := steadyInput
	scaleDownInput.ObservedState.InstanceStatuses = append(
		append([]workload.InstanceStatus(nil), steadyInput.ObservedState.InstanceStatuses...),
		workload.InstanceStatus{Index: 1, Phase: workload.InstancePhaseReady})
	terminalInput := steadyInput
	terminalInput.ObservedState.Migrations = []workload.MigrationRecord{{
		SourceInstance: 0,
		Phase:          workload.MigrationPhaseDraining,
	}}

	for _, tc := range []struct {
		name  string
		input workload.ReconcileInput
		plan  workload.ComponentPlan
		want  bool
	}{
		{name: "steady single pod", input: steadyInput, plan: singlePlan, want: false},
		{name: "planned gang", input: steadyInput, plan: multiPlan, want: true},
		{name: "scale down", input: scaleDownInput, plan: singlePlan, want: true},
		{name: "terminal finalization", input: terminalInput, plan: singlePlan, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.requiresAuthoritativePodGroupInventory(context.Background(), tc.input, tc.plan)
			if err != nil {
				t.Fatalf("requiresAuthoritativePodGroupInventory: %v", err)
			}
			if got != tc.want {
				t.Fatalf("requiresAuthoritativePodGroupInventory = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcile_SinglePodForeignGroupDoesNotTriggerInventory(t *testing.T) {
	ir := baselineIR("llama-engine", "podgroup-foreign-single", 1)
	pgName := query.PodGroupName(ir.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), 0)
	pg := ownedPodGroupForIR(ir, pgName, 0)
	pg.OwnerReferences[0].UID = "foreign-ir-uid"
	r, c := newReconciler(t, ir, pg)
	reader := &podGroupListCountingReader{Reader: c}
	r.APIReader = reader
	r.GangSchedulingAvailable = true

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if reader.lists != 0 {
		t.Fatalf("foreign PodGroup triggered %d authoritative inventory LIST(s), want 0", reader.lists)
	}
	retained := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pg), retained); err != nil {
		t.Fatalf("foreign PodGroup was removed: %v", err)
	}
}

// TestReconcile_MultiToSinglePodGroupCleanupGatesWorkload pins the ownership
// handoff when an Instance has a single-Pod plan. A stale PodGroup must enter
// deletion and disappear before Create admission can resume.
func TestReconcile_MultiToSinglePodGroupCleanupGatesWorkload(t *testing.T) {
	ir := baselineIR("llama-engine", "podgroup-transition", 1)
	ir.Finalizers = []string{TeardownFinalizer}
	pgName := query.PodGroupName(ir.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), 0)
	pg := ownedPodGroupForIR(ir, pgName, 0)
	pg.Labels = nil
	pg.Finalizers = []string{"test.ome.io/hold"}
	r, c := newReconciler(t, ir, pg)
	reader := &podGroupListCountingReader{Reader: c}
	r.APIReader = reader
	r.GangSchedulingAvailable = true
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	for pass := 1; pass <= 2; pass++ {
		result, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("gated pass %d: %v", pass, err)
		}
		if result.RequeueAfter != testScaleDownRequeueInterval {
			t.Fatalf("gated pass %d must poll for PodGroup deletion, got %+v", pass, result)
		}
		gotIR := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), gotIR); err != nil {
			t.Fatalf("gated pass %d get IR: %v", pass, err)
		}
		if len(gotIR.Status.InstanceStatuses) != 0 {
			t.Fatalf("gated pass %d admitted workload before PodGroup removal: %+v", pass, gotIR.Status.InstanceStatuses)
		}
		pods := &corev1.PodList{}
		if err := c.List(context.Background(), pods, client.InNamespace(ir.Namespace)); err != nil {
			t.Fatalf("gated pass %d list Pods: %v", pass, err)
		}
		if len(pods.Items) != 0 {
			t.Fatalf("gated pass %d created Pods before PodGroup removal: %v", pass, podNames(pods.Items))
		}
	}
	if reader.lists != 2 {
		t.Fatalf("gated passes issued %d authoritative PodGroup LISTs, want 2", reader.lists)
	}

	terminatingPG := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ir.Namespace, Name: pgName}, terminatingPG); err != nil {
		t.Fatalf("get terminating PodGroup: %v", err)
	}
	if terminatingPG.DeletionTimestamp == nil {
		t.Fatal("stale PodGroup was not placed into deletion")
	}
	terminatingPG.Finalizers = nil
	if err := c.Update(context.Background(), terminatingPG); err != nil {
		t.Fatalf("release PodGroup test finalizer: %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(terminatingPG), &schedulingv1alpha1.PodGroup{}); !apierrors.IsNotFound(err) {
		t.Fatalf("released PodGroup must be gone before workload resumes: %v", err)
	}

	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("resumed pass: %v", err)
	}
	if result.IsZero() {
		t.Fatalf("resumed workload must remain in progress, got %+v", result)
	}
	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), gotIR); err != nil {
		t.Fatalf("resumed pass get IR: %v", err)
	}
	if len(gotIR.Status.InstanceStatuses) != 1 ||
		gotIR.Status.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceCreating ||
		gotIR.Status.InstanceStatuses[0].Operation == nil ||
		gotIR.Status.InstanceStatuses[0].Operation.Type != v1beta1.InstanceOperationCreate {
		t.Fatalf("workload did not resume with Create admission after PodGroup removal: %+v", gotIR.Status.InstanceStatuses)
	}
	if reader.lists != 2 {
		t.Fatalf("post-removal single-pod pass issued another authoritative PodGroup LIST: got %d total, want 2", reader.lists)
	}
}

func TestReconcile_MultiToSinglePodGroupCleanupUsesScaleDownBudget(t *testing.T) {
	const replicas = int32(5)
	ir := baselineIR("llama-engine", "podgroup-transition-bounded", replicas)
	ir.Finalizers = []string{TeardownFinalizer}
	objects := make([]client.Object, 0, replicas+1)
	objects = append(objects, ir)
	for index := int32(0); index < replicas; index++ {
		name := query.PodGroupName(ir.Spec.ParentRef.Name,
			v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), index)
		objects = append(objects, ownedPodGroupForIR(ir, name, index))
	}

	r, base := newReconciler(t, objects...)
	deleted := make([]string, 0, replicas)
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if pg, ok := obj.(*schedulingv1alpha1.PodGroup); ok {
				deleted = append(deleted, pg.Name)
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r.APIReader = base
	r.GangSchedulingAvailable = true
	budget := int32(2)
	r.ScaleDownPodBatchSize = &budget
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	for pass, wantRemaining := range []int{3, 1, 0} {
		before := len(deleted)
		result, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("cleanup pass %d: %v", pass+1, err)
		}
		if result.RequeueAfter != testScaleDownRequeueInterval {
			t.Fatalf("cleanup pass %d must poll for authoritative absence, got %+v", pass+1, result)
		}
		if passDeletes := len(deleted) - before; passDeletes < 1 || passDeletes > int(budget) {
			t.Fatalf("cleanup pass %d deleted %d PodGroups, want 1..%d", pass+1, passDeletes, budget)
		}
		groups := &schedulingv1alpha1.PodGroupList{}
		if err := base.List(context.Background(), groups, client.InNamespace(ir.Namespace)); err != nil {
			t.Fatalf("cleanup pass %d list PodGroups: %v", pass+1, err)
		}
		if len(groups.Items) != wantRemaining {
			t.Fatalf("cleanup pass %d remaining PodGroups = %d, want %d", pass+1, len(groups.Items), wantRemaining)
		}
		stored := &v1beta1.InferenceReplica{}
		if err := base.Get(context.Background(), client.ObjectKeyFromObject(ir), stored); err != nil {
			t.Fatalf("cleanup pass %d get IR: %v", pass+1, err)
		}
		if len(stored.Status.InstanceStatuses) != 0 {
			t.Fatalf("cleanup pass %d admitted workload before stale PodGroups were absent: %+v",
				pass+1, stored.Status.InstanceStatuses)
		}
	}
	if len(deleted) != int(replicas) {
		t.Fatalf("deleted PodGroups = %d, want %d", len(deleted), replicas)
	}
}

func TestReconcile_MultiToSinglePodGroupCleanupCountsTerminatingAgainstBudget(t *testing.T) {
	const replicas = int32(4)
	ir := baselineIR("llama-engine", "podgroup-transition-held-budget", replicas)
	ir.Finalizers = []string{TeardownFinalizer}
	objects := make([]client.Object, 0, replicas+1)
	objects = append(objects, ir)
	for index := int32(0); index < replicas; index++ {
		name := query.PodGroupName(ir.Spec.ParentRef.Name,
			v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), index)
		pg := ownedPodGroupForIR(ir, name, index)
		pg.Finalizers = []string{"test.ome.io/hold"}
		objects = append(objects, pg)
	}

	r, c := newReconciler(t, objects...)
	r.APIReader = c
	r.GangSchedulingAvailable = true
	budget := int32(2)
	r.ScaleDownPodBatchSize = &budget
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}

	for pass := 1; pass <= 2; pass++ {
		result, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("cleanup pass %d: %v", pass, err)
		}
		if result.RequeueAfter != testScaleDownRequeueInterval {
			t.Fatalf("cleanup pass %d must poll, got %+v", pass, result)
		}
		terminating := 0
		for index := int32(0); index < replicas; index++ {
			name := query.PodGroupName(ir.Spec.ParentRef.Name,
				v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), index)
			pg := &schedulingv1alpha1.PodGroup{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ir.Namespace, Name: name}, pg); err != nil {
				t.Fatalf("cleanup pass %d get PodGroup %d: %v", pass, index, err)
			}
			if pg.DeletionTimestamp != nil {
				terminating++
			}
		}
		if terminating != int(budget) {
			t.Fatalf("cleanup pass %d has %d Terminating PodGroups, want %d", pass, terminating, budget)
		}
	}
}

func TestReconcile_MultiToSinglePodGroupRetainsLiveInstanceWithoutGroupLabel(t *testing.T) {
	ir := baselineIR("llama-engine", "podgroup-transition-live", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:           0,
		Incarnation:     1,
		Phase:           v1beta1.OMENativeInstanceReady,
		PodCount:        1,
		ReadyPodCount:   1,
		ServingPodCount: 1,
		ActiveOrdinal:   0,
	}}
	pod := podForIR(ir, 0, "default", 0, true, true)
	delete(pod.Labels, query.LabelPodGroup)
	pgName := query.PodGroupName(ir.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), 0)
	pg := ownedPodGroupForIR(ir, pgName, 0)
	pg.Finalizers = []string{"test.ome.io/hold"}
	r, c := newReconciler(t, ir, pod, pg)
	r.GangSchedulingAvailable = true

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	retained := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pg), retained); err != nil {
		t.Fatalf("live Instance PodGroup was deleted: %v", err)
	}
	if retained.DeletionTimestamp != nil {
		t.Fatal("live Instance PodGroup entered deletion without its PodGroup-name label")
	}
}

func TestReconcile_MultiToSinglePodGroupRejectsMalformedOwnedPodIndex(t *testing.T) {
	ir := baselineIR("llama-engine", "podgroup-transition-invalid-index", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceReady,
	}}
	pod := podForIR(ir, 0, "default", 0, true, true)
	delete(pod.Labels, query.LabelPodGroup)
	pod.Labels[query.LabelInstanceIdx] = "malformed"
	pgName := query.PodGroupName(ir.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), 0)
	pg := ownedPodGroupForIR(ir, pgName, 0)
	pg.Finalizers = []string{"test.ome.io/hold"}
	r, c := newReconciler(t, ir, pod, pg)
	r.GangSchedulingAvailable = true

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)})
	if err == nil || !strings.Contains(err.Error(), "authoritative stale-PodGroup snapshot") {
		t.Fatalf("Reconcile error = %v, want malformed owned Pod rejection", err)
	}
	retained := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pg), retained); err != nil {
		t.Fatalf("malformed Pod evidence removed PodGroup: %v", err)
	}
	if retained.DeletionTimestamp != nil {
		t.Fatal("malformed Pod evidence started PodGroup deletion")
	}
}

func TestReconcile_MultiToSinglePodGroupSkipsStaleDeleteOwnedRebound(t *testing.T) {
	cached := baselineIR("llama-engine", "podgroup-transition-rebound", 1)
	cached.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceDeleting,
		Operation: &v1beta1.InstanceOperation{
			ID:   "delete-stale",
			Type: v1beta1.InstanceOperationDelete,
			Step: "Drain",
		},
	}}
	live := cached.DeepCopy()
	live.Status.InstanceStatuses[0].Operation.ID = "delete-current"
	pgName := query.PodGroupName(live.Spec.ParentRef.Name,
		v1beta1convert.ComponentTypeToWorkload(live.Spec.Component), 0)
	pg := ownedPodGroupForIR(live, pgName, 0)
	pg.Finalizers = []string{"test.ome.io/hold"}
	scheme := testScheme(t)
	liveClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(live, pg).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithIndex(&schedulingv1alpha1.PodGroup{}, workloadgang.PodGroupControllerUIDIndexField, workloadgang.PodGroupControllerUIDIndexExtractor).
		Build()
	staleReader := &firstStaleInferenceReplicaReader{Reader: liveClient, stale: cached}
	r := &Reconciler{
		Client:                  &staleReadingClient{Client: liveClient, reader: staleReader},
		APIReader:               liveClient,
		Log:                     logf.Log.WithName("test"),
		InstanceStatusTarget:    irstatus.EncodingDenseV1,
		Expectations:            workload.NewExpectations(),
		GangSchedulingAvailable: true,
	}

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cached)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("stale Delete identity must replan immediately, got %+v", result)
	}
	retained := &schedulingv1alpha1.PodGroup{}
	if err := liveClient.Get(context.Background(), client.ObjectKeyFromObject(pg), retained); err != nil {
		t.Fatalf("stale prerequisite deleted rebound PodGroup: %v", err)
	}
	if retained.DeletionTimestamp != nil {
		t.Fatal("stale prerequisite started PodGroup deletion before Delete ownership preflight")
	}
	stored := &v1beta1.InferenceReplica{}
	if err := liveClient.Get(context.Background(), client.ObjectKeyFromObject(live), stored); err != nil {
		t.Fatalf("get live IR: %v", err)
	}
	if got := stored.Status.InstanceStatuses[0].Operation.ID; got != "delete-current" {
		t.Fatalf("stale Delete identity changed live owner: got %q", got)
	}
}

// TestReconcile_StatusAggregator_RollsUpCounters pins the aggregator:
// with three Instance entries (2 Ready, 1 Creating) the Component-level
// Replicas / ReadyReplicas should reflect the per-Instance state after
// one reconcile pass.
func TestReconcile_StatusAggregator_RollsUpCounters(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 3)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, ActiveOrdinal: 0},
		{Index: 2, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
			PodCount: 1, ReadyPodCount: 0, ServingPodCount: 0, ActiveOrdinal: 0},
	}
	pod0 := podForIR(ir, 0, "default", 0, true, true)
	pod1 := podForIR(ir, 1, "default", 0, true, true)
	pod2 := podForIR(ir, 2, "default", 0, false, false)
	r, c := newReconciler(t, ir, pod0, pod1, pod2)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(),
		types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
		got)).To(gomega.Succeed())
	g.Expect(got.Status.Replicas).To(gomega.Equal(int32(3)),
		"Replicas counter should match the InstanceStatuses entry count")
	g.Expect(got.Status.ReadyReplicas).To(gomega.Equal(int32(2)),
		"ReadyReplicas counter should reflect the 2 Ready Instances")
	g.Expect(got.Status.ServingReplicas).To(gomega.Equal(int32(2)),
		"ServingReplicas counter should reflect the 2 Serving Instances")
}

// TestReconcile_NilPodSpec_NoRevisionEnsured pins the defensive
// nil-PodSpec branch: a Runner with an empty PodSpec (no containers)
// produces a non-nil PodSpec so EnsureControllerRevision can run;
// this test verifies that the IR builder doesn't crash on the
// PodSpec=nil short-circuit (which is dead code in production but
// kept as a safety guard).
func TestReconcile_NilPodSpec_NoRevisionEnsured(t *testing.T) {
	g := gomega.NewWithT(t)
	// Build an IR with no Runners — webhook would reject this in
	// production (Runners has +kubebuilder:validation:MinItems=1) but
	// the reconciler's defensive nil-PodSpec branch must still produce
	// no panic.
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = nil
	r, _ := newReconciler(t, ir)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	// An IR with no Runners produces a desired PodSpec=nil; workload
	// Create returns immediately and the status writer stamps
	// Replicas=0 (no Instances). Either no error or a build-plan
	// error is acceptable; what's critical is NO panic.
	if err == nil {
		// fine — soft path
		return
	}
	// Sanity: the error path should be a build-plan or workload error,
	// not an internal Go panic message.
	g.Expect(err.Error()).NotTo(gomega.ContainSubstring("runtime error"))
}

// podForIR builds a fake pod matching what workload/ops/render would
// produce for a given (IR, instance, runner, ordinal) tuple. The
// label set mirrors render.go's podLabels(); the pod's
// ContainersReady + ome.io/serving conditions are toggled by the
// (ready, serving) booleans.
//
// Owner ref points at the IR (Kind=InferenceReplica) so the
// expectation cache + workload-side query.ListOMENativePods see this
// as a real workload pod, not a foreign one.
func podForIR(ir *v1beta1.InferenceReplica, instanceIdx int32, runnerName string, ordinal int32, ready, serving bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(ir.Spec.ParentRef.Name, v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), instanceIdx, runnerName, ordinal),
			Namespace: ir.Namespace,
			UID:       types.UID(fmt.Sprintf("%s-%d-%s-%d-uid", ir.Name, instanceIdx, runnerName, ordinal)),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: ir.Spec.ParentRef.Name,
				constants.OMEComponentLabel:           string(ir.Spec.Component),
				query.LabelInstanceIdx:                intToLabel(int64(instanceIdx)),
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     runnerName,
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 intToLabel(int64(ordinal)),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       ir.Name,
				UID:        ir.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
		},
	}
	now := metav1.Now()
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.ContainersReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               query.ServingConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		})
	}
	return pod
}

// podNames extracts the names from a slice of pods for assertion
// readability.
func podNames(pods []corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
}

// sliceForIRPod constructs an EndpointSlice carrying one endpoint for
// pod against the IR's per-Component headless Service. Used by status
// tests that need AvailableReplicas to mirror ReadyReplicas — the
// aggregator reads availability off the EndpointSlice (same
// as the omenative direct path), so without a slice every pod is
// invisible to the availability counter regardless of ContainersReady.
//
// Returns a slice with Endpoints[0].Conditions.Ready set to ready —
// the same toggle the omenative sliceWithEndpoint helper exposes.
// AddressType=IPv4 + a fixed bogus address keep the fake-client
// validation happy; the controller's availability counter only reads
// TargetRef.Name + Ready, not the IP.
func sliceForIRPod(ir *v1beta1.InferenceReplica, pod *corev1.Pod, ready bool) *discoveryv1.EndpointSlice {
	serviceName := query.HeadlessServiceName(ir.Spec.ParentRef.Name, v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name + "-slice",
			Namespace: ir.Namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: ptr.To(ready),
				},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			},
		},
	}
}

// intToLabel formats a non-negative int64 as the label-safe ASCII
// string the workload-side pod label helpers produce. Mirrors the
// helper in omenative/test_helpers_test.go (test packages can't
// share unexported helpers across directory boundaries).
func intToLabel(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// newReconcilerWithGrace returns a reconciler with a fake clientset that
// supplies the given stuck-pod grace period, so fast-escalation tests work
// without wiring a real config cache.
func newReconcilerWithGrace(t *testing.T, grace time.Duration, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	r, c := newReconciler(t, objs...)

	// Wire a fake clientset + config cache that resolves the grace.
	lifecycleCfg := fmt.Sprintf(`{"stuckPodGracePeriod":"%s"}`, grace.String())
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inferenceservice-config",
			Namespace: "ome",
		},
		Data: map[string]string{
			"lifecycle": lifecycleCfg,
		},
	}
	fakeCS := kubefake.NewSimpleClientset(cm)
	r.Clientset = fakeCS
	r.ConfigCache = controllerconfig.NewConfigCache(0) // zero TTL = always refetch

	return r, c
}

// A deferred status-write failure must not skip the retention sweep:
// the sweep is best-effort against the last committed status and the
// tail must stay symmetric with the non-nil-primary-error path. The
// EndpointSlice List interceptor fails only the aggregator's
// availability read, so the primary reconcile succeeds and the failure
// surfaces purely through the deferred tail.
func TestReconcile_StatusWriteFailureStillSweepsRevisions(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "prod", 1)
	// Retention rides the per-IR spec limit so the sweep is configured
	// without wiring a clientset.
	ir.Spec.RevisionHistoryLimit = ptr.To(int32(testRevisionRetention))

	liveCR := seedControllerRevision(ir, "live", 1)
	ir.Status.CurrentRevision = liveCR.Name

	const extra = 5
	nonLive := make([]*appsv1.ControllerRevision, 0, testRevisionRetention+extra)
	for i := 0; i < testRevisionRetention+extra; i++ {
		nonLive = append(nonLive, seedControllerRevision(ir, fmt.Sprintf("nonlive%02d", i), int64(i+2)))
	}
	objs := []client.Object{ir, liveCR}
	for _, cr := range nonLive {
		objs = append(objs, cr)
	}

	boom := errors.New("endpointslice list unavailable")
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*discoveryv1.EndpointSliceList); ok {
					return boom
				}
				return cl.List(ctx, list, opts...)
			},
		}).
		Build()
	r := &Reconciler{
		Client:               c,
		APIReader:            c,
		Log:                  logf.Log.WithName("test"),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
		Expectations:         workload.NewExpectations(),
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	})
	g.Expect(err).To(gomega.HaveOccurred(), "the status-write failure must still surface")
	g.Expect(err.Error()).To(gomega.ContainSubstring("write status"))

	survivors := listRevisionNames(t, c, ir.Namespace)
	g.Expect(survivors).To(gomega.HaveKey(liveCR.Name))
	for i := 0; i < extra; i++ {
		g.Expect(survivors).NotTo(gomega.HaveKey(nonLive[i].Name),
			"retention sweep must run even when the deferred status write fails")
	}
}

// reconcileRelocationDirectives loads the projection-path ledger via
// the CACHED client — the steady-state hot path must not pay a live
// GET per pass. The ledger here exists ONLY behind APIReader, so an
// empty exclusion map proves the cached client is the load source.
func TestReconcileRelocationDirectives_ProjectionUsesCachedClient(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	r, _ := newReconciler(t, ir)

	readerClient := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	ledger := &audit.Ledger{}
	ledger.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	if err := audit.PersistLedgerForOwner(context.Background(), readerClient, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed live-reader ledger: %v", err)
	}
	r.APIReader = readerClient

	if got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3); got != nil {
		t.Fatalf("exclusions: got %v want nil (projection must read the cache, where no ledger exists)", got)
	}
}

func TestTerminalFinalizationOwned_GangSourceMarkerSurvivesRemovalRetry(t *testing.T) {
	surgeIndex := int32(7)
	observed := workload.WorkloadObservedState{
		InstanceStatuses: []workload.InstanceStatus{
			{
				Index: 3,
				Operation: &workload.InstanceOperation{
					Type:       workload.InstanceOperationUpdate,
					Step:       workloadops.UpdateStepSurgeDrain,
					SurgeIndex: &surgeIndex,
				},
			},
			{
				Index: surgeIndex,
				Operation: &workload.InstanceOperation{
					Type: workload.InstanceOperationUpdate,
					Step: workload.UpdateStepGangSurgeTargetCleanup,
				},
			},
		},
	}

	owned := terminalFinalizationOwned(observed)
	if _, found := owned[3]; !found {
		t.Fatal("persisted gang source cleanup marker did not suppress PodGroup ensure")
	}
	if _, found := owned[surgeIndex]; !found {
		t.Fatal("persisted gang target cleanup marker did not suppress PodGroup ensure")
	}

	observed.InstanceStatuses[0].Operation.Step = workloadops.UpdateStepSurge
	observed.InstanceStatuses[1].Operation.Step = workload.UpdateStepGangSurgeTarget
	if _, found := terminalFinalizationOwned(observed)[3]; found {
		t.Fatal("pre-terminal gang source unexpectedly suppressed PodGroup ensure")
	}
	if _, found := terminalFinalizationOwned(observed)[surgeIndex]; found {
		t.Fatal("pre-terminal gang target unexpectedly suppressed PodGroup ensure")
	}
}
func TestTerminalFinalizationOwned_MigrationAndOrdinaryDelete(t *testing.T) {
	const index int32 = 4
	tests := []struct {
		name     string
		observed workload.WorkloadObservedState
		want     bool
	}{
		{
			name: "draining migration owns finalization",
			observed: workload.WorkloadObservedState{Migrations: []workload.MigrationRecord{{
				SourceInstance: index,
				Phase:          workload.MigrationPhaseDraining,
			}}},
			want: true,
		},
		{
			name: "surge-ready migration does not own finalization",
			observed: workload.WorkloadObservedState{Migrations: []workload.MigrationRecord{{
				SourceInstance: index,
				Phase:          workload.MigrationPhaseSurgeReady,
			}}},
		},
		{
			name: "completed migration does not own finalization",
			observed: workload.WorkloadObservedState{Migrations: []workload.MigrationRecord{{
				SourceInstance: index,
				Phase:          workload.MigrationPhaseCompleted,
			}}},
		},
		{
			name: "ordinary delete owns finalization",
			observed: workload.WorkloadObservedState{InstanceStatuses: []workload.InstanceStatus{{
				Index: index,
				Phase: workload.InstancePhaseDeleting,
				Operation: &workload.InstanceOperation{
					Type: workload.InstanceOperationDelete,
				},
			}}},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, found := terminalFinalizationOwned(test.observed)[index]
			if found != test.want {
				t.Fatalf("terminal ownership: got %v want %v", found, test.want)
			}
		})
	}
}
