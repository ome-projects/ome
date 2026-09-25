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
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

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
	desired := workloadtypes.WorkloadDesiredSpec{TopologyKey: "topology.example.com/canary"}
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
	desired := workloadtypes.WorkloadDesiredSpec{}

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
	desired := workloadtypes.WorkloadDesiredSpec{TopologyKey: "topology.example.com/current"}

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
	desired := workloadtypes.WorkloadDesiredSpec{TopologyKey: "topology.example.com/current"}

	err := r.applyRollbackPayload(context.Background(), ir, &desired, payload, target)
	if err == nil || !strings.Contains(err.Error(), "conflicting OME-generated topology") {
		t.Fatalf("ambiguous legacy rollback topology: got %v", err)
	}
}

func TestApplyRollbackPayload_LegacyTopologyFreeDoesNotBlock(t *testing.T) {
	desired := workloadtypes.WorkloadDesiredSpec{}
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

// TestReconcile_TerminatingPodGroupHoldsWithoutPods: a deterministic
// PodGroup name still occupied by an object being collected withholds
// that Instance's members and records the wait on its operation, so an
// operator sees why the gang has not started instead of a silent poll.
func TestReconcile_TerminatingPodGroupHoldsWithoutPods(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "leader", Image: "test:v1"}}}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "test:v1"}}}}},
	}
	now := metav1.Now()
	pg := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:              query.PodGroupName(ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, 0),
		Namespace:         ir.Namespace,
		UID:               "terminating-pg",
		DeletionTimestamp: &now,
		Finalizers:        []string{"example.com/hold"},
		OwnerReferences:   []metav1.OwnerReference{*metav1.NewControllerRef(ir, IRGVK())},
	}}
	r, c := newReconciler(t, ir, pg)
	r.APIReader = c
	r.GangSchedulingAvailable = true

	// Two passes: the first commits the Create marker the hold is recorded
	// on, the second observes that marker and parks it.
	for pass := 0; pass < 2; pass++ {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}); err != nil {
			t.Fatalf("reconcile terminating PodGroup (pass %d): %v", pass, err)
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("created %d pods while their PodGroup name was terminating", len(pods.Items))
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), got); err != nil {
		t.Fatalf("get InferenceReplica: %v", err)
	}
	if len(got.Status.InstanceStatuses) != 1 {
		t.Fatalf("instance statuses: got %d want 1", len(got.Status.InstanceStatuses))
	}
	op := got.Status.InstanceStatuses[0].Operation
	if op == nil || op.Waiting != workloadtypes.WaitingReasonPodGroupTerminating {
		t.Fatalf("Operation.Waiting: got %+v want %q", op, workloadtypes.WaitingReasonPodGroupTerminating)
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
				Step: workloadtypes.UpdateStepGangSurgeTarget, TargetRevision: "llama-engine-retired",
			},
		},
	}
	targetPG := &schedulingv1alpha1.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:            query.PodGroupName(ir.Spec.ParentRef.Name, workloadtypes.ComponentEngine, surgeIndex),
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
	g.Expect(result.Requeue || result.RequeueAfter > 0).To(gomega.BeTrue(),
		"deferred Instances must cause a follow-up reconcile; with no configured cadence that is the rate-limited requeue")

	pods := &corev1.PodList{}
	g.Expect(c.List(context.Background(), pods, client.InNamespace(ir.Namespace))).To(gomega.Succeed())
	g.Expect(pods.Items).To(gomega.HaveLen(2),
		"configured scale-up Pod batch size must cap a reconcile to two missing Pods")
	g.Expect(podNames(pods.Items)).To(gomega.ConsistOf(
		query.PodName("llama", workloadtypes.ComponentEngine, 0, "default", 0),
		query.PodName("llama", workloadtypes.ComponentEngine, 1, "default", 0),
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
// service.ReconcileHeadlessService alongside workload.Reconcile so a
// per-Component headless Service appears in the same pass that creates
// the pods. The Service rendering itself is unit-tested in
// workload/service/service_test.go — this test only verifies the wire-in.
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
	singlePlan := workloadtypes.ComponentPlan{
		Component: workloadtypes.ComponentEngine,
		Instances: []workloadtypes.InstancePlan{{
			Index:   0,
			Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}},
		}},
	}
	multiPlan := singlePlan
	multiPlan.Instances = []workloadtypes.InstancePlan{{
		Index: 0,
		Runners: []workloadtypes.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 1},
		},
	}}
	steadyInput := workloadtypes.ReconcileInput{
		OwnerObject: ir,
		ObservedState: workloadtypes.WorkloadObservedState{InstanceStatuses: []workloadtypes.InstanceStatus{{
			Index: 0,
			Phase: workloadtypes.InstancePhaseReady,
		}}},
	}
	scaleDownInput := steadyInput
	scaleDownInput.ObservedState.InstanceStatuses = append(
		append([]workloadtypes.InstanceStatus(nil), steadyInput.ObservedState.InstanceStatuses...),
		workloadtypes.InstanceStatus{Index: 1, Phase: workloadtypes.InstancePhaseReady})
	terminalInput := steadyInput
	terminalInput.ObservedState.Migrations = []workloadtypes.MigrationRecord{{
		SourceInstance: 0,
		Phase:          workloadtypes.MigrationPhaseDraining,
	}}

	for _, tc := range []struct {
		name  string
		input workloadtypes.ReconcileInput
		plan  workloadtypes.ComponentPlan
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
		Expectations:            workloadtypes.NewExpectations(),
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

// podNames extracts the names from a slice of pods for assertion
// readability.
func podNames(pods []corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
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
		Expectations:         workloadtypes.NewExpectations(),
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
	observed := workloadtypes.WorkloadObservedState{
		InstanceStatuses: []workloadtypes.InstanceStatus{
			{
				Index: 3,
				Operation: &workloadtypes.InstanceOperation{
					Type:       workloadtypes.InstanceOperationUpdate,
					Step:       workloadtypes.UpdateStepSurgeDrain,
					SurgeIndex: &surgeIndex,
				},
			},
			{
				Index: surgeIndex,
				Operation: &workloadtypes.InstanceOperation{
					Type: workloadtypes.InstanceOperationUpdate,
					Step: workloadtypes.UpdateStepGangSurgeTargetCleanup,
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

	observed.InstanceStatuses[0].Operation.Step = workloadtypes.UpdateStepSurge
	observed.InstanceStatuses[1].Operation.Step = workloadtypes.UpdateStepGangSurgeTarget
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
		observed workloadtypes.WorkloadObservedState
		want     bool
	}{
		{
			name: "draining migration owns finalization",
			observed: workloadtypes.WorkloadObservedState{Migrations: []workloadtypes.MigrationRecord{{
				SourceInstance: index,
				Phase:          workloadtypes.MigrationPhaseDraining,
			}}},
			want: true,
		},
		{
			name: "surge-ready migration does not own finalization",
			observed: workloadtypes.WorkloadObservedState{Migrations: []workloadtypes.MigrationRecord{{
				SourceInstance: index,
				Phase:          workloadtypes.MigrationPhaseSurgeReady,
			}}},
		},
		{
			name: "completed migration does not own finalization",
			observed: workloadtypes.WorkloadObservedState{Migrations: []workloadtypes.MigrationRecord{{
				SourceInstance: index,
				Phase:          workloadtypes.MigrationPhaseCompleted,
			}}},
		},
		{
			name: "ordinary delete owns finalization",
			observed: workloadtypes.WorkloadObservedState{InstanceStatuses: []workloadtypes.InstanceStatus{{
				Index: index,
				Phase: workloadtypes.InstancePhaseDeleting,
				Operation: &workloadtypes.InstanceOperation{
					Type: workloadtypes.InstanceOperationDelete,
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

// The gang-member-loss rebuild below Ready, driven through the adapter.
//
// Outside Phase=Ready the rebuild trigger compares the live pods of an
// Instance against the pod count the row RECORDS, and that counter is
// written only by the adapter's status publication — never by
// workload.Reconcile. A harness that drives the engine alone can never
// put a complete count on a row whose pods are already gone, so these
// two arrows are covered here, where the published counter is part of
// the persisted status the reconcile reads.

// gangLossIR is a two-pod Instance (one leader, one worker) under the
// RecreateInstanceOnPodRestart policy, its single row parked at Pending
// with the published pod count reporting the complete gang.
func gangLossIR(name, namespace string) *v1beta1.InferenceReplica {
	ir := baselineIR(name, namespace, 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
		}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "sgl:1.0"}},
		}}},
	}
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{
		RestartPolicy: ptr.To(v1beta1.InstanceRestartPolicyRecreateInstance),
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstancePending,
		PodCount:    2,
	}}
	return ir
}

// instanceRow reads the single Instance row back off the stored IR.
func instanceRow(t *testing.T, c client.Client, ir *v1beta1.InferenceReplica) v1beta1.OMENativeInstanceStatus {
	t.Helper()
	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	if len(fresh.Status.InstanceStatuses) != 1 {
		t.Fatalf("instance statuses: %+v", fresh.Status.InstanceStatuses)
	}
	return fresh.Status.InstanceStatuses[0]
}

// TestRestart_PendingGangMemberTerminal_RebuildsInstance drives the
// rebuild off a terminal member: the row still reports both pods, but
// one of them is Failed and therefore holds no capacity, so the survivor
// is drained and the gang rebuilt at a bumped incarnation.
func TestRestart_PendingGangMemberTerminal_RebuildsInstance(t *testing.T) {
	ir := gangLossIR("llama-engine", "prod")
	survivor := podForIR(ir, 0, string(v1beta1.RunnerNameLeader), 0, true, true)
	terminal := podForIR(ir, 0, string(v1beta1.RunnerNameWorker), 0, false, false)
	terminal.Status.Phase = corev1.PodFailed
	r, c := newReconciler(t, ir, survivor, terminal)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	row := instanceRow(t, c, ir)
	if row.Phase != v1beta1.OMENativeInstanceRestarting ||
		row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationRestart || row.Operation.Step != "Drain" {
		t.Fatalf("terminal gang member must open a Restart Drain, got %+v (op %+v)", row, row.Operation)
	}
	if len(eventsContaining(drainEvents(rec), "gang member lost")) == 0 {
		t.Errorf("the RestartTriggered event must name the loss")
	}
}

// TestRestart_PendingGangMemberLost_RebuildsOnceRetryBlockIsDue drives
// the rebuild off the retry clock: the partial gang is rebuilt on the
// pass where the RetryBlock recorded against its revision has come due,
// and is not while the block still denies the revision.
//
// While the block denies, the rebuild is not merely postponed — the
// Create pass fills the missing member instead, because below Ready it
// owns an Instance the rebuild gate declined. That is the behavior the
// held case records.
func TestRestart_PendingGangMemberLost_RebuildsOnceRetryBlockIsDue(t *testing.T) {
	for _, tc := range []struct {
		name          string
		block         v1beta1.RetryBlockState
		nextRetryAt   *metav1.Time
		wantRestarted bool
	}{
		{name: "due", block: v1beta1.RetryBlockBackoff, nextRetryAt: ptr.To(metav1.NewTime(time.Now().Add(-time.Minute))), wantRestarted: true},
		{name: "held", block: v1beta1.RetryBlockHeld, wantRestarted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := time.Now()
			ir := gangLossIR("llama-engine", "prod")
			survivor := podForIR(ir, 0, string(v1beta1.RunnerNameLeader), 0, true, true)
			worker := podForIR(ir, 0, string(v1beta1.RunnerNameWorker), 0, true, true)
			r, c := newReconciler(t, ir, survivor, worker)
			r.Clock = clocktesting.NewFakeClock(base)
			r.Recorder = record.NewFakeRecorder(32)
			request := ctrl.Request{NamespacedName: types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}}

			// One settling pass names the revision the gang runs, which is
			// the revision a rebuild would re-materialize and therefore the
			// one a RetryBlock has to answer for.
			if _, err := r.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("settle Reconcile: %v", err)
			}
			fresh := &v1beta1.InferenceReplica{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
				t.Fatalf("get IR: %v", err)
			}
			blocked := fresh.Status.UpdateRevision
			if blocked == "" {
				t.Fatalf("no update revision on the settled IR: %+v", fresh.Status)
			}
			fresh.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
				Index:           0,
				Incarnation:     1,
				Phase:           v1beta1.OMENativeInstancePending,
				PodCount:        2,
				RunningRevision: blocked,
			}}
			fresh.Status.RetryBlocks = []v1beta1.RetryBlock{{
				TargetRevision:  blocked,
				State:           tc.block,
				AttemptsStarted: 1,
				NextRetryAt:     tc.nextRetryAt,
			}}
			if err := c.Status().Update(context.Background(), fresh); err != nil {
				t.Fatalf("seed the row: %v", err)
			}
			// The worker is gone: one of the two recorded pods is lost.
			if err := c.Delete(context.Background(), worker); err != nil {
				t.Fatalf("lose the worker: %v", err)
			}
			r.Expectations = workloadtypes.NewExpectations()

			if _, err := r.Reconcile(context.Background(), request); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			row := instanceRow(t, c, ir)
			restarted := row.Phase == v1beta1.OMENativeInstanceRestarting &&
				row.Operation != nil && row.Operation.Type == v1beta1.InstanceOperationRestart && row.Operation.Step == "Drain"
			if restarted != tc.wantRestarted {
				t.Fatalf("restarted=%v want %v; row %+v (op %+v)", restarted, tc.wantRestarted, row, row.Operation)
			}
		})
	}
}

// reconcileForReadyTimeout runs one pass over a fresh IR and returns the
// stored object. lifecycleJSON of "" leaves the operator ConfigMap
// unwired, which is how an operator who supplies no lifecycle block at
// all is seen from inside the controller.
func reconcileForReadyTimeout(t *testing.T, ir *v1beta1.InferenceReplica, lifecycleJSON string) (*v1beta1.InferenceReplica, []string) {
	t.Helper()
	g := gomega.NewWithT(t)
	r, c := newReconciler(t, ir)
	rec := record.NewFakeRecorder(16)
	r.Recorder = rec
	if lifecycleJSON != "" {
		withLifecycleConfig(r, lifecycleJSON)
	}

	key := client.ObjectKeyFromObject(ir)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	stored := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, stored)).To(gomega.Succeed())
	return stored, drainEvents(rec)
}

func conditionOfType(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

// TestInstanceReadyTimeout_UnconfiguredOpensWithNoDeadline pins the
// honest-unconfigured contract: with no readiness window at either level
// the Create operation opens with NO deadline — never one equal to its
// own start, which the next pass would read as elapsed — and the
// Component says so through a condition and a Warning event.
func TestInstanceReadyTimeout_UnconfiguredOpensWithNoDeadline(t *testing.T) {
	g := gomega.NewWithT(t)

	stored, events := reconcileForReadyTimeout(t, baselineIR("llama-engine", "default", 1), "")

	g.Expect(stored.Status.InstanceStatuses).To(gomega.HaveLen(1))
	op := stored.Status.InstanceStatuses[0].Operation
	g.Expect(op).NotTo(gomega.BeNil())
	g.Expect(op.Deadline.IsZero()).To(gomega.BeTrue(),
		"an unconfigured readiness window must open the operation with no deadline")

	cond := conditionOfType(stored.Status.Conditions, string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured))
	g.Expect(cond).NotTo(gomega.BeNil())
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue))
	g.Expect(cond.Reason).To(gomega.Equal(string(workloadtypes.ReasonInstanceReadyTimeoutUnconfigured)))
	g.Expect(eventsContaining(events, string(workloadtypes.EventReasonInstanceReadyTimeoutUnconfigured))).To(gomega.HaveLen(1))
}

// TestInstanceReadyTimeout_ConfigSuppliesTheWindow pins the fallback: a
// Component that sets none of its own takes the operator's
// lifecycle.instanceReadyTimeout, and the condition clears.
func TestInstanceReadyTimeout_ConfigSuppliesTheWindow(t *testing.T) {
	g := gomega.NewWithT(t)

	before := time.Now()
	stored, events := reconcileForReadyTimeout(t, baselineIR("llama-engine", "default", 1),
		`{"instanceReadyTimeout":"30m"}`)

	g.Expect(stored.Status.InstanceStatuses).To(gomega.HaveLen(1))
	op := stored.Status.InstanceStatuses[0].Operation
	g.Expect(op).NotTo(gomega.BeNil())
	g.Expect(op.Deadline.Time).To(gomega.BeTemporally("~", before.Add(30*time.Minute), time.Minute))

	cond := conditionOfType(stored.Status.Conditions, string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured))
	g.Expect(cond).NotTo(gomega.BeNil())
	g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(cond.Reason).To(gomega.Equal(string(workloadtypes.ReasonInstanceReadyTimeoutConfigured)))
	g.Expect(eventsContaining(events, string(workloadtypes.EventReasonInstanceReadyTimeoutUnconfigured))).To(gomega.BeEmpty())
}

// TestInstanceReadyTimeout_SpecWinsOverConfig pins the precedence: an
// explicit per-resource window overrides the operator's.
func TestInstanceReadyTimeout_SpecWinsOverConfig(t *testing.T) {
	g := gomega.NewWithT(t)

	ir := baselineIR("llama-engine", "default", 1)
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{
		InstanceReadyTimeout: &metav1.Duration{Duration: 5 * time.Minute},
	}

	before := time.Now()
	stored, _ := reconcileForReadyTimeout(t, ir, `{"instanceReadyTimeout":"30m"}`)

	g.Expect(stored.Status.InstanceStatuses).To(gomega.HaveLen(1))
	op := stored.Status.InstanceStatuses[0].Operation
	g.Expect(op).NotTo(gomega.BeNil())
	g.Expect(op.Deadline.Time).To(gomega.BeTemporally("~", before.Add(5*time.Minute), time.Minute))
}

func TestReconcileDenseV1IsUnchangedByTheDecoder(t *testing.T) {
	ctx := context.Background()
	ir := decodeFixtureIR()
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	plain, cPlain := newReconciler(t, ir.DeepCopy())
	bounded, cBounded := newReconciler(t, ir.DeepCopy())
	bounded.InstanceStatusDecoder = irstatus.NewDecoder(8)
	// Both reconciles stamp the attempts they open at "now"; a second
	// boundary between the two runs would otherwise make the published
	// timestamps differ for a reason unrelated to the decoder.
	fixed := clocktesting.NewFakeClock(time.Now())
	plain.Clock, bounded.Clock = fixed, fixed

	resPlain, errPlain := plain.Reconcile(ctx, req)
	resBounded, errBounded := bounded.Reconcile(ctx, req)
	if resPlain != resBounded || (errPlain == nil) != (errBounded == nil) {
		t.Fatalf("a configured bound must not change DenseV1 reconciliation: %+v/%v vs %+v/%v", resPlain, errPlain, resBounded, errBounded)
	}
	storedPlain, storedBounded := &v1beta1.InferenceReplica{}, &v1beta1.InferenceReplica{}
	if err := cPlain.Get(ctx, req.NamespacedName, storedPlain); err != nil {
		t.Fatal(err)
	}
	if err := cBounded.Get(ctx, req.NamespacedName, storedBounded); err != nil {
		t.Fatal(err)
	}
	storedPlain.Status.Conditions, storedBounded.Status.Conditions = nil, nil
	if !equality.Semantic.DeepEqual(storedPlain.Status, storedBounded.Status) {
		t.Fatalf("published DenseV1 status differs with a configured bound:\n plain:   %+v\n bounded: %+v", storedPlain.Status, storedBounded.Status)
	}
}

// TestReconcileMetadataWriteKeepsDecodedRows pins the in-memory object after
// the entry pass's finalizer write: the API response carries the stored
// representation, so a ColumnarV2 object must be decoded again before the
// lifecycle observes it. A ColumnarV2-stored object under the ColumnarV2
// target must plan exactly what its DenseV1 twin plans; observing an empty
// row set would recreate every Instance.
func TestReconcileMetadataWriteKeepsDecodedRows(t *testing.T) {
	ctx := context.Background()
	dense := fixtureIRWithRows("llama-engine", uniformFixtureRows(64))
	if len(dense.Finalizers) != 0 {
		t.Fatal("fixture must start without the teardown finalizer")
	}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(dense)}

	rDense, cDense := newReconciler(t, dense.DeepCopy())
	denseResult, err := rDense.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("dense reconcile: %v", err)
	}
	rColumnar, cColumnar := newColumnarReconciler(t, columnarTwin(t, dense))
	columnarResult, err := rColumnar.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("columnar reconcile: %v", err)
	}
	if columnarResult != denseResult {
		t.Fatalf("results differ by stored encoding: dense %+v, columnar %+v", denseResult, columnarResult)
	}

	rowsOf := func(r *Reconciler) ([]v1beta1.OMENativeInstanceStatus, []string) {
		ir := &v1beta1.InferenceReplica{}
		if _, err := irstatus.GetDecoded(ctx, r.cachedReader(), req.NamespacedName, ir); err != nil {
			t.Fatalf("decoded read: %v", err)
		}
		return ir.Status.InstanceStatuses, ir.Finalizers
	}
	denseRows, denseFinalizers := rowsOf(rDense)
	columnarRows, columnarFinalizers := rowsOf(rColumnar)
	if len(denseFinalizers) != 1 || !reflect.DeepEqual(denseFinalizers, columnarFinalizers) {
		t.Fatalf("finalizer write differs: dense %v, columnar %v", denseFinalizers, columnarFinalizers)
	}
	if len(columnarRows) != len(denseRows) {
		t.Fatalf("row count differs: dense %d, columnar %d", len(denseRows), len(columnarRows))
	}
	for i := range denseRows {
		want, got := denseRows[i], columnarRows[i]
		if want.Index != got.Index || want.Phase != got.Phase || (want.Operation == nil) != (got.Operation == nil) ||
			(want.Operation != nil && want.Operation.Type != got.Operation.Type) {
			t.Fatalf("lifecycle decision differs at row %d: dense phase %s op %+v, columnar phase %s op %+v", i, want.Phase, want.Operation, got.Phase, got.Operation)
		}
	}
	if got, want := len(listPods(t, cColumnar, dense.Namespace)), len(listPods(t, cDense, dense.Namespace)); got != want {
		t.Fatalf("Pod effect differs: dense %d, columnar %d", want, got)
	}
}

// The force-delete sweep inside the migration source drain, driven
// through the adapter.
//
// The sweep runs after the surge has passed its rotation and
// availability gates, and the availability half reads the row's PodCount
// and AvailablePodCount — counters only the adapter's status publication
// writes. Driving the arrow therefore needs a walk that publishes them,
// which is what this full-loop reconcile does.

// TestReconcile_MigrationSourceWedged_ForceDeletesAndCompletes drives the
// migration whose source pods never leave: the kubelet behind them is
// gone, so the graceful delete the drain issues leaves a Terminating
// object nothing will ever clear. With lifecycle.forceDelete configured
// the drain force-deletes them at grace zero and the migration completes
// on the resulting absence.
func TestReconcile_MigrationSourceWedged_ForceDeletesAndCompletes(t *testing.T) {
	base := time.Now()
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "test:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
		}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "test:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
		}}},
	}

	// The kubelet behind the source Instance is gone: its pods keep the
	// deletion timestamp the graceful delete stamped and no finalizer,
	// and only a grace-zero delete takes them out. The fake client cannot
	// store a DeletionTimestamp without finalizers, so the wedge is held
	// here and presented on every List, the way the apiserver would.
	wedged := map[string]metav1.Time{}
	var forced []string
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithIndex(&schedulingv1alpha1.PodGroup{}, workloadgang.PodGroupControllerUIDIndexField, workloadgang.PodGroupControllerUIDIndexExtractor).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := cl.List(ctx, list, opts...); err != nil {
					return err
				}
				pl, ok := list.(*corev1.PodList)
				if !ok {
					return nil
				}
				for i := range pl.Items {
					if dt, stuck := wedged[pl.Items[i].Name]; stuck {
						pl.Items[i].DeletionTimestamp = &dt
					}
				}
				return nil
			},
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				pod, ok := obj.(*corev1.Pod)
				if !ok {
					return cl.Delete(ctx, obj, opts...)
				}
				do := &client.DeleteOptions{}
				for _, o := range opts {
					o.ApplyToDelete(do)
				}
				if do.GracePeriodSeconds != nil && *do.GracePeriodSeconds == 0 {
					forced = append(forced, pod.Name)
					return cl.Delete(ctx, obj, opts...)
				}
				if pod.Labels[query.LabelInstanceIdx] == "0" {
					// Graceful delete on a pod whose node is dead: the
					// object stays, overdue from the instant of the request.
					wedged[pod.Name] = metav1.NewTime(base.Add(-10 * time.Minute))
					return nil
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	rec := record.NewFakeRecorder(256)
	r := &Reconciler{
		Client:                   c,
		APIReader:                c,
		Log:                      logf.Log.WithName("test"),
		InstanceStatusTarget:     irstatus.EncodingDenseV1,
		Expectations:             workloadtypes.NewExpectations(),
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
		Recorder:                 rec,
		Clock:                    clocktesting.NewFakeClock(base),
		GangSchedulingAvailable:  true,
	}
	withLifecycleConfig(r, `{"audit":{"maxInFlightMigrations":3,"maxMigrationsPerWindow":10,"window":"1h"},`+
		`"forceDelete":{"overdueSlack":"2m","nodeUnreachableThreshold":"5m"}}`)

	ctx := context.Background()
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	var events []string
	step := func(tag string) {
		r.Expectations = workloadtypes.NewExpectations()
		if _, err := r.Reconcile(ctx, request); err != nil {
			t.Fatalf("%s reconcile: %v", tag, err)
		}
		events = append(events, drainEvents(rec)...)
		gangMigSimulate(t, c, ir.Namespace, ir.Spec.ParentRef.Name)
	}
	get := func() *v1beta1.InferenceReplica {
		fresh := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(ir), fresh); err != nil {
			t.Fatalf("get IR: %v", err)
		}
		return fresh
	}

	for i := 0; i < 10; i++ {
		step("startup")
	}
	if fresh := get(); len(fresh.Status.InstanceStatuses) != 1 ||
		fresh.Status.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("gang did not reach Ready: %+v", fresh.Status.InstanceStatuses)
	}

	fresh := get()
	fresh.Status.Migrations = []v1beta1.MigrationStatus{{
		RequestUUID:    "mig-source-wedged",
		Trigger:        v1beta1.MigrationTriggerManual,
		Phase:          v1beta1.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       gangMigSourceNode,
		Reason:         "test",
		StartedAt:      metav1.NewTime(base),
		Deadline:       metav1.NewTime(base.Add(30 * time.Minute)),
	}}
	if err := c.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("seed migration record: %v", err)
	}

	completed := false
	for i := 0; i < 30 && !completed; i++ {
		step("migration")
		got := get()
		if len(got.Status.Migrations) != 1 {
			t.Fatalf("migration record count: %+v", got.Status.Migrations)
		}
		switch got.Status.Migrations[0].Phase {
		case v1beta1.MigrationPhaseCompleted:
			completed = true
		case v1beta1.MigrationPhaseFailed:
			t.Fatalf("migration failed: %+v", got.Status.Migrations[0])
		}
	}
	if !completed {
		got := get()
		t.Fatalf("migration never completed: record=%+v instances=%+v wedged=%v forced=%v",
			got.Status.Migrations[0], got.Status.InstanceStatuses, wedged, forced)
	}

	if len(wedged) == 0 {
		t.Fatal("the source pods were never wedged, so the sweep was not the thing that cleared them")
	}
	for name := range wedged {
		if !containsString(forced, name) {
			t.Errorf("wedged source pod %s was not force-deleted at grace zero; forced=%v", name, forced)
		}
	}
	if len(eventsContaining(events, string(workloadtypes.EventReasonPodForceDeleted))) == 0 {
		t.Errorf("the sweep must warn PodForceDeleted; events=%v", events)
	}
	if got := get(); len(got.Status.InstanceStatuses) != 1 || got.Status.InstanceStatuses[0].Index == 0 {
		t.Errorf("only the promoted surge survives, got %+v", got.Status.InstanceStatuses)
	}
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}

// Full-loop gang migration walk through the real Reconcile dispatch
// (plan build, EnsurePodGroups, Migrate op, guarded status seams)
// against a lockstep-simulated environment: scheduler binding, kubelet
// readiness (gate-aware PodReady), the coordination layer's
// per-revision routing Service, and the endpointslice controller.
//
// The walk crosses the window the ops-level fixtures cannot reach: the
// plan releases the source index the moment the surge is promoted
// Ready, while the migration record is still Draining. The completion
// tail must keep computing the gang-shaped desired pod set from the
// surge's own plan entry — losing the shape collapses the surge to the
// single-pod fallback, renders a spurious "default" runner pod that
// can never enter the leader-only routing rotation, and parks the
// record at Draining forever.

const (
	gangMigSourceNode = "node-a"
	gangMigOtherNode  = "node-b"
)

// gangMigSimulate advances the simulated environment one step:
// binds unscheduled pods (the source leader to gangMigSourceNode,
// everything else — including the NotIn[source-node] surge — to
// gangMigOtherNode), flips ContainersReady, computes PodReady as
// ContainersReady AND the ome.io/serving gate (kubelet's readiness-gate
// contract), and mirrors pod state into EndpointSlices for the
// per-revision routing Service (leaders only, ready follows PodReady)
// and the component headless Service (all pods, publishNotReadyAddresses
// semantics).
func gangMigSimulate(t *testing.T, c client.Client, ns, isvcName string) {
	t.Helper()
	ctx := context.Background()
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ns)); err != nil {
		t.Fatalf("sim list pods: %v", err)
	}

	leadersByHash := map[string][]*corev1.Pod{}
	all := []*corev1.Pod{}
	for i := range pods.Items {
		p := &pods.Items[i]
		all = append(all, p)
		if p.DeletionTimestamp != nil {
			continue
		}
		if p.Spec.NodeName == "" {
			node := gangMigOtherNode
			if p.Labels[query.LabelRunner] == "leader" && p.Labels[query.LabelInstanceIdx] == "0" {
				node = gangMigSourceNode
			}
			p.Spec.NodeName = node
			if err := c.Update(ctx, p); err != nil {
				t.Fatalf("sim bind pod: %v", err)
			}
		}
		changed := gangMigSetCond(p, corev1.ContainersReady, corev1.ConditionTrue)
		ready := corev1.ConditionFalse
		if podreadiness.IsServing(p) {
			ready = corev1.ConditionTrue
		}
		changed = gangMigSetCond(p, corev1.PodReady, ready) || changed
		p.Status.Phase = corev1.PodRunning
		if changed {
			if err := c.Status().Update(ctx, p); err != nil {
				t.Fatalf("sim kubelet status: %v", err)
			}
		}
		if p.Labels[query.LabelRunner] == "leader" {
			if hash := p.Labels[query.LabelRevisionHash]; hash != "" {
				leadersByHash[hash] = append(leadersByHash[hash], p)
			}
		}
	}

	for hash, leaders := range leadersByHash {
		svcName := query.PerRevisionServiceName(isvcName, workloadtypes.ComponentEngine, hash)
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: svcName}}
		if err := c.Get(ctx, client.ObjectKeyFromObject(svc), svc); apierrors.IsNotFound(err) {
			svc.Spec = corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 8080}}}
			if err := c.Create(ctx, svc); err != nil {
				t.Fatalf("sim create routing svc: %v", err)
			}
		}
		eps := make([]discoveryv1.Endpoint, 0, len(leaders))
		for i, p := range leaders {
			podReady := gangMigHasCond(p, corev1.PodReady, corev1.ConditionTrue)
			eps = append(eps, discoveryv1.Endpoint{
				Addresses: []string{fmt.Sprintf("10.0.0.%d", i+1)},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       ptr.To(podReady && p.DeletionTimestamp == nil),
					Serving:     ptr.To(podReady),
					Terminating: ptr.To(p.DeletionTimestamp != nil),
				},
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: p.UID},
			})
		}
		gangMigUpsertSlice(t, c, ns, svcName, eps)
	}

	headless := query.HeadlessServiceName(isvcName, workloadtypes.ComponentEngine)
	eps := make([]discoveryv1.Endpoint, 0, len(all))
	for i, p := range all {
		eps = append(eps, discoveryv1.Endpoint{
			Addresses: []string{fmt.Sprintf("10.0.1.%d", i+1)},
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ptr.To(p.DeletionTimestamp == nil),
				Serving:     ptr.To(true),
				Terminating: ptr.To(p.DeletionTimestamp != nil),
			},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: p.UID},
		})
	}
	gangMigUpsertSlice(t, c, ns, headless, eps)
}

func gangMigUpsertSlice(t *testing.T, c client.Client, ns, svcName string, eps []discoveryv1.Endpoint) {
	t.Helper()
	ctx := context.Background()
	name := svcName + "-sim"
	existing := &discoveryv1.EndpointSlice{}
	err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, existing)
	if apierrors.IsNotFound(err) {
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns, Name: name,
				Labels: map[string]string{discoveryv1.LabelServiceName: svcName},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints:   eps,
		}
		if cerr := c.Create(ctx, slice); cerr != nil {
			t.Fatalf("sim create slice: %v", cerr)
		}
		return
	}
	if err != nil {
		t.Fatalf("sim get slice: %v", err)
	}
	existing.Endpoints = eps
	if uerr := c.Update(ctx, existing); uerr != nil {
		t.Fatalf("sim update slice: %v", uerr)
	}
}

func gangMigSetCond(p *corev1.Pod, ct corev1.PodConditionType, st corev1.ConditionStatus) bool {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == ct {
			if p.Status.Conditions[i].Status == st {
				return false
			}
			p.Status.Conditions[i].Status = st
			p.Status.Conditions[i].LastTransitionTime = metav1.Now()
			return true
		}
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type: ct, Status: st, LastTransitionTime: metav1.Now(),
	})
	return true
}

func gangMigHasCond(p *corev1.Pod, ct corev1.PodConditionType, st corev1.ConditionStatus) bool {
	for _, cond := range p.Status.Conditions {
		if cond.Type == ct {
			return cond.Status == st
		}
	}
	return false
}

// The walk also carries the migration tail's two terminal arrows, which
// need the published pod counters the surge availability gate reads:
// the surge promotes to Ready on the source's revision with its pin
// cleared, and the drained source's row is then removed with the record
// stamped Completed.
func TestReconcile_GangMigrationCompletesAfterSourcePlanRelease(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "test:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
		}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "test:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
		}}},
	}
	r, c := newReconciler(t, ir)
	withMigrationCapacityConfig(r)
	r.GangSchedulingAvailable = true
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	ctx := context.Background()

	get := func() *v1beta1.InferenceReplica {
		fresh := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(ir), fresh); err != nil {
			t.Fatalf("get IR: %v", err)
		}
		return fresh
	}
	// One reconcile + one environment step; expectations are reset first
	// (a fresh cache reads as satisfied — the informer-caught-up state).
	step := func(tag string) {
		r.Expectations = workloadtypes.NewExpectations()
		if _, err := r.Reconcile(ctx, request); err != nil {
			t.Fatalf("%s reconcile: %v", tag, err)
		}
		gangMigSimulate(t, c, ir.Namespace, ir.Spec.ParentRef.Name)
	}

	for i := 0; i < 10; i++ {
		step("startup")
	}
	fresh := get()
	if len(fresh.Status.InstanceStatuses) != 1 || fresh.Status.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("gang did not reach Ready: %+v", fresh.Status.InstanceStatuses)
	}

	// Accept-shaped record: migrate the gang off the leader's node.
	fresh.Status.Migrations = []v1beta1.MigrationStatus{{
		RequestUUID:    "mig-gang-release",
		Trigger:        v1beta1.MigrationTriggerManual,
		Phase:          v1beta1.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       gangMigSourceNode,
		Reason:         "test",
		StartedAt:      metav1.Now(),
		Deadline:       metav1.NewTime(time.Now().Add(30 * time.Minute)),
	}}
	if err := c.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("seed migration record: %v", err)
	}

	completed := false
	for i := 0; i < 30 && !completed; i++ {
		step("migration")
		got := get()
		if len(got.Status.Migrations) != 1 {
			t.Fatalf("migration record count: %+v", got.Status.Migrations)
		}
		switch got.Status.Migrations[0].Phase {
		case v1beta1.MigrationPhaseCompleted:
			completed = true
		case v1beta1.MigrationPhaseFailed:
			t.Fatalf("migration failed: %+v", got.Status.Migrations[0])
		}
	}
	if !completed {
		got := get()
		t.Fatalf("migration never completed: record=%+v instances=%+v",
			got.Status.Migrations[0], got.Status.InstanceStatuses)
	}

	// Exactly the promoted surge survives, unpinned.
	got := get()
	if len(got.Status.InstanceStatuses) != 1 {
		t.Fatalf("instance statuses after completion: %+v", got.Status.InstanceStatuses)
	}
	surge := got.Status.InstanceStatuses[0]
	if surge.Index == 0 || surge.Phase != v1beta1.OMENativeInstanceReady || surge.Operation != nil {
		t.Fatalf("promoted surge shape: %+v", surge)
	}

	// The surge kept the gang shape end to end: one leader + one worker,
	// no single-pod-fallback "default" runner pod, and no source pod left.
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ir.Namespace)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	runners := map[string]int{}
	for i := range pods.Items {
		p := pods.Items[i]
		if p.Labels[query.LabelInstanceIdx] == "0" {
			t.Fatalf("source gang pod survived completion: %s", p.Name)
		}
		runners[p.Labels[query.LabelRunner]]++
	}
	if runners["default"] != 0 || runners["leader"] != 1 || runners["worker"] != 1 {
		t.Fatalf("surge runner layout: %+v", runners)
	}
}

// directiveEntry builds one terminal relocation-directive ledger row.
func directiveEntry(uid, component string, idx int32, node string) audit.Entry {
	return audit.Entry{
		RequestUUID:    uid,
		Component:      component,
		SourceInstance: idx,
		Phase:          audit.PhaseCompleted,
		Reason:         audit.ReasonAutoRecover,
		Outcome:        audit.OutcomeRelocateRecreate,
		FromNode:       node,
		StartedAt:      time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
	}
}

// reconcileRelocationDirectives projects the exclusion map from the
// ledger's AutoRecover directives, bounded to the most recent
// autoMigrateBudget DISTINCT nodes per instance, scoped to the IR's
// component, and ignoring non-AutoRecover rows.
func TestReconcileRelocationDirectives_BuildsBoundedExclusionMap(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	r, c := newReconciler(t, ir)

	ledger := &audit.Ledger{}
	// Five directives for instance 0 — dedup happens BEFORE the budget
	// window, so the repeated n4 doesn't shrink the memory: the last
	// three DISTINCT nodes {n2,n3,n4} survive.
	for i, node := range []string{"n1", "n2", "n3", "n4", "n4"} {
		ledger.UpsertEntry(directiveEntry(fmt.Sprintf("u%d", i), "engine", 0, node))
	}
	// Noise: other component + non-AutoRecover operator migration.
	ledger.UpsertEntry(directiveEntry("dec", "decoder", 0, "n9"))
	ledger.UpsertEntry(audit.Entry{RequestUUID: "op1", Component: "engine", SourceInstance: 0,
		Phase: audit.PhaseStarted, Reason: "fragmentation", FromNode: "n8",
		StartedAt: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)})
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 {
		t.Fatalf("map: got %v want exactly instance 0", got)
	}
	nodes := got[0]
	if len(nodes) != 3 || nodes[0] != "n2" || nodes[1] != "n3" || nodes[2] != "n4" {
		t.Errorf("instance 0 exclusions: got %v want [n2 n3 n4] (last 3 distinct nodes)", nodes)
	}

	// Budget 0 (unconfigured) → no exclusions at all.
	if got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 0); got != nil {
		t.Errorf("budget 0: got %v want nil", got)
	}
}

// An instance observed Phase=Ready with no in-flight Operation has
// proven its placement: its AutoRecover directives are pruned from the
// persisted ledger (success-prune mirror of the RetryBlock prune) and
// drop out of the exclusion map. Foreign entries survive.
func TestReconcileRelocationDirectives_PrunesOnReady(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	r, c := newReconciler(t, ir)

	ledger := &audit.Ledger{}
	ledger.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	ledger.UpsertEntry(directiveEntry("u1", "engine", 1, "n2"))
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 || len(got[1]) != 1 || got[1][0] != "n2" {
		t.Fatalf("map: got %v want only instance 1 → [n2] (instance 0 pruned on Ready)", got)
	}

	// The prune persisted: instance 0's directive is gone from the CM,
	// instance 1's survives.
	after, err := audit.LoadLedgerForOwner(context.Background(), c, ir)
	if err != nil {
		t.Fatalf("reload ledger: %v", err)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 0) != 0 {
		t.Errorf("instance 0 directives not pruned: %+v", after.Entries)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 1) != 1 {
		t.Errorf("instance 1 directives must survive: %+v", after.Entries)
	}
}

// The prune-persist branch re-loads the ledger LIVE and re-prunes
// before writing: the persist writes the snapshot wholesale, so basing
// it on a lagged cache would drop rows written concurrently by sibling
// IRs. The sibling decoder row here exists ONLY behind APIReader and
// must survive the persisted prune.
func TestReconcileRelocationDirectives_PrunePersistsFromLiveSnapshot(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	r, c := newReconciler(t, ir)

	cached := &audit.Ledger{}
	cached.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	cached.UpsertEntry(directiveEntry("u1", "engine", 1, "n2"))
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, cached); err != nil {
		t.Fatalf("seed cached ledger: %v", err)
	}
	readerClient := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	live := &audit.Ledger{}
	live.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	live.UpsertEntry(directiveEntry("u1", "engine", 1, "n2"))
	live.UpsertEntry(directiveEntry("dec", "decoder", 0, "n9"))
	if err := audit.PersistLedgerForOwner(context.Background(), readerClient, ir, irGVK, live); err != nil {
		t.Fatalf("seed live ledger: %v", err)
	}
	r.APIReader = readerClient

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 || len(got[1]) != 1 || got[1][0] != "n2" {
		t.Fatalf("map: got %v want only instance 1 → [n2]", got)
	}

	after, err := audit.LoadLedgerForOwner(context.Background(), c, ir)
	if err != nil {
		t.Fatalf("reload ledger: %v", err)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 0) != 0 {
		t.Errorf("instance 0 directives not pruned: %+v", after.Entries)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 1) != 1 {
		t.Errorf("instance 1 directives must survive: %+v", after.Entries)
	}
	if audit.CountAutoRecoverAttempts(after, "decoder", 0) != 1 {
		t.Errorf("sibling decoder row must survive the wholesale persist: %+v", after.Entries)
	}
}

// failingGetReader errors every Get — stands in for a live re-load
// failure inside the prune-persist branch.
type failingGetReader struct{ client.Reader }

func (failingGetReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return fmt.Errorf("live read down")
}

// A live re-load failure in the persist branch fails open: the
// projection still uses the cache-pruned in-memory view, and the
// persisted ledger stays untouched so the prune retries next pass.
func TestReconcileRelocationDirectives_LiveReloadFailureSkipsPersist(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	r, c := newReconciler(t, ir)

	ledger := &audit.Ledger{}
	ledger.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	ledger.UpsertEntry(directiveEntry("u1", "engine", 1, "n2"))
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	r.APIReader = failingGetReader{}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 || len(got[1]) != 1 || got[1][0] != "n2" {
		t.Fatalf("map: got %v want only instance 1 → [n2] (cache-pruned view)", got)
	}

	after, err := audit.LoadLedgerForOwner(context.Background(), c, ir)
	if err != nil {
		t.Fatalf("reload ledger: %v", err)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 0) != 1 {
		t.Errorf("persist must be skipped on live re-load failure (prune retries next pass): %+v", after.Entries)
	}
}

// autoRecord builds one born-terminal Auto migration status record.
func autoRecord(uuid string, idx int32, node string, startedAt time.Time) v1beta1.MigrationStatus {
	started := metav1.NewTime(startedAt)
	completed := started
	return v1beta1.MigrationStatus{
		RequestUUID:    uuid,
		Trigger:        v1beta1.MigrationTriggerAuto,
		SourceInstance: idx,
		FromNode:       node,
		Phase:          v1beta1.MigrationPhaseRelocated,
		Attempt:        1,
		Reason:         audit.ReasonAutoRecover,
		StartedAt:      started,
		Deadline:       started,
		CompletedAt:    &completed,
	}
}

// The success touch: an instance observed Ready with no in-flight
// Operation stamps Succeeded=true + CompletedAt on its NEWEST
// un-Succeeded Auto record only — older un-Succeeded records and other
// instances' records stay untouched — while the same pass prunes the
// instance's ledger rows. The asymmetry is deliberate: ledger =
// working memory for exclusions (pruned on Ready), record = visible
// history (persists until the trim window).
func TestReconcileRelocationDirectives_SuccessTouchStampsNewestAutoRecord(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	ir := baselineIR("llama-engine", "prod", 2)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate}},
	}
	ir.Status.Migrations = []v1beta1.MigrationStatus{
		autoRecord("u-old", 0, "n1", t0.Add(-10*time.Minute)),
		autoRecord("u-new", 0, "n2", t0.Add(-5*time.Minute)),
		autoRecord("u-other", 1, "n3", t0.Add(-5*time.Minute)),
	}
	r, c := newReconciler(t, ir)

	ledger := &audit.Ledger{}
	ledger.UpsertEntry(directiveEntry("u-old", "engine", 0, "n1"))
	ledger.UpsertEntry(directiveEntry("u-new", "engine", 0, "n2"))
	ledger.UpsertEntry(directiveEntry("u-other", "engine", 1, "n3"))
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 || len(got[1]) != 1 || got[1][0] != "n3" {
		t.Fatalf("exclusion map: got %v want only instance 1 → [n3]", got)
	}

	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	byUUID := map[string]v1beta1.MigrationStatus{}
	for _, e := range fresh.Status.Migrations {
		byUUID[e.RequestUUID] = e
	}
	newest := byUUID["u-new"]
	if newest.Succeeded == nil || !*newest.Succeeded {
		t.Errorf("u-new Succeeded: got %v want true (newest record for the Ready instance)", newest.Succeeded)
	}
	if newest.CompletedAt == nil || !newest.CompletedAt.Time.After(t0.Add(-5*time.Minute)) {
		t.Errorf("u-new CompletedAt: got %v want restamped at success time", newest.CompletedAt)
	}
	if older := byUUID["u-old"]; older.Succeeded != nil {
		t.Errorf("u-old Succeeded: got %v want nil (only the newest record is stamped)", *older.Succeeded)
	}
	if other := byUUID["u-other"]; other.Succeeded != nil {
		t.Errorf("u-other Succeeded: got %v want nil (instance 1 is not Ready)", *other.Succeeded)
	}
	if len(ir.Status.Migrations) != 3 || ir.Status.Migrations[1].Succeeded == nil {
		t.Errorf("in-memory mirror: got %+v want the committed stamp mirrored back", ir.Status.Migrations)
	}

	// The ledger rows for instance 0 pruned in the same pass; the
	// status records persist — the asymmetry under test.
	after, err := audit.LoadLedgerForOwner(context.Background(), c, ir)
	if err != nil {
		t.Fatalf("reload ledger: %v", err)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 0) != 0 {
		t.Errorf("instance 0 ledger rows must prune on Ready: %+v", after.Entries)
	}
	if audit.CountAutoRecoverAttempts(after, "engine", 1) != 1 {
		t.Errorf("instance 1 ledger rows must survive: %+v", after.Entries)
	}
}

// Crash-window heal: the ledger rows were already pruned (prior pass
// crashed between the prune persist and the stamp) — the success touch
// still fires because it is keyed on the status records, not the
// ledger.
func TestReconcileRelocationDirectives_SuccessTouchIndependentOfLedger(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
	}
	ir.Status.Migrations = []v1beta1.MigrationStatus{autoRecord("u-orphan", 0, "n1", t0)}
	r, c := newReconciler(t, ir)

	// No ledger seeded at all.
	if got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3); got != nil {
		t.Fatalf("exclusion map: got %v want nil (empty ledger)", got)
	}

	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(ir), fresh); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	if len(fresh.Status.Migrations) != 1 || fresh.Status.Migrations[0].Succeeded == nil || !*fresh.Status.Migrations[0].Succeeded {
		t.Errorf("record: got %+v want u-orphan stamped Succeeded=true without any ledger rows", fresh.Status.Migrations)
	}
}

func TestStampAutoRelocationSuccess_SameNameReplacementUntouched(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	stale := baselineIR("llama-engine", "prod", 1)
	stale.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
	}
	stale.Status.Migrations = []v1beta1.MigrationStatus{autoRecord("u-stale", 0, "n1", t0)}

	replacement := stale.DeepCopy()
	replacement.UID = "replacement-uid"
	r, c := newReconciler(t, replacement)

	err := r.stampAutoRelocationSuccess(context.Background(), stale)
	if !errors.Is(err, workloadtypes.ErrStatusOwnerGone) {
		t.Fatalf("stamp error: got %v want ErrStatusOwnerGone", err)
	}

	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(replacement), fresh); err != nil {
		t.Fatalf("re-read replacement: %v", err)
	}
	if got := fresh.Status.Migrations[0].Succeeded; got != nil {
		t.Fatalf("replacement Succeeded: got %v want nil", *got)
	}
	if got := stale.Status.Migrations[0].Succeeded; got != nil {
		t.Fatalf("stale in-memory Succeeded: got %v want nil", *got)
	}
}

// A Ready instance with an in-flight Operation (e.g. a fresh update
// just stamped) is NOT pruned — only settled Ready-no-op instances
// reset their relocation memory.
func TestReconcileRelocationDirectives_ReadyWithOpNotPruned(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate,
				StartedAt: metav1.NewTime(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))}},
	}
	r, c := newReconciler(t, ir)
	ledger := &audit.Ledger{}
	ledger.UpsertEntry(directiveEntry("u0", "engine", 0, "n1"))
	if err := audit.PersistLedgerForOwner(context.Background(), c, ir, irGVK, ledger); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	got := r.reconcileRelocationDirectives(context.Background(), logf.Log.WithName("test"), ir, nil, 3)
	if len(got) != 1 || len(got[0]) != 1 || got[0][0] != "n1" {
		t.Fatalf("map: got %v want instance 0 → [n1] (no prune while op in flight)", got)
	}
}

func TestBumpCollisionCount_SameNameReplacementUntouched(t *testing.T) {
	stale := baselineIR("llama-engine", "prod", 1)
	replacement := stale.DeepCopy()
	replacement.UID = "replacement-uid"
	count := int32(7)
	replacement.Status.CollisionCount = &count
	r, c := newReconciler(t, replacement)

	bumped, err := r.bumpCollisionCount(context.Background(), stale)
	if !errors.Is(err, workloadtypes.ErrStatusOwnerGone) {
		t.Fatalf("bump error: got %v want ErrStatusOwnerGone", err)
	}
	if bumped != nil {
		t.Fatalf("bumped count: got %d want nil", *bumped)
	}

	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(replacement), fresh); err != nil {
		t.Fatalf("re-read replacement: %v", err)
	}
	if fresh.Status.CollisionCount == nil || *fresh.Status.CollisionCount != count {
		t.Fatalf("replacement CollisionCount: got %v want %d", fresh.Status.CollisionCount, count)
	}
	if stale.Status.CollisionCount != nil {
		t.Fatalf("stale in-memory CollisionCount: got %d want nil", *stale.Status.CollisionCount)
	}
}

func TestParseExcludedAnnotationKeys(t *testing.T) {
	g := gomega.NewWithT(t)

	g.Expect(parseExcludedAnnotationKeys(&v1beta1.InferenceReplica{})).To(gomega.BeNil(),
		"no annotation -> nil")

	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		constants.RevisionExcludedAnnotationKeysAnnotationKey: "a,b,,c",
	}}}
	got := parseExcludedAnnotationKeys(ir)
	g.Expect(got).To(gomega.HaveLen(3), "empty split entries are dropped")
	g.Expect(got).To(gomega.HaveKey("a"))
	g.Expect(got).To(gomega.HaveKey("b"))
	g.Expect(got).To(gomega.HaveKey("c"))
}

func TestStripExcludedAnnotations(t *testing.T) {
	g := gomega.NewWithT(t)

	// nil excluded -> same pointer, untouched.
	meta := &metav1.ObjectMeta{Annotations: map[string]string{"a": "1"}}
	g.Expect(stripExcludedAnnotations(meta, nil)).To(gomega.BeIdenticalTo(meta))

	// nil meta -> nil.
	g.Expect(stripExcludedAnnotations(nil, map[string]struct{}{"x": {}})).To(gomega.BeNil())

	excluded := map[string]struct{}{"drop": {}}
	in := &metav1.ObjectMeta{
		Labels:      map[string]string{"l": "1"},
		Annotations: map[string]string{"keep": "1", "drop": "2"},
	}
	out := stripExcludedAnnotations(in, excluded)
	g.Expect(out).NotTo(gomega.BeIdenticalTo(in), "must return a copy, not mutate in place")
	g.Expect(out.Annotations).To(gomega.HaveKey("keep"))
	g.Expect(out.Annotations).NotTo(gomega.HaveKey("drop"))
	g.Expect(in.Annotations).To(gomega.HaveKey("drop"),
		"input must be untouched — pod rendering still uses the full annotation set")
	g.Expect(out.Labels).To(gomega.HaveKeyWithValue("l", "1"), "labels must be preserved")

	// nothing to drop -> same pointer (no needless copy).
	g.Expect(stripExcludedAnnotations(in, map[string]struct{}{"absent": {}})).
		To(gomega.BeIdenticalTo(in))
}

// TestRevisionHashStableUnderExcludedAnnotations verifies inherited ISVC annotations do not
// affect revision identity while component annotations remain hash inputs.
func TestRevisionHashStableUnderExcludedAnnotations(t *testing.T) {
	g := gomega.NewWithT(t)
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img:1"}}}

	base := &metav1.ObjectMeta{
		Labels:      map[string]string{"app": "x"},
		Annotations: map[string]string{"ome.io/declared": "1"},
	}
	baseHash, _, err := revision.HashWithWorker(podSpec, nil, base, nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Projected metadata retains the inherited annotation for pod rendering.
	withAmbient := &metav1.ObjectMeta{
		Labels: map[string]string{"app": "x"},
		Annotations: map[string]string{
			"ome.io/declared":                     "1",
			"editor.example.com/resource-version": "v1beta1",
		},
	}
	excluded := parseExcludedAnnotationKeys(&v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			constants.RevisionExcludedAnnotationKeysAnnotationKey: "editor.example.com/resource-version",
		}},
	})

	strippedHash, _, err := revision.HashWithWorker(
		podSpec, nil, stripExcludedAnnotations(withAmbient, excluded), nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(strippedHash).To(gomega.Equal(baseHash),
		"an excluded inherited annotation must not change the revision hash")

	// The unfiltered metadata remains a distinct revision input.
	unstrippedHash, _, err := revision.HashWithWorker(podSpec, nil, withAmbient, nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(unstrippedHash).NotTo(gomega.Equal(baseHash),
		"the inherited annotation must change the hash before filtering")

	// A component annotation remains part of the revision identity.
	declaredChanged := &metav1.ObjectMeta{
		Labels:      map[string]string{"app": "x"},
		Annotations: map[string]string{"ome.io/declared": "2"},
	}
	declHash, _, err := revision.HashWithWorker(
		podSpec, nil, stripExcludedAnnotations(declaredChanged, excluded), nil, "uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(declHash).NotTo(gomega.Equal(baseHash),
		"a deliberate declared-annotation change must still produce a new revision")
}

// TestRevisionHashUnchangedByIROperatorVerbAnnotations verifies the
// InferenceReplica operator verbs (release-held-revision, reset-instances)
// never mint a revision: stamped on the IR object they are not hash
// inputs at all, and even inherited onto the pod-template metadata they
// are filtered as lifecycle annotations. Each hash is computed on a fresh
// Reconciler so the memoization cache cannot mask a drift.
func TestRevisionHashUnchangedByIROperatorVerbAnnotations(t *testing.T) {
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img:1"}}}
	templateMeta := &metav1.ObjectMeta{Annotations: map[string]string{"ome.io/declared": "1"}}
	input := workloadtypes.ReconcileInput{DesiredSpec: workloadtypes.WorkloadDesiredSpec{
		PodSpec:               podSpec,
		PodTemplateObjectMeta: templateMeta,
	}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Name: "engine", Namespace: "default", UID: "ir-uid", Generation: 1,
	}}
	verbs := map[string]string{
		constants.ReleaseHeldRevisionAnnotationKey: "llama-engine-aaaaaaaa",
		constants.ResetInstancesAnnotationKey:      "13,14",
	}
	for key, val := range verbs {
		t.Run(key, func(t *testing.T) {
			g := gomega.NewWithT(t)

			baseHash, _, err := (&Reconciler{}).revisionHash(ir, input, nil, "scope-uid")
			g.Expect(err).NotTo(gomega.HaveOccurred())

			annotated := ir.DeepCopy()
			annotated.Annotations = map[string]string{key: val}
			objectHash, _, err := (&Reconciler{}).revisionHash(annotated, input, nil, "scope-uid")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(objectHash).To(gomega.Equal(baseHash),
				"%s on the IR object must not change the revision", key)

			inherited := input
			inherited.DesiredSpec.PodTemplateObjectMeta = &metav1.ObjectMeta{Annotations: map[string]string{
				"ome.io/declared": "1",
				key:               val,
			}}
			templateHash, _, err := (&Reconciler{}).revisionHash(ir, inherited, nil, "scope-uid")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(templateHash).To(gomega.Equal(baseHash),
				"%s inherited onto the pod template must be filtered from the revision", key)
		})
	}
}

func TestRevisionHashCacheInvalidatesWhenExcludedAnnotationsChange(t *testing.T) {
	g := gomega.NewWithT(t)
	const inheritedKey = "editor.example.com/resource-version"

	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img:1"}}}
	meta := &metav1.ObjectMeta{Annotations: map[string]string{inheritedKey: "v1beta1"}}
	input := workloadtypes.ReconcileInput{DesiredSpec: workloadtypes.WorkloadDesiredSpec{
		PodSpec:               podSpec,
		PodTemplateObjectMeta: meta,
	}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Name:       "engine",
		Namespace:  "default",
		UID:        "ir-uid",
		Generation: 1,
	}}
	r := &Reconciler{}

	initialHash, _, err := r.revisionHash(ir, input, nil, "scope-uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	ir.Annotations = map[string]string{
		constants.RevisionExcludedAnnotationKeysAnnotationKey: inheritedKey,
	}
	updatedHash, _, err := r.revisionHash(ir, input, nil, "scope-uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())

	expectedMeta := stripExcludedAnnotations(meta, map[string]struct{}{inheritedKey: {}})
	expectedHash, _, err := revision.HashWithWorkerAndTopology(
		podSpec, nil, expectedMeta, "", nil, "scope-uid")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(initialHash).NotTo(gomega.Equal(expectedHash))
	g.Expect(updatedHash).To(gomega.Equal(expectedHash))
}
