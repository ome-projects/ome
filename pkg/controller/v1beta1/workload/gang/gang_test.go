package gang

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// Test fixtures use a v1beta1.InferenceService stand-in so the
// PodGroup OwnerReference can resolve through metav1.NewControllerRef
// the way production code does (the workload reconciler is
// owner-agnostic but the tests pin the ISVC adapter shape so the
// PodGroup names + labels line up with the existing fixtures pre-
// created in the multi-instance / scale-down scenarios). The
// production-side gang.go itself contains no v1beta1 import.

// testOwnerGVK is what gang.EnsurePodGroups stamps onto every
// PodGroup's OwnerReference. Tests construct it via the ISVC scheme
// so a downstream consumer that round-trips the PodGroup through
// owner-aware kubectl tooling sees the same shape every other
// OMENative-emitted resource carries.
var testOwnerGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

func newOwner(ns, name string) client.Object {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(name + "-uid"),
		},
	}
}

func newGangClient(t *testing.T, initObjs ...client.Object) client.Client {
	t.Helper()
	var owner client.Object
	for _, obj := range initObjs {
		if _, isPodGroup := obj.(*schedulingv1alpha1.PodGroup); !isPodGroup {
			if _, isPod := obj.(*corev1.Pod); !isPod {
				owner = obj
				break
			}
		}
	}
	if owner != nil {
		for _, obj := range initObjs {
			pg, ok := obj.(*schedulingv1alpha1.PodGroup)
			if !ok || metav1.GetControllerOfNoCopy(pg) != nil {
				continue
			}
			pg.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(owner, testOwnerGVK)}
		}
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := schedulingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheduling/v1alpha1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initObjs...).
		WithIndex(&schedulingv1alpha1.PodGroup{}, PodGroupControllerUIDIndexField, PodGroupControllerUIDIndexExtractor).
		Build()
}

// inputWithConditionStore returns a ReconcileInput wired to record
// every WriteAggregateCondition call into condStore. The store
// doubles as the test's view onto what the reconciler attempted to
// stamp on the Component status — production paths persist into the
// owner's status subtree, but tests don't need that round-trip to
// assert behavior. Single-condition Replace semantics mirror what
// the retry-wrapped status update would persist.
func inputWithConditionStore(owner client.Object, multiPod bool, workerSize int32, gangAvailable bool, schedulerName string) (workload.ReconcileInput, *conditionStore) {
	store := newConditionStore()

	var podSpec, workerSpec *corev1.PodSpec
	podSpec = &corev1.PodSpec{
		Containers:    []corev1.Container{{Name: "main", Image: "test:v1"}},
		SchedulerName: schedulerName,
	}
	if multiPod {
		workerSpec = &corev1.PodSpec{
			Containers:    []corev1.Container{{Name: "main", Image: "test:v1"}},
			SchedulerName: schedulerName,
		}
	}
	_ = workerSize // surfaces via planFor below

	input := workload.ReconcileInput{
		OwnerObject: owner,
		OwnerGVK:    testOwnerGVK,
		EventTarget: owner,
		Key: workload.Key{
			Namespace: owner.GetNamespace(),
			Component: workload.ComponentEngine,
			OwnerName: owner.GetName(),
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec:                 podSpec,
			WorkerPodSpec:           workerSpec,
			GangSchedulingAvailable: gangAvailable,
		},
		WriteAggregateCondition: store.write,
	}
	return input, store
}

// planFor builds the workload.ComponentPlan a single-instance
// multi-pod (or single-pod) reconcile would produce.
func planFor(component workload.ComponentType, instanceIdxs []int32, multiPod bool, workerSize int32, instanceReadyTimeout time.Duration) workload.ComponentPlan {
	instances := make([]workload.InstancePlan, 0, len(instanceIdxs))
	for _, idx := range instanceIdxs {
		var runners []workload.RunnerPlan
		if multiPod {
			runners = []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: workerSize}}
		} else {
			runners = []workload.RunnerPlan{{Name: "default", Size: 1}}
		}
		instances = append(instances, workload.InstancePlan{Index: idx, Incarnation: 1, Runners: runners})
	}
	return workload.ComponentPlan{
		Component:            component,
		Replicas:             int32(len(instanceIdxs)),
		Instances:            instances,
		InstanceReadyTimeout: instanceReadyTimeout,
	}
}

// conditionStore captures WriteAggregateCondition invocations so
// tests can assert the (Status, Reason, Message) shape without a
// real apiserver round-trip.
type conditionStore struct {
	conditions []metav1.Condition
}

func newConditionStore() *conditionStore { return &conditionStore{} }

func (c *conditionStore) write(_ context.Context, cond metav1.Condition) error {
	// Mirror apimeta.SetStatusCondition Replace semantics so a
	// second-pass write replaces the first (idempotency tests rely
	// on this).
	for i, existing := range c.conditions {
		if existing.Type == cond.Type {
			c.conditions[i] = cond
			return nil
		}
	}
	c.conditions = append(c.conditions, cond)
	return nil
}

func (c *conditionStore) find(condType string) *metav1.Condition {
	for i := range c.conditions {
		if c.conditions[i].Type == condType {
			return &c.conditions[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// EnsurePodGroups — orchestration around the PodGroup primitives plus
// the GangSchedulingUnavailable condition.
// ---------------------------------------------------------------------

// TestEnsurePodGroups_SinglePodNoPodGroupConditionFalse — single-pod
// Component: no PodGroup, condition=False (not required).
func TestEnsurePodGroups_SinglePodNoPodGroupConditionFalse(t *testing.T) {
	owner := newOwner("prod", "llama")
	c := newGangClient(t, owner)
	input, store := inputWithConditionStore(owner, false, 0, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, false, 0, 5*time.Minute)

	if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c}, input, plan); err != nil {
		t.Fatalf("EnsurePodGroups: %v", err)
	}

	// No PodGroup created.
	pg := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound (single-pod = no PodGroup), got err=%v pg=%+v", err, pg)
	}

	// Condition=False, reason=GangSchedulingAvailable.
	assertCondition(t, store, metav1.ConditionFalse, string(workload.ReasonGangSchedulingAvailable))
}

// TestEnsurePodGroups_MultiPodCRDAvailableCreatesGang — multi-pod
// Component WITH CRD available: PodGroup created, condition=False
// (gang available, not degraded).
func TestEnsurePodGroups_MultiPodCRDAvailableCreatesGang(t *testing.T) {
	owner := newOwner("prod", "llama")
	c := newGangClient(t, owner)
	input, store := inputWithConditionStore(owner, true, 2, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 2, 5*time.Minute)

	if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c}, input, plan); err != nil {
		t.Fatalf("EnsurePodGroups: %v", err)
	}

	// PodGroup created for instance 0 with minMember=3 (1 leader + 2 workers).
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg); err != nil {
		t.Fatalf("expected PodGroup created, got %v", err)
	}
	if pg.Spec.MinMember != 3 {
		t.Errorf("MinMember: got %d want 3", pg.Spec.MinMember)
	}

	// Condition=False (gang available, not degraded).
	assertCondition(t, store, metav1.ConditionFalse, string(workload.ReasonGangSchedulingAvailable))
}

func TestEnsurePodGroups_HoldsLivePodTopologyUntilGroupIsEmpty(t *testing.T) {
	owner := newOwner("prod", "llama")
	const oldTopology = "topology.example.com/old"
	const newTopology = "topology.example.com/new"
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 0)
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pgName,
			Namespace: "prod",
		},
		Spec: schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	worker := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "llama-engine-0-worker-0",
			Namespace: "prod",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "llama",
				constants.OMEComponentLabel:           string(workload.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                "0",
				query.LabelRunner:                     "worker",
				query.LabelPodGroup:                   pgName,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
			Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					TopologyKey: oldTopology,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						constants.InferenceServicePodLabelKey: "llama",
						constants.OMEComponentLabel:           string(workload.ComponentEngine),
						query.LabelInstanceIdx:                "0",
						query.LabelRunner:                     "leader",
					}},
				}},
			}},
		},
	}
	c := newGangClient(t, owner, existing, worker)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)
	plan.TopologyKey = newTopology

	effective, err := EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: c}, input, plan)
	if err != nil {
		t.Fatalf("EnsurePodGroups with live old-topology pod: %v", err)
	}
	if effective[0] != oldTopology {
		t.Fatalf("effective topology for partial active gang: got %q want %q", effective[0], oldTopology)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(existing), got); err != nil {
		t.Fatalf("get held PodGroup: %v", err)
	}
	if got.Annotations[query.AnnotationTopologyKey] != oldTopology {
		t.Fatalf("live PodGroup topology changed under immutable pod: got %q want %q",
			got.Annotations[query.AnnotationTopologyKey], oldTopology)
	}

	if err := c.Delete(context.Background(), worker); err != nil {
		t.Fatalf("delete old-topology pod: %v", err)
	}
	effective, err = EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: c}, input, plan)
	if err != nil {
		t.Fatalf("EnsurePodGroups after group empty: %v", err)
	}
	if effective[0] != newTopology {
		t.Fatalf("effective topology for empty group: got %q want %q", effective[0], newTopology)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(existing), got); err != nil {
		t.Fatalf("get advanced PodGroup: %v", err)
	}
	if got.Annotations[query.AnnotationTopologyKey] != newTopology {
		t.Fatalf("empty PodGroup topology did not advance: got %q want %q",
			got.Annotations[query.AnnotationTopologyKey], newTopology)
	}
}

func TestEnsurePodGroups_UpgradeAnnotatesLegacyTPUDerivedTopology(t *testing.T) {
	owner := newOwner("prod", "llama")
	const legacyTopology = "cloud.google.com/gke-tpu-partition-2x2x1-id"
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 0)
	timeout := int32(300)
	legacyPG := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pgName, Namespace: "prod"},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:              2,
			ScheduleTimeoutSeconds: &timeout,
		},
	}
	worker := topologyWorkerPod("prod", "llama", workload.ComponentEngine, 0, pgName, legacyTopology)
	c := newGangClient(t, owner, legacyPG, worker)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)

	effective, err := EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: c}, input, plan)
	if err != nil {
		t.Fatalf("EnsurePodGroupsWithTopology: %v", err)
	}
	if effective[0] != legacyTopology {
		t.Fatalf("legacy TPU topology: got %q want %q", effective[0], legacyTopology)
	}
	got := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(legacyPG), got); err != nil {
		t.Fatalf("get upgraded PodGroup: %v", err)
	}
	if got.Annotations[query.AnnotationTopologyKey] != legacyTopology {
		t.Fatalf("upgraded PodGroup annotation: got %#v", got.Annotations)
	}
}

func TestEnsurePodGroups_UsesAPIReaderForTopologySafety(t *testing.T) {
	owner := newOwner("prod", "llama")
	const oldTopology = "topology.example.com/live-old"
	const newTopology = "topology.example.com/desired-new"
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 0)
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pgName, Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	worker := topologyWorkerPod("prod", "llama", workload.ComponentEngine, 0, pgName, oldTopology)

	// The cached writer has not observed the worker yet; the live reader has.
	// Reconciliation must retain the immutable live key rather than advance to
	// the desired key visible through the stale cache.
	cached := newGangClient(t, owner, existing)
	live := newGangClient(t, owner, existing.DeepCopy(), worker)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)
	plan.TopologyKey = newTopology

	effective, err := EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: cached, APIReader: live}, input, plan)
	if err != nil {
		t.Fatalf("EnsurePodGroupsWithTopology: %v", err)
	}
	if effective[0] != oldTopology {
		t.Fatalf("effective topology from live reader: got %q want %q", effective[0], oldTopology)
	}
}

func TestEnsurePodGroups_LeaderOnlyTrustsExistingPodGroupTopology(t *testing.T) {
	owner := newOwner("prod", "llama")
	const heldTopology = "topology.example.com/held"
	const desiredTopology = "topology.example.com/new"
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 0)
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pgName,
			Namespace:   "prod",
			Annotations: map[string]string{query.AnnotationTopologyKey: heldTopology},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	leader := topologyLeaderPod("prod", "llama", workload.ComponentEngine, 0, pgName)
	c := newGangClient(t, owner, existing, leader)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)
	plan.TopologyKey = desiredTopology

	effective, err := EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: c}, input, plan)
	if err != nil {
		t.Fatalf("EnsurePodGroupsWithTopology: %v", err)
	}
	if effective[0] != heldTopology {
		t.Fatalf("leader-only gang topology: got %q want held %q", effective[0], heldTopology)
	}
}

func TestEnsurePodGroups_CRDMissingPreservesLiveTopology(t *testing.T) {
	owner := newOwner("prod", "llama")
	const oldTopology = "topology.example.com/live-old"
	const newTopology = "topology.example.com/desired-new"
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 0)
	worker := topologyWorkerPod("prod", "llama", workload.ComponentEngine, 0, pgName, oldTopology)
	cached := newGangClient(t, owner)
	live := newGangClient(t, owner, worker)
	input, _ := inputWithConditionStore(owner, true, 1, false, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)
	plan.TopologyKey = newTopology

	effective, err := EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: cached, APIReader: live}, input, plan)
	if err != nil {
		t.Fatalf("EnsurePodGroupsWithTopology without CRD: %v", err)
	}
	if effective[0] != oldTopology {
		t.Fatalf("missing-CRD effective topology: got %q want live %q", effective[0], oldTopology)
	}
}

func TestEnsurePodGroups_CRDMissingLeaderOnlyWithDesiredTopologyFailsClosed(t *testing.T) {
	owner := newOwner("prod", "llama")
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 0)
	leader := topologyLeaderPod("prod", "llama", workload.ComponentEngine, 0, pgName)
	cached := newGangClient(t, owner)
	live := newGangClient(t, owner, leader)
	input, _ := inputWithConditionStore(owner, true, 1, false, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)
	plan.TopologyKey = "topology.example.com/desired"

	if _, err := EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: cached, APIReader: live}, input, plan); err == nil {
		t.Fatal("leader-only partial create without PodGroup state must hold when desired topology is nonempty")
	}
}

func TestEnsurePodGroups_CRDMissingLeaderOnlyWithoutDesiredTopologyContinues(t *testing.T) {
	owner := newOwner("prod", "llama")
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 0)
	leader := topologyLeaderPod("prod", "llama", workload.ComponentEngine, 0, pgName)
	cached := newGangClient(t, owner)
	live := newGangClient(t, owner, leader)
	input, _ := inputWithConditionStore(owner, true, 1, false, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)

	effective, err := EnsurePodGroupsWithTopology(context.Background(), workload.Deps{Client: cached, APIReader: live}, input, plan)
	if err != nil {
		t.Fatalf("intentional no-topology partial create must continue: %v", err)
	}
	if key, found := effective[0]; !found || key != "" {
		t.Fatalf("intentional no-topology override: got %q found=%v", key, found)
	}
}

func TestEnsureSurgePodGroup_UsesAPIReaderForTopologySafety(t *testing.T) {
	owner := newOwner("prod", "llama")
	const oldTopology = "topology.example.com/live-old"
	const newTopology = "topology.example.com/desired-new"
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 1)
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: pgName, Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	worker := topologyWorkerPod("prod", "llama", workload.ComponentEngine, 1, pgName, oldTopology)
	cached := newGangClient(t, owner, existing)
	live := newGangClient(t, owner, existing.DeepCopy(), worker)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	plan := planFor(workload.ComponentEngine, []int32{1}, true, 1, 5*time.Minute)
	plan.TopologyKey = newTopology
	deps := workload.Deps{Client: cached, APIReader: live}

	key, err := EnsureSurgePodGroup(deps)(context.Background(), input, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("EnsureSurgePodGroup: %v", err)
	}
	if key != oldTopology {
		t.Fatalf("surge effective topology from live reader: got %q want %q", key, oldTopology)
	}
}

func TestEnsureSurgePodGroup_CRDMissingPreservesLiveTopology(t *testing.T) {
	owner := newOwner("prod", "llama")
	const oldTopology = "topology.example.com/live-old"
	const newTopology = "topology.example.com/desired-new"
	pgName := query.PodGroupName("llama", workload.ComponentEngine, 1)
	worker := topologyWorkerPod("prod", "llama", workload.ComponentEngine, 1, pgName, oldTopology)
	cached := newGangClient(t, owner)
	live := newGangClient(t, owner, worker)
	input, _ := inputWithConditionStore(owner, true, 1, false, "")
	plan := planFor(workload.ComponentEngine, []int32{1}, true, 1, 5*time.Minute)
	plan.TopologyKey = newTopology
	deps := workload.Deps{Client: cached, APIReader: live}

	key, err := EnsureSurgePodGroup(deps)(context.Background(), input, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("EnsureSurgePodGroup without CRD: %v", err)
	}
	if key != oldTopology {
		t.Fatalf("missing-CRD surge effective topology: got %q want live %q", key, oldTopology)
	}
}

func topologyWorkerPod(namespace, ownerName string, component workload.ComponentType, index int32, podGroupName, topologyKey string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(ownerName, component, index, "worker", 0),
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: ownerName,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                fmt.Sprintf("%d", index),
				query.LabelRunner:                     "worker",
				query.LabelPodGroup:                   podGroupName,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "test:v1"}},
			Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					TopologyKey: topologyKey,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
						constants.InferenceServicePodLabelKey: ownerName,
						constants.OMEComponentLabel:           string(component),
						query.LabelInstanceIdx:                fmt.Sprintf("%d", index),
						query.LabelRunner:                     "leader",
					}},
				}},
			}},
		},
	}
}

func topologyLeaderPod(namespace, ownerName string, component workload.ComponentType, index int32, podGroupName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(ownerName, component, index, "leader", 0),
			Namespace: namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: ownerName,
				constants.OMEComponentLabel:           string(component),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                fmt.Sprintf("%d", index),
				query.LabelRunner:                     "leader",
				query.LabelPodGroup:                   podGroupName,
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
}

// TestEnsurePodGroups_MultiPodCRDMissingSetsConditionTrueNoPodGroup —
// the degradation path for multi-pod when the CRD is absent.
func TestEnsurePodGroups_MultiPodCRDMissingSetsConditionTrueNoPodGroup(t *testing.T) {
	owner := newOwner("prod", "llama")
	c := newGangClient(t, owner)
	input, store := inputWithConditionStore(owner, true, 2, false, "") // CRD absent — soft-fail
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 2, 5*time.Minute)

	if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c}, input, plan); err != nil {
		t.Fatalf("EnsurePodGroups: %v", err)
	}

	// No PodGroup created.
	pg := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound (CRD missing = no PodGroup), got err=%v pg=%+v", err, pg)
	}

	// Condition=True, reason=PodGroupCRDNotInstalled.
	assertCondition(t, store, metav1.ConditionTrue, string(workload.ReasonPodGroupCRDNotInstalled))
}

// TestEnsurePodGroups_ScaleDownLeavesExtraPodGroupsForFinalizer verifies the
// prerequisite pass never gets ahead of Pod drain. The terminal lifecycle
// owner removes the extra group after the shared Pod observation is empty.
func TestEnsurePodGroups_ScaleDownLeavesExtraPodGroupsForFinalizer(t *testing.T) {
	owner := newOwner("prod", "llama") // plan replicas=1 → index 0 stays, index 1 drops
	// Pre-existing PodGroups for both Instances.
	pg0 := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-0", Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	pg1 := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-1", Namespace: "prod"},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 2},
	}
	c := newGangClient(t, owner, pg0, pg1)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	// Observed: two Ready instances; plan covers only index 0.
	input.ObservedState = workload.WorkloadObservedState{
		InstanceStatuses: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseReady, Incarnation: 1},
			{Index: 1, Phase: workload.InstancePhaseReady, Incarnation: 1},
		},
	}
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)

	if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c}, input, plan); err != nil {
		t.Fatalf("EnsurePodGroups: %v", err)
	}

	// Index 0 PodGroup stays (updated).
	gotPg0 := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, gotPg0); err != nil {
		t.Errorf("expected llama-engine-0 to remain, got %v", err)
	}
	// Index 1 remains until the finalization callback owns deletion.
	gotPg1 := &schedulingv1alpha1.PodGroup{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-1"}, gotPg1)
	if err != nil {
		t.Errorf("expected llama-engine-1 to remain for terminal finalization, got %v", err)
	}
}

// TestEnsurePodGroups_MultiInstanceMultiPodPodGroupsPerInstance —
// 3 multi-pod Instances ⇒ 3 PodGroups, one per Instance.
func TestEnsurePodGroups_MultiInstanceMultiPodPodGroupsPerInstance(t *testing.T) {
	owner := newOwner("prod", "llama")
	c := newGangClient(t, owner)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0, 1, 2}, true, 1, 5*time.Minute)

	if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c}, input, plan); err != nil {
		t.Fatalf("EnsurePodGroups: %v", err)
	}

	for i := int32(0); i < 3; i++ {
		pg := &schedulingv1alpha1.PodGroup{}
		key := client.ObjectKey{Namespace: "prod", Name: podGroupNameFor("llama", workload.ComponentEngine, i)}
		if err := c.Get(context.Background(), key, pg); err != nil {
			t.Errorf("expected PodGroup %s, got %v", key, err)
		}
	}
}

// TestEnsurePodGroups_Idempotent — two calls back-to-back, no errors,
// no condition transition.
func TestEnsurePodGroups_Idempotent(t *testing.T) {
	owner := newOwner("prod", "llama")
	c := newGangClient(t, owner)
	input, _ := inputWithConditionStore(owner, true, 1, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 1, 5*time.Minute)

	for i := 0; i < 2; i++ {
		if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c}, input, plan); err != nil {
			t.Fatalf("EnsurePodGroups pass %d: %v", i, err)
		}
	}
	// PodGroup exists once.
	pg := &schedulingv1alpha1.PodGroup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "llama-engine-0"}, pg); err != nil {
		t.Errorf("PodGroup missing after idempotent ensure: %v", err)
	}
}

// TestEnsurePodGroups_NilClientErrors guards against a wiring slip.
func TestEnsurePodGroups_NilClientErrors(t *testing.T) {
	owner := newOwner("prod", "llama")
	input, _ := inputWithConditionStore(owner, false, 0, false, "")
	if err := EnsurePodGroups(context.Background(), workload.Deps{}, input, workload.ComponentPlan{}); err == nil {
		t.Fatal("expected nil-client error")
	}
}

// ---------------------------------------------------------------------
// patchGangSchedulingCondition — drift correction + idempotency on
// the WriteAggregateCondition callback path.
// ---------------------------------------------------------------------

// TestPatchGangSchedulingCondition_StatusUpdateFlipsCondition writes
// True→False (recovery) and asserts the callback observed it. The
// store stand-in captures the post-write shape the apiserver round-trip
// would have persisted.
func TestPatchGangSchedulingCondition_StatusUpdateFlipsCondition(t *testing.T) {
	owner := newOwner("prod", "llama")
	store := newConditionStore()
	// Pre-seed with True (degraded) so the test exercises the flip.
	_ = store.write(context.Background(), metav1.Condition{
		Type:    string(workload.ConditionGangSchedulingUnavailable),
		Status:  metav1.ConditionTrue,
		Reason:  string(workload.ReasonPodGroupCRDNotInstalled),
		Message: "scheduler-plugins scheduling.x-k8s.io/v1alpha1 PodGroup CRD is not installed; multi-pod Instances may schedule partially",
	})
	input := workload.ReconcileInput{
		OwnerObject:             owner,
		WriteAggregateCondition: store.write,
	}

	// CRD became available between reconciles → condition flips to False.
	if err := patchGangSchedulingCondition(context.Background(), input, true, true); err != nil {
		t.Fatalf("patchGangSchedulingCondition: %v", err)
	}

	got := store.find(string(workload.ConditionGangSchedulingUnavailable))
	if got == nil {
		t.Fatal("GangSchedulingUnavailable condition missing")
	}
	if got.Status != metav1.ConditionFalse {
		t.Errorf("Status: got %s want False (CRD now available)", got.Status)
	}
	if got.Reason != string(workload.ReasonGangSchedulingAvailable) {
		t.Errorf("Reason: got %q want %q", got.Reason, string(workload.ReasonGangSchedulingAvailable))
	}
}

// TestPatchGangSchedulingCondition_ISVCGoneIsSafe — the helper must
// not error when the owner was deleted mid-reconcile. The production
// callback (internalsource.buildWriteAggregateCondition) translates
// apierrors.IsNotFound at Get / Status().Update into a nil return so
// the gang reconciler doesn't escalate. This test verifies the
// workload-side wrapper passes through that nil cleanly.
func TestPatchGangSchedulingCondition_ISVCGoneIsSafe(t *testing.T) {
	owner := newOwner("prod", "llama")
	input := workload.ReconcileInput{
		OwnerObject: owner,
		WriteAggregateCondition: func(_ context.Context, _ metav1.Condition) error {
			// Simulate the NotFound branch the production closure
			// swallows.
			return nil
		},
	}

	// Should not error even though Get would return NotFound in the
	// production closure.
	if err := patchGangSchedulingCondition(context.Background(), input, true, true); err != nil {
		t.Errorf("expected nil error when owner missing, got %v", err)
	}
}

// ---------------------------------------------------------------------
// maybeWarnNoGangScheduler — heuristic warning when the rendered pod
// template's schedulerName is empty or "default-scheduler" while the
// reconciler is creating PodGroup objects (which a stock kube-scheduler
// will silently ignore).
// ---------------------------------------------------------------------

// TestEnsurePodGroups_MaybeNoGangSchedulerWarning_Matrix exercises
// the four scenarios in the spec: empty/default schedulerName +
// multi-pod fires; custom schedulerName + multi-pod is silent;
// single-pod is silent regardless of schedulerName.
func TestEnsurePodGroups_MaybeNoGangSchedulerWarning_Matrix(t *testing.T) {
	tests := []struct {
		name          string
		schedulerName string
		multiPod      bool
		workerSize    int32
		wantFired     bool
	}{
		{
			name:          "empty schedulerName multi-pod fires",
			schedulerName: "",
			multiPod:      true,
			workerSize:    2,
			wantFired:     true,
		},
		{
			name:          "default-scheduler multi-pod fires",
			schedulerName: corev1.DefaultSchedulerName,
			multiPod:      true,
			workerSize:    2,
			wantFired:     true,
		},
		{
			name:          "custom schedulerName multi-pod silent",
			schedulerName: "scheduler-plugins-scheduler",
			multiPod:      true,
			workerSize:    2,
			wantFired:     false,
		},
		{
			name:          "single-pod default-scheduler silent",
			schedulerName: corev1.DefaultSchedulerName,
			multiPod:      false,
			workerSize:    0,
			wantFired:     false,
		},
		{
			name:          "single-pod empty silent",
			schedulerName: "",
			multiPod:      false,
			workerSize:    0,
			wantFired:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Each subtest gets a fresh dedup map so prior subtests
			// can't suppress the next one (different owners would
			// also work, but resetting is simpler and tests the
			// helper).
			resetMaybeNoGangSchedulerSeen()

			owner := newOwner("prod", "llama-"+strings.ReplaceAll(tc.name, " ", "-"))
			c := newGangClient(t, owner)
			rec := record.NewFakeRecorder(8)
			input, _ := inputWithConditionStore(owner, tc.multiPod, tc.workerSize, true, tc.schedulerName)
			plan := planFor(workload.ComponentEngine, []int32{0}, tc.multiPod, tc.workerSize, 5*time.Minute)

			if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, plan); err != nil {
				t.Fatalf("EnsurePodGroups: %v", err)
			}

			events := drainGangSchedulerEvents(rec)
			fired := false
			for _, e := range events {
				if strings.Contains(e, string(workload.EventReasonMaybeNoGangScheduler)) {
					fired = true
					break
				}
			}
			if fired != tc.wantFired {
				t.Errorf("MaybeNoGangScheduler event fired=%v want=%v (events=%v)", fired, tc.wantFired, events)
			}
			if fired {
				// Sanity: the warning must mention the suggested
				// scheduler so the operator has a copy-paste fix.
				var warning string
				for _, e := range events {
					if strings.Contains(e, string(workload.EventReasonMaybeNoGangScheduler)) {
						warning = e
						break
					}
				}
				if !strings.Contains(warning, "scheduler-plugins-scheduler") {
					t.Errorf("warning should suggest scheduler-plugins-scheduler, got %q", warning)
				}
				if !strings.HasPrefix(warning, "Warning ") {
					t.Errorf("warning event should be Warning-type, got %q", warning)
				}
			}
		})
	}
}

// TestEnsurePodGroups_MaybeNoGangScheduler_DedupPerProcess verifies
// that two reconciles for the same (owner, Component) only fire ONE
// MaybeNoGangScheduler event — the dedup map's job.
func TestEnsurePodGroups_MaybeNoGangScheduler_DedupPerProcess(t *testing.T) {
	resetMaybeNoGangSchedulerSeen()

	owner := newOwner("prod", "dedup-llama")
	c := newGangClient(t, owner)
	rec := record.NewFakeRecorder(16)
	// Empty schedulerName — should warn.
	input, _ := inputWithConditionStore(owner, true, 2, true, "")
	plan := planFor(workload.ComponentEngine, []int32{0}, true, 2, 5*time.Minute)

	// Two reconciles back-to-back.
	for i := 0; i < 2; i++ {
		if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, plan); err != nil {
			t.Fatalf("EnsurePodGroups pass %d: %v", i, err)
		}
	}

	events := drainGangSchedulerEvents(rec)
	count := 0
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonMaybeNoGangScheduler)) {
			count++
		}
	}
	if count != 1 {
		t.Errorf("MaybeNoGangScheduler event count: got %d want 1 (events=%v)", count, events)
	}
}

// TestEnsurePodGroups_MaybeNoGangScheduler_DedupSeparateComponents
// verifies the dedup key is (owner, Component) — same owner but a
// different Component must still get a warning. A key collapsed to
// just the owner name would suppress it.
func TestEnsurePodGroups_MaybeNoGangScheduler_DedupSeparateComponents(t *testing.T) {
	resetMaybeNoGangSchedulerSeen()

	owner := newOwner("prod", "multi-comp")
	c := newGangClient(t, owner)
	rec := record.NewFakeRecorder(16)
	emptySpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}}

	// Engine reconcile.
	inputEng, _ := inputWithConditionStore(owner, true, 2, true, "")
	planEng := planFor(workload.ComponentEngine, []int32{0}, true, 2, 5*time.Minute)
	if err := EnsurePodGroups(context.Background(), workload.Deps{Client: c, Recorder: rec}, inputEng, planEng); err != nil {
		t.Fatalf("engine EnsurePodGroups: %v", err)
	}

	// Decoder reconcile — directly invoke the helper with the
	// Decoder component (no Decoder spec on the owner, so we test
	// the helper directly rather than rebuilding a Decoder plan).
	maybeWarnNoGangScheduler(rec, inputEng, workload.ComponentDecoder, emptySpec, emptySpec)

	events := drainGangSchedulerEvents(rec)
	count := 0
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonMaybeNoGangScheduler)) {
			count++
		}
	}
	if count != 2 {
		t.Errorf("MaybeNoGangScheduler event count (engine+decoder): got %d want 2 (events=%v)", count, events)
	}
}

// TestMaybeWarnNoGangScheduler_NilSafety guards the helper against
// the test-fixture pattern where Recorder / owner / PodSpec may be
// nil.
func TestMaybeWarnNoGangScheduler_NilSafety(t *testing.T) {
	resetMaybeNoGangSchedulerSeen()

	// nil owner: no-op (early return).
	rec := record.NewFakeRecorder(2)
	input := workload.ReconcileInput{}
	maybeWarnNoGangScheduler(rec, input, workload.ComponentEngine, nil, nil)
	if len(rec.Events) != 0 {
		t.Errorf("nil owner should be no-op, got %d events", len(rec.Events))
	}

	// nil PodSpec + nil WorkerPodSpec + non-nil owner: schedulerName
	// is "" so the warning DOES fire (caller's responsibility to gate
	// at EnsurePodGroups, which already does — single-pod / no-podgroup
	// paths never reach this helper). Verifies the helper itself
	// handles nil specs gracefully.
	rec2 := record.NewFakeRecorder(2)
	owner := newOwner("ns", "x")
	input2 := workload.ReconcileInput{OwnerObject: owner, EventTarget: owner}
	maybeWarnNoGangScheduler(rec2, input2, workload.ComponentEngine, nil, nil)
	if len(rec2.Events) != 1 {
		t.Errorf("nil specs + valid owner should fire once, got %d events", len(rec2.Events))
	}

	// nil Recorder + valid rest: helper must not panic. Dedup map is
	// still updated though, so reset to keep test isolation clean.
	resetMaybeNoGangSchedulerSeen()
	input3 := workload.ReconcileInput{OwnerObject: owner, EventTarget: owner}
	maybeWarnNoGangScheduler(nil, input3, workload.ComponentEngine, nil, nil) // no panic
}

// TestEffectiveSchedulerName_LeaderWinsThenWorker covers the
// precedence: leader's schedulerName takes over worker's; both empty
// → empty. Pure-helper test.
func TestEffectiveSchedulerName_LeaderWinsThenWorker(t *testing.T) {
	tests := []struct {
		name   string
		leader *corev1.PodSpec
		worker *corev1.PodSpec
		want   string
	}{
		{"both nil", nil, nil, ""},
		{"both empty", &corev1.PodSpec{}, &corev1.PodSpec{}, ""},
		{"leader set", &corev1.PodSpec{SchedulerName: "custom"}, &corev1.PodSpec{}, "custom"},
		{"only worker set", &corev1.PodSpec{}, &corev1.PodSpec{SchedulerName: "worker-sched"}, "worker-sched"},
		{"leader wins", &corev1.PodSpec{SchedulerName: "leader"}, &corev1.PodSpec{SchedulerName: "worker"}, "leader"},
		{"nil leader, worker set", nil, &corev1.PodSpec{SchedulerName: "worker"}, "worker"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveSchedulerName(tc.leader, tc.worker); got != tc.want {
				t.Errorf("effectiveSchedulerName(%+v, %+v) = %q want %q", tc.leader, tc.worker, got, tc.want)
			}
		})
	}
}

// drainGangSchedulerEvents returns all events currently buffered on
// rec. Equivalent to coordination/sequential_event_test.go's
// drainEvents — duplicated here to keep this test file's import
// surface narrow.
func drainGangSchedulerEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// assertCondition asserts the condition store recorded the named
// (status, reason) pair for workload.ConditionGangSchedulingUnavailable.
func assertCondition(t *testing.T, store *conditionStore, wantStatus metav1.ConditionStatus, wantReason string) {
	t.Helper()
	cond := store.find(string(workload.ConditionGangSchedulingUnavailable))
	if cond == nil {
		t.Fatalf("GangSchedulingUnavailable condition missing")
	}
	if cond.Status != wantStatus {
		t.Errorf("Status: got %s want %s", cond.Status, wantStatus)
	}
	if cond.Reason != wantReason {
		t.Errorf("Reason: got %q want %q", cond.Reason, wantReason)
	}
}

// podGroupNameFor mirrors the production naming convention used by
// the gang reconciler (`<owner>-<component>-<idx>`) without importing
// workload/query which takes a v1beta1.ComponentType parameter. The
// v1beta1.ComponentType here is the test-side convenience for
// constructing fixtures; production code uses the workload-typed
// component throughout.
func podGroupNameFor(owner string, component workload.ComponentType, idx int32) string {
	return owner + "-" + string(component) + "-" + itoaSmall(int(idx))
}

func itoaSmall(n int) string {
	if n == 0 {
		return "0"
	}
	digits := "0123456789"
	var b []byte
	for n > 0 {
		b = append([]byte{digits[n%10]}, b...)
		n /= 10
	}
	return string(b)
}
