package ops

// A pod the kubelet refuses to admit is dead on arrival: phase Failed,
// no container ever started, and its stable name still occupied. The
// Create and Restart passes recycle it and rebuild the name. These
// tests pin the same handling for every site that places a REPLACEMENT
// pod — the per-pod surge, the recreate's Phase B, the gang surge, and
// the migration surge — so a rejection frees its name instead of
// holding the step to the operation deadline. The recycle is
// environment-caused: the kubelet's reason reaches the evidence and no
// revision is blamed.

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const siteAdmissionReason = "UnexpectedAdmissionError"

// rejectAtAdmission marks a pod as refused by the kubelet after
// scheduling: phase Failed carrying only a pod-level reason.
func rejectAtAdmission(pod *corev1.Pod, reason string) *corev1.Pod {
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = reason
	pod.Status.Message = "Pod was rejected: Node didn't have enough resource"
	pod.Status.Conditions = nil
	pod.Status.ContainerStatuses = nil
	return pod
}

func sitePodGone(t *testing.T, c client.Client, pod *corev1.Pod) bool {
	t.Helper()
	fetched := &corev1.Pod{}
	err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fetched)
	return err != nil || fetched.DeletionTimestamp != nil
}

// TestQueryPodAdmissionRejected pins the reason family: the kubelet's
// catch-all plus every OutOf<resource> refusal, and nothing else. A pod
// that merely reached a terminal phase is not an admission rejection.
func TestQueryPodAdmissionRejected(t *testing.T) {
	rejected := []string{"UnexpectedAdmissionError", "OutOfcpu", "OutOfmemory", "OutOfpods", "OutOfnvidia.com/gpu"}
	for _, reason := range rejected {
		pod := rejectAtAdmission(&corev1.Pod{}, reason)
		if got, ok := query.PodAdmissionRejected(pod); !ok || got != reason {
			t.Errorf("PodAdmissionRejected(%s): got (%q, %v) want (%q, true)", reason, got, ok, reason)
		}
	}
	notRejected := []string{
		"Evicted",            // kubelet pressure, not admission
		"OutOf",              // no resource named
		"OutOfOrderDelivery", // an upper-case suffix is not a resource name
		"Shutdown",
	}
	for _, reason := range notRejected {
		pod := rejectAtAdmission(&corev1.Pod{}, reason)
		if _, ok := query.PodAdmissionRejected(pod); ok {
			t.Errorf("PodAdmissionRejected(%s): got true want false", reason)
		}
	}
	running := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, Reason: "OutOfcpu"}}
	if _, ok := query.PodAdmissionRejected(running); ok {
		t.Errorf("running pod: got true want false")
	}
	if _, ok := query.PodAdmissionRejected(nil); ok {
		t.Errorf("nil pod: got true want false")
	}
}

// TestSurgeCreate_AdmissionRejectedTargetIsRecycled: the surge
// replacement is refused at admission. The step must delete it and
// rebuild the name instead of waiting out the operation deadline, and
// the kubelet's reason must survive on the evidence.
func TestSurgeCreate_AdmissionRejectedTargetIsRecycled(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := makeCR(t, base, isvc, "llama-70b-engine-rev-abc12345")
	sourcePod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := base.Create(context.Background(), sourcePod); err != nil {
		t.Fatalf("seed source pod: %v", err)
	}
	// The surge slot holds a pod the kubelet refused.
	target := rejectAtAdmission(surgePodAtOrdinal(isvc, 0, 1, 1, false, false), siteAdmissionReason)
	target.Labels[query.LabelRevisionHash] = query.RevisionOf(tcr).Hash()
	if err := base.Create(context.Background(), target); err != nil {
		t.Fatalf("seed rejected surge pod: %v", err)
	}
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := surgePlan()

	done, err := surgeUpdate(context.Background(), legacyTestDeps(base), input, plan, plan.Instances[0], tcr,
		[]*corev1.Pod{sourcePod, target})
	if err != nil {
		t.Fatalf("surgeUpdate: %v", err)
	}
	if done {
		t.Fatal("surgeUpdate reported done while recycling the rejected target")
	}
	if !sitePodGone(t, base, target) {
		t.Fatalf("rejected surge target %s must be deleted so the name can be rebuilt", target.Name)
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("instance 0: got Failed want the surge still in flight")
	}
	if s.LastFailure == nil || s.LastFailure.Reason != siteAdmissionReason {
		t.Errorf("LastFailure: got %+v want the kubelet's admission reason", s.LastFailure)
	}
}

// TestRecreatePhaseB_AdmissionRejectedTargetIsRecycled: the recreate's
// replacement is refused at admission. Phase B recycles the name rather
// than polling the readiness gate to the deadline.
func TestRecreatePhaseB_AdmissionRejectedTargetIsRecycled(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	isvc.Spec.Engine.ComponentExtensionSpec.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategyRecreatePod},
	}
	targetSpec := legacyTargetSpecImage("example.com/app:v2")
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := legacyEnsureTargetCR(t, base, isvc, targetSpec)
	// Phase A already drained the previous incarnation; the rebuild at
	// the bumped incarnation is what the kubelet refused.
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 1,
		Phase:       v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: tcr.Name,
		},
	}
	if err := base.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed Phase B status: %v", err)
	}
	dead := rejectAtAdmission(legacyPodAtIncarnation(isvc, 0, 2, false, false), "OutOfnvidia.com/gpu")
	if err := base.Create(context.Background(), dead); err != nil {
		t.Fatalf("seed rejected recreate pod: %v", err)
	}
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := legacyComponentPlan(workload.UpdateStrategyRecreatePod, nil)

	done, err := Update(context.Background(), legacyTestDeps(base), input, plan, plan.Instances[0], tcr, targetSpec)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if done {
		t.Fatal("Update reported done while recycling the rejected replacement")
	}
	if !sitePodGone(t, base, dead) {
		t.Fatalf("rejected replacement %s must be deleted so the name can be rebuilt", dead.Name)
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.LastFailure == nil || s.LastFailure.Reason != "OutOfnvidia.com/gpu" {
		t.Errorf("LastFailure: got %+v want the kubelet's OutOf reason", s.LastFailure)
	}
}

// gangRejectionFixture is a gang surge mid-flight whose replacement
// leader the kubelet refused: the source row owns the Update operation
// and points at the surge index, while the pods live under the surge.
type gangRejectionFixture struct {
	isvcName   string
	namespace  string
	surgeIndex int32
	input      workload.ReconcileInput
	deps       workload.Deps
	plan       workload.ComponentPlan
	target     *appsv1.ControllerRevision
	store      *terminalMutationStore
	client     client.Client
	dead       *corev1.Pod
}

func newGangRejectionFixture(t *testing.T, reason string) *gangRejectionFixture {
	t.Helper()
	const isvcName, namespace = "gang-reject", "test-ns"
	surgeIndex := int32(2)
	revision := "gang-reject-engine-newrev"
	source := workload.InstanceStatus{
		Index:           0,
		Incarnation:     3,
		Phase:           workload.InstancePhaseUpdating,
		RunningRevision: "gang-reject-engine-oldrev",
		TargetRevision:  revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-0",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			TargetRevision: revision,
			SurgeIndex:     &surgeIndex,
		},
	}
	marker := workload.InstanceStatus{
		Index:          surgeIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: revision,
		Operation: &workload.InstanceOperation{
			ID:             "gang-update-target-2",
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTarget,
			TargetRevision: revision,
		},
	}
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]workload.InstanceStatus{
			source.Index: cloneTerminalStatus(source),
			surgeIndex:   cloneTerminalStatus(marker),
		},
	}
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(workload.ComponentEngine),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	input := workload.ReconcileInput{
		OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Key: workload.Key{
			Namespace:      namespace,
			OwnerName:      isvcName,
			Component:      workload.ComponentEngine,
			SelectorLabels: selector,
		},
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{cloneTerminalStatus(source), cloneTerminalStatus(marker)},
		},
		DesiredSpec: workload.WorkloadDesiredSpec{
			PodSpec: legacyTargetSpecImage("example.com/app:v2"),
		},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workload.InstanceStatus) bool) error {
			return store.apply(context.Background(),
				[]workload.InstanceMutation{{Index: idx, Mutate: mutate}}, "", nil)
		},
		FinalizeInstanceResources:            func(context.Context, int32) (bool, error) { return true, nil },
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategySurgeThenDrain)

	labels := map[string]string{query.LabelInstanceIdx: "2"}
	for k, v := range selector {
		labels[k] = v
	}
	dead := rejectAtAdmission(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      query.PodName(isvcName, workload.ComponentEngine, surgeIndex, plan.Instances[0].Runners[0].Name, 0),
		Namespace: namespace,
		Labels:    labels,
	}}, reason)
	base := legacyNewFakeClient(t, dead)
	return &gangRejectionFixture{
		isvcName: isvcName, namespace: namespace, surgeIndex: surgeIndex,
		input: input, deps: legacyTestDeps(base), plan: plan,
		target: &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}},
		store:  store, client: base, dead: dead,
	}
}

// TestGangSurgeCreate_AdmissionRejectedTargetIsRecycled: one member of
// the replacement gang is refused at admission. The recycle runs under
// the SURGE index (where the pods and their expectations live) while the
// bookkeeping lands on the SOURCE row, which owns the operation.
func TestGangSurgeCreate_AdmissionRejectedTargetIsRecycled(t *testing.T) {
	legacyResetExpectations(t)
	f := newGangRejectionFixture(t, "OutOfmemory")

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while recycling a rejected gang member")
	}
	if !sitePodGone(t, f.client, f.dead) {
		t.Fatalf("rejected gang member %s must be deleted so the name can be rebuilt", f.dead.Name)
	}
	src := f.store.statuses[0]
	if src.LastFailure == nil || src.LastFailure.Reason != "OutOfmemory" {
		t.Errorf("source LastFailure: got %+v want the kubelet's OutOf reason", src.LastFailure)
	}
	if src.Phase == workload.InstancePhaseFailed {
		t.Errorf("source Phase: got Failed want the surge still in flight")
	}
}

// TestMigrateSurge_AdmissionRejectedTargetIsRecycled: the migration's
// surge pod is refused at admission. Without a recycle the name stays
// occupied by a dead object and the request can only expire at its
// record deadline; the recycle frees it so the surge is placed again
// inside the window, with the kubelet's reason on the source's evidence.
func TestMigrateSurge_AdmissionRejectedTargetIsRecycled(t *testing.T) {
	f := newSinglePodMigFixture(t)
	const uuid = "mig-surge-admission-rejected"
	f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}

	// Drive the accept + surge-create passes so the surge pod exists.
	f.pass(t, uuid)
	f.pass(t, uuid)
	var surge *corev1.Pod
	for _, pod := range f.listPods(t) {
		if pod.Labels[query.LabelInstanceIdx] != "0" {
			surge = pod
		}
	}
	if surge == nil {
		t.Fatalf("no surge pod after the create pass; pods=%v", f.listPods(t))
	}

	// The kubelet refuses the surge on the node the scheduler picked.
	rejectAtAdmission(surge, siteAdmissionReason)
	if err := f.c.Status().Update(context.Background(), surge); err != nil {
		t.Fatalf("mark surge rejected: %v", err)
	}

	if _, _, err := f.passResult(t, uuid); err != nil {
		t.Fatalf("Migrate pass over the rejected surge: %v", err)
	}
	if !sitePodGone(t, f.c, surge) {
		t.Fatalf("rejected migration surge pod %s must be deleted so the name can be rebuilt", surge.Name)
	}
	if rec := f.record(t, uuid); rec.Phase == workload.MigrationPhaseFailed {
		t.Errorf("record phase: got Failed want the migration still in flight")
	}
	source := f.getIR(t).Status.InstanceStatuses[0]
	if source.LastFailure == nil || source.LastFailure.Reason != siteAdmissionReason {
		t.Errorf("source LastFailure: got %+v want the kubelet's admission reason", source.LastFailure)
	}
}

// TestGangSurgeCreate_RecycleWaitsOnTheSurgeBucketExpectations: the gang
// recycle takes two indices because the pods and the operation live on
// different rows. The expectations gate must consult the SURGE bucket,
// where the pods actually are — gating on the source would let the
// recycle re-issue a delete the watch has not reported yet and drive a
// second, duplicate teardown.
func TestGangSurgeCreate_RecycleWaitsOnTheSurgeBucketExpectations(t *testing.T) {
	legacyResetExpectations(t)
	f := newGangRejectionFixture(t, "OutOfmemory")

	// The source bucket is settled; the surge bucket has a delete the
	// watch has not observed yet.
	workload.DefaultExpectations.ExpectDeletes(f.namespace, f.isvcName, workload.ComponentEngine, f.surgeIndex, 1)
	if workload.DefaultExpectations.Satisfied(f.namespace, f.isvcName, workload.ComponentEngine, f.surgeIndex) {
		t.Fatalf("surge bucket must report an outstanding delete")
	}
	if !workload.DefaultExpectations.Satisfied(f.namespace, f.isvcName, workload.ComponentEngine, 0) {
		t.Fatalf("source bucket must be settled for this test to mean anything")
	}

	done, err := gangSurgeUpdate(context.Background(), f.deps, f.input, f.plan, f.plan.Instances[0], f.target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while the surge bucket has work in flight")
	}
	if sitePodGone(t, f.client, f.dead) {
		t.Errorf("rejected member %s was deleted again while its prior delete is unobserved", f.dead.Name)
	}
	if src := f.store.statuses[0]; src.LastFailure != nil {
		t.Errorf("source LastFailure: got %+v want none (the recycle never ran)", src.LastFailure)
	}
}
