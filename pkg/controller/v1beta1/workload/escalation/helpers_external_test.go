package escalation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Fixtures the escalation tests share with the reconcile root's: the
// fake scheme, the minimal input and plan, and the atomic mutation store
// the batched status writes land in.

// actionKinds projects the Decision's action kinds in order.
func actionKinds(d workload.Decision) []workload.ActionKind {
	kinds := make([]workload.ActionKind, 0, len(d.Actions))
	for _, a := range d.Actions {
		kinds = append(kinds, a.Kind)
	}
	return kinds
}

// findAction returns the first action of the given kind, or nil.
func findAction(d workload.Decision, kind workload.ActionKind) *workload.PlannedAction {
	for i := range d.Actions {
		if d.Actions[i].Kind == kind {
			return &d.Actions[i]
		}
	}
	return nil
}

// forbidMutations wires every mutation callback on input to a t.Error
// recorder, so any write attempted during Plan fails the test.
func forbidMutations(t *testing.T, input *workloadtypes.ReconcileInput) {
	t.Helper()
	input.MutateInstance = func(_ context.Context, idx int32, _ func(*workloadtypes.InstanceStatus) bool) error {
		t.Errorf("Plan must not call MutateInstance (idx=%d)", idx)
		return nil
	}
	input.ApplyInstanceMutations = func(_ context.Context, muts []workloadtypes.InstanceMutation) error {
		t.Errorf("Plan must not call ApplyInstanceMutations (%d mutations)", len(muts))
		return nil
	}
	input.RemoveInstance = func(_ context.Context, idx int32) (bool, error) {
		t.Errorf("Plan must not call RemoveInstance (idx=%d)", idx)
		return false, nil
	}
	input.WriteAggregateCondition = func(_ context.Context, cond metav1.Condition) error {
		t.Errorf("Plan must not call WriteAggregateCondition (%s)", cond.Type)
		return nil
	}
	input.WarnInstanceFailed = func(idx int32, _, _ string) {
		t.Errorf("Plan must not call WarnInstanceFailed (idx=%d)", idx)
	}
	input.WarnRetryHeld = func(rev string, _ int32, _ string) {
		t.Errorf("Plan must not call WarnRetryHeld (%s)", rev)
	}
	input.MutateMigration = func(_ context.Context, uuid string, _ func(*workloadtypes.MigrationRecord) bool) error {
		t.Errorf("Plan must not call MutateMigration (%s)", uuid)
		return nil
	}
	input.AppendMigration = func(_ context.Context, rec workloadtypes.MigrationRecord) error {
		t.Errorf("Plan must not call AppendMigration (%s)", rec.RequestUUID)
		return nil
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, _ func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		t.Errorf("Plan must not call MutateRetryBlock (%s)", rev)
		return nil
	}
	input.UpdateGate = func(_ string, _ workloadtypes.UpdateStrategyType, _, _ int32) (bool, workloadtypes.RolloutHoldGate, string) {
		t.Error("Plan must not consult UpdateGate (Execute owns the consult)")
		return true, "", ""
	}
}

// planSnapshot builds a snapshot over the given pods the way a reconcile
// does: through a client that holds them, so Plan lists and buckets them
// by their labels.
func planSnapshot(input workloadtypes.ReconcileInput, byIdx map[int32][]*corev1.Pod) *workload.ObservedSnapshot {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	var objs []client.Object
	for _, pods := range byIdx {
		for _, p := range pods {
			objs = append(objs, p)
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	deps := workloadtypes.Deps{Client: c, APIReader: c}
	return workload.NewObservedSnapshot(deps, input, input.Key.Component, input.ObservedState.InstanceStatuses)
}

// planTargetOrFail runs Plan against a target ControllerRevision and
// fails the test on error.
func planTargetOrFail(t *testing.T, input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, target *appsv1.ControllerRevision, snapshot *workload.ObservedSnapshot) workload.Decision {
	t.Helper()
	d, err := workload.Plan(context.Background(), input, plan, target, snapshot)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	return d
}

// updateTarget is the target ControllerRevision the update-selection
// tests roll toward.
func updateTarget() *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}
}

// enginePod fabricates a single-pod "default" engine pod at ordinal 0
// with the labels Render stamps (managed-by + instance-idx + ordinal +
// component + isvc), so query selectors and instance-index filters
// recognize it.
func enginePod(isvc, ns string, idx int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      query.PodName(isvc, workloadtypes.ComponentEngine, idx, "default", 0),
			Namespace: ns,
			UID:       types.UID(fmt.Sprintf("%s-engine-%d-uid", isvc, idx)),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
				query.LabelInstanceIdx:                fmt.Sprintf("%d", idx),
				query.LabelInstanceIncarnation:        "1",
				query.LabelRunner:                     "default",
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelPodOrdinal:                 "0",
				query.LabelRevisionHash:               "priorrev",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "test:v1"}}},
	}
}

// makeScheme builds the runtime.Scheme the fake client needs.
func makeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add discoveryv1: %v", err)
	}
	return scheme
}

func minimalInput(t *testing.T) workloadtypes.ReconcileInput {
	t.Helper()
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "llama-70b", Namespace: "prod", UID: "uid-1",
	}}
	in := workloadtypes.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: workloadtypes.Key{
			Namespace: "prod",
			Component: workloadtypes.ComponentEngine,
			OwnerName: "llama-70b",
		},
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
		Requeue:                  testRequeueIntervals,
		DesiredSpec: workloadtypes.WorkloadDesiredSpec{
			Replicas: 1,
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{
				{Name: "main", Image: "test:v1"},
			}},
		},
	}
	stubInputCallbacks(&in)
	return in
}

// minimalPlan returns a ComponentPlan covering a single Instance at
// index 0 with a single-pod "default" Runner.
func minimalPlan() workloadtypes.ComponentPlan {
	return workloadtypes.ComponentPlan{
		Component: workloadtypes.ComponentEngine,
		Replicas:  1,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{
				{Name: "default", Size: 1},
			}},
		},
	}
}

// stubInputCallbacks fills the callback fields that
// workload.ReconcileInput panics on if left nil. Tests only set
// behavior on the callbacks they care about; the rest stay no-ops.
func stubInputCallbacks(input *workloadtypes.ReconcileInput) {
	if input.MutateInstance == nil {
		input.MutateInstance = func(_ context.Context, _ int32, _ func(*workloadtypes.InstanceStatus) bool) error {
			return nil
		}
	}
	if input.RemoveInstance == nil {
		input.RemoveInstance = func(_ context.Context, _ int32) (bool, error) { return false, nil }
	}
	if input.WriteAggregateCondition == nil {
		input.WriteAggregateCondition = func(_ context.Context, _ metav1.Condition) error { return nil }
	}
	if input.WarnInstanceFailed == nil {
		input.WarnInstanceFailed = func(_ int32, _, _ string) {}
	}
	if input.MutateMigration == nil {
		input.MutateMigration = func(_ context.Context, _ string, _ func(*workloadtypes.MigrationRecord) bool) error {
			return nil
		}
	}
}

// testRequeueIntervals is the dispatcher cadence these tests run under,
// standing in for the operator config a deployed chart supplies. A test
// pinning the unconfigured path clears ReconcileInput.Requeue.
var testRequeueIntervals = workloadtypes.RequeueIntervals{
	Operation: 5 * time.Second,
	Gate:      3 * time.Second,
}

// minimalInput builds a ReconcileInput with the bare minimum fields
// the dispatcher reads. Tests pad observed state + plan as needed.
const testScaleDownRequeueInterval = 37 * time.Second

// servingPod builds a pod that is ContainersReady AND carries the
// ome.io/serving readiness gate — a pod in the load-balancer rotation.
func servingPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: "ome.io/serving", Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// escalationFixture wires a ReconcileInput whose MutateInstance applies
// the callback to the local store and whose WarnInstanceFailed /
// MutateRetryBlock record their calls.
type escalationRecorder struct {
	store  []workloadtypes.InstanceStatus
	warns  []string
	blocks []workloadtypes.RetryBlock
}

func escalationFixture(insts []workloadtypes.InstanceStatus) (workloadtypes.ReconcileInput, *escalationRecorder) {
	rec := &escalationRecorder{store: append([]workloadtypes.InstanceStatus(nil), insts...)}
	input := workloadtypes.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			for i := range rec.store {
				if rec.store[i].Index == idx {
					mutate(&rec.store[i])
					return nil
				}
			}
			return nil
		},
		MutateRetryBlock: func(_ context.Context, rev string, mutate func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			b := workloadtypes.RetryBlock{TargetRevision: rev}
			mutate(&b)
			rec.blocks = append(rec.blocks, b)
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, reason string) {
			rec.warns = append(rec.warns, reason)
		},
	}
	input.ObservedState.InstanceStatuses = append([]workloadtypes.InstanceStatus(nil), insts...)
	return input, rec
}

// incarnationPod builds a Pending pod of Instance 0 at the given
// incarnation, labeled the way the live lister selects.
func incarnationPod(name string, incarnation int64, created time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			CreationTimestamp: metav1.NewTime(created.Add(-time.Hour)),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "own",
				constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                "0",
				query.LabelInstanceIncarnation:        strconv.FormatInt(incarnation, 10),
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
			}},
		},
	}
}

// terminating marks the pod as deleting: a deletion stamp held open by a
// finalizer, the way a pod reads between its delete and its removal. The
// stamp is the pod's own deletion deadline, the request plus its grace,
// and sits ahead of the pass unless the pod already carries one.
func terminating(pod *corev1.Pod) *corev1.Pod {
	if pod.DeletionTimestamp == nil {
		deadline := metav1.NewTime(time.Now().Add(time.Hour))
		pod.DeletionTimestamp = &deadline
	}
	pod.Finalizers = []string{"example.com/termination"}
	return pod
}

// terminatingOverdue marks the pod as deleting past its own deletion
// deadline: the kubelet behind it stopped before it acknowledged the
// delete, so only that kubelet, a finalizer owner or the force-delete
// sweep ever removes the object. The finalizer stands in for the missing
// acknowledgement, which the fake apiserver cannot hold a pod on; the
// stamp reads the deadline, never the finalizer.
func terminatingOverdue(pod *corev1.Pod) *corev1.Pod {
	deadline := metav1.NewTime(time.Now().Add(-time.Hour))
	pod.DeletionTimestamp = &deadline
	return terminating(pod)
}

// heldOutOfRotationBy is a pod whose serving gate the named writer holds
// off: containers ready, out of rotation, not deleting.
func heldOutOfRotationBy(name, userAgent, key string) *corev1.Pod {
	writers, err := json.Marshal([]podreadiness.Message{{UserAgent: userAgent, Key: key}})
	if err != nil {
		panic(err)
	}
	pod := servingPod(name)
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: podreadiness.ConditionType, Status: corev1.ConditionFalse, Message: string(writers)},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}
	return pod
}

// wedgedButGatedPod is the shape the failed-while-serving guard must not
// read as healthy capacity: conditions still say ContainersReady and the
// serving gate is still True, while the container is parked in a
// terminal waiting reason.
func wedgedButGatedPod(name, reason string) *corev1.Pod {
	pod := servingPod(name)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	return pod
}

func surgingInstanceWithWaiting(waiting string) []workloadtypes.InstanceStatus {
	return []workloadtypes.InstanceStatus{{
		Index:    0,
		Phase:    workloadtypes.InstancePhaseUpdating,
		PodCount: 1,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			Step:           workloadtypes.UpdateStepSurge,
			TargetRevision: "own-engine-targethash",
			Deadline:       metav1.NewTime(time.Now().Add(30 * time.Minute)),
			Waiting:        waiting,
		},
	}}
}

// gateNotFoldedPod builds the gate-limbo shape: phase Running, the container
// running and Ready, ContainersReady=True, and a declared readiness gate
// whose condition nobody has satisfied, so Ready stays False.
func gateNotFoldedPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Spec: corev1.PodSpec{
			ReadinessGates: []corev1.PodReadinessGate{{ConditionType: podreadiness.ConditionType}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.ContainersReady,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(since),
				},
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionFalse,
					Reason:             "ReadinessGatesNotReady",
					Message:            "corresponding condition of pod readiness gate \"ome.io/serving\" does not exist",
					LastTransitionTime: metav1.NewTime(since),
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// secondReadinessGate is a readiness gate another writer owns, declared
// on the pod beside the serving gate.
const secondReadinessGate corev1.PodConditionType = "example.com/load-balancer"

// gateWrittenNeverReadyPod builds that shape: phase Running, the container
// running and Ready, ContainersReady=True, the serving gate True, and the
// second gate declared with no condition for it, so Ready stays False.
func gateWrittenNeverReadyPod(name string, since time.Time) *corev1.Pod {
	pod := gateNotFoldedPod(name, since)
	pod.Spec.ReadinessGates = append(pod.Spec.ReadinessGates, corev1.PodReadinessGate{ConditionType: secondReadinessGate})
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               podreadiness.ConditionType,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(since),
	})
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].Message = "corresponding condition of pod readiness gate \"" + string(secondReadinessGate) + "\" does not exist"
		}
	}
	return pod
}

const probeMessage = "containers with unready status: [main]"

// runningNotReadyPod builds the limbo shape: phase Running, every
// container started, ContainersReady=False with the kubelet's own reason
// and message.
func runningNotReadyPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.ContainersReady,
				Status:             corev1.ConditionFalse,
				Reason:             evidence.ReasonContainersNotReady,
				Message:            probeMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// parkedPodAt is a pod created an hour ago, parked in reason for that
// whole hour, at the named incarnation and revision hash: the shape of a
// pod the fast path reads as stuck under any grace.
func parkedPodAt(name, reason, incarnation, hash string, now time.Time) *corev1.Pod {
	hourAgo := metav1.NewTime(now.Add(-time.Hour))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: hourAgo,
			Labels:            map[string]string{query.LabelInstanceIncarnation: incarnation, query.LabelRevisionHash: hash},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: hourAgo}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
			}},
		},
	}
}

// crashLoopRowNamingTheLoop is a settled or restarting row whose promoted
// set crashed after it entered Ready and that names the loop on its
// failure record, as the restart pass writes it while a held revision's
// set serves between crashes.
func crashLoopRowNamingTheLoop(now time.Time, phase workloadtypes.InstancePhase, incarnation int64, revision string) workloadtypes.InstanceStatus {
	readySince := metav1.NewTime(now.Add(-time.Hour))
	return workloadtypes.InstanceStatus{
		Index:           0,
		Incarnation:     incarnation,
		Phase:           phase,
		PodCount:        1,
		RunningRevision: revision,
		ReadySince:      &readySince,
		LastFailure: &workloadtypes.InstanceTermination{
			PodName: "engine-0-default-0", ContainerName: "main", Reason: "CrashLoopBackOff",
			Message: "back-off restarting failed container; " + workloadops.CrashLoopServingNote,
			Time:    metav1.NewTime(now.Add(-10 * time.Minute)),
		},
	}
}

// crashLoopPodBack is a pod of a crash loop as it reads at one moment:
// serving again since brokeOrBackAgo, or down again in CrashLoopBackOff
// since then. It was created an hour ago, so its age is past any grace.
func crashLoopPodBack(now time.Time, incarnation, hash string, serving bool, since time.Duration) *corev1.Pod {
	at := metav1.NewTime(now.Add(-since))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "engine-0-default-0",
			CreationTimestamp: metav1.NewTime(now.Add(-time.Hour)),
			Labels:            map[string]string{query.LabelInstanceIncarnation: incarnation, query.LabelRevisionHash: hash},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	status := corev1.ContainerStatus{Name: "main", RestartCount: 3,
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", FinishedAt: at}}}
	if serving {
		status.Ready = true
		status.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: at}}
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: at},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: at},
			{Type: "ome.io/serving", Status: corev1.ConditionTrue, LastTransitionTime: at},
		}
	} else {
		status.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: at},
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: at},
			{Type: "ome.io/serving", Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))},
		}
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{status}
	return pod
}

// routedDeps is a client with no routed Service or EndpointSlice behind it,
// which is what the hold pass reads a serving source's rotation through.
func routedDeps(t *testing.T) workloadtypes.Deps {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	return workloadtypes.Deps{Client: c, APIReader: c}
}

// surgePairAtIncarnation is a gang surge in flight: the source row at idx,
// at the given incarnation, surging to surgeIdx, beside the replacement's
// marker row at incarnation one, both pinned to the target revision with
// the same attempt deadline.
func surgePairAtIncarnation(idx, surgeIdx int32, incarnation int64, deadline time.Time) []workloadtypes.InstanceStatus {
	k := surgeIdx
	return []workloadtypes.InstanceStatus{
		{
			Index: idx, Incarnation: incarnation, Phase: workloadtypes.InstancePhaseUpdating, PodCount: 2,
			RunningRevision: "own-engine-oldhash", TargetRevision: "own-engine-newhash",
			Operation: &workloadtypes.InstanceOperation{
				ID: "gangsurge-1-1", Type: workloadtypes.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge,
				SurgeIndex: &k, TargetRevision: "own-engine-newhash", Strategy: workloadtypes.UpdateStrategySurgeThenDrain,
				Deadline: metav1.NewTime(deadline),
			},
		},
		{
			Index: surgeIdx, Incarnation: 1, Phase: workloadtypes.InstancePhaseCreating, TargetRevision: "own-engine-newhash",
			Operation: &workloadtypes.InstanceOperation{
				ID: "gangsurgetarget-2-1", Type: workloadtypes.InstanceOperationUpdate, Step: workloadtypes.UpdateStepGangSurgeTarget,
				TargetRevision: "own-engine-newhash", Deadline: metav1.NewTime(deadline),
			},
		},
	}
}

// gangPlan plans a two-member gang, a leader and one worker, at each index.
func gangPlan(indexes ...int32) workloadtypes.ComponentPlan {
	var plan workloadtypes.ComponentPlan
	for _, idx := range indexes {
		plan.Instances = append(plan.Instances, workloadtypes.InstancePlan{
			Index:   idx,
			Runners: []workloadtypes.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		})
	}
	return plan
}

// gangMemberAt is a gang member at the named incarnation and revision
// hash: serving, or Running and not yet ready.
func gangMemberAt(name, incarnation, hash string, serving bool, now time.Time) *corev1.Pod {
	pod := servingPod(name)
	pod.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	pod.Labels = map[string]string{query.LabelInstanceIncarnation: incarnation, query.LabelRevisionHash: hash}
	if !serving {
		pod.Status.Conditions = nil
	}
	return pod
}

// warnsByIndex records every WarnInstanceFailed call as "<index>:<pod>:<reason>".
func warnsByIndex(input *workloadtypes.ReconcileInput) *[]string {
	var warned []string
	input.WarnInstanceFailed = func(idx int32, pod, reason string) {
		warned = append(warned, strconv.Itoa(int(idx))+":"+pod+":"+reason)
	}
	return &warned
}
