package ops_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Create-path apiserver-rejection tests. An apiserver answer is evidence:
// a 422 names a broken pod template, a namespace-terminating refusal names
// a dead environment, a quota refusal names missing capacity, a 429 names
// an overloaded server. Each one gets its own outcome instead of an opaque
// error retried on blind backoff until the operation deadline lies about
// what happened.

// rejectionTargetCR is the revision the fixture's creates converge toward.
// Its name follows the "<owner>-<component>-<hash>" shape the revision
// helpers parse.
func rejectionTargetCR() *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-testrev1", Namespace: "prod"},
	}
}

// rejectionFixture builds a Create pass over `replicas` single-pod
// Instances whose Pod creates are answered by rejectPod: a nil return
// lets the create through, a non-nil return is the apiserver's rejection
// of that pod name.
func rejectionFixture(t *testing.T, replicas int32, rejectPod func(podName string) error) (
	client.Client,
	*v1beta1.InferenceService,
	workload.ReconcileInput,
	workload.ComponentPlan,
	*appsv1.ControllerRevision,
	*record.FakeRecorder,
	*[]retryBlockWrite,
) {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", int(replicas))
	target := rejectionTargetCR()

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("build scheme: %v", err)
		}
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithObjects(isvc, target).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if pod, ok := obj.(*corev1.Pod); ok && rejectPod != nil {
					if err := rejectPod(pod.Name); err != nil {
						return err
					}
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).
		Build()

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = target.Name
	// The dispatcher allocates one pacing sink per pass; these tests call
	// the op directly, so they supply it themselves.
	input.Pacing = &workload.APIPacing{}
	writes := &[]retryBlockWrite{}
	input.MutateRetryBlock = func(_ context.Context, rev string, mutate func(*workload.RetryBlock) workload.RetryBlockDisposition) error {
		b := workload.RetryBlock{TargetRevision: rev}
		d := mutate(&b)
		if d != workload.RetryBlockUnchanged {
			*writes = append(*writes, retryBlockWrite{rev: rev, block: b})
		}
		return nil
	}
	return c, isvc, input, buildPlanSinglePodEngine(replicas), target, record.NewFakeRecorder(16), writes
}

// retryBlockWrite is one committed RetryBlock mutation.
type retryBlockWrite struct {
	rev   string
	block workload.RetryBlock
}

// rejectionEvents drains the recorder.
func rejectionEvents(recorder *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func countRejectionEvents(events []string, reason workload.EventReason) int {
	n := 0
	for _, ev := range events {
		if strings.Contains(ev, string(reason)) {
			n++
		}
	}
	return n
}

func rejectionPodNames(t *testing.T, c client.Client) []string {
	t.Helper()
	list := &corev1.PodList{}
	if err := c.List(context.Background(), list, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	names := make([]string, 0, len(list.Items))
	for _, p := range list.Items {
		names = append(names, p.Name)
	}
	return names
}

func invalidPodError(name string) error {
	return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, name, field.ErrorList{
		field.Invalid(field.NewPath("spec", "containers").Index(0).Child("resources", "limits"), "-1", "must be greater than or equal to 0"),
	})
}

func namespaceTerminatingPodError(name string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    403,
		Reason:  metav1.StatusReasonForbidden,
		Message: `pods "` + name + `" is forbidden: unable to create new content in namespace prod because it is being terminated`,
		Details: &metav1.StatusDetails{
			Kind: "pods", Name: name,
			Causes: []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause, Message: "namespace prod is being terminated"}},
		},
	}}
}

func quotaPodError(name string) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, name, errors.New(
		"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=56, limited: requests.nvidia.com/gpu=64"))
}

// TestCreate_InvalidPodSpec_FailsOnlyTheRejectedInstance: a 422 is the
// apiserver saying the pod template is unacceptable. Retrying the same
// revision reproduces it, so the rejected Instance is failed with a
// RetryBlock against its target revision — while every other Instance in
// the same batch is created as usual.
func TestCreate_InvalidPodSpec_FailsOnlyTheRejectedInstance(t *testing.T) {
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 2, func(podName string) error {
		if podName == "llama-70b-engine-0-default-0" {
			return invalidPodError(podName)
		}
		return nil
	})

	result, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v (a permanent rejection is disposed, not returned)", err)
	}
	if result.RequeueAfter != ops.CreateRequeueInterval {
		t.Errorf("RequeueAfter: got %v want %v", result.RequeueAfter, ops.CreateRequeueInterval)
	}

	rejected := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if rejected == nil {
		t.Fatalf("instance 0 status: missing")
	}
	if rejected.Phase != v1beta1.OMENativeInstanceFailed || rejected.Operation != nil {
		t.Errorf("instance 0: got phase=%q op=%+v want Failed with the operation cleared", rejected.Phase, rejected.Operation)
	}
	if rejected.LastFailure == nil || rejected.LastFailure.Reason != workload.RejectionReasonInvalidPodSpec {
		t.Fatalf("instance 0 LastFailure: got %+v want reason=%s", rejected.LastFailure, workload.RejectionReasonInvalidPodSpec)
	}
	if !strings.Contains(rejected.LastFailure.Message, "resources.limits") {
		t.Errorf("instance 0 LastFailure.Message: got %q want the apiserver's own detail", rejected.LastFailure.Message)
	}

	if len(*writes) != 1 || (*writes)[0].rev != target.Name {
		t.Fatalf("RetryBlock writes: got %+v want one for %s", *writes, target.Name)
	}

	// The sibling Instance is untouched by its neighbour's bad luck.
	sibling := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 1)
	if sibling == nil || sibling.Phase != v1beta1.OMENativeInstanceCreating {
		t.Errorf("instance 1: got %+v want Creating", sibling)
	}
	names := rejectionPodNames(t, c)
	if len(names) != 1 || names[0] != "llama-70b-engine-1-default-0" {
		t.Errorf("pods: got %v want only llama-70b-engine-1-default-0", names)
	}

	events := rejectionEvents(recorder)
	if got := countRejectionEvents(events, workload.EventReasonInstanceRejected); got != 1 {
		t.Errorf("rejection events: got %d want 1 (%v)", got, events)
	}
}

// TestCreate_NamespaceTerminating_FailsWithoutBlamingTheRevision: the
// namespace is going away, which no revision can fix. The Instance is
// failed so the operator sees why, but the revision keeps a clean retry
// ladder.
func TestCreate_NamespaceTerminating_FailsWithoutBlamingTheRevision(t *testing.T) {
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 1, namespaceTerminatingPodError)

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err != nil {
		t.Fatalf("Create: %v (an environment rejection is disposed, not returned)", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil {
		t.Fatalf("instance 0: got %+v want Failed with the operation cleared", s)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != workload.RejectionReasonNamespaceTerminating {
		t.Errorf("LastFailure: got %+v want reason=%s", s.LastFailure, workload.RejectionReasonNamespaceTerminating)
	}
	if len(*writes) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none (the revision is blameless)", *writes)
	}
	if got := countRejectionEvents(rejectionEvents(recorder), workload.EventReasonInstanceRejected); got != 1 {
		t.Errorf("rejection events: got %d want 1", got)
	}
	if names := rejectionPodNames(t, c); len(names) != 0 {
		t.Errorf("pods: got %v want none", names)
	}
}

// TestCreate_QuotaExceeded_WaitsWithoutFailing: out of quota is a
// capacity wait, not a failure. The Instance keeps its operation, records
// the quota as its waiting token (which parks the InstanceReadyTimeout
// clock — see ReconcileGatedDeadlines), and the pass retries on the
// ordinary create interval.
//
// The apiserver's quota message is DIFFERENT on every pass — it names
// current usage and the pod that lost the race — so the test varies it
// across passes to pin that neither status nor the event follows it: one
// status write and one event for the whole episode, however long the
// namespace stays full.
func TestCreate_QuotaExceeded_WaitsWithoutFailing(t *testing.T) {
	pass := 0
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 1, func(podName string) error {
		pass++
		return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, podName, fmt.Errorf(
			"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=%d, limited: requests.nvidia.com/gpu=64", 48+pass))
	})

	result, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v (a quota refusal is a wait, not an error)", err)
	}
	if result.RequeueAfter != ops.CreateRequeueInterval {
		t.Errorf("RequeueAfter: got %v want %v", result.RequeueAfter, ops.CreateRequeueInterval)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want Creating with its operation intact", s)
	}
	if s.Operation.Waiting != workload.RejectionReasonQuotaExceeded {
		t.Errorf("Operation.Waiting: got %q want %q", s.Operation.Waiting, workload.RejectionReasonQuotaExceeded)
	}
	if s.Operation.Reason != "" {
		t.Errorf("Operation.Reason: got %q want untouched — Reason names why the operation exists", s.Operation.Reason)
	}
	if strings.Contains(s.Operation.Waiting, "used:") {
		t.Errorf("Operation.Waiting: got %q want a bare token; the volatile quota message belongs in the event", s.Operation.Waiting)
	}
	if s.LastFailure != nil {
		t.Errorf("LastFailure: got %+v want none (a wait is not a failure)", s.LastFailure)
	}
	if len(*writes) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none", *writes)
	}
	events := rejectionEvents(recorder)
	if got := countRejectionEvents(events, workload.EventReasonInstanceQuotaBlocked); got != 1 {
		t.Fatalf("quota events after the first pass: got %d want 1", got)
	}
	if !anyEventContains(events, "used: requests.nvidia.com/gpu=49") {
		t.Errorf("quota event must carry the apiserver's message; got %v", events)
	}
	rvAfterBlock := irResourceVersion(t, c, isvc)

	// Two more still-blocked passes, each answered with a DIFFERENT quota
	// message: no second event and no further status write.
	for i := 0; i < 2; i++ {
		next := buildTestInput(isvc, c, workload.ComponentEngine)
		next.ObservedState.UpdateRevision = target.Name
		next.Pacing = &workload.APIPacing{}
		next.MutateRetryBlock = input.MutateRetryBlock
		if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, next, plan, target); err != nil {
			t.Fatalf("Create (blocked pass %d): %v", i+2, err)
		}
	}
	if got := countRejectionEvents(rejectionEvents(recorder), workload.EventReasonInstanceQuotaBlocked); got != 0 {
		t.Errorf("quota events on repeat blocked passes: got %d want 0 (one per episode)", got)
	}
	if rv := irResourceVersion(t, c, isvc); rv != rvAfterBlock {
		t.Errorf("status resourceVersion moved on repeat blocked passes: %s -> %s (the episode must write once)", rvAfterBlock, rv)
	}
}

// TestCreate_QuotaCleared_OnAlreadyExistingPods: a blocked Instance whose
// targets all come back AlreadyExists creates nothing, yet it is plainly
// no longer blocked — the pods are there. Releasing the token on pass
// COMPLETION rather than on a create count is what stops such an Instance
// keeping a stale block and a parked deadline forever.
func TestCreate_QuotaCleared_OnAlreadyExistingPods(t *testing.T) {
	blocked := true
	c, isvc, input, plan, target, recorder, _ := rejectionFixture(t, 1, func(podName string) error {
		if blocked {
			return quotaPodError(podName)
		}
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, podName)
	})

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err != nil {
		t.Fatalf("Create (blocked): %v", err)
	}
	if s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0); s == nil ||
		s.Operation == nil || s.Operation.Waiting != workload.RejectionReasonQuotaExceeded {
		t.Fatalf("instance 0 after the blocked pass: got %+v want the quota token recorded", s)
	}

	blocked = false
	resetExpectations(t)
	next := buildTestInput(isvc, c, workload.ComponentEngine)
	next.ObservedState.UpdateRevision = target.Name
	next.Pacing = &workload.APIPacing{}
	next.MutateRetryBlock = input.MutateRetryBlock
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, next, plan, target); err != nil {
		t.Fatalf("Create (all targets AlreadyExists): %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want a live operation", s)
	}
	if s.Operation.Waiting != "" {
		t.Fatalf("Operation.Waiting: got %q want cleared — no create landed, but nothing is blocking either", s.Operation.Waiting)
	}
	// The cleared token is what lets the parking step re-arm the deadline.
	insts := v1beta1convert.InstanceStatusSliceToWorkload(
		[]v1beta1.OMENativeInstanceStatus{*s})
	if workload.OperationCapacityBlocked(insts[0].Operation) {
		t.Errorf("instance 0 still reads as capacity-blocked: %+v", insts[0].Operation)
	}
}

// TestCreate_QuotaCleared_ReleasesTheWaitingToken: once quota frees up
// and the create lands, the waiting token is removed — which is what
// re-arms the parked deadline from that moment.
func TestCreate_QuotaCleared_ReleasesTheWaitingToken(t *testing.T) {
	blocked := true
	c, isvc, input, plan, target, recorder, _ := rejectionFixture(t, 1, func(podName string) error {
		if blocked {
			return quotaPodError(podName)
		}
		return nil
	})

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err != nil {
		t.Fatalf("Create (blocked): %v", err)
	}
	blocked = false
	resetExpectations(t)
	input2 := buildTestInput(isvc, c, workload.ComponentEngine)
	input2.ObservedState.UpdateRevision = target.Name
	input2.Pacing = &workload.APIPacing{}
	input2.MutateRetryBlock = input.MutateRetryBlock
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input2, plan, target); err != nil {
		t.Fatalf("Create (unblocked): %v", err)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want a live operation", s)
	}
	if s.Operation.Waiting != "" {
		t.Errorf("Operation.Waiting: got %q want cleared once the create landed", s.Operation.Waiting)
	}
	if names := rejectionPodNames(t, c); len(names) != 1 {
		t.Errorf("pods: got %v want the create to have landed", names)
	}
}

// TestCreate_Throttled_HonorsServerDelay: a 429 carrying Retry-After is
// the apiserver pacing us. The pass waits exactly that long and touches
// no status — the attempt is intact.
func TestCreate_Throttled_HonorsServerDelay(t *testing.T) {
	c, isvc, input, plan, target, recorder, writes := rejectionFixture(t, 1, func(podName string) error {
		return apierrors.NewTooManyRequests("apiserver is shedding load", 7)
	})

	result, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target)
	if err != nil {
		t.Fatalf("Create: %v (throttling is paced, not failed)", err)
	}
	if result.RequeueAfter != 7*time.Second {
		t.Errorf("RequeueAfter: got %v want the server's suggested 7s", result.RequeueAfter)
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil {
		t.Fatalf("instance 0: got %+v want Creating with its operation intact", s)
	}
	if s.Operation.Reason != "" || s.LastFailure != nil {
		t.Errorf("instance 0: got reason=%q lastFailure=%+v want both unset (throttling writes no status)", s.Operation.Reason, s.LastFailure)
	}
	if len(*writes) != 0 {
		t.Errorf("RetryBlock writes: got %+v want none", *writes)
	}
	if events := rejectionEvents(recorder); countRejectionEvents(events, workload.EventReasonInstanceRejected) != 0 {
		t.Errorf("rejection events: got %v want none", events)
	}
}

// TestCreate_Conflict_StaysAnOpaqueError: an unclassified rejection is not
// interpreted — the error surfaces and controller-runtime's own backoff
// owns the retry.
func TestCreate_Conflict_StaysAnOpaqueError(t *testing.T) {
	c, isvc, input, plan, target, recorder, _ := rejectionFixture(t, 1, func(podName string) error {
		return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, podName, errors.New("object was modified"))
	})

	if _, err := ops.Create(context.Background(), workload.Deps{Client: c, Recorder: recorder}, input, plan, target); err == nil {
		t.Fatal("Create: got nil want the conflict surfaced as an error")
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s != nil && s.Phase == v1beta1.OMENativeInstanceFailed {
		t.Errorf("instance 0: got Failed want the attempt left alone for the caller's backoff")
	}
}

// irResourceVersion reads the persisted InferenceReplica's
// resourceVersion — the object every instance-status write lands on, so
// an unchanged value proves a pass wrote no status.
func irResourceVersion(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) string {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: irName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("read IR: %v", err)
	}
	return ir.ResourceVersion
}

func anyEventContains(events []string, want string) bool {
	for _, ev := range events {
		if strings.Contains(ev, want) {
			return true
		}
	}
	return false
}
