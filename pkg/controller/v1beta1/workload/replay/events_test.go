package replay

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestApplyOperationDeadlineNeedsASlack keeps the harness from inventing a
// behavioral value the scenario did not state.
func TestApplyOperationDeadlineNeedsASlack(t *testing.T) {
	d := &driver{store: NewRowStore(nil, nil, nil)}
	_, err := applyOperationDeadline(context.Background(), d, TimelineEvent{ID: "timer.operationDeadline"})
	if err == nil || !strings.Contains(err.Error(), "no in-flight operation") {
		t.Fatalf("expected the no-deadline error, got %v", err)
	}
}

// TestApplyStuckTerminatingNeedsADeletionInFlight keeps the event an
// observation about a deletion already issued rather than a way to conjure
// one: a pod nobody has deleted has nothing to be stuck at.
func TestApplyStuckTerminatingNeedsADeletionInFlight(t *testing.T) {
	d := &driver{opts: DefaultOptions(), terminating: map[string]*podTeardown{}}
	_, err := applyStuckTerminating(context.Background(), d, TimelineEvent{
		ID:   "pod.stuckTerminating",
		Args: EventArgs{Pod: &PodRef{Index: 0}},
	})
	if err == nil || !strings.Contains(err.Error(), "is not Terminating") {
		t.Fatalf("expected the not-Terminating error, got %v", err)
	}
}

// TestApplyForceDeleteNeedsAConfiguredPolicy holds the same line for the
// clock: the overdue slack is the operator's value, so an unconfigured
// escalation has no deadline to move onto.
func TestApplyForceDeleteNeedsAConfiguredPolicy(t *testing.T) {
	d := &driver{opts: DefaultOptions()}
	_, err := applyForceDelete(context.Background(), d, TimelineEvent{ID: "timer.forceDelete"})
	if err == nil || !strings.Contains(err.Error(), "config.forceDelete is not set") {
		t.Fatalf("expected the unconfigured-policy error, got %v", err)
	}
}

// TestApplyTeardownDeadlineNeedsADeletedOwnerAndAConfiguredDeadline keeps
// the teardown clock honest: it runs only from an owner's deletion, and
// only when the operator configured a deadline; without one, teardown
// holds strictly and there is nothing to elapse.
func TestApplyTeardownDeadlineNeedsADeletedOwnerAndAConfiguredDeadline(t *testing.T) {
	d := &driver{opts: DefaultOptions(), cfg: ConfigState{TeardownDeadline: "10m"}}
	_, err := applyTeardownDeadline(context.Background(), d, TimelineEvent{ID: "timer.teardownDeadline", Args: EventArgs{Slack: "1s"}})
	if err == nil || !strings.Contains(err.Error(), "not Terminating") {
		t.Fatalf("expected the not-Terminating error, got %v", err)
	}
	d.teardown = true
	d.cfg.TeardownDeadline = ""
	_, err = applyTeardownDeadline(context.Background(), d, TimelineEvent{ID: "timer.teardownDeadline", Args: EventArgs{Slack: "1s"}})
	if err == nil || !strings.Contains(err.Error(), "config.teardownDeadline is not set") {
		t.Fatalf("expected the unconfigured-deadline error, got %v", err)
	}
}

// TestApplyConfigChangedRewritesOneKnob pins the shape of an operator
// config edit: the variant names the knob, the argument is the knob's
// whole new value, a field outside the knob is refused, and a knob the
// driver has no input for is refused by name.
func TestApplyConfigChangedRewritesOneKnob(t *testing.T) {
	ctx := context.Background()
	d := &driver{cfg: ConfigState{StuckPodGrace: "1h", RequeueOperation: "5s"}}
	detail, err := applyConfigChanged(ctx, d, TimelineEvent{
		ID: "config.changed", Variant: "stuckPodGrace",
		Args: EventArgs{Config: &ConfigState{StuckPodGrace: "2m"}},
	})
	if err != nil || d.cfg.StuckPodGrace != "2m" || detail != "stuckPodGrace=2m" {
		t.Fatalf("rewrite: got (%q, %v) cfg=%+v", detail, err, d.cfg)
	}
	if d.cfg.RequeueOperation != "5s" {
		t.Fatalf("a knob edit must leave the other knobs alone: %+v", d.cfg)
	}
	if _, err := applyConfigChanged(ctx, d, TimelineEvent{
		ID: "config.changed", Variant: "stuckPodGrace",
		Args: EventArgs{Config: &ConfigState{StuckPodGrace: "2m", RequeueGate: "1s"}},
	}); err == nil || !strings.Contains(err.Error(), "outside the stuckPodGrace knob") {
		t.Fatalf("a field outside the knob must fail the run: %v", err)
	}
	if detail, err := applyConfigChanged(ctx, d, TimelineEvent{ID: "config.changed", Variant: "forceDelete"}); err != nil || detail != "forceDelete=nil" || d.cfg.ForceDelete != nil {
		t.Fatalf("an empty group must clear the knob: (%q, %v) cfg=%+v", detail, err, d.cfg)
	}
	if _, err := applyConfigChanged(ctx, d, TimelineEvent{ID: "config.changed", Variant: "gangScheduleTimeout"}); err == nil || !strings.Contains(err.Error(), "no gangScheduleTimeout input") {
		t.Fatalf("an unmodelled knob must be refused by name: %v", err)
	}
	if _, err := applyConfigChanged(ctx, d, TimelineEvent{ID: "config.changed"}); err == nil {
		t.Fatal("a config edit must name its knob")
	}
	for variant := range configKnobs {
		if _, err := applyConfigChanged(ctx, &driver{}, TimelineEvent{ID: "config.changed", Variant: variant}); err != nil {
			t.Fatalf("%s: clearing the knob must be accepted: %v", variant, err)
		}
	}
}

// TestSchedulingGateEventsHoldAndReleaseAPod pins the pair of admission
// events: a gated pod is what the engine's admission predicate reads and
// is unscheduled for the SchedulingGated reason; admission takes every
// gate off; and the pair refuses the shapes that make no sense, a bound
// pod gated and an ungated pod admitted.
func TestSchedulingGateEventsHoldAndReleaseAPod(t *testing.T) {
	const pending = `
scenario: pending-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Pending, runningRevision: current}]
  pods: [{index: 0, phase: Pending}]
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, pending)
	ref := &PodRef{Index: 0}
	name := d.podName(*ref)
	if _, err := applySchedulingGated(ctx, d, TimelineEvent{ID: "pod.schedulingGated", Args: EventArgs{Pod: ref}}); err != nil {
		t.Fatalf("gate: %v", err)
	}
	pod, err := d.getPod(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if !types.PodAdmissionGated(pod) || pod.Status.Phase != corev1.PodPending {
		t.Fatalf("a gated pod must carry a gate and stay Pending: gates=%v phase=%s", pod.Spec.SchedulingGates, pod.Status.Phase)
	}
	if got := findCondition(pod, corev1.PodScheduled); got == nil || got.Status != corev1.ConditionFalse || got.Reason != corev1.PodReasonSchedulingGated {
		t.Fatalf("a gated pod reports PodScheduled=False/SchedulingGated, got %+v", got)
	}
	if _, err := applyAdmitted(ctx, d, TimelineEvent{ID: "pod.admitted", Args: EventArgs{Pod: ref}}); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if pod, err = d.getPod(ctx, name); err != nil || types.PodAdmissionGated(pod) || findCondition(pod, corev1.PodScheduled) != nil {
		t.Fatalf("admission must take the gate and the report off: err=%v gates=%v", err, pod.Spec.SchedulingGates)
	}
	if _, err := applyAdmitted(ctx, d, TimelineEvent{ID: "pod.admitted", Args: EventArgs{Pod: ref}}); err == nil || !strings.Contains(err.Error(), "no scheduling gate") {
		t.Fatalf("admitting an ungated pod must fail the run: %v", err)
	}
	if _, err := applyPodScheduled(ctx, d, TimelineEvent{ID: "pod.scheduled", Args: EventArgs{Pod: ref}}); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if _, err := applySchedulingGated(ctx, d, TimelineEvent{ID: "pod.schedulingGated", Args: EventArgs{Pod: ref}}); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("gating a bound pod must fail the run: %v", err)
	}
}

// TestApplyPolicyTuningRewritesEachKnob pins the lifecycle knobs a
// scenario may edit mid-run, each spelled as the owner spec spells it,
// and the refusals: a knob with no value, a malformed value, an unknown
// knob.
func TestApplyPolicyTuningRewritesEachKnob(t *testing.T) {
	ctx := context.Background()
	d := mustDriver(t, minimalScenario)
	tune := func(variant, value string) error {
		_, err := applyPolicyTuning(ctx, d, TimelineEvent{ID: "spec.policyTuning", Variant: variant, Args: EventArgs{Value: value}})
		return err
	}
	for _, tc := range []struct{ variant, value string }{
		{"restartPolicy", "RecreateInstanceOnPodRestart"},
		{"migrationPolicy", "Auto"},
		{"instanceReadyTimeout", "45m"},
		{"minReadySeconds", "600"},
		{"markNotReady", "false"},
	} {
		if err := tune(tc.variant, tc.value); err != nil {
			t.Fatalf("%s: %v", tc.variant, err)
		}
	}
	if d.spec.RestartPolicy != "RecreateInstanceOnPodRestart" || d.spec.MigrationMode != "Auto" ||
		d.spec.InstanceReadyTimeout != "45m" || d.spec.MinReadySeconds != 600 ||
		d.spec.MarkNotReady == nil || *d.spec.MarkNotReady {
		t.Fatalf("knobs not rewritten: %+v", d.spec)
	}
	if d.owner.Generation != 5 {
		t.Fatalf("every edit bumps the owner generation once: got %d", d.owner.Generation)
	}
	for _, tc := range []struct{ variant, value, want string }{
		{"minReadySeconds", "", "needs the new setting"},
		{"minReadySeconds", "soon", "non-negative number"},
		{"instanceReadyTimeout", "later", "instanceReadyTimeout"},
		{"markNotReady", "maybe", "not a boolean"},
		{"pacing", "x", "not a knob"},
		{"", "x", "needs a variant"},
	} {
		if err := tune(tc.variant, tc.value); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s=%q: want an error naming %q, got %v", tc.variant, tc.value, tc.want, err)
		}
	}
}

// TestApplyContainersNotReadyRegressesARunningPod pins the regression the
// kubelet reports when a running pod stops being ready: ContainersReady
// and Ready both drop while the containers keep running, and a pod that
// was never ContainersReady has nothing to regress from.
func TestApplyContainersNotReadyRegressesARunningPod(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods:
    - {index: 0, ready: true, serving: true, routed: true}
    - {index: 0, ordinal: 1, phase: Pending}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	ref := &PodRef{Index: 0}
	if _, err := applyContainersNotReady(ctx, d, TimelineEvent{ID: "pod.containersNotReady", Args: EventArgs{Pod: ref}}); err != nil {
		t.Fatalf("regress: %v", err)
	}
	pod, err := d.getPod(ctx, d.podName(*ref))
	if err != nil {
		t.Fatal(err)
	}
	for _, condType := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
		if cond := findCondition(pod, condType); cond == nil || cond.Status != corev1.ConditionFalse {
			t.Fatalf("%s must read False after the regression, got %+v", condType, cond)
		}
	}
	if pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) == 0 || pod.Status.ContainerStatuses[0].Ready || pod.Status.ContainerStatuses[0].State.Running == nil {
		t.Fatalf("the pod keeps running with its containers reported not ready: phase=%s statuses=%+v", pod.Status.Phase, pod.Status.ContainerStatuses)
	}
	never := &PodRef{Index: 0, Ordinal: 1}
	if _, err := applyContainersNotReady(ctx, d, TimelineEvent{ID: "pod.containersNotReady", Args: EventArgs{Pod: never}}); err == nil || !strings.Contains(err.Error(), "not ContainersReady") {
		t.Fatalf("a pod that was never ready cannot regress: %v", err)
	}
}

// TestApplyAdmissionRejectedIsWhatTheEnginePredicateReads pins the shape
// of a kubelet refusal against the engine's own predicate for it, for
// both variants, and the refusals: an unbound pod, a resource on the
// variant that names none, no resource on the one that needs it.
func TestApplyAdmissionRejectedIsWhatTheEnginePredicateReads(t *testing.T) {
	const scheduled = `
scenario: scheduled-pods
arrows: [T-empty-create]
initial:
  spec: {replicas: 2, image: registry.example.com/runtime:v1}
  rows:
    - {index: 0, phase: Pending, runningRevision: current}
    - {index: 1, phase: Pending, runningRevision: current}
  pods:
    - {index: 0, node: node-0, phase: Pending}
    - {index: 1, node: node-1, phase: Pending}
    - {index: 1, ordinal: 1, phase: Pending}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, scheduled)
	reject := func(ref PodRef, variant, resource string) error {
		_, err := applyAdmissionRejected(ctx, d, TimelineEvent{ID: "pod.admissionRejected", Variant: variant,
			Args: EventArgs{Pod: &ref, Resource: resource, Message: "refused"}})
		return err
	}
	if err := reject(PodRef{Index: 0}, "OutOfResource", "nvidia.com/gpu"); err != nil {
		t.Fatalf("OutOfResource: %v", err)
	}
	if err := reject(PodRef{Index: 1}, "UnexpectedAdmissionError", ""); err != nil {
		t.Fatalf("UnexpectedAdmissionError: %v", err)
	}
	for _, tc := range []struct {
		ref  PodRef
		want string
	}{
		{PodRef{Index: 0}, "OutOfnvidia.com/gpu"},
		{PodRef{Index: 1}, query.ReasonUnexpectedAdmissionError},
	} {
		pod, err := d.getPod(ctx, d.podName(tc.ref))
		if err != nil {
			t.Fatal(err)
		}
		reason, rejected := query.PodAdmissionRejected(pod)
		if !rejected || reason != tc.want || pod.Status.Message != "refused" || len(pod.Status.ContainerStatuses) != 0 {
			t.Fatalf("%s: the engine must read the refusal: rejected=%t reason=%q pod=%+v", pod.Name, rejected, reason, pod.Status)
		}
	}
	if err := reject(PodRef{Index: 1, Ordinal: 1}, "OutOfResource", "cpu"); err == nil || !strings.Contains(err.Error(), "not bound") {
		t.Fatalf("an unbound pod cannot be refused by a node: %v", err)
	}
	if err := reject(PodRef{Index: 0}, "OutOfResource", ""); err == nil || !strings.Contains(err.Error(), "needs the resource") {
		t.Fatalf("OutOfResource names its resource: %v", err)
	}
	if err := reject(PodRef{Index: 0}, "UnexpectedAdmissionError", "cpu"); err == nil || !strings.Contains(err.Error(), "names no resource") {
		t.Fatalf("UnexpectedAdmissionError takes no resource: %v", err)
	}
	if err := reject(PodRef{Index: 0}, "", ""); err == nil {
		t.Fatal("the refusal names its variant")
	}
}

// TestApplyUIDRotatedReplacesThePodUnderANewIdentity pins the rotation:
// the same name and labels under a new UID, fresh and unscheduled, the
// finalizer the driver holds every pod with, and no expectation left open
// because the watch saw a delete and an add.
func TestApplyUIDRotatedReplacesThePodUnderANewIdentity(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods: [{index: 0, node: node-0, ready: true, serving: true, routed: true}]
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	ref := &PodRef{Index: 0}
	name := d.podName(*ref)
	before, err := d.getPod(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	detail, err := applyUIDRotated(ctx, d, TimelineEvent{ID: "pod.uidRotated", Args: EventArgs{Pod: ref}})
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	after, err := d.getPod(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if after.UID == before.UID || after.UID == "" {
		t.Fatalf("the successor must carry a new uid: before=%s after=%s", before.UID, after.UID)
	}
	if detail != "pod="+name+" uid=uid#1→uid#2" {
		t.Fatalf("detail: %q", detail)
	}
	if after.Labels[query.LabelRevisionHash] != before.Labels[query.LabelRevisionHash] || after.Labels[query.LabelInstanceIdx] != before.Labels[query.LabelInstanceIdx] {
		t.Fatalf("the successor keeps the labels: before=%v after=%v", before.Labels, after.Labels)
	}
	if after.Spec.NodeName != "" || after.Status.Phase != corev1.PodPending || len(after.Status.Conditions) != 0 || after.DeletionTimestamp != nil {
		t.Fatalf("the successor is fresh and unscheduled: %+v", after)
	}
	if !contains(after.Finalizers, podLifecycleFinalizer) {
		t.Fatalf("the successor is held like every pod: %v", after.Finalizers)
	}
	if !d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatal("a rotation leaves no expectation open")
	}
	if _, err := applyUIDRotated(ctx, d, TimelineEvent{ID: "pod.uidRotated", Args: EventArgs{Pod: &PodRef{Index: 3}}}); err == nil {
		t.Fatal("a pod that does not exist cannot reappear")
	}
}

// TestApplyEvictedFailsTheRunningPod pins the kubelet eviction: a bound
// running pod goes Failed with the Evicted reason and its containers
// killed, and a pod no node holds cannot be evicted by one.
func TestApplyEvictedFailsTheRunningPod(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods:
    - {index: 0, node: node-0, ready: true, serving: true, routed: true}
    - {index: 0, ordinal: 1, phase: Pending}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	ref := &PodRef{Index: 0}
	if _, err := applyEvicted(ctx, d, TimelineEvent{ID: "pod.evicted", Args: EventArgs{Pod: ref, Message: "The node was low on resource: memory."}}); err != nil {
		t.Fatalf("evict: %v", err)
	}
	pod, err := d.getPod(ctx, d.podName(*ref))
	if err != nil {
		t.Fatal(err)
	}
	if pod.Status.Phase != corev1.PodFailed || pod.Status.Reason != "Evicted" || pod.Status.Message == "" {
		t.Fatalf("an evicted pod is Failed/Evicted with the kubelet's message: %+v", pod.Status)
	}
	if len(pod.Status.ContainerStatuses) == 0 || pod.Status.ContainerStatuses[0].State.Terminated == nil || pod.Status.ContainerStatuses[0].Ready {
		t.Fatalf("the containers are killed: %+v", pod.Status.ContainerStatuses)
	}
	if _, err := applyEvicted(ctx, d, TimelineEvent{ID: "pod.evicted", Args: EventArgs{Pod: &PodRef{Index: 0, Ordinal: 1}}}); err == nil || !strings.Contains(err.Error(), "not bound") {
		t.Fatalf("an unbound pod cannot be evicted by a node: %v", err)
	}
}

// TestApplyAnnotationOnlyIsJudgedByTheHasher pins the rule for a metadata
// edit: an edit of a key the revision hasher derives rather than reads as
// intent keeps the revision and bumps the generation, an edit the hasher
// reads is refused as a template change, and a seeded template's metadata
// reaches the rendered pod.
func TestApplyAnnotationOnlyIsJudgedByTheHasher(t *testing.T) {
	const queued = `
scenario: queued
arrows: [T-empty-create]
initial:
  spec:
    replicas: 1
    image: registry.example.com/runtime:v1
    podLabels: {kueue.x-k8s.io/queue-name: queue-a, team: a}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods: [{index: 0, ready: true, serving: true, routed: true}]
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, queued)
	pod, err := d.getPod(ctx, d.podName(PodRef{Index: 0}))
	if err != nil {
		t.Fatal(err)
	}
	if pod.Labels["kueue.x-k8s.io/queue-name"] != "queue-a" || pod.Labels["team"] != "a" {
		t.Fatalf("the template's labels must reach the rendered pod: %v", pod.Labels)
	}
	revisionBefore, err := d.revisionFor(ctx, "current")
	if err != nil {
		t.Fatal(err)
	}
	edit := func(labels map[string]string) (string, error) {
		return applyAnnotationOnly(ctx, d, TimelineEvent{ID: "spec.annotationOnly", Args: EventArgs{Labels: labels}})
	}
	detail, err := edit(map[string]string{"kueue.x-k8s.io/queue-name": "queue-b", "team": "a"})
	if err != nil || detail != "labels=[kueue.x-k8s.io/queue-name:queue-b team:a] generation=1" {
		t.Fatalf("a queue re-point is annotation-only: (%q, %v)", detail, err)
	}
	if revisionAfter, err := d.revisionFor(ctx, "current"); err != nil || revisionAfter != revisionBefore {
		t.Fatalf("the revision must not move: %s→%s (%v)", revisionBefore, revisionAfter, err)
	}
	if _, err := edit(map[string]string{"kueue.x-k8s.io/queue-name": "queue-b", "team": "b"}); err == nil || !strings.Contains(err.Error(), "moves the revision hash") {
		t.Fatalf("a hashed label edit is a template change: %v", err)
	}
	if d.spec.PodLabels["team"] != "a" {
		t.Fatalf("a refused edit must leave the template as it was: %v", d.spec.PodLabels)
	}
	if _, err := applyAnnotationOnly(ctx, d, TimelineEvent{ID: "spec.annotationOnly"}); err == nil {
		t.Fatal("the edit names what it rewrites")
	}
}

// TestApplyNodeLostIsWhatTheEvidenceReads pins the three node-death
// forms against the engine's own evidence: the taint and the NodeReady
// transition age from the instant they are written, the gone node reads
// as gone, the pod leaves Ready for the node lifecycle controller's
// reason, and the nodeUnreachable clock lands on the threshold.
func TestApplyNodeLostIsWhatTheEvidenceReads(t *testing.T) {
	const bound = `
scenario: bound-pods
arrows: [T-empty-create]
initial:
  spec: {replicas: 3, image: registry.example.com/runtime:v1}
  config:
    forceDelete: {overdueSlack: 1m, nodeUnreachableThreshold: 5m}
  rows:
    - {index: 0, phase: Ready, runningRevision: current, readySince: 0s}
    - {index: 1, phase: Ready, runningRevision: current, readySince: 0s}
    - {index: 2, phase: Ready, runningRevision: current, readySince: 0s}
  pods:
    - {index: 0, node: node-0, ready: true, serving: true, routed: true}
    - {index: 1, node: node-1, ready: true, serving: true, routed: true}
    - {index: 2, node: node-2, ready: true, serving: true, routed: true}
    - {index: 2, ordinal: 1, phase: Pending}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, bound)
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	lose := func(index int32, variant string) error {
		_, err := applyNodeLost(ctx, d, TimelineEvent{ID: "pod.nodeLost", Variant: variant, Args: EventArgs{Pod: &PodRef{Index: index}}})
		return err
	}
	for index, variant := range map[int32]string{0: "UnreachableTaint", 1: "NodeNotReady", 2: "NodeGone"} {
		if err := lose(index, variant); err != nil {
			t.Fatalf("%s: %v", variant, err)
		}
	}
	want := map[int32]evidence.TerminatingClass{0: evidence.NodeNotDeadLongEnough, 1: evidence.NodeNotDeadLongEnough, 2: evidence.NodeGone}
	later := map[int32]evidence.TerminatingClass{0: evidence.NodeUnreachableTaint, 1: evidence.NodeNotReady, 2: evidence.NodeGone}
	read := func(index int32) (*corev1.Pod, evidence.TerminatingClass) {
		pod, err := d.getPod(ctx, d.podName(PodRef{Index: index}))
		if err != nil {
			t.Fatal(err)
		}
		return pod, evidence.NodeDeath(ctx, d.cli, pod, policy, d.clock.Now(), 0).Kind
	}
	for index, kind := range want {
		pod, got := read(index)
		if got != kind {
			t.Fatalf("index %d: evidence inside the threshold reads %s, want %s", index, got, kind)
		}
		if index != 2 {
			if cond := findCondition(pod, corev1.PodReady); cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != nodeNotReadyReason {
				t.Fatalf("index %d: the pod leaves Ready for the node's reason, got %+v", index, cond)
			}
		}
	}
	detail, err := applyForceDelete(ctx, d, TimelineEvent{ID: "timer.forceDelete", Variant: "nodeUnreachable", Args: EventArgs{Slack: "1s"}})
	if err != nil || detail != "to=t+5m1s" {
		t.Fatalf("the nodeUnreachable clock lands on the threshold: (%q, %v)", detail, err)
	}
	for index, kind := range later {
		if _, got := read(index); got != kind {
			t.Fatalf("index %d: evidence past the threshold reads %s, want %s", index, got, kind)
		}
	}
	if err := lose(2, "UnreachableTaint"); err != nil {
		t.Fatalf("a gone node may come back tainted: %v", err)
	}
	if _, err := applyNodeLost(ctx, d, TimelineEvent{ID: "pod.nodeLost", Variant: "NodeGone", Args: EventArgs{Pod: &PodRef{Index: 2, Ordinal: 1}}}); err == nil || !strings.Contains(err.Error(), "not bound") {
		t.Fatalf("an unbound pod has no node to lose: %v", err)
	}
	if _, err := applyNodeLost(ctx, d, TimelineEvent{ID: "pod.nodeLost", Args: EventArgs{Pod: &PodRef{Index: 0}}}); err == nil {
		t.Fatal("the loss names its form")
	}
}

// TestApplyPartitionRewritesTheCanaryHold pins the partition edit: the
// hold reaches the plan as rollingUpdate.partition, every edit bumps the
// owner generation, and a missing or negative value is refused.
func TestApplyPartitionRewritesTheCanaryHold(t *testing.T) {
	ctx := context.Background()
	d := mustDriver(t, minimalScenario)
	one := int32(1)
	if detail, err := applyPartition(ctx, d, TimelineEvent{ID: "spec.partition", Args: EventArgs{To: &one}}); err != nil || detail != "partition=1 generation=1" {
		t.Fatalf("edit: (%q, %v)", detail, err)
	}
	desired, err := d.desiredSpec()
	if err != nil {
		t.Fatal(err)
	}
	if desired.Lifecycle.UpdateStrategy.RollingUpdate == nil || desired.Lifecycle.UpdateStrategy.RollingUpdate.Partition == nil || *desired.Lifecycle.UpdateStrategy.RollingUpdate.Partition != 1 {
		t.Fatalf("the partition must reach the plan: %+v", desired.Lifecycle.UpdateStrategy)
	}
	if desired.Pacing != nil {
		t.Fatalf("the user's partition projects no step hold: %+v", desired.Pacing)
	}
	two := int32(2)
	if detail, err := applyPartition(ctx, d, TimelineEvent{ID: "spec.partition", Args: EventArgs{To: &two, Canary: true}}); err != nil || detail != "canaryPartition=2 generation=2" {
		t.Fatalf("canary edit: (%q, %v)", detail, err)
	}
	if desired, err = d.desiredSpec(); err != nil {
		t.Fatal(err)
	}
	if desired.Pacing == nil || desired.Pacing.Partition == nil || *desired.Pacing.Partition != 2 {
		t.Fatalf("the step's partition must reach the plan as pacing: %+v", desired.Pacing)
	}
	if *desired.Lifecycle.UpdateStrategy.RollingUpdate.Partition != 1 {
		t.Fatalf("the step's partition leaves the user's own where it was: %+v", desired.Lifecycle.UpdateStrategy)
	}
	if _, err := applyPartition(ctx, d, TimelineEvent{ID: "spec.partition"}); err == nil {
		t.Fatal("the edit names the new partition")
	}
	negative := int32(-1)
	if _, err := applyPartition(ctx, d, TimelineEvent{ID: "spec.partition", Args: EventArgs{To: &negative}}); err == nil {
		t.Fatal("a negative partition is refused")
	}
}

// TestApplyPodWaitingTakesPodReadyDown pins the kubelet's reading of a
// waiting container: ContainersReady and PodReady go False together in
// the same status, whatever the serving gate says, so a pod that served
// never reads as in rotation while it wedges.
func TestApplyPodWaitingTakesPodReadyDown(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows:
    - {index: 0, phase: Ready, runningRevision: current, readySince: 0s}
  pods:
    - {index: 0, ready: true, serving: true, routed: true}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	ref := &PodRef{Index: 0}
	waiting := TimelineEvent{ID: "pod.waiting", Variant: "ImagePullBackOff", Args: EventArgs{Pod: ref, Message: "manifest unknown"}}
	if _, err := applyPodWaiting(ctx, d, waiting); err != nil {
		t.Fatalf("waiting: %v", err)
	}
	pod, err := d.getPod(ctx, d.podName(*ref))
	if err != nil {
		t.Fatal(err)
	}
	conditions := map[corev1.PodConditionType]corev1.PodCondition{}
	for _, c := range pod.Status.Conditions {
		conditions[c.Type] = c
	}
	containers, podReady := conditions[corev1.ContainersReady], conditions[corev1.PodReady]
	if containers.Status != corev1.ConditionFalse || podReady.Status != corev1.ConditionFalse {
		t.Fatalf("ContainersReady=%s PodReady=%s, want both False while the container waits", containers.Status, podReady.Status)
	}
	if !podReady.LastTransitionTime.Equal(&containers.LastTransitionTime) {
		t.Fatalf("PodReady transitioned at %v, ContainersReady at %v, want the same status", podReady.LastTransitionTime, containers.LastTransitionTime)
	}
	if got := conditions[query.ServingConditionType]; got.Status != corev1.ConditionTrue {
		t.Fatalf("the serving gate is the controller's to write and stays %s, got %s", corev1.ConditionTrue, got.Status)
	}
}

// TestApplyPodWaitingKeepsTheRunnersRestartRecord pins what a waiting
// episode leaves on a container that restarted before it: the kubelet
// keeps the restart count and the last termination while the container
// waits, which is the record the crash-loop ladder dates its count by.
// A container that never restarted parks with an empty record.
func TestApplyPodWaitingKeepsTheRunnersRestartRecord(t *testing.T) {
	const ready = `
scenario: ready-pods
arrows: [T-empty-create]
initial:
  spec: {replicas: 2, image: registry.example.com/runtime:v1}
  rows:
    - {index: 0, phase: Ready, runningRevision: current, readySince: 0s}
    - {index: 1, phase: Ready, runningRevision: current, readySince: 0s}
  pods:
    - {index: 0, ready: true, serving: true, routed: true}
    - {index: 1, ready: true, serving: true, routed: true}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	restarted := &PodRef{Index: 0}
	if _, err := applyContainerRestart(ctx, d, TimelineEvent{ID: "pod.containerRestart", Args: EventArgs{Pod: restarted, Message: "Error"}}); err != nil {
		t.Fatalf("restart: %v", err)
	}
	waiting := TimelineEvent{ID: "pod.waiting", Variant: "CrashLoopBackOff", Args: EventArgs{Pod: restarted, Message: "back-off restarting failed container"}}
	if _, err := applyPodWaiting(ctx, d, waiting); err != nil {
		t.Fatalf("waiting: %v", err)
	}
	pod, err := d.getPod(ctx, d.podName(*restarted))
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Status.ContainerStatuses) != 1 {
		t.Fatalf("one container status expected, got %+v", pod.Status.ContainerStatuses)
	}
	cs := pod.Status.ContainerStatuses[0]
	if cs.State.Waiting == nil || cs.State.Waiting.Reason != "CrashLoopBackOff" {
		t.Fatalf("the container must wait in CrashLoopBackOff, got %+v", cs.State)
	}
	if cs.RestartCount != 1 || cs.LastTerminationState.Terminated == nil || cs.LastTerminationState.Terminated.Reason != "Error" {
		t.Fatalf("the restart record must survive the waiting episode, got restartCount=%d lastState=%+v", cs.RestartCount, cs.LastTerminationState)
	}
	never := &PodRef{Index: 1}
	waiting.Args.Pod = never
	if _, err := applyPodWaiting(ctx, d, waiting); err != nil {
		t.Fatalf("waiting: %v", err)
	}
	pod, err = d.getPod(ctx, d.podName(*never))
	if err != nil {
		t.Fatal(err)
	}
	if cs := pod.Status.ContainerStatuses[0]; cs.RestartCount != 0 || cs.LastTerminationState.Terminated != nil {
		t.Fatalf("a container that never restarted parks with an empty record, got restartCount=%d lastState=%+v", cs.RestartCount, cs.LastTerminationState)
	}
}

// TestTerminalWaitTimer_CreateStillInFlight_NoGraceRuns checks the timer
// against the engine's terminal waiting set: a CreateContainerError for a
// create the runtime has not finished runs no grace, so the timer has
// nothing to land on, while a refused create runs one.
func TestTerminalWaitTimer_CreateStillInFlight_NoGraceRuns(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  config: {stuckPodGrace: 1m}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods:
    - {index: 0, ready: true, serving: true, routed: true}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	waiting := func(message string) {
		t.Helper()
		ev := TimelineEvent{ID: "pod.waiting", Variant: "CreateContainerError", Args: EventArgs{Pod: &PodRef{Index: 0}, Message: message}}
		if _, err := applyPodWaiting(ctx, d, ev); err != nil {
			t.Fatalf("waiting: %v", err)
		}
	}
	for _, tc := range []struct{ name, message, wantErr string }{
		{name: "a create still in flight runs no grace", message: "context deadline exceeded", wantErr: "no pod holds a terminal waiting reason"},
		{name: "a refused create runs the grace", message: "failed to create containerd container: error unpacking image: no space left on device"},
	} {
		waiting(tc.message)
		_, err := applyStuckPodGrace(ctx, d, TimelineEvent{Args: EventArgs{Slack: "1s"}})
		if (tc.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
			t.Fatalf("%s: got err %v, want %q", tc.name, err, tc.wantErr)
		}
	}
}

// TestApplyRunningNotReadyIsWhatTheEvidenceReads pins the kubelet's report
// of a pod that runs but fails its readiness probes against the engine's
// own readers: the pod is in readiness limbo, dark once the grace has run,
// the conditions carry the kubelet's reason and message, the serving gate
// stays the controller's, and a pod that never served measures its grace
// from its creation. The event refuses to run without a grace or a slack.
func TestApplyRunningNotReadyIsWhatTheEvidenceReads(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  config: {stuckPodGrace: 1m}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods:
    - {index: 0, ready: true, serving: true, routed: true}
    - {index: 0, ordinal: 1, phase: Pending}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	served := &PodRef{Index: 0}
	event := func(ref *PodRef, slack string) TimelineEvent {
		return TimelineEvent{ID: "pod.runningNotReady", Variant: "source", Args: EventArgs{Pod: ref, Slack: slack}}
	}
	if _, err := applyRunningNotReady(ctx, d, event(served, "")); err == nil || !strings.Contains(err.Error(), "needs a slack") {
		t.Fatalf("the slack is the scenario's to state: %v", err)
	}
	d.clock.Step(10 * time.Second)
	regressedAt := d.clock.Now()
	detail, err := applyRunningNotReady(ctx, d, event(served, "1s"))
	if err != nil {
		t.Fatalf("running not ready: %v", err)
	}
	pod, err := d.getPod(ctx, d.podName(*served))
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.PodRunningNotReady(pod) {
		t.Fatalf("the engine must read the pod as running but not ready: phase=%s conditions=%+v statuses=%+v", pod.Status.Phase, pod.Status.Conditions, pod.Status.ContainerStatuses)
	}
	if want := regressedAt.Add(time.Minute + time.Second); !d.clock.Now().Equal(want) {
		t.Fatalf("the clock lands on the end of the grace plus the slack: got %v, want %v (%s)", d.clock.Now(), want, detail)
	}
	if !evidence.PodUnreadyPastGrace(pod, d.clock.Now(), time.Minute) || !evidence.ContainersUnreadyPastGrace(pod, d.clock.Now(), time.Minute) {
		t.Fatalf("a served pod unready for the grace is dark to both readers: %+v", pod.Status.Conditions)
	}
	for _, condType := range []corev1.PodConditionType{corev1.ContainersReady, corev1.PodReady} {
		cond := findCondition(pod, condType)
		if cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != containersNotReadyReason || !strings.Contains(cond.Message, "[main]") {
			t.Fatalf("%s must carry the kubelet's report, got %+v", condType, cond)
		}
		if !cond.LastTransitionTime.Time.Equal(regressedAt) {
			t.Fatalf("%s transitioned at %v, want the regression instant %v", condType, cond.LastTransitionTime.Time, regressedAt)
		}
	}
	if gate := findCondition(pod, query.ServingConditionType); gate == nil || gate.Status != corev1.ConditionTrue {
		t.Fatalf("the serving gate is the controller's to write and stays True, got %+v", gate)
	}
	if cs := pod.Status.ContainerStatuses; len(cs) != 1 || cs[0].Ready || cs[0].State.Running == nil {
		t.Fatalf("the container keeps running and reads not ready: %+v", cs)
	}

	// A pod that never served has held ContainersReady False since its
	// creation, so its grace ran from there and has already elapsed.
	never := &PodRef{Index: 0, Ordinal: 1}
	before := d.clock.Now()
	detail, err = applyRunningNotReady(ctx, d, event(never, "1s"))
	if err != nil {
		t.Fatalf("never served: %v", err)
	}
	if !strings.Contains(detail, "unreadySince=t+0s") || !strings.Contains(detail, "already-elapsed") || !d.clock.Now().Equal(before) {
		t.Fatalf("a pod unready since its creation has had its grace: %s (clock %v, was %v)", detail, d.clock.Now(), before)
	}
	pod, err = d.getPod(ctx, d.podName(*never))
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.PodRunningNotReady(pod) || !evidence.ContainersUnreadyPastGrace(pod, d.clock.Now(), time.Minute) {
		t.Fatalf("the never-served pod reads running, not ready and past the grace: %+v", pod.Status)
	}

	d.cfg.StuckPodGrace = ""
	if _, err := applyRunningNotReady(ctx, d, event(served, "1s")); err == nil || !strings.Contains(err.Error(), "config.stuckPodGrace is not set") {
		t.Fatalf("no grace configured means no readiness grace runs: %v", err)
	}
}

// TestApplyMinReadyLandsOnTheWindow pins the Ready window against the
// promote bar's own reader: the clock lands where podreadiness.IsPodAvailable
// stops holding the earliest Ready pod back, plus the slack; a Component
// with no window and a set with no window running both refuse the event.
func TestApplyMinReadyLandsOnTheWindow(t *testing.T) {
	const windowed = `
scenario: windowed
arrows: [T-empty-create]
initial:
  spec: {replicas: 2, image: registry.example.com/runtime:v1, minReadySeconds: 60}
  rows:
    - {index: 0, phase: Ready, runningRevision: current, readySince: 0s}
    - {index: 1, phase: Ready, runningRevision: current, readySince: 0s}
  pods:
    - {index: 0, ready: true, serving: true, routed: true, readyAt: 30s}
    - {index: 1, ready: true, serving: true, routed: true}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, windowed)
	event := TimelineEvent{ID: "timer.minReady", Args: EventArgs{Slack: "1s"}}
	detail, err := applyMinReady(ctx, d, event)
	if err != nil {
		t.Fatalf("minReady: %v", err)
	}
	if want := d.opts.Start.Add(61 * time.Second); !d.clock.Now().Equal(want) || !strings.Contains(detail, "pod=svc-a-engine-1-default-0") {
		t.Fatalf("the earliest window ends on the pod Ready at the start: clock %v want %v (%s)", d.clock.Now(), want, detail)
	}
	pod, err := d.getPod(ctx, d.podName(PodRef{Index: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if available, _ := podreadiness.IsPodAvailable(pod, 60, d.clock.Now()); !available {
		t.Fatalf("the promote bar must read the pod Available once the clock has landed")
	}
	if detail, err = applyMinReady(ctx, d, event); err != nil || !strings.Contains(detail, "pod=svc-a-engine-0-default-0") {
		t.Fatalf("the later window is next: %s %v", detail, err)
	}
	if _, err := applyMinReady(ctx, d, event); err == nil || !strings.Contains(err.Error(), "no Ready pod is inside") {
		t.Fatalf("with every window elapsed nothing is running: %v", err)
	}
	d.spec.MinReadySeconds = 0
	if _, err := applyMinReady(ctx, d, event); err == nil || !strings.Contains(err.Error(), "spec.minReadySeconds is not set") {
		t.Fatalf("no window configured means no window runs: %v", err)
	}
}

// TestApplyExpectationsTTLIsTheCachesOwnDeadline pins the expectation clock
// against the engine's cache: the clock lands where the cache stops waiting
// for a delete it never saw land, read through the cache's own Satisfied
// and the instant the pass recorded it at, plus the slack; a cache that
// waits for nothing, and a zero slack, refuse the event.
func TestApplyExpectationsTTLIsTheCachesOwnDeadline(t *testing.T) {
	const ready = `
scenario: ready-pod
arrows: [T-empty-create]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods:
    - {index: 0, ready: true, serving: true, routed: true}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, ready)
	event := TimelineEvent{ID: "timer.expectationsTTL", Args: EventArgs{Slack: "1s"}}
	if _, err := applyExpectationsTTL(ctx, d, event); err == nil || !strings.Contains(err.Error(), "waits for nothing") {
		t.Fatalf("a cache with nothing outstanding runs no clock: %v", err)
	}
	// A pass that deletes a pod the cluster keeps Terminating leaves the
	// cache waiting on that Instance.
	d.clock.Step(5 * time.Second)
	d.notePassClock()
	stamp := d.clock.Now()
	d.expectations.ExpectDeletes(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0, 1)
	if _, err := applyExpectationsTTL(ctx, d, TimelineEvent{ID: "timer.expectationsTTL", Args: EventArgs{Slack: "0s"}}); err == nil || !strings.Contains(err.Error(), "positive slack") {
		t.Fatalf("the cache stops waiting strictly after its deadline: %v", err)
	}
	detail, err := applyExpectationsTTL(ctx, d, event)
	if err != nil {
		t.Fatalf("expectationsTTL: %v", err)
	}
	if !strings.Contains(detail, "waiting=0") {
		t.Fatalf("the detail names the Instance waited on: %s", detail)
	}
	deadline := d.clock.Now().Add(-time.Second)
	if !deadline.After(stamp) {
		t.Fatalf("the deadline lies after the pass that recorded the expectation: %v, pass at %v", deadline, stamp)
	}
	// An independent cache records the same expectation at the same instant:
	// it still waits at the resolved deadline and not one nanosecond
	// past it, so the instant is the cache's own and not the driver's.
	clk := clocktesting.NewFakeClock(stamp)
	cache := types.NewExpectationsWithClock(clk)
	cache.ExpectDeletes(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0, 1)
	clk.SetTime(deadline)
	if cache.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("the cache still waits at the deadline %v", deadline)
	}
	clk.SetTime(deadline.Add(time.Nanosecond))
	if !cache.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("the cache stops waiting one nanosecond past the deadline %v", deadline)
	}
	if !d.expectations.Satisfied(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, 0) {
		t.Fatalf("the engine's own cache reads the expectation as satisfied at the landed clock")
	}
}

// TestApplyGangWidthIsARevisionChange pins the width edit against the
// engine's own hashing: a worker count that moves mints a new revision, a
// count that stays is refused, and a Component without workers has no width
// to edit — a worker added to it is a template change, staged as
// spec.revision.
func TestApplyGangWidthIsARevisionChange(t *testing.T) {
	const gang = `
scenario: gang
arrows: [T-empty-create]
initial:
  spec:
    replicas: 1
    workers: 1
    topologyKey: topology.example.com/fabric-domain
    image: registry.example.com/runtime:v1
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods:
    - {index: 0, runner: leader, ready: true, serving: true, routed: true}
    - {index: 0, runner: worker, ready: true, serving: true}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, gang)
	before, err := d.revisionFor(ctx, "current")
	if err != nil {
		t.Fatal(err)
	}
	edit := func(to int32) (string, error) {
		return applyGangWidth(ctx, d, TimelineEvent{ID: "spec.gangWidth", Variant: "grow", Args: EventArgs{To: &to}})
	}
	detail, err := edit(2)
	if err != nil || detail != "workers=2 generation=1" {
		t.Fatalf("a width grow is a spec write: (%q, %v)", detail, err)
	}
	after, err := d.revisionFor(ctx, "current")
	if err != nil {
		t.Fatal(err)
	}
	if after == before {
		t.Fatalf("a width edit is a template change: the revision must move, both widths resolve to %s", before)
	}
	if _, err := edit(2); err == nil || !strings.Contains(err.Error(), "did not change") {
		t.Fatalf("a width that stays is not an edit: %v", err)
	}
	if d.spec.Workers != 2 {
		t.Fatalf("a refused edit must leave the template as it was: workers=%d", d.spec.Workers)
	}

	const single = `
scenario: single
arrows: [T-empty-create]
initial:
  spec:
    replicas: 1
    image: registry.example.com/runtime:v1
  rows: [{index: 0, phase: Ready, runningRevision: current, readySince: 0s}]
  pods: [{index: 0, ready: true, serving: true, routed: true}]
timeline:
  - tick: 1
`
	s := mustDriver(t, single)
	one := int32(1)
	if _, err := applyGangWidth(ctx, s, TimelineEvent{ID: "spec.gangWidth", Variant: "grow", Args: EventArgs{To: &one}}); err == nil || !strings.Contains(err.Error(), "spec.revision") {
		t.Fatalf("a Component without workers has no width; the first worker is staged as spec.revision: %v", err)
	}
	if s.spec.Workers != 0 {
		t.Fatalf("a refused edit must leave the template as it was: workers=%d", s.spec.Workers)
	}
}

// TestApplyGateDenyStagesADrainAnswerForAPodAndAStartAnswerForEveryStart
// keeps gate.deny an honest answer: the Pairing hold is the drain-time
// answer, it names a pod and carries the reason the gate would give; a
// start-time answer (Ratio, Sequential, Budget) names no pod and stands
// for every fresh start; a hold with no reason, a start-time answer with
// a pod, or an answer of the plan gate or the retry record fails the run.
func TestApplyGateDenyStagesADrainAnswerForAPodAndAStartAnswerForEveryStart(t *testing.T) {
	d := &driver{opts: DefaultOptions()}
	pod := &PodRef{Index: 0, Ordinal: 1}
	for _, variant := range []string{"RetryBlock", "Held", "Plan"} {
		if _, err := applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: variant, Args: EventArgs{Message: "x"}}); err == nil || !strings.Contains(err.Error(), "can be staged") {
			t.Fatalf("%s: expected the cannot-be-staged error, got %v", variant, err)
		}
	}
	if _, err := applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: "Pairing", Args: EventArgs{Message: "x"}}); err == nil || !strings.Contains(err.Error(), "needs a pod reference") {
		t.Fatalf("expected the missing-pod error, got %v", err)
	}
	if _, err := applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: "Pairing", Args: EventArgs{Pod: pod}}); err == nil || !strings.Contains(err.Error(), "needs the reason") {
		t.Fatalf("expected the missing-reason error, got %v", err)
	}
	if _, err := applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: "Ratio", Args: EventArgs{Pod: pod, Message: "x"}}); err == nil || !strings.Contains(err.Error(), "names no pod") {
		t.Fatalf("expected the no-pod error for a start-time answer, got %v", err)
	}
	detail, err := applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: "Pairing", Args: EventArgs{Pod: pod, Message: "the other side still pairs with it"}})
	if err != nil {
		t.Fatalf("stage a Pairing hold: %v", err)
	}
	name := d.podName(*pod)
	if detail != "pod="+name || d.pairingHolds[name] != "the other side still pairs with it" {
		t.Fatalf("the hold must be recorded under the pod's name with its reason, got detail=%q holds=%v", detail, d.pairingHolds)
	}
	detail, err = applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: "Ratio", Args: EventArgs{Message: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"}})
	if err != nil {
		t.Fatalf("stage a start-time hold: %v", err)
	}
	if detail != "gate=Ratio" || d.startHold.gate != types.RolloutHoldGateRatio || d.startHold.reason != "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)" {
		t.Fatalf("the start-time hold must be recorded with its gate and reason, got detail=%q hold=%+v", detail, d.startHold)
	}
}

// TestApplyGateAllowEndsAStagedHoldOnly keeps gate.allow an answer about a
// hold that exists: ending a hold no pod carries, or a start-time hold
// none staged, fails the run, and ending a staged one removes it.
func TestApplyGateAllowEndsAStagedHoldOnly(t *testing.T) {
	d := &driver{opts: DefaultOptions()}
	pod := &PodRef{Index: 0}
	if _, err := applyGateAllow(context.Background(), d, TimelineEvent{ID: "gate.allow", Args: EventArgs{Pod: pod}}); err == nil || !strings.Contains(err.Error(), "no Pairing hold to lift") {
		t.Fatalf("expected the no-hold error, got %v", err)
	}
	if _, err := applyGateAllow(context.Background(), d, TimelineEvent{ID: "gate.allow"}); err == nil || !strings.Contains(err.Error(), "no start-time hold") {
		t.Fatalf("expected the no-start-time-hold error, got %v", err)
	}
	if _, err := applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: "Pairing", Args: EventArgs{Pod: pod, Message: "held"}}); err != nil {
		t.Fatalf("stage a Pairing hold: %v", err)
	}
	if _, err := applyGateDeny(context.Background(), d, TimelineEvent{ID: "gate.deny", Variant: "Budget", Args: EventArgs{Message: "held"}}); err != nil {
		t.Fatalf("stage a start-time hold: %v", err)
	}
	if _, err := applyGateAllow(context.Background(), d, TimelineEvent{ID: "gate.allow", Args: EventArgs{Pod: pod}}); err != nil {
		t.Fatalf("end the pairing hold: %v", err)
	}
	if len(d.pairingHolds) != 0 || d.startHold.reason == "" {
		t.Fatalf("the pairing hold must be gone and the start-time hold kept, got %v and %+v", d.pairingHolds, d.startHold)
	}
	if detail, err := applyGateAllow(context.Background(), d, TimelineEvent{ID: "gate.allow"}); err != nil || detail != "gate=Budget" {
		t.Fatalf("end the start-time hold: detail=%q err=%v", detail, err)
	}
	if d.startHold.reason != "" {
		t.Fatalf("the start-time hold must be gone, got %+v", d.startHold)
	}
}

// TestApplySpecRevisionRenderedFromTheRuntimeKeepsTheOwnerGeneration holds
// what a runtime render changes about a template edit: the template moves
// and the owner's generation does not, on the driver's spec and on the
// stored owner alike; a write of the spec moves it, and a source other than
// the runtime is refused.
func TestApplySpecRevisionRenderedFromTheRuntimeKeepsTheOwnerGeneration(t *testing.T) {
	ctx := context.Background()
	d := mustDriver(t, minimalScenario)
	before := d.owner.Generation
	detail, err := applySpecRevision(ctx, d, TimelineEvent{ID: "spec.revision", Args: EventArgs{Image: "registry.example.com/runtime:v2", RenderedFrom: "runtime"}})
	if err != nil || detail != fmt.Sprintf("image=registry.example.com/runtime:v2 renderedFrom=runtime generation=%d", before) {
		t.Fatalf("runtime render: (%q, %v)", detail, err)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := d.cli.Get(ctx, client.ObjectKeyFromObject(d.owner), stored); err != nil {
		t.Fatal(err)
	}
	if d.spec.Image != "registry.example.com/runtime:v2" || d.owner.Generation != before || stored.Generation != before {
		t.Fatalf("image=%s generation=%d stored=%d, want the image moved at generation %d", d.spec.Image, d.owner.Generation, stored.Generation, before)
	}
	detail, err = applySpecRevision(ctx, d, TimelineEvent{ID: "spec.revision", Args: EventArgs{Image: "registry.example.com/runtime:v3"}})
	if err != nil || detail != fmt.Sprintf("image=registry.example.com/runtime:v3 generation=%d", before+1) {
		t.Fatalf("spec write: (%q, %v)", detail, err)
	}
	if _, err := applySpecRevision(ctx, d, TimelineEvent{ID: "spec.revision", Args: EventArgs{Image: "registry.example.com/runtime:v4", RenderedFrom: "operator"}}); err == nil || !strings.Contains(err.Error(), "renderedFrom is runtime or absent") {
		t.Fatalf("unknown source: %v", err)
	}
}

// TestApplyCtrlCrashDropsTheExpectationsAndWaitsForTheLease holds the
// restart to the two things it is: the expectations cache is gone, so a
// pending delete reads as satisfied, and the clock moves by the Lease wait
// the scenario names, and by nothing when it names none.
func TestApplyCtrlCrashDropsTheExpectationsAndWaitsForTheLease(t *testing.T) {
	opts := DefaultOptions()
	clk := clocktesting.NewFakeClock(opts.Start)
	d := &driver{opts: opts, clock: clk, expectations: types.NewExpectationsWithClock(clk)}
	d.expectations.ExpectDeletes(opts.Namespace, opts.OwnerName, opts.Component, 0, 1)
	if d.expectations.Satisfied(opts.Namespace, opts.OwnerName, opts.Component, 0) {
		t.Fatal("a pending delete reads as unsatisfied before the restart")
	}

	detail, err := applyCtrlCrash(context.Background(), d, TimelineEvent{ID: "ctrl.crash"})
	if err != nil || detail != "expectations=dropped" {
		t.Fatalf("restart with no Lease wait: detail %q, err %v", detail, err)
	}
	if !clk.Now().Equal(opts.Start) {
		t.Fatalf("a restart with no Lease wait moved the clock to %s", clk.Now())
	}
	if !d.expectations.Satisfied(opts.Namespace, opts.OwnerName, opts.Component, 0) {
		t.Fatal("the restarted engine still waits on a delete the stopped manager issued")
	}

	detail, err = applyCtrlCrash(context.Background(), d, TimelineEvent{ID: "ctrl.crash", Args: EventArgs{LeaseWait: "1m"}})
	if err != nil || detail != "expectations=dropped leaseWait=1m0s" {
		t.Fatalf("restart with a Lease wait: detail %q, err %v", detail, err)
	}
	if got := clk.Now().Sub(opts.Start); got != time.Minute {
		t.Fatalf("the Lease wait moved the clock by %s, want 1m", got)
	}

	_, err = applyCtrlCrash(context.Background(), d, TimelineEvent{ID: "ctrl.crash", Args: EventArgs{LeaseWait: "-1s"}})
	if err == nil || !strings.Contains(err.Error(), "does not run backwards") {
		t.Fatalf("a negative Lease wait: %v", err)
	}
	_, err = applyCtrlCrash(context.Background(), d, TimelineEvent{ID: "ctrl.crash", Variant: "hard"})
	if err == nil || !strings.Contains(err.Error(), "takes no variant") {
		t.Fatalf("a restart with a variant: %v", err)
	}
}

// TestApplySpecStrategyRewritesTheTypeOrABudget states what a spec.strategy
// edit writes: the strategy type, a rollingUpdate budget under the same
// type, or both, each read by the next pass through the desired spec, and
// each a write of the owner spec that moves its generation. The trace
// names only the fields the edit carried, so an edit of the type alone
// reads as it always has.
func TestApplySpecStrategyRewritesTheTypeOrABudget(t *testing.T) {
	ctx := context.Background()
	d := mustDriver(t, minimalScenario)
	edit := func(args EventArgs) (string, error) {
		return applySpecStrategy(ctx, d, TimelineEvent{ID: "spec.strategy", Args: args})
	}
	text, err := edit(EventArgs{MaxSurge: "25%"})
	if err != nil {
		t.Fatalf("budget edit: %v", err)
	}
	if text != "maxSurge=25% generation=1" || d.spec.MaxSurge != "25%" || d.spec.Strategy != "" {
		t.Fatalf("a surge budget edit rewrites the budget alone: text %q, spec %+v", text, d.spec)
	}
	desired, err := d.desiredSpec()
	if err != nil {
		t.Fatalf("desiredSpec: %v", err)
	}
	if rolling := desired.Lifecycle.UpdateStrategy.RollingUpdate; rolling == nil || rolling.MaxSurge == nil || rolling.MaxSurge.String() != "25%" {
		t.Fatalf("the next pass reads the edited budget off the desired spec, got %+v", desired.Lifecycle.UpdateStrategy)
	}
	text, err = edit(EventArgs{Strategy: "RecreatePod", MaxUnavailable: "2"})
	if err != nil {
		t.Fatalf("type and budget edit: %v", err)
	}
	if text != "strategy=RecreatePod maxUnavailable=2 generation=2" || d.spec.Strategy != "RecreatePod" || d.spec.MaxUnavailable != "2" || d.spec.MaxSurge != "25%" {
		t.Fatalf("an edit of the type beside a drain budget keeps the other budget: text %q, spec %+v", text, d.spec)
	}
	text, err = edit(EventArgs{Strategy: "SurgeThenDrain"})
	if err != nil || text != "strategy=SurgeThenDrain generation=3" {
		t.Fatalf("an edit of the type alone reads as it always has: text %q, err %v", text, err)
	}
	for _, tc := range []struct {
		args EventArgs
		want string
	}{
		{EventArgs{}, "needs the new strategy, maxSurge or maxUnavailable"},
		{EventArgs{MaxUnavailable: "many"}, "not an integer or a percent"},
		{EventArgs{MaxSurge: "-1"}, "not an integer or a percent"},
	} {
		if _, err := edit(tc.args); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%+v: want an error naming %q, got %v", tc.args, tc.want, err)
		}
	}
	if d.owner.Generation != 3 {
		t.Fatalf("a refused edit moves no generation: got %d", d.owner.Generation)
	}
}

// TestApplyAdmissionUnavailableIsWhatTheEngineClassifierReads holds the
// outage against the engine's own classifier: a standing outage refuses
// every matching create and nothing else, the error reads as an admission
// outage and not as a quota or a throttle, the restore clears it and
// reports what it cleared, a restore of nothing fails, and a counted
// outage ends on its own after as many refusals.
func TestApplyAdmissionUnavailableIsWhatTheEngineClassifierReads(t *testing.T) {
	const settled = `
scenario: settled-pods
cells: [instance/Ready/ctrl.resync/before]
initial:
  spec: {replicas: 1, image: registry.example.com/runtime:v1}
  rows:
    - {index: 0, phase: Ready, runningRevision: current}
  pods:
    - {index: 0, ready: true}
timeline:
  - tick: 1
`
	ctx := context.Background()
	d := mustDriver(t, settled)
	outage := TimelineEvent{ID: "api.admissionUnavailable"}
	detail, err := applyAdmissionUnavailable(ctx, d, outage)
	if err != nil || detail != "verb=create resource=pods until=api.admissionRestored" {
		t.Fatalf("staging the outage: %q, %v", detail, err)
	}
	for i := 0; i < 3; i++ {
		err, label := d.takeRejection("create", &corev1.Pod{})
		if err == nil || label != "api.admissionUnavailable" {
			t.Fatalf("create %d under the outage: err=%v label=%q, want every create refused", i+1, err, label)
		}
		if class := evidence.ClassifyAPIError(err).Class; class != types.APIRejectionAdmissionUnavailable {
			t.Fatalf("the engine reads the refusal as %v, want an admission outage", class)
		}
	}
	if err, _ := d.takeRejection("delete", &corev1.Pod{}); err != nil {
		t.Fatalf("a delete is no write a pod webhook sees: %v", err)
	}
	detail, err = applyAdmissionRestored(ctx, d, TimelineEvent{ID: "api.admissionRestored"})
	if err != nil || detail != "cleared=1" {
		t.Fatalf("the restore: %q, %v", detail, err)
	}
	if err, _ := d.takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a create after the restore: %v", err)
	}
	if _, err := applyAdmissionRestored(ctx, d, TimelineEvent{ID: "api.admissionRestored"}); err == nil || !strings.Contains(err.Error(), "no admission outage stands") {
		t.Fatalf("a restore of nothing must fail the run: %v", err)
	}
	counted := TimelineEvent{ID: "api.admissionUnavailable", Args: EventArgs{Count: 2}}
	if detail, err := applyAdmissionUnavailable(ctx, d, counted); err != nil || detail != "verb=create resource=pods count=2" {
		t.Fatalf("staging a counted outage: %q, %v", detail, err)
	}
	for i := 0; i < 2; i++ {
		if err, _ := d.takeRejection("create", &corev1.Pod{}); err == nil {
			t.Fatalf("create %d of a counted outage was not refused", i+1)
		}
	}
	if err, _ := d.takeRejection("create", &corev1.Pod{}); err != nil {
		t.Fatalf("a counted outage ends after its count: %v", err)
	}
}
