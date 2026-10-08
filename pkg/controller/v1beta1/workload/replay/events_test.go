package replay

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
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
