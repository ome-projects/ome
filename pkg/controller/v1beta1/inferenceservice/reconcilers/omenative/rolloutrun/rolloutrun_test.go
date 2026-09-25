package rolloutrun

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

const (
	oldRev = "llm-a-engine-aaaaaaaa"
	newRev = "llm-a-engine-bbbbbbbb"
)

func canaryBody(steps ...int32) *v1beta1.GroupCanary {
	c := &v1beta1.GroupCanary{}
	for _, traffic := range steps {
		capacity := intstr.FromString("100%")
		if traffic < 100 {
			capacity = intstr.FromString("50%")
		}
		c.Steps = append(c.Steps, v1beta1.RolloutGroupStep{Capacity: capacity, Traffic: traffic})
	}
	return c
}

func isvcFixture(group v1beta1.RolloutGroup) *v1beta1.InferenceService {
	one := 1
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a", Namespace: "ns"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}},
			Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}},
		},
	}
}

// irFixture publishes a consistent IR snapshot; target != current is the
// divergence that opens a run.
func irFixture(current, target string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a-engine", Namespace: "ns", Generation: 1},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 1,
			CurrentRevision:    current,
			UpdateRevision:     target,
		},
	}
}

func testInputs(t *testing.T, isvc *v1beta1.InferenceService, objects ...runtime.Object) Inputs {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	// Seed the ISVC itself so annotation consumption (a live Update) works,
	// and read it back so the local pointer carries the server's resource
	// version.
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(append([]runtime.Object{isvc}, objects...)...).Build()
	status := isvc.Status.DeepCopy()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), isvc); err != nil {
		t.Fatal(err)
	}
	isvc.Status = *status
	return Inputs{
		Client:         c,
		Reader:         c,
		ISVC:           isvc,
		Now:            time.Unix(1000, 0),
		FeatureEnabled: true,
		BoundProviders: map[string]struct{}{"cluster-prometheus": {}},
	}
}

func planReady(isvc *v1beta1.InferenceService) *apis.Condition {
	return isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanReadyCondition))
}

func TestOpenPinsInlinePlan(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev))

	out, err := Reconcile(context.Background(), in)
	if err != nil || out.Parked {
		t.Fatalf("unexpected: %v %+v", err, out)
	}
	if !out.Opened || out.Adopted {
		t.Fatalf("new rollout boundary = %+v, want fresh open", out)
	}
	active := isvc.Status.Rollout.ActiveRun
	if active == nil || len(active.Plan.Groups) != 1 {
		t.Fatalf("run not pinned: %+v", isvc.Status.Rollout)
	}
	g := active.Plan.Groups[0]
	if g.Source != v1beta1.RolloutPlanSourceInline || g.Group.Canary == nil || !strings.HasPrefix(g.PortableDigest, "rp1:") {
		t.Fatalf("pinned group = %+v", g)
	}
	if len(active.TargetRevisions) != 1 || active.TargetRevisions[0].Revision != "bbbbbbbb" ||
		active.TargetRevisions[0].StableRevision != "aaaaaaaa" {
		t.Fatalf("targets = %+v", active.TargetRevisions)
	}
	if cond := planReady(isvc); cond == nil || cond.Status != corev1.ConditionTrue || cond.Reason != v1beta1.RolloutPlanReasonPinned {
		t.Fatalf("condition = %+v", cond)
	}
}

func TestOpenPinsUnchangedGroupMemberAsStable(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	router := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a-router", Namespace: "ns", Generation: 1},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 1,
			CurrentRevision:    "llm-a-router-rrrrrrrr",
			UpdateRevision:     "llm-a-router-rrrrrrrr",
		},
	}
	in := testInputs(t, isvc, router, irFixture(oldRev, newRev))

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	got := map[v1beta1.ComponentType]v1beta1.RolloutRunTarget{}
	for _, target := range isvc.Status.Rollout.ActiveRun.TargetRevisions {
		got[target.Component] = target
	}
	if got[v1beta1.RouterComponent].Revision != "rrrrrrrr" || got[v1beta1.RouterComponent].StableRevision != "rrrrrrrr" {
		t.Fatalf("unchanged router identity = %+v", got[v1beta1.RouterComponent])
	}
	if got[v1beta1.EngineComponent].Revision != "bbbbbbbb" || got[v1beta1.EngineComponent].StableRevision != "aaaaaaaa" {
		t.Fatalf("changed engine identity = %+v", got[v1beta1.EngineComponent])
	}
}

func TestRefParksOnMissingPolicy(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "canary-std-v1", Progression: v1beta1.RolloutProgressionCanary},
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev))

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Parked || v1beta1.RolloutRunActive(isvc) {
		t.Fatalf("must park with no run: %+v active=%v", out, v1beta1.RolloutRunActive(isvc))
	}
	if cond := planReady(isvc); cond == nil || cond.Status != corev1.ConditionFalse || cond.Reason != v1beta1.RolloutPlanReasonPolicyNotFound {
		t.Fatalf("condition = %+v", cond)
	}
}

func TestRefParksOnProgressionMismatch(t *testing.T) {
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "bg-std-v1", Namespace: "ns"},
		Spec:       v1beta1.RolloutPolicySpec{BlueGreen: &v1beta1.GroupBlueGreen{}},
	}
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "bg-std-v1", Progression: v1beta1.RolloutProgressionCanary},
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev), policy)

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Parked {
		t.Fatalf("mismatch must park: %+v", out)
	}
	if cond := planReady(isvc); cond == nil || cond.Reason != v1beta1.RolloutPlanReasonProgressionMismatch {
		t.Fatalf("condition = %+v", cond)
	}
}

func TestRefPinsPolicyBody(t *testing.T) {
	body := canaryBody(10, 100)
	body.Prometheus = &v1beta1.AnalysisPrometheus{ProviderRef: &v1beta1.MetricProviderRef{Name: "cluster-prometheus"}}
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-std-v1", Namespace: "ns", Generation: 4},
		Spec:       v1beta1.RolloutPolicySpec{Canary: body},
	}
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "canary-std-v1", Progression: v1beta1.RolloutProgressionCanary},
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev), policy)

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	active := isvc.Status.Rollout.ActiveRun
	if active == nil {
		t.Fatal("run not pinned")
	}
	g := active.Plan.Groups[0]
	if g.Source != v1beta1.RolloutPlanSourcePolicy || g.PolicyRef == nil || g.PolicyGeneration != 4 {
		t.Fatalf("provenance = %+v", g)
	}
	if g.Group.PolicyRef != nil || g.Group.Canary == nil || len(g.Group.Canary.Steps) != 2 {
		t.Fatalf("pinned group must carry the resolved body, no ref: %+v", g.Group)
	}
}

func TestUnboundProviderParks(t *testing.T) {
	body := canaryBody(10, 100)
	body.Steps[0].Analysis = &v1beta1.RolloutAnalysis{
		Interval:     metav1.Duration{Duration: time.Minute},
		FailureLimit: 1,
		Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "1"}},
	}
	body.Prometheus = &v1beta1.AnalysisPrometheus{ProviderRef: &v1beta1.MetricProviderRef{Name: "unbound"}}
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-std-v1", Namespace: "ns"},
		Spec:       v1beta1.RolloutPolicySpec{Canary: body},
	}
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "canary-std-v1", Progression: v1beta1.RolloutProgressionCanary},
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev), policy)

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Parked {
		t.Fatal("unbound provider must park at open — the gate could never sample")
	}
	if cond := planReady(isvc); cond == nil || cond.Reason != v1beta1.RolloutPlanReasonProviderUnbound {
		t.Fatalf("condition = %+v", cond)
	}
}

func TestMidRunEditIsInertWithDrift(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	pinnedDigest := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest

	// Edit the live ladder mid-run: the pinned plan must not move.
	isvc.Spec.Rollout.Groups[0].Canary = canaryBody(50, 100)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest; got != pinnedDigest {
		t.Fatalf("pinned plan drifted: %s -> %s", pinnedDigest, got)
	}
	drift := isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanDriftCondition))
	if drift == nil || drift.Status != corev1.ConditionTrue || drift.Reason != v1beta1.RolloutPlanDriftReasonSpecNewerThanRun {
		t.Fatalf("drift = %+v", drift)
	}
}

func TestRetargetClosesSupersededAndReopens(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	ir := irFixture(oldRev, newRev)
	in := testInputs(t, isvc, ir)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	firstID := isvc.Status.Rollout.ActiveRun.RunID

	// Retarget: the IR now points at a third revision; pending edits land in
	// the fresh run. Rebuild the reader with the updated IR snapshot.
	ir.Status.UpdateRevision = "llm-a-engine-cccccccc"
	isvc.Spec.Rollout.Groups[0].Canary = canaryBody(25, 100)
	in = testInputs(t, isvc, ir)
	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	active := isvc.Status.Rollout.ActiveRun
	if active == nil || active.RunID == firstID {
		t.Fatalf("retarget must open a fresh run: %+v", active)
	}
	if !out.Opened || out.Adopted {
		t.Fatalf("retarget boundary = %+v, want fresh open rather than adoption", out)
	}
	if isvc.Status.Rollout.LastRun == nil || isvc.Status.Rollout.LastRun.Outcome != v1beta1.RolloutRunSuperseded {
		t.Fatalf("lastRun = %+v", isvc.Status.Rollout.LastRun)
	}
	if got := active.TargetRevisions[0].StableRevision; got != "aaaaaaaa" {
		t.Fatalf("retargeted run stable revision = %q, want original stable %q", got, "aaaaaaaa")
	}
	if got := isvc.Status.Rollout.LastRun.TargetRevisions[0].StableRevision; got != "aaaaaaaa" {
		t.Fatalf("superseded record stable revision = %q, want %q", got, "aaaaaaaa")
	}
	if active.Plan.Groups[0].Group.Canary.Steps[0].Traffic != 25 {
		t.Fatal("fresh run must render the edited plan")
	}
}

func TestStatusLossRecoveryReportsAdoption(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "bbbbbbbb",
		StableRevisionHash: "aaaaaaaa",
		CurrentStep:        0,
	}
	in := testInputs(t, isvc, irFixture(oldRev, newRev))

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Opened || !out.Adopted {
		t.Fatalf("status-loss recovery boundary = %+v, want adopted open", out)
	}
}

func TestRetargetDoesNotGuessMissingStableRevision(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	ir := irFixture(oldRev, newRev)
	in := testInputs(t, isvc, ir)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	firstID := isvc.Status.Rollout.ActiveRun.RunID
	isvc.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = ""
	ir.Status.CurrentRevision = newRev
	ir.Status.UpdateRevision = "llm-a-engine-cccccccc"

	in = testInputs(t, isvc, ir)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	active := isvc.Status.Rollout.ActiveRun
	if active.RunID == firstID {
		t.Fatal("the changed target must supersede the legacy active run")
	}
	if got := active.TargetRevisions[0].StableRevision; got != "" {
		t.Fatalf("unknown original stable revision must remain unset, got %q", got)
	}
}

func TestCompletedCloseKeepsBoundedRecord(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	})
	ir := irFixture(oldRev, newRev)
	in := testInputs(t, isvc, ir)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if !v1beta1.RolloutRunActive(isvc) {
		t.Fatal("run must open on divergence")
	}

	// Convergence: the IR fully lands on the pinned target.
	ir.Status.CurrentRevision = newRev
	in = testInputs(t, isvc, ir)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if v1beta1.RolloutRunActive(isvc) {
		t.Fatal("converged run must close")
	}
	last := isvc.Status.Rollout.LastRun
	if last == nil || last.Outcome != v1beta1.RolloutRunCompleted || len(last.Groups) != 1 {
		t.Fatalf("lastRun = %+v", last)
	}
	if len(last.TargetRevisions) != 1 || last.TargetRevisions[0].Revision != "bbbbbbbb" ||
		last.TargetRevisions[0].StableRevision != "aaaaaaaa" {
		t.Fatalf("lastRun targets = %+v", last.TargetRevisions)
	}
}

func TestRepinCASAndClamp(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 30, 100),
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	// Mid-ladder with 30% programmed; the operator shrinks the ladder and
	// repins. The clamped index lands on the final 100% step, which must NOT
	// program traffic — pre-step hold instead.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "bbbbbbbb", CurrentStep: 2, ObservedTrafficWeight: 30}
	isvc.Spec.Rollout.Groups[0].Canary = canaryBody(100)

	// CAS mismatch first: a stale expected digest is rejected and consumed.
	isvc.Annotations = map[string]string{constants.RolloutRepinAnnotation: "rp1:stale"}
	pinnedBefore := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest; got != pinnedBefore {
		t.Fatal("CAS-mismatched repin must not replace the plan")
	}
	if _, still := isvc.Annotations[constants.RolloutRepinAnnotation]; still {
		t.Fatal("rejected repin must consume the annotation")
	}

	// "now" applies: plan replaced, step clamped, pre-step hold armed
	// (clamped step traffic 100 > programmed 30).
	isvc.Annotations[constants.RolloutRepinAnnotation] = "now"
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	active := isvc.Status.Rollout.ActiveRun
	if len(active.Plan.Groups[0].Group.Canary.Steps) != 1 {
		t.Fatalf("plan not replaced: %+v", active.Plan.Groups[0].Group.Canary)
	}
	if isvc.Status.Canary.CurrentStep != 0 || !isvc.Status.Canary.PreStepHold {
		t.Fatalf("clamp = step %d hold %v; want step 0 with pre-step hold",
			isvc.Status.Canary.CurrentStep, isvc.Status.Canary.PreStepHold)
	}
}

func TestResolutionViewPublishesShadowedRef(t *testing.T) {
	policy := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-std-v1", Namespace: "ns"},
		Spec:       v1beta1.RolloutPolicySpec{Canary: canaryBody(10, 100)},
	}
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "canary-std-v1", Progression: v1beta1.RolloutProgressionCanary},
	})
	in := testInputs(t, isvc, irFixture(oldRev, oldRev), policy)

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	view := isvc.Status.Rollout.Groups
	if len(view) != 1 || view[0].Source != v1beta1.RolloutPlanSourceInline {
		t.Fatalf("view = %+v", view)
	}
	shadow := view[0].ShadowedPolicyRef
	if shadow == nil || shadow.Name != "canary-std-v1" || shadow.WouldPinDigest == "" {
		t.Fatalf("shadow preview missing: %+v", shadow)
	}
	// The inline body equals the policy body, so the preview digest must
	// equal the observed inline digest — the migration no-op proof.
	if shadow.WouldPinDigest != view[0].ObservedDigest {
		t.Fatalf("identical bodies must digest equal: %s vs %s", shadow.WouldPinDigest, view[0].ObservedDigest)
	}
}

func TestFeatureDisabledRefParks(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "canary-std-v1", Progression: v1beta1.RolloutProgressionCanary},
	})
	in := testInputs(t, isvc, irFixture(oldRev, newRev))
	in.FeatureEnabled = false

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Parked {
		t.Fatal("a stored ref on a non-feature cluster must park, never default-blueGreen")
	}
	if cond := planReady(isvc); cond == nil || cond.Reason != v1beta1.RolloutPlanReasonPlanInvalid {
		t.Fatalf("condition = %+v", cond)
	}
}

// The client.Object interface is exercised via consumeAnnotation on the live
// object; the fake client needs the object to exist for the repin test.
var _ = client.ObjectKeyFromObject

// A revert to the current revision mid-roll leaves stragglers with no
// target divergence — the straggler counters must still open a run, or the
// plan gate would hold the straggler updates forever.
func TestStragglersOpenARun(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	})
	ir := irFixture(oldRev, oldRev)
	ir.Status.Replicas = 2
	ir.Status.UpdatedReplicas = 1
	in := testInputs(t, isvc, ir)

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if !v1beta1.RolloutRunActive(isvc) {
		t.Fatal("stragglers (updated < replicas) must open a run even with target == current")
	}

	// Converged counters close it.
	ir.Status.UpdatedReplicas = 2
	in = testInputs(t, isvc, ir)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if v1beta1.RolloutRunActive(isvc) {
		t.Fatal("counter convergence must close the run")
	}
}

// The sticky-rejected canary target must not re-open runs: after a rollback
// hold, the IR keeps naming the rejected revision as its spec target and its
// instances sit on stable, so both divergence clauses would otherwise fire.
func TestStickyRejectSuppressesReopen(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	isvc.Status.Canary = &v1beta1.CanaryStatus{RolledBackRevisionHash: "bbbbbbbb"}
	ir := irFixture(oldRev, newRev)
	ir.Status.Replicas = 2
	ir.Status.UpdatedReplicas = 0
	in := testInputs(t, isvc, ir)

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if v1beta1.RolloutRunActive(isvc) {
		t.Fatal("a sticky-rejected target is a hold, not a pending roll — no run may open")
	}
}

func TestStickyRejectUsesPerComponentTargets(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Status.Canary = &v1beta1.CanaryStatus{RolledBackRevisionHash: "router-new"}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.RouterComponent, Revision: "router-new", StableRevision: "router-old"},
			{Component: v1beta1.EngineComponent, Revision: "engine-new", StableRevision: "engine-old"},
		},
	}}
	targets := map[v1beta1.ComponentType]targetPair{
		v1beta1.RouterComponent: {current: "router-old", target: "router-new", replicas: 1},
		v1beta1.EngineComponent: {current: "engine-old", target: "engine-new", replicas: 2},
	}

	if divergedMember(isvc, targets) {
		t.Fatal("the exact rejected target set must suppress every group member")
	}
	targets[v1beta1.EngineComponent] = targetPair{current: "engine-old", target: "engine-next", replicas: 2}
	if !divergedMember(isvc, targets) {
		t.Fatal("a different secondary target must open a new rollout")
	}
}

func TestRetargetDuringRollbackUsesActiveComponentTargets(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Status.Canary = &v1beta1.CanaryStatus{RolledBackRevisionHash: "router-new"}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.RouterComponent, Revision: "router-new", StableRevision: "router-old"},
			{Component: v1beta1.EngineComponent, Revision: "engine-new", StableRevision: "engine-old"},
		},
		Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
			Group: *isvc.Spec.Rollout.Groups[0].DeepCopy(),
		}}},
	}}
	targets := map[v1beta1.ComponentType]targetPair{
		v1beta1.RouterComponent: {current: "router-old", target: "router-new"},
		v1beta1.EngineComponent: {current: "engine-old", target: "engine-next"},
	}

	if !retargeted(isvc, isvc.Status.Rollout.ActiveRun, targets) {
		t.Fatal("a new secondary target during rollback must supersede the active run")
	}
}

// A conflict retry re-bases an in-memory status onto the fresher live
// object; a stale writer must not roll the pinned plan back to a
// predecessor, and must not disarm the pre-step hold.
func TestPreserveNewerRunKeepsFresherPin(t *testing.T) {
	older := metav1.NewTime(time.Unix(1000, 0))
	newer := metav1.NewTime(time.Unix(2000, 0))

	stale := &v1beta1.InferenceServiceStatus{
		Canary: &v1beta1.CanaryStatus{},
		Rollout: &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID: "run-1", OpenedAt: older, PinnedAt: older,
			Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{PortableDigest: "rp1:old"}}},
		}},
	}
	live := &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID: "run-1", OpenedAt: older, PinnedAt: newer,
		Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{PortableDigest: "rp1:new"}}},
	}}

	PreserveNewerRun(stale, live, true)
	if stale.Rollout.ActiveRun.Plan.Groups[0].PortableDigest != "rp1:new" {
		t.Fatalf("stale writer must keep the fresher pin, got %s", stale.Rollout.ActiveRun.Plan.Groups[0].PortableDigest)
	}
	if !stale.Canary.PreStepHold {
		t.Fatal("an armed pre-step hold must survive the re-base")
	}

	// The reverse direction keeps dst: the current pass's boundary is newer.
	dst := &v1beta1.InferenceServiceStatus{Rollout: &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID: "run-2", OpenedAt: newer, PinnedAt: newer,
		Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{PortableDigest: "rp1:current"}}},
	}}}
	liveOld := &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID: "run-1", OpenedAt: older, PinnedAt: older,
	}}
	PreserveNewerRun(dst, liveOld, false)
	if dst.Rollout.ActiveRun.RunID != "run-2" {
		t.Fatal("a newer dst boundary must win over an older live state")
	}

	// A closed live run (fresher ClosedAt) beats a stale still-open copy.
	closedAt := metav1.NewTime(time.Unix(3000, 0))
	staleOpen := &v1beta1.InferenceServiceStatus{Rollout: &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID: "run-2", OpenedAt: newer, PinnedAt: newer,
	}}}
	liveClosed := &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunCompleted, ClosedAt: &closedAt,
	}}
	PreserveNewerRun(staleOpen, liveClosed, false)
	if staleOpen.Rollout.ActiveRun != nil || staleOpen.Rollout.LastRun == nil {
		t.Fatal("a fresher close must not be resurrected by a stale open copy")
	}
}

// twoUnitRun pins a run over two canary groups, one per unit, whose stable
// revisions were unknown when it opened (the shape a run has at creation).
func twoUnitRun(isvc *v1beta1.InferenceService, routerRev, engineRev string) {
	groups := []v1beta1.RolloutRunGroup{
		{Source: v1beta1.RolloutPlanSourceInline, Group: v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: canaryBody(50, 100)}},
		{Source: v1beta1.RolloutPlanSourceInline, Group: v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryBody(25, 100)}},
	}
	concurrent := v1beta1.RolloutGroupOrderingConcurrent
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{GroupOrdering: &concurrent, Groups: []v1beta1.RolloutGroup{groups[0].Group, groups[1].Group}}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID:    "creation",
		OpenedAt: metav1.NewTime(time.Unix(900, 0)),
		PinnedAt: metav1.NewTime(time.Unix(900, 0)),
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.RouterComponent, Revision: routerRev},
			{Component: v1beta1.EngineComponent, Revision: engineRev},
		},
		Plan: v1beta1.RolloutRunPlan{Groups: groups},
	}}
}

func routerIR(current, target string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a-router", Namespace: "ns", Generation: 1},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 1,
			CurrentRevision:    current,
			UpdateRevision:     target,
			Replicas:           1,
			UpdatedReplicas:    1,
		},
	}
}

// A retarget of one unit supersedes the open run and carries its stable
// revisions forward. An unknown stable stays unknown only for the component
// that moved; a component that stayed put is on the revision it targets, and
// that is its stable. Carrying the unknown would make the idle unit look
// retargeted and arm its ladder.
func TestRetargetKeepsUnmovedComponentStableKnown(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{})
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	twoUnitRun(isvc, "rrrrrrrr", "aaaaaaaa")
	in := testInputs(t, isvc, routerIR("llm-a-router-rrrrrrrr", "llm-a-router-ssssssss"), irFixture(oldRev, oldRev))

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	active := isvc.Status.Rollout.ActiveRun
	if active == nil || active.RunID == "creation" {
		t.Fatalf("the router retarget must open a fresh run, got %+v", active)
	}
	got := map[v1beta1.ComponentType]v1beta1.RolloutRunTarget{}
	for _, target := range active.TargetRevisions {
		got[target.Component] = target
	}
	if got[v1beta1.RouterComponent].Revision != "ssssssss" || got[v1beta1.RouterComponent].StableRevision != "" {
		t.Fatalf("moved router keeps its unknown stable: %+v", got[v1beta1.RouterComponent])
	}
	if got[v1beta1.EngineComponent].Revision != "aaaaaaaa" || got[v1beta1.EngineComponent].StableRevision != "aaaaaaaa" {
		t.Fatalf("unmoved engine must record its current revision as stable: %+v", got[v1beta1.EngineComponent])
	}
}

// A run opened with no stable revision (creation) whose canary unit never
// armed must still close once the unit converges; otherwise the creation
// plan stays pinned and later spec edits are ignored.
func TestCreationRunClosesWithoutCanaryState(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{})
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	twoUnitRun(isvc, "rrrrrrrr", "aaaaaaaa")
	engine := irFixture(oldRev, oldRev)
	engine.Status.Replicas, engine.Status.UpdatedReplicas = 1, 1
	in := testInputs(t, isvc, routerIR("llm-a-router-rrrrrrrr", "llm-a-router-rrrrrrrr"), engine)

	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if isvc.Status.Rollout.ActiveRun != nil {
		t.Fatalf("a converged run with no canary state must close, still active: %+v", isvc.Status.Rollout.ActiveRun)
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("last run = %+v, want Completed", last)
	}
}

// Concurrent groups: revisions for the two-group isolation fixtures.
const (
	routerStableRev   = "llm-a-router-11111111"
	routerRejectedRev = "llm-a-router-22222222"
	engineStableRev   = "llm-a-engine-33333333"
	engineNewRev      = "llm-a-engine-44444444"
)

func hashOf(t *testing.T, revName string) string {
	t.Helper()
	h := query.RevisionFromName(revName).Hash()
	if h == "" {
		t.Fatalf("revision %q has no hash", revName)
	}
	return h
}

// concurrentCanaryISVC is the two-independent-canaries shape: a router group
// and an engine group over disjoint Components, declared Concurrent. The
// router carries a standing rollback; the engine has never failed.
func concurrentCanaryISVC(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	one := 1
	ordering := v1beta1.RolloutGroupOrderingConcurrent
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a", Namespace: "ns"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}},
			Router: &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}},
			Rollout: &v1beta1.RolloutSpec{
				GroupOrdering: &ordering,
				Groups: []v1beta1.RolloutGroup{
					{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: canaryBody(10, 100)},
					{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: canaryBody(10, 100)},
				},
			},
		},
	}
	// The router's ladder failed and settled on its sticky hold; the engine's
	// has no failure of its own.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.RouterComponent: {
			RolloutPhase: v1beta1.RolloutPhaseRolledBack,
			Canary: &v1beta1.CanaryStatus{
				CurrentStep:            2,
				RolledBackRevisionHash: hashOf(t, routerRejectedRev),
				StableRevisionHash:     hashOf(t, routerStableRev),
			},
		},
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{
		LastRun: &v1beta1.RolloutRunRecord{
			Outcome: v1beta1.RolloutRunRolledBack,
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.RouterComponent, Revision: hashOf(t, routerRejectedRev), StableRevision: hashOf(t, routerStableRev)},
				{Component: v1beta1.EngineComponent, Revision: hashOf(t, engineStableRev), StableRevision: hashOf(t, engineStableRev)},
			},
		},
	}
	return isvc
}

func namedIR(name, current, target string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Generation: 1},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 1,
			CurrentRevision:    current,
			UpdateRevision:     target,
		},
	}
}

func pinnedTargetFor(t *testing.T, run *v1beta1.RolloutRun, comp v1beta1.ComponentType) v1beta1.RolloutRunTarget {
	t.Helper()
	for _, target := range run.TargetRevisions {
		if target.Component == comp {
			return target
		}
	}
	t.Fatalf("run pins no target for %s (pinned: %+v)", comp, run.TargetRevisions)
	return v1beta1.RolloutRunTarget{}
}

func TestOpenDoesNotRearmAnotherGroupsRejectedRevision(t *testing.T) {
	// An engine-only retarget opens the run. The router's IR target still
	// points at the revision the router's own canary rejected — that is the
	// sticky hold, not a fresh ask. Pinning it would re-present a revision
	// already known to fail, and its rollback would close the shared run and
	// throw away the engine's progress.
	isvc := concurrentCanaryISVC(t)
	in := testInputs(t, isvc,
		namedIR("llm-a-router", routerStableRev, routerRejectedRev),
		namedIR("llm-a-engine", engineStableRev, engineNewRev),
	)

	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !out.Opened {
		t.Fatalf("engine retarget did not open a run (outcome %+v, planReady %+v)", out, planReady(isvc))
	}
	active := isvc.Status.Rollout.ActiveRun
	if active == nil {
		t.Fatal("no ActiveRun after open")
	}

	router := pinnedTargetFor(t, active, v1beta1.RouterComponent)
	if router.Revision == hashOf(t, routerRejectedRev) {
		t.Errorf("router re-armed at its rejected revision %s", router.Revision)
	}
	if router.Revision != hashOf(t, routerStableRev) {
		t.Errorf("router Revision: got %s want its stable %s", router.Revision, hashOf(t, routerStableRev))
	}
	if engine := pinnedTargetFor(t, active, v1beta1.EngineComponent); engine.Revision != hashOf(t, engineNewRev) {
		t.Errorf("engine Revision: got %s want the retarget %s", engine.Revision, hashOf(t, engineNewRev))
	}
}

func TestStandingRollbackDoesNotCloseAnotherGroupsRun(t *testing.T) {
	// The router's rollback is settled and this run never presented the
	// rejected revision. Closing the run on it would end the engine's rollout
	// for a failure that is not the engine's and that this run did not cause —
	// the loop that makes a broken group block every other group forever.
	isvc := concurrentCanaryISVC(t)
	in := testInputs(t, isvc,
		namedIR("llm-a-router", routerStableRev, routerRejectedRev),
		namedIR("llm-a-engine", engineStableRev, engineNewRev),
	)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("open: %v", err)
	}
	if isvc.Status.Rollout.ActiveRun == nil {
		t.Fatal("no ActiveRun after open")
	}

	// Next pass, same observed state: the run must still be open and driving.
	out, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if isvc.Status.Rollout.ActiveRun == nil {
		last := isvc.Status.Rollout.LastRun
		t.Fatalf("run closed on the router's standing rollback (lastRun %+v, outcome %+v)", last, out)
	}
}

func TestStickyRejectIsScopedToTheGroupThatRolledBack(t *testing.T) {
	// The rejection belongs to the router's ladder. Crediting it to the engine
	// would read the engine's live target as a hold and stall it behind a
	// failure it had no part in.
	isvc := concurrentCanaryISVC(t)
	targets := map[v1beta1.ComponentType]targetPair{
		v1beta1.RouterComponent: {current: hashOf(t, routerStableRev), target: hashOf(t, routerRejectedRev)},
		v1beta1.EngineComponent: {current: hashOf(t, engineStableRev), target: hashOf(t, engineNewRev)},
	}
	rejected := stickyRejectHashes(isvc, targets)
	if got := rejected[v1beta1.RouterComponent]; got != hashOf(t, routerRejectedRev) {
		t.Errorf("router reject: got %q want %q", got, hashOf(t, routerRejectedRev))
	}
	if got, ok := rejected[v1beta1.EngineComponent]; ok {
		t.Errorf("engine carries a reject %q it never earned", got)
	}
	if !divergedMember(isvc, targets) {
		t.Error("engine retarget must count as divergence; it is held behind the router's rollback")
	}
}

func TestRolledBackRunStillClosesForTheGroupThatFailed(t *testing.T) {
	// Per-group reject scoping must not swallow a real rollback: when the run
	// pinned the revision the canary rejected, it closes RolledBack.
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			RolloutPhase: v1beta1.RolloutPhaseRolledBack,
			Canary: &v1beta1.CanaryStatus{
				CurrentStep:            1,
				RolledBackRevisionHash: hashOf(t, newRev),
				StableRevisionHash:     hashOf(t, oldRev),
			},
		},
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{
		ActiveRun: &v1beta1.RolloutRun{
			RunID: "llm-a-run",
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, Revision: hashOf(t, newRev), StableRevision: hashOf(t, oldRev)},
			},
			Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
				Group: v1beta1.RolloutGroup{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Canary:     canaryBody(10, 100),
				},
			}}},
		},
	}
	in := testInputs(t, isvc, namedIR("llm-a-engine", oldRev, newRev))
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if isvc.Status.Rollout.ActiveRun != nil {
		t.Fatal("run stayed open through its own rollback")
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunRolledBack {
		t.Errorf("lastRun outcome: got %+v want RolledBack", last)
	}
}
