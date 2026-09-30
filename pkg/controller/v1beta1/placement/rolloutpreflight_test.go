package placement

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

const testRolloutPolicyName = "canary-std-v1"

// canaryPolicyBody is a valid, portable canary progression (percent-only, no
// analysis, final step at 100/100).
func canaryPolicyBody() *v1beta1.GroupCanary {
	return &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 10,
			Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: time.Minute}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}}
}

func canaryRolloutPolicy(name string) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: name},
		Spec:       v1beta1.RolloutPolicySpec{Canary: canaryPolicyBody()},
	}
}

// srcISVCWithRolloutRef is srcISVC plus a single ref-only rollout group — the
// input that activates the rollout preflight and derive-time inflation.
func srcISVCWithRolloutRef(selector string) *v1beta1.InferenceService {
	isvc := srcISVC(selector)
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		PolicyRef: &v1beta1.RolloutPolicyRef{
			Name: testRolloutPolicyName, Progression: v1beta1.RolloutProgressionCanary,
		},
	}}}
	return isvc
}

func placedSrcISVCWithRolloutRef(selector string) *v1beta1.InferenceService {
	isvc := srcISVCWithRolloutRef(selector)
	url := &apis.URL{Scheme: "https", Host: "svc.example.com"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced, Endpoint: url,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: url, AdmittedReplicas: 1, ReadyReplicas: 1,
			Rollout: &v1beta1.CandidateRolloutStatus{ActiveRunID: "stale-run"},
		}},
	}
	isvc.Status.URL = url
	isvc.Status.SetConditions([]apis.Condition{{
		Type: apis.ConditionReady, Status: corev1.ConditionTrue, Reason: placementReadyReasonReady,
	}})
	return isvc
}

// rolloutCapabilityLabels satisfies both the match selector and the
// rollout-policy capability gate.
func rolloutCapabilityLabels() map[string]string {
	return map[string]string{
		"gpu": "gb300",
		constants.WorkloadClusterRolloutPolicyLabel: WorkloadClusterRolloutPolicyCapability,
	}
}

func mustPortableDigest(t *testing.T, spec *v1beta1.RolloutPolicySpec) string {
	t.Helper()
	d, err := rolloutpolicy.PortableDigest(spec)
	require.NoError(t, err)
	return d
}

func TestInflateRolloutGroups(t *testing.T) {
	pol := canaryRolloutPolicy(testRolloutPolicyName)
	digest := mustPortableDigest(t, &pol.Spec)
	src := srcISVCWithRolloutRef("gpu=gb300")
	src.Spec.Rollout.Groups = append(src.Spec.Rollout.Groups,
		// Inline arm coexisting with a ref: inline wins, derives verbatim.
		v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
			PolicyRef:  &v1beta1.RolloutPolicyRef{Name: testRolloutPolicyName, Progression: v1beta1.RolloutProgressionBlueGreen},
		},
		// No ref at all: left completely untouched.
		v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
	)

	d := DeriveISVC(src, "cp-east", "")
	require.NoError(t, inflateRolloutGroups(d, map[string]resolvedRolloutPolicy{
		testRolloutPolicyName: {spec: pol.Spec.DeepCopy(), digest: digest},
	}))

	// Ref-only group: policy body written inline, ref cleared.
	g0 := d.Spec.Rollout.Groups[0]
	require.NotNil(t, g0.Canary, "ref-only group must carry the policy body inline")
	assert.Equal(t, canaryPolicyBody(), g0.Canary)
	assert.Nil(t, g0.PolicyRef)
	// Inline-wins group: body verbatim, ref cleared.
	g1 := d.Spec.Rollout.Groups[1]
	assert.NotNil(t, g1.BlueGreen)
	assert.Nil(t, g1.Canary)
	assert.Nil(t, g1.PolicyRef)
	// Untouched plain group.
	g2 := d.Spec.Rollout.Groups[2]
	assert.Nil(t, g2.Canary)
	assert.Nil(t, g2.BlueGreen)
	assert.Nil(t, g2.PolicyRef)

	// Provenance: exact format, ONLY the ref-only group.
	assert.Equal(t, "0="+testRolloutPolicyName+"@"+digest,
		d.Annotations[constants.RolloutPlanSourceAnnotation])

	// The source object is never mutated (deep-copy contract).
	assert.NotNil(t, src.Spec.Rollout.Groups[0].PolicyRef)
	assert.Nil(t, src.Spec.Rollout.Groups[0].Canary)
}

func TestInflateRolloutGroups_MissingResolutionErrors(t *testing.T) {
	d := DeriveISVC(srcISVCWithRolloutRef("gpu=gb300"), "", "")
	err := inflateRolloutGroups(d, nil)
	require.Error(t, err, "a ref with no staged body must fail, never half-inflate")
	assert.Contains(t, err.Error(), testRolloutPolicyName)
}

// The rollout verbs and the plan-source provenance are control-plane-owned:
// repin and resume must never ride to a member (each copy would consume it,
// and the source's live copy would re-derive it back on the next sync), and a
// user-supplied plan-source value must never masquerade as system provenance.
func TestDeriveISVC_StripsRolloutOwnedAnnotations(t *testing.T) {
	src := srcISVC("gpu=gb300")
	src.Annotations[constants.RolloutRepinAnnotation] = "now"
	src.Annotations[constants.RolloutResumeAnnotation] = "e5d6f79d"
	src.Annotations[constants.RolloutPlanSourceAnnotation] = "9=user-forged@rp1:deadbeef"

	d := DeriveISVC(src, "", "")
	for _, k := range []string{constants.RolloutRepinAnnotation, constants.RolloutResumeAnnotation, constants.RolloutPlanSourceAnnotation} {
		_, has := d.Annotations[k]
		assert.Falsef(t, has, "annotation %q must be stripped from the derived object", k)
	}
}

// A user-forged plan-source value on a source WITH real refs is replaced by
// the system-authored entries, not merged with them.
func TestDerivedFor_ReplacesUserPlanSource(t *testing.T) {
	s := testScheme(t)
	src := srcISVCWithRolloutRef("gpu=gb300")
	src.Annotations[constants.RolloutPlanSourceAnnotation] = "9=user-forged@rp1:deadbeef"
	pol := canaryRolloutPolicy(testRolloutPolicyName)
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, src, pol)

	out := r.preflightRolloutPolicies(context.Background(), src, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", rolloutCapabilityLabels())})
	require.NotNil(t, out)
	require.False(t, out.hold)

	d, err := r.derivedFor(src)
	require.NoError(t, err)
	assert.Equal(t, "0="+testRolloutPolicyName+"@"+mustPortableDigest(t, &pol.Spec),
		d.Annotations[constants.RolloutPlanSourceAnnotation])
}

// End-to-end: a ref-only source fans out an INFLATED derived — inline groups,
// no ref, exact provenance annotation — so the member needs no policy objects.
func TestReconcile_RolloutInflateEndToEnd(t *testing.T) {
	s := testScheme(t)
	w := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	pol := canaryRolloutPolicy(testRolloutPolicyName)
	r, cp := newPlacer(s, clusters, srcISVCWithRolloutRef("gpu=gb300"), pol,
		readyWC("a", rolloutCapabilityLabels()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	derived := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, derived))
	require.NotNil(t, derived.Spec.Rollout)
	require.Len(t, derived.Spec.Rollout.Groups, 1)
	assert.Nil(t, derived.Spec.Rollout.Groups[0].PolicyRef, "the derived receives a plain inline group")
	assert.Equal(t, canaryPolicyBody(), derived.Spec.Rollout.Groups[0].Canary)
	assert.Equal(t, "0="+testRolloutPolicyName+"@"+mustPortableDigest(t, &pol.Spec),
		derived.Annotations[constants.RolloutPlanSourceAnnotation])

	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	pre := o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition))
	require.NotNil(t, pre)
	assert.Equal(t, corev1.ConditionTrue, pre.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPassed, pre.Reason)
}

// Fail-closed: an unresolvable ref holds placement Pending with the run-layer
// reason — the member must never receive a spec whose gate silently vanished.
func TestReconcile_RolloutRefMissingHoldsPlacement(t *testing.T) {
	s := testScheme(t)
	w := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, srcISVCWithRolloutRef("gpu=gb300"),
		readyWC("a", rolloutCapabilityLabels()))
	r.ControlPlaneID = "cp-east"

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, w), "no fan-out while the ref is unresolvable")
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	assert.Equal(t, v1beta1.PlacementPhasePending, o.Status.Placement.Phase)
	pre := o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition))
	require.NotNil(t, pre)
	assert.Equal(t, corev1.ConditionFalse, pre.Status)
	assert.Equal(t, v1beta1.RolloutPlanReasonPolicyNotFound, pre.Reason)
	assert.Contains(t, pre.Message, "cp-east", "message must name the control plane")
}

func TestReconcile_RolloutHoldRefreshesStandingHomeAndProvenance(t *testing.T) {
	s := testScheme(t)
	worker := workerWithReplicaCounts(t, s, "fresh.example.com", 2, 1)
	src := placedSrcISVCWithRolloutRef("gpu=gb300")
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, src, readyWC("a", rolloutCapabilityLabels()))
	ensurePlacedStatus(t, cp, src)

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, r.requeue(), res.RequeueAfter)

	derived := &v1beta1.InferenceService{}
	require.NoError(t, worker.Get(context.Background(), req().NamespacedName, derived))
	assert.Nil(t, derived.Spec.Rollout, "the unresolved source policy must not be applied")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, int32(2), p.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(1), p.Candidates[0].ReadyReplicas)
	assert.Equal(t, "fresh.example.com", p.Candidates[0].Endpoint.Host)
	assert.Nil(t, p.Candidates[0].Rollout, "fresh member status clears stale provenance")
}

func TestReconcile_RolloutHoldRefreshesProvenanceWhenIRIsUnknown(t *testing.T) {
	s := testScheme(t)
	worker := irGetFailClient{WithWatch: workerWithReplicaCounts(t, s, "fresh.example.com", 2, 1)}
	src := placedSrcISVCWithRolloutRef("gpu=gb300")
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, src, readyWC("a", rolloutCapabilityLabels()))
	ensurePlacedStatus(t, cp, src)

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)
	assert.Equal(t, "fresh.example.com", p.Candidates[0].Endpoint.Host)
	assert.Nil(t, p.Candidates[0].Rollout, "readable derived status is authoritative")
}

func TestPreflightRollout_ProgressionMismatchHolds(t *testing.T) {
	s := testScheme(t)
	pol := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: testRolloutPolicyName},
		Spec:       v1beta1.RolloutPolicySpec{BlueGreen: &v1beta1.GroupBlueGreen{}},
	}
	isvc := srcISVCWithRolloutRef("gpu=gb300") // ref declares canary
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc, pol)

	out := r.preflightRolloutPolicies(context.Background(), isvc, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", rolloutCapabilityLabels())})
	require.NotNil(t, out)
	assert.True(t, out.hold)

	cond, ok := r.rolloutState().preflightFor(isvc.UID)
	require.True(t, ok)
	assert.Equal(t, corev1.ConditionFalse, cond.cond.Status)
	assert.Equal(t, v1beta1.RolloutPlanReasonProgressionMismatch, cond.cond.Reason)
}

func TestPreflightRollout_InvalidPolicyHolds(t *testing.T) {
	s := testScheme(t)
	pol := &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: testRolloutPolicyName},
		// Absolute capacity: valid inline, but rejected by the policy-only
		// portability rules every member webhook enforces.
		Spec: v1beta1.RolloutPolicySpec{Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromInt32(3), Traffic: 100},
		}}},
	}
	isvc := srcISVCWithRolloutRef("gpu=gb300")
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc, pol)

	out := r.preflightRolloutPolicies(context.Background(), isvc, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", rolloutCapabilityLabels())})
	require.NotNil(t, out)
	assert.True(t, out.hold)

	cond, ok := r.rolloutState().preflightFor(isvc.UID)
	require.True(t, ok)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonInvalidPolicy, cond.cond.Reason)
}

func TestPreflightRollout_CapabilityGate(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRolloutRef("gpu=gb300")
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}},
		isvc, canaryRolloutPolicy(testRolloutPolicyName))

	// b matches the selector but does not declare the rollout capability.
	wcs := []v1beta1.WorkloadCluster{
		*readyWC("a", rolloutCapabilityLabels()),
		*readyWC("b", map[string]string{"gpu": "gb300"}),
	}
	out := r.preflightRolloutPolicies(context.Background(), isvc, []string{"a", "b"}, wcs)
	require.NotNil(t, out)
	assert.False(t, out.hold)
	assert.Equal(t, []string{"a"}, out.eligible)

	cond, ok := r.rolloutState().preflightFor(isvc.UID)
	require.True(t, ok)
	assert.Equal(t, corev1.ConditionTrue, cond.cond.Status)
	assert.Contains(t, cond.cond.Message, "b ("+v1beta1.PlacementPolicyPreflightReasonCapabilityMissing)

	// No eligible candidate at all: hold loudly.
	out = r.preflightRolloutPolicies(context.Background(), isvc, []string{"b"}, wcs)
	require.NotNil(t, out)
	assert.True(t, out.hold)
	cond, _ = r.rolloutState().preflightFor(isvc.UID)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonCapabilityMissing, cond.cond.Reason)
}

// The capability gate also fires for INLINE groups whose plan carries fields
// an old member schema would prune (providerRef / readyTimeout) — no ref
// needed. A plain inline plan gates nothing.
func TestPreflightRollout_InlineSchemaSensitiveFields(t *testing.T) {
	s := testScheme(t)
	wcs := []v1beta1.WorkloadCluster{
		*readyWC("a", rolloutCapabilityLabels()),
		*readyWC("b", map[string]string{"gpu": "gb300"}),
	}

	inlineWith := func(c *v1beta1.GroupCanary) *v1beta1.InferenceService {
		isvc := srcISVC("gpu=gb300")
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: c,
		}}}
		return isvc
	}

	// providerRef on an inline canary → gate applies.
	withProvider := canaryPolicyBody()
	withProvider.Prometheus = &v1beta1.AnalysisPrometheus{ProviderRef: &v1beta1.MetricProviderRef{Name: "prom-main"}}
	isvc := inlineWith(withProvider)
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc)
	out := r.preflightRolloutPolicies(context.Background(), isvc, []string{"a", "b"}, wcs)
	require.NotNil(t, out)
	assert.Equal(t, []string{"a"}, out.eligible)

	// readyTimeout on an inline canary → gate applies.
	withTimeout := canaryPolicyBody()
	withTimeout.ReadyTimeout = &metav1.Duration{Duration: 5 * time.Minute}
	isvc = inlineWith(withTimeout)
	r, _ = newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc)
	out = r.preflightRolloutPolicies(context.Background(), isvc, []string{"a", "b"}, wcs)
	require.NotNil(t, out)
	assert.Equal(t, []string{"a"}, out.eligible)

	// Plain inline canary: candidates flow untouched, nothing staged.
	isvc = inlineWith(canaryPolicyBody())
	r, _ = newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc)
	assert.Nil(t, r.preflightRolloutPolicies(context.Background(), isvc, []string{"a", "b"}, wcs))
	_, staged := r.rolloutState().preflightFor(isvc.UID)
	assert.False(t, staged)
}

func TestPreflightRollout_NoRolloutIsNil(t *testing.T) {
	s := testScheme(t)
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}})
	assert.Nil(t, r.preflightRolloutPolicies(context.Background(), srcISVC("gpu=gb300"), []string{"a"}, nil))
}

// A capability verdict cannot evict the standing winner: it holds actuation
// while the controller continues observing that home.
func TestPreflightRollout_WinnerSurvivesCapabilityLoss(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRolloutRef("gpu=gb300")
	isvc.Status.Placement = &v1beta1.PlacementStatus{Cluster: "a", Phase: v1beta1.PlacementPhasePlaced}
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}},
		isvc, canaryRolloutPolicy(testRolloutPolicyName))

	out := r.preflightRolloutPolicies(context.Background(), isvc, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", map[string]string{"gpu": "gb300"})})
	require.NotNil(t, out)
	assert.True(t, out.holdAsIs)
	assert.False(t, out.hold)
	pre, staged := r.rolloutState().preflightFor(isvc.UID)
	require.True(t, staged)
	assert.Equal(t, corev1.ConditionFalse, pre.cond.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonCapabilityMissing, pre.cond.Reason)
}

// A bare manual Pause in a placed plan is a dead end (the promote verb is
// stripped from deriveds and not forwarded): warn once per content, at plan
// time, naming the member-side workaround.
func TestPreflightRollout_ManualPauseWarning(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVC("gpu=gb300")
	manual := &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: manual,
	}}}
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc)
	rec := record.NewFakeRecorder(4)
	r.Recorder = rec

	wcs := []v1beta1.WorkloadCluster{*readyWC("a", rolloutCapabilityLabels())}
	r.preflightRolloutPolicies(context.Background(), isvc, []string{"a"}, wcs)
	select {
	case ev := <-rec.Events:
		assert.Contains(t, ev, rolloutManualPauseWarningReason)
		assert.Contains(t, ev, "group 0 step 0")
		assert.Contains(t, ev, "derived InferenceService")
	default:
		t.Fatal("expected a manual-Pause warning event")
	}

	// Unchanged content: no re-emit at the poll cadence.
	r.preflightRolloutPolicies(context.Background(), isvc, []string{"a"}, wcs)
	select {
	case ev := <-rec.Events:
		t.Fatalf("unexpected second event: %s", ev)
	default:
	}
}

func TestLiftCandidateRollout(t *testing.T) {
	derived := &v1beta1.InferenceService{}
	assert.Nil(t, liftCandidateRollout(derived), "no run state -> nothing lifted")

	derived.Status.Rollout = &v1beta1.RolloutStatus{
		ActiveRun: &v1beta1.RolloutRun{
			RunID: "run-7",
			Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{
				{Source: v1beta1.RolloutPlanSourcePolicy,
					PolicyRef:      &v1beta1.RolloutPolicyRef{Name: testRolloutPolicyName},
					PortableDigest: "rp1:aaa"},
				{Source: v1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:bbb"},
			}},
		},
		LastRun: &v1beta1.RolloutRunRecord{
			Outcome: v1beta1.RolloutRunCompleted,
			Groups: []v1beta1.RolloutRunProvenance{
				{Source: v1beta1.RolloutPlanSourcePolicy, PortableDigest: "rp1:old"},
			},
		},
	}

	got := liftCandidateRollout(derived)
	require.NotNil(t, got)
	assert.Equal(t, "run-7", got.ActiveRunID)
	require.Len(t, got.ActiveGroups, 2)
	assert.Equal(t, v1beta1.CandidateRolloutGroup{
		Source: v1beta1.RolloutPlanSourcePolicy, PolicyName: testRolloutPolicyName, PortableDigest: "rp1:aaa",
	}, got.ActiveGroups[0])
	assert.Equal(t, v1beta1.CandidateRolloutGroup{
		Source: v1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:bbb",
	}, got.ActiveGroups[1])
	require.NotNil(t, got.LastRun)
	assert.Equal(t, v1beta1.RolloutRunCompleted, got.LastRun.Outcome)
	assert.Equal(t, rolloutpolicy.CombinedDigest([]string{"rp1:old"}), got.LastRun.Digest)
}

// The funnel lift: an observed home's run provenance is attached to that
// candidate on the next status write, and the staged condition rides along.
func TestRolloutStatusForWrite_AttachesLift(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRolloutRef("gpu=gb300")
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}},
		isvc, canaryRolloutPolicy(testRolloutPolicyName))

	derived := &v1beta1.InferenceService{}
	derived.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID: "run-1",
		Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
			Source:         v1beta1.RolloutPlanSourcePolicy,
			PolicyRef:      &v1beta1.RolloutPolicyRef{Name: testRolloutPolicyName},
			PortableDigest: "rp1:aaa",
		}}},
	}}
	r.observeDerivedRolloutStatus(isvc, "a", derived)
	r.preflightRolloutPolicies(context.Background(), isvc, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", rolloutCapabilityLabels())})

	res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}, {Cluster: "b"}}}
	conds := r.rolloutStatusForWrite(isvc, res)
	require.Len(t, conds, 1)
	assert.Equal(t, corev1.ConditionTrue, conds[0].cond.Status)

	require.NotNil(t, res.candidates[0].Rollout)
	assert.Equal(t, "run-1", res.candidates[0].Rollout.ActiveRunID)
	assert.Equal(t, testRolloutPolicyName, res.candidates[0].Rollout.ActiveGroups[0].PolicyName)
	assert.Nil(t, res.candidates[1].Rollout, "unobserved home carries no lift")

	// A source with no policy-shaped rollout drops its bookkeeping and stages
	// nothing (clearing the shared condition is the autoscaler side's job).
	plain := srcISVC("gpu=gb300")
	assert.Nil(t, r.rolloutStatusForWrite(plain, &placementResult{}))
}

// The no-ref path stays zero-cost end-to-end: no rollout condition, no
// candidate rollout block. (Guards the shared-condition merge from leaking a
// rollout verdict onto plain sources.)
func TestReconcile_NoRolloutRefs_NoRolloutStatus(t *testing.T) {
	s := testScheme(t)
	w := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"), readyWC("a", rolloutCapabilityLabels()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	assert.Nil(t, o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition)))
	for _, c := range o.Status.Placement.Candidates {
		assert.Nil(t, c.Rollout)
	}
}

func TestMergePolicyConditions(t *testing.T) {
	condType := apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition)
	trueA := policyCondition{condType: condType, cond: apis.Condition{Status: corev1.ConditionTrue, Reason: "Passed", Message: "autoscaler ok"}}
	trueB := policyCondition{condType: condType, cond: apis.Condition{Status: corev1.ConditionTrue, Reason: "Passed", Message: "rollout ok"}}
	falseB := policyCondition{condType: condType, cond: apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.RolloutPlanReasonPolicyNotFound, Message: "missing"}}
	clearA := policyCondition{condType: condType, clear: true}

	// A real condition beats a clear from the other preflight.
	got := mergePolicyConditions([]policyCondition{clearA}, []policyCondition{falseB})
	require.Len(t, got, 1)
	assert.False(t, got[0].clear)
	assert.Equal(t, v1beta1.RolloutPlanReasonPolicyNotFound, got[0].cond.Reason)

	// False beats True regardless of order.
	got = mergePolicyConditions([]policyCondition{trueA}, []policyCondition{falseB})
	require.Len(t, got, 1)
	assert.Equal(t, corev1.ConditionFalse, got[0].cond.Status)

	// Two True verdicts join their messages.
	got = mergePolicyConditions([]policyCondition{trueA}, []policyCondition{trueB})
	require.Len(t, got, 1)
	assert.Equal(t, "autoscaler ok; rollout ok", got[0].cond.Message)

	// Distinct types pass through untouched.
	agg := policyCondition{condType: apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition), clear: true}
	got = mergePolicyConditions([]policyCondition{trueA, agg}, []policyCondition{trueB})
	assert.Len(t, got, 2)
}
