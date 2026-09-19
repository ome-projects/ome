package coordination

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// testScheme returns the runtime.Scheme for fake client builders in tests.
func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	return scheme
}

// pinActiveRun pins the ISVC's current spec.rollout as its active rollout run
// (the state the run layer produces at run open). Gate tests pin because a
// grouped Component fails closed (RolloutHoldGatePlan) while no run is open.
// A pinned plan is inert to later spec edits — re-pin after mutating
// spec.rollout when the edit should take effect.
func pinActiveRun(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	if isvc == nil || isvc.Spec.Rollout == nil {
		return isvc
	}
	groups := make([]v1beta1.RolloutRunGroup, 0, len(isvc.Spec.Rollout.Groups))
	for i := range isvc.Spec.Rollout.Groups {
		groups = append(groups, v1beta1.RolloutRunGroup{
			Source: v1beta1.RolloutPlanSourceInline,
			Group:  *isvc.Spec.Rollout.Groups[i].DeepCopy(),
		})
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID:    "test",
		OpenedAt: metav1.Now(),
		Plan:     v1beta1.RolloutRunPlan{Groups: groups},
	}}
	return isvc
}

// fakeClientForISVC creates a fake client seeded with InferenceReplica objects
// whose status is derived from the ISVC component lifecycle fixtures.
func fakeClientForISVC(isvc *v1beta1.InferenceService) client.Client {
	return fakeClientForISVCWithInstances(isvc, nil)
}

// fakeClientForISVCWithInstances is fakeClientForISVC plus per-component
// InstanceStatuses overlaid onto the derived IRs.
func fakeClientForISVCWithInstances(isvc *v1beta1.InferenceService, instancesByComponent map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus) client.Client {
	return fakeClientForISVCWithDesiredAndInstances(isvc, nil, instancesByComponent)
}

func fakeClientForISVCWithDesiredAndInstances(
	isvc *v1beta1.InferenceService,
	desiredByComponent map[v1beta1.ComponentType]*int32,
	instancesByComponent map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus,
) client.Client {
	if isvc == nil || (isvc.Status.Components == nil && len(instancesByComponent) == 0) {
		return fake.NewClientBuilder().WithScheme(testScheme()).Build()
	}
	var irs []client.Object
	covered := map[v1beta1.ComponentType]bool{}
	for c, cs := range isvc.Status.Components {
		if cs.Lifecycle == nil {
			continue
		}
		replicas := cs.Lifecycle.Replicas
		desired, specified := desiredByComponent[c]
		if !specified {
			desired = &replicas
		}
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{
				Name:      isvc.Name + "-" + string(c),
				Namespace: isvc.Namespace,
			},
			Spec: v1beta1.InferenceReplicaSpec{
				Replicas: desired,
			},
			Status: v1beta1.InferenceReplicaStatus{
				Replicas:           cs.Lifecycle.Replicas,
				UpdatedReplicas:    cs.Lifecycle.UpdatedReplicas,
				ServingReplicas:    cs.Lifecycle.ServingReplicas,
				CurrentRevision:    cs.Lifecycle.CurrentRevision,
				UpdateRevision:     cs.Lifecycle.UpdateRevision,
				ObservedGeneration: cs.Lifecycle.ObservedGeneration,
				Conditions:         cs.Lifecycle.Conditions,
			},
		}
		ir.Status.InstanceStatuses = instancesByComponent[c]
		irs = append(irs, ir)
		covered[c] = true
	}
	// Components with instances but no Lifecycle entry still need an IR so the
	// gate can read their instance detail. An empty overlay creates no IR.
	for c, insts := range instancesByComponent {
		if covered[c] || len(insts) == 0 {
			continue
		}
		irs = append(irs, &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{
				Name:      isvc.Name + "-" + string(c),
				Namespace: isvc.Namespace,
			},
			Status: v1beta1.InferenceReplicaStatus{InstanceStatuses: insts},
		})
	}
	return fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(irs...).Build()
}

func TestEvaluateSurge_NoSurgeRequested(t *testing.T) {
	d := EvaluateSurge(v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced}, RatioState{}, v1beta1.EngineComponent, 0)
	if d.AllowedSurgeDelta != 0 {
		t.Errorf("zero delta: got %d want 0", d.AllowedSurgeDelta)
	}
	if d.SkewRejected {
		t.Errorf("zero delta should not be a rejection")
	}
}

func TestEvaluateSurge_PerComponentPassThrough(t *testing.T) {
	d := EvaluateSurge(v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingPerComponent}, RatioState{}, v1beta1.EngineComponent, 3)
	if d.AllowedSurgeDelta != 3 {
		t.Errorf("PerComponent should pass through: got %d want 3", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_SingleComponentGroupAllowsSurge(t *testing.T) {
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 10},
		NewPods:  map[v1beta1.ComponentType]int32{},
	}
	d := EvaluateSurge(
		v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced},
		state,
		v1beta1.EngineComponent,
		5,
	)
	if d.AllowedSurgeDelta != 5 {
		t.Errorf("single component: got %d want 5", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_2to1RatioWithinTolerance(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// engine 40 : decoder 20 = 2:1. 10 new engine + 5 new decoder
	// keeps the ratio exact.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  0,
			v1beta1.DecoderComponent: 5,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 10)
	if d.AllowedSurgeDelta != 10 {
		t.Errorf("balanced 10 engine vs 5 decoder: got %d want 10", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_AlreadySkewedRejectsAnyFurtherSurge(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// 40 engine : 20 decoder. The component being surged already
	// has 15 new pods (37.5%) while decoder has only 5 (25%). The
	// skew is 12.5% > 5% band before any new surge. Any further
	// engine surge can only increase the skew, so every back-off
	// step also gets rejected — the function returns zero with
	// SkewRejected=true.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  15,
			v1beta1.DecoderComponent: 5,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if !d.SkewRejected {
		t.Errorf("expected skew rejection: got %+v", d)
	}
	if d.AllowedSurgeDelta != 0 {
		t.Errorf("rejected surge should produce zero delta: got %d", d.AllowedSurgeDelta)
	}
}

func TestEvaluateSurge_NilToleranceDisablesDriftBound(t *testing.T) {
	// A RatioBalanced pacing whose tolerance never resolved (group omitted it
	// and the operator configured no default) has no band to enforce: the
	// guard admits the surge instead of inventing a bound. Same skewed state
	// as the rejection test above — only the nil tolerance differs.
	pacing := v1beta1.CoordinationPacing{
		Type: v1beta1.CoordinationPacingRatioBalanced,
	}
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  15,
			v1beta1.DecoderComponent: 5,
		},
		Current: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Desired: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  0,
			v1beta1.DecoderComponent: 0,
		},
		RecoveryEligible: map[v1beta1.ComponentType]bool{
			v1beta1.EngineComponent:  true,
			v1beta1.DecoderComponent: true,
		},
		InFlightSurge: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent: 1,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if d.SkewRejected || d.AllowedSurgeDelta != 5 {
		t.Errorf("nil tolerance must disable the drift bound: got %+v want AllowedSurgeDelta=5", d)
	}
}

func TestEvaluateSurge_BacksOffWhenLiveRatioWouldExceedBand(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// Live serving ratio (larger/smaller).
	// 40 engine : 20 decoder original → orig_ratio = 2.0,
	// band [1.9, 2.1] at tol=5%.
	// Serving snapshot: engine=10, decoder=5 → live = 2.0 (in band).
	// Surge engine +1 → 11/5 = 2.2 → out of band → reject.
	// Surge decoder +1 → 10/6 = 1.67 → out of band → reject.
	// All deltas rejected at this tight tolerance.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  10,
			v1beta1.DecoderComponent: 5,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if d.AllowedSurgeDelta != 0 || !d.SkewRejected {
		t.Errorf("tight tolerance should reject all engine surges from balanced state: got %+v", d)
	}
}

func TestEvaluateSurge_AllowsSurgeWhenLiveRatioStaysInBand(t *testing.T) {
	tol := int32(20)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// 40 engine : 20 decoder original → orig_ratio = 2.0,
	// band [1.6, 2.4] at tol=20%.
	// Serving: engine=40, decoder=20 → live = 2.0 (in band).
	// Surge engine: +1 → 41/20=2.05 in. +2 → 42/20=2.1 in. +8 → 48/20=2.4 boundary in.
	// +9 → 49/20=2.45 out.
	// Propose 10 → backs off to 8.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 10)
	if d.AllowedSurgeDelta != 8 {
		t.Errorf("partial back-off to band ceiling: got %d want 8 (49/20=2.45 out, 48/20=2.4 boundary)", d.AllowedSurgeDelta)
	}
	if d.SkewRejected {
		t.Errorf("partial back-off should NOT be flagged as full rejection: got %+v", d)
	}
}

func TestEvaluateSurge_RejectsWhenPeerSerivngWouldBeZero(t *testing.T) {
	tol := int32(50)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// Peer's serving count is zero → ratio undefined → any surge of
	// the other component is rejected. This is the mass-drain
	// failure mode the gate must block.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  10,
			v1beta1.DecoderComponent: 0,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 5)
	if !d.SkewRejected || d.AllowedSurgeDelta != 0 {
		t.Errorf("zero-peer-serving must reject all surges (would compute infinite ratio): got %+v", d)
	}
}

func TestEvaluateSurge_3WayRatioIsPairwise(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// router 1 : engine 40 : decoder 20. Propose 4 new router pods
	// (fraction = 400%) — should be rejected wholly because router
	// is so small that any surge skews.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.RouterComponent:  1,
			v1beta1.EngineComponent:  40,
			v1beta1.DecoderComponent: 20,
		},
		NewPods: map[v1beta1.ComponentType]int32{},
	}
	d := EvaluateSurge(pacing, state, v1beta1.RouterComponent, 4)
	if !d.SkewRejected {
		t.Errorf("router surge should be rejected (out of pairwise band): got %+v", d)
	}
}

func TestEvaluateSurge_ZeroOriginalReplicasIgnored(t *testing.T) {
	tol := int32(5)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tol,
	}
	// One component has zero original replicas — it shouldn't
	// anchor ratio enforcement. Treating it as a valid pair would
	// reject all surges.
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent: 10,
			v1beta1.RouterComponent: 0,
		},
	}
	d := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 3)
	if d.AllowedSurgeDelta != 3 {
		t.Errorf("zero-original component should not block: got %d want 3", d.AllowedSurgeDelta)
	}
}

func TestSnapshotOriginal(t *testing.T) {
	in := map[v1beta1.ComponentType]int32{
		v1beta1.EngineComponent:  40,
		v1beta1.DecoderComponent: 0,
	}
	out := SnapshotOriginal(in)
	if out[v1beta1.EngineComponent] != 40 {
		t.Errorf("engine: got %d want 40", out[v1beta1.EngineComponent])
	}
	if out[v1beta1.DecoderComponent] != 1 {
		t.Errorf("zero clamped to 1: got %d want 1", out[v1beta1.DecoderComponent])
	}
	// Original input should not be mutated.
	if in[v1beta1.DecoderComponent] != 0 {
		t.Errorf("input mutated: %v", in)
	}
}

// =============================================================================
// CheckRatioGate
// =============================================================================

func TestCheckRatioGate_NilISVC(t *testing.T) {
	allowed, _ := CheckRatioGate(nil, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("nil ISVC should be allowed (no-op gate)")
	}
}

func TestCheckRatioGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no coord block should be allowed: %s", reason)
	}
}

func TestCheckRatioGate_PerComponentPacingBypasses(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	pinActiveRun(isvc)
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("PerComponent pacing should bypass the gate: %s", reason)
	}
}

func TestCheckRatioGate_NoAnchorWithCompleteIRAllowed(t *testing.T) {
	// RatioBalanced configured with complete positive IR observations but no
	// ObservedRatio snapshot yet. There is no stable ratio anchor to enforce.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
			},
		},
	}
	pinActiveRun(isvc)
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("complete IR observations without an anchor must allow: %s", reason)
	}
}

func TestCheckRatioGate_NoAnchorAllowsWithoutIR(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
				}},
			},
		},
	}
	pinActiveRun(isvc)
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed || !strings.Contains(reason, "no ObservedRatio") {
		t.Fatalf("the no-anchor fast path must not require IR observations: allowed=%t reason=%q", allowed, reason)
	}
}

func TestCheckRatioGate_PDScenarioEngineMustWaitForDecoder(t *testing.T) {
	// Canonical PD: engine 20 pods, decoder 5 pods (4:1, decoder=25%).
	// Tolerance 5%. After engine surges 1 pod (1/20 = 5%) and decoder is
	// still at 0/5 (0%), the next engine surge would push 2/20 = 10%
	// against 0% → 10pp diff > 5pp tolerance → MUST be denied.
	isvc := mkPDFixture(int32Ptr(5))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        20,
				UpdatedReplicas: 1,
			},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				UpdatedReplicas: 0,
			},
		},
	}

	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("engine should be gated when decoder hasn't moved (1/20=5%% vs 0/5=0%%, asking for 2/20=10%%): got allowed=true reason=%s", reason)
	}
}

func TestCheckRatioGate_PDScenarioDecoderAllowedToCatchUp(t *testing.T) {
	// Same PD setup; ask whether decoder can move. decoder going from
	// 0/5 to 1/5 = 20% is a 15pp leap from 5% (engine), beyond 5%
	// tolerance — so the strict band-check rejects decoder too. This is
	// the "ratio convergence freezes" failure mode the operator must
	// address by widening tolerance or restructuring replicas. The gate
	// reports it honestly.
	isvc := mkPDFixture(int32Ptr(5))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        20,
				UpdatedReplicas: 1,
			},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				UpdatedReplicas: 0,
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.DecoderComponent, 0)
	// At tolerance=5, the discrete granularity prevents decoder from
	// moving. The test pins this honest deadlock so a reviewer can see
	// it; widening tolerance fixes it.
	if allowed {
		t.Errorf("expected decoder gated at tolerance=5: discrete granularity (decoder pod = 20%% jump) cannot satisfy band; got allowed=true")
	}
}

func TestCheckRatioGate_PDScenarioWiderToleranceAllowsDrain(t *testing.T) {
	// Wider tolerance lets an engine drain through that tighter tolerance
	// would block. Setup: engine 20, decoder 5 → orig_ratio=4.0,
	// tol=20% → band [3.2, 4.8].
	// Live state: full capacity (engine=20, decoder=5).
	// Engine drain –1 → projected (19, 5), ratio = 19/5 = 3.8 → in band.
	// (Tighter tol=5% → band [3.8, 4.2] → 3.8 sits at boundary, still in;
	// at tol=2% → band [3.92, 4.08] → 3.8 < 3.92 → out → reject. The
	// 20% tolerance test pins the "wider band gives more drain headroom"
	// property the redesign promises.)
	isvc := mkPDFixture(int32Ptr(20))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        20,
				ServingReplicas: 20,
			},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ServingReplicas: 5,
			},
		},
	}
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("expected engine drain allowed at tolerance=20: projected ratio 19/5=3.8 in band [3.2, 4.8]; got reason=%s", reason)
	}
}

func TestCheckRatioGate_SingleComponentGroupBypasses(t *testing.T) {
	// A single-Component group has no peer to ratio-balance against, so the
	// gate bypasses. (v2: single-Component groups can't carry MaintainRatio
	// meaningfully — and rollingUpdate, the MaintainRatio carrier, requires
	// 2+ Components — so a lone Component is a plain blueGreen group.)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	pinActiveRun(isvc)
	allowed, _ := CheckRatioGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("single-component group must bypass the gate")
	}
}

func TestCheckRatioGate_ComponentNotInAnyGroup(t *testing.T) {
	// router not in declared group → no gate, allowed.
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
				}},
			},
		},
	}
	allowed, _ := CheckRatioGate(isvc, v1beta1.RouterComponent, 0)
	if !allowed {
		t.Errorf("component outside any coord group must bypass the gate")
	}
}

// TestCheckRatioGate_InFlightProjectionTightensBand pins the
// in-wake-up stale-snapshot fix. status.ServingReplicas reports 30
// (snapshot from the prior reconcile) but the dispatcher has already
// pulled 3 engine pods from rotation this wake-up — projected serving
// is 27. Tolerance 10% on a 30:10 fleet → band [2.7, 3.3]. Without
// the in-flight delta, the gate sees 30/10 = 3.0 → allowed. With
// inFlightDelta=3, it sees (30-3)/10 = 2.7 → at the boundary. The
// caller asks for one more drain; gate must REJECT because
// (27-1)/10 = 2.6 sits outside the band.
func TestCheckRatioGate_InFlightProjectionTightensBand(t *testing.T) {
	tol := int32(10)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        30,
						ServingReplicas: 30,
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        10,
						ServingReplicas: 10,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  30,
							v1beta1.DecoderComponent: 10,
						},
					},
				}},
			},
		},
	}

	pinActiveRun(isvc)
	// Baseline: no in-flight → projected serving stays at status (30/10=3.0
	// → in band) → allowed.
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0); !allowed {
		t.Fatalf("baseline (0 in-flight) should be allowed: %s", reason)
	}

	// At the band boundary: 3 already drained → projected serving 27,
	// asking for +1 puts us at (27-1)/10 = 2.6 → out of band → MUST reject.
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 3); allowed {
		t.Fatalf("with 3 in-flight, one more drain would cross the band; expected reject, got allowed (reason=%s)", reason)
	}

	// Sanity: at 2 in-flight, (28-1)/10 = 2.7 → exactly the band edge →
	// still allowed (band is inclusive).
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 2); !allowed {
		t.Fatalf("with 2 in-flight, projected 2.7 sits at band edge and should remain allowed: %s", reason)
	}
}

// TestCheckRatioGate_BlueGreenRatioBalancedDoesNotDeadlock pins the
// fix for the BlueGreen + RatioBalanced deadlock observed in KIND tests.
//
// Reproduction shape: 4 engine + 2 decoder, BlueGreen + RatioBalanced
// (tol 25%, band [1.5, 2.5]). The original ObservedRatio anchor must
// reflect the 4:2 fleet — NOT the bogus {1, 1} that SnapshotOriginal
// produced on the very first reconcile (when status.OMENative.Replicas
// hadn't been written yet, so DesiredReplicas defaulted to 0 → clamped
// to 1 by SnapshotOriginal). With the bogus 1:1 anchor, every drain
// projection produced a ratio outside the 0.75-1.25 band derived from
// 1.0 * (1 ± 25%), even though the LIVE 4:2 = 2.0 ratio sat perfectly
// inside the intended 2.0 * (1 ± 25%) = [1.5, 2.5] band.
//
// This test asserts the gate ALLOWS the first drain when Original
// matches the cluster's actual 4:2 shape — the property that broke
// when buildRatioState snapshotted from an empty status.
func TestCheckRatioGate_BlueGreenRatioBalancedDoesNotDeadlock(t *testing.T) {
	tol := int32(25)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        4,
						ServingReplicas: 4,
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 2,
						},
					},
				}},
			},
		},
	}

	pinActiveRun(isvc)
	// Engine drain (-1): projected serving = {eng: 3, dec: 2}, ratio
	// 3/2 = 1.5, lower band 2.0 * (1 - 0.25) = 1.5 → boundary, in band
	// (inclusive) → MUST be allowed. With Original anchored at {1, 1}
	// rather than {4, 2} this gate would reject EVERY surge.
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0); !allowed {
		t.Fatalf("BlueGreen + RatioBalanced with 4:2 fleet and 25%% tolerance: first engine drain should be allowed (projected 3/2=1.5 sits at band lower edge [1.5, 2.5]); got allowed=false reason=%s", reason)
	}
}

// TestCheckRatioGate_BogusOnePerComponentAnchorReproducesDeadlock pins
// the failure side of the same invariant: with a {1, 1} anchor (the
// shape an empty-status snapshot produces when status.OMENative.Replicas
// is 0 at snapshot time), the gate rejects every drain on a fleet
// whose live ratio differs from 1:1. This test documents the failure
// mode so an empty-status snapshot trips here AND in the
// ratiobalanced_kind / loadtest_kind KIND specs together.
func TestCheckRatioGate_BogusOnePerComponentAnchorReproducesDeadlock(t *testing.T) {
	tol := int32(25)
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        4,
						ServingReplicas: 4,
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					// Bogus 1:1 anchor — what the old buildRatioState
					// snapshotted when status.OMENative.Replicas was 0
					// at first reconcile (SnapshotOriginal clamps zero
					// to one).
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  1,
							v1beta1.DecoderComponent: 1,
						},
					},
				}},
			},
		},
	}

	pinActiveRun(isvc)
	// With anchor {1, 1}, band = 1.0 * (1 ± 0.25) = [0.75, 1.25].
	// Live ratio 4/2 = 2.0 is OUT of band. Engine drain projects
	// 3/2 = 1.5 → still out of band → REJECT.
	// (This is the deadlock: the gate rejects every drain because the
	// anchor doesn't match the cluster shape.)
	if allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0); allowed {
		t.Fatalf("with bogus 1:1 anchor against 4:2 live fleet, gate should reject (projection 3:2=1.5 is outside band [0.75, 1.25] derived from 1.0); got allowed=true reason=%q — if this test PASSES with the bogus anchor it means the band semantics changed and the deadlock can't be reproduced from this anchor any more", reason)
	}
}

// mkPDFixture returns a coordinated ISVC with engine+decoder in one
// RatioBalanced group + an ObservedRatio snapshot of 20:5 (4:1, decoder
// = 25% of engine). Caller sets Status.Components.
//
// v2: cross-Component ratio guarding rides on MaintainRatio on a
// multi-Component rollingUpdate group. A nil argument makes the fixture
// spell out tolerance 5 explicitly (the value production resolves from
// operator configuration).
func mkPDFixture(tolerance *int32) *v1beta1.InferenceService {
	tol := int32(5)
	if tolerance != nil {
		tol = *tolerance
	}
	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyBlueGreen,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  20,
							v1beta1.DecoderComponent: 5,
						},
					},
				}},
			},
		},
	})
}

func int32Ptr(v int32) *int32 { return &v }

// =============================================================================
// CheckUnavailabilityGate
// =============================================================================

func TestCheckUnavailabilityGate_NilISVC(t *testing.T) {
	allowed, _ := CheckUnavailabilityGate(nil, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("nil ISVC should bypass the gate")
	}
}

func TestCheckUnavailabilityGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no coord block should bypass the gate")
	}
}

func TestCheckUnavailabilityGate_ComponentNotInAnyGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.RouterComponent, 0)
	if !allowed {
		t.Errorf("component outside any coord group must bypass the gate")
	}
}

func TestCheckUnavailabilityGate_NoObservationYet(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	// No Status.Components populated.
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("missing OMENative observation must bypass the gate (first reconcile)")
	}
}

func TestCheckUnavailabilityGate_WithinBudget(t *testing.T) {
	// MaxUnavailable=1, 5 replicas, 5 Ready → currentUnavailable=0,
	// projected=1, budget=1 → ALLOWED.
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ReadyReplicas:   5,
				ServingReplicas: 5,
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("0 current unavailable + budget=1 must allow: %s", reason)
	}
}

func TestCheckUnavailabilityGate_BudgetExhausted(t *testing.T) {
	// MaxUnavailable=1, 5 replicas, 4 Ready → currentUnavailable=1,
	// projected=2, budget=1 → DENIED.
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ReadyReplicas:   4,
				ServingReplicas: 4,
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("1 current unavailable + budget=1 must deny next termination: %s", reason)
	}
}

func TestCheckUnavailabilityGate_ZeroBudgetDeadlocks(t *testing.T) {
	// MaxUnavailable=0 + 5 Ready/5 Replicas → projected=1 > budget=0 → DENIED.
	// This is the documented honest semantic: zero maxUnavailable without
	// surge-then-drain support deadlocks rollouts.
	isvc := mkUnavailFixture(iosInt(0))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        5,
				ReadyReplicas:   5,
				ServingReplicas: 5,
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("MaxUnavailable=0 + all Ready must STILL deny (no surge-then-drain support)")
	}
}

func TestCheckUnavailabilityGate_PercentBudget(t *testing.T) {
	// MaxUnavailable=20%, 10 replicas → budget=2. 0 unavailable, projected=1
	// → ALLOWED.
	pct := intstr.FromString("20%")
	isvc := mkUnavailFixture(&pct)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        10,
				ReadyReplicas:   10,
				ServingReplicas: 10,
			},
		},
	}
	allowed, _ := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("20%% of 10 = 2 budget; first termination must be allowed")
	}
}

func TestCheckUnavailabilityGate_InFlightDeltaCharges(t *testing.T) {
	// MaxUnavailable=2, 10 replicas, all serving. Budget=2.
	// inFlightDelta=0 → projected=1 → allowed.
	// inFlightDelta=1 → projected=2 → allowed (boundary).
	// inFlightDelta=2 → projected=3 → DENIED.
	// Proves the wake-up counter closes the within-pass stale-state hole
	// through which the dispatcher would otherwise fire every instance at once.
	pct := intstr.FromString("20%")
	isvc := mkUnavailFixture(&pct)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        10,
				ReadyReplicas:   10,
				ServingReplicas: 10,
			},
		},
	}
	cases := []struct {
		inFlight int32
		want     bool
	}{
		{0, true},
		{1, true}, // boundary
		{2, false},
		{5, false},
	}
	for _, c := range cases {
		got, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, c.inFlight)
		if got != c.want {
			t.Errorf("inFlight=%d: got allowed=%v want %v (%s)", c.inFlight, got, c.want, reason)
		}
	}
}

func TestCheckUnavailabilityGate_ServingDivergesFromReady(t *testing.T) {
	// ReadyReplicas=10 but ServingReplicas=7 — simulates the in-place
	// drain mid-rollout state where containers are still Ready but the
	// controller has flipped serving=False on 3 pods. The gate must
	// see 3 currently unavailable (from serving count), not 0 (from
	// ready count). MaxUnavailable=3 → projected=4 > budget=3 → DENY.
	// Counting from ReadyReplicas instead would admit every drain at once.
	isvc := mkUnavailFixture(iosInt(3))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{
				Replicas:        10,
				ReadyReplicas:   10, // misleading — kubelet sees containers Ready
				ServingReplicas: 7,  // truth — 3 pods are out of rotation
			},
		},
	}
	allowed, reason := CheckUnavailabilityGate(isvc, v1beta1.EngineComponent, 0)
	if allowed {
		t.Errorf("gate must use ServingReplicas, not ReadyReplicas, so the drained-but-Ready state denies more drains: %s", reason)
	}
}

func iosInt(v int) *intstr.IntOrString {
	x := intstr.FromInt(v)
	return &x
}

// mkUnavailFixture returns a coordinated ISVC with engine in one group
// carrying the specified MaxUnavailable budget. Caller sets
// Status.Components.
//
// v2: the surge / unavailable budget rides on a rollingUpdate group (the
// only v2 progression that carries MaxSurge / MaxUnavailable — blueGreen
// drops them). The gate path reads the resolved pacing directly and does
// not run ValidateGroupShape, so a single-Component rollingUpdate fixture
// exercises the budget math (which is what these tests pin).
func mkUnavailFixture(maxUnavail *intstr.IntOrString) *v1beta1.InferenceService {
	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{
						MaxUnavailable: maxUnavail,
					},
				}},
			},
		},
	})
}

// ----------------------------------------------------------------------
// CheckSurgeGate tests — parallel of CheckUnavailabilityGate, but
// counts in-flight surge operations against the MaxSurge budget rather
// than offline pods against the MaxUnavailable budget.
// ----------------------------------------------------------------------

// mkSurgeFixture is the surge analog of mkUnavailFixture: a rollingUpdate
// group carrying the specified MaxSurge budget. Caller sets
// Status.Components. (v2: MaxSurge rides on rollingUpdate; see
// mkUnavailFixture for why a single-Component rollingUpdate fixture is the
// right gate-test shape.)
func mkSurgeFixture(maxSurge *intstr.IntOrString) *v1beta1.InferenceService {
	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{
						MaxSurge: maxSurge,
					},
				}},
			},
		},
	})
}

func TestCheckSurgeGate_NilISVC(t *testing.T) {
	allowed, _ := CheckSurgeGate(nil, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("nil ISVC: allow (bypass)")
	}
}

func TestCheckSurgeGate_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no coord: allow (bypass)")
	}
}

func TestCheckSurgeGate_NoObservationYet(t *testing.T) {
	isvc := mkSurgeFixture(iosInt(1))
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("first reconcile (no status): allow")
	}
}

func TestCheckSurgeGate_NoInFlight_WithinBudget(t *testing.T) {
	// MaxSurge=1, 5 replicas, no in-flight surge → projected=1, budget=1
	// → ALLOWED.
	isvc := mkSurgeFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
	}
	allowed, reason := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0)
	if !allowed {
		t.Errorf("no in-flight + budget=1: allow; got reason=%s", reason)
	}
}

func TestCheckSurgeGate_OneInFlight_BudgetExhausted(t *testing.T) {
	// MaxSurge=1, 5 replicas, ONE instance already on Step=Surge → projected=2 > budget=1
	// → DENIED.
	isvc := mkSurgeFixture(iosInt(1))
	insts := []v1beta1.OMENativeInstanceStatus{{
		Index:     0,
		Phase:     v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "Surge"},
	}}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
	}
	allowed, reason := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0, insts...)
	if allowed {
		t.Errorf("1 in-flight Surge + budget=1: deny; got allow reason=%s", reason)
	}
}

func TestCheckSurgeGate_SurgeDrainStepAlsoCounts(t *testing.T) {
	// SurgeDrain is the Phase 2 step (surge ready, draining old). Pod
	// still counts as in-flight surge for budget purposes — the extra
	// pod is alive until old is deleted.
	isvc := mkSurgeFixture(iosInt(1))
	insts := []v1beta1.OMENativeInstanceStatus{{
		Index:     0,
		Phase:     v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "SurgeDrain"},
	}}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
	}
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0, insts...)
	if allowed {
		t.Errorf("SurgeDrain step must count toward in-flight surge budget")
	}
}

func TestCheckSurgeGate_SurgeDrainSettleStepAlsoCounts(t *testing.T) {
	isvc := mkSurgeFixture(iosInt(1))
	insts := []v1beta1.OMENativeInstanceStatus{{
		Index:     0,
		Phase:     v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "SurgeDrainSettle"},
	}}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
	}
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0, insts...)
	if allowed {
		t.Errorf("SurgeDrainSettle step must count toward in-flight surge budget")
	}
}

func TestCheckSurgeGate_OtherStepsDoNotCount(t *testing.T) {
	// Step=Drain (recreate) and Step=InPlace are NOT surge — they don't
	// add an extra pod. Gate must not count them.
	for _, step := range []string{"Drain", "InPlace"} {
		isvc := mkSurgeFixture(iosInt(1))
		insts := []v1beta1.OMENativeInstanceStatus{{
			Index:     0,
			Phase:     v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: step},
		}}
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
		}
		allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0, insts...)
		if !allowed {
			t.Errorf("step=%s (not surge) must not consume surge budget", step)
		}
	}
}

func TestCheckSurgeGate_InFlightDeltaProjection(t *testing.T) {
	// No status in-flight, but dispatcher's wake-up tally says we already
	// kicked off 1 surge this wake-up. With MaxSurge=1, projected=2 → DENIED.
	isvc := mkSurgeFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5}},
	}
	allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 1)
	if allowed {
		t.Errorf("inFlightDelta=1 + budget=1 must deny the next surge")
	}
}

func TestCheckSurgeGate_PercentBudget(t *testing.T) {
	// MaxSurge=25%, 8 replicas → ceil(8*0.25)=2 budget. 0 in-flight,
	// projected=1 → ALLOWED. inFlightDelta=2 would project=3 → DENIED.
	pct := intstr.FromString("25%")
	isvc := mkSurgeFixture(&pct)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 8}},
	}
	if allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 0); !allowed {
		t.Errorf("25%% of 8 = 2, 0 in-flight, projected=1: allow")
	}
	if allowed, _ := CheckSurgeGate(isvc, v1beta1.EngineComponent, 2); allowed {
		t.Errorf("25%% of 8 = 2, in-flight=2, projected=3: deny")
	}
}

// mkSymmetricRatioFixture builds a symmetric engine 4 : decoder 4
// RatioBalanced group with an ObservedRatio snapshot of 4:4 and both
// Components fully serving (4/4). This is the symmetric deadlock
// shape: a 1:1 ratio where, at tolerance 25%, the discrete pod
// granularity leaves no room for a single -1 drain.
func mkSymmetricRatioFixture(tol int32) *v1beta1.InferenceService {
	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
			},
		},
	})
}

// TestCheckRatio_SurgeVsDrainProjection_SymmetricBand pins that a
// symmetric 4:4 RatioBalanced pair (tol=25% → band [0.75, 1.25]) is not
// deadlocked in EITHER direction:
//   - surge (+1) projects 5/4 = 1.25, in band → allowed (the
//     projection must carry the surge sign);
//   - drain (-1) projects 4/3 = 1.333, which the STRICT band rejects, but
//     the discrete-granularity tiebreaker allows one rounding-error
//     pod from a balanced baseline (1.333 <= 2x band 1.5) → allowed.
//
// If the code hardcoded -1 for ALL strategies, even a SurgeThenDrain
// roll would hit the drain math and BOTH members would deadlock at the
// start. Asymmetric large skews still stay denied — see
// ratio_drain_tiebreaker_test.go.
func TestCheckRatio_SurgeVsDrainProjection_SymmetricBand(t *testing.T) {
	isvc := mkSymmetricRatioFixture(25)
	ctx := ResolveGateContext(context.Background(), fakeClientForISVC(isvc), isvc, v1beta1.EngineComponent)

	if allowed, reason := ctx.CheckRatio(0, 0, +1); !allowed {
		t.Errorf("surge projection (+1) on symmetric 4:4 tol=25%% must be allowed (5/4=1.25 <= 1.25); got denied: %s", reason)
	}
	if allowed, reason := ctx.CheckRatio(0, 0, -1); !allowed {
		t.Errorf("drain (-1) on symmetric 4:4 tol=25%% must be allowed by the discrete-granularity tiebreaker (1.333 <= 2x band 1.5); got denied: %s", reason)
	}
}

// TestEvaluateUpdateGate_RatioBalancedSurgeNotDeadlocked is the
// end-to-end symmetric-deadlock regression: a SurgeThenDrain roll on a
// symmetric 4:4 RatioBalanced group must NOT be blocked by the gate
// stack (it would be if CheckRatio projected -1 for it).
func TestEvaluateUpdateGate_RatioBalancedSurgeNotDeadlocked(t *testing.T) {
	isvc := mkSymmetricRatioFixture(25)
	client := fakeClientForISVC(isvc)

	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Errorf("SurgeThenDrain RatioBalanced 4:4 must be allowed (no deadlock); got denied: %s", reason)
	}
	// (RecreatePod on a symmetric pair is likewise allowed, by the
	// tiebreaker — see TestEvaluateUpdateGate_RecreatePodSymmetricCleared.)
}

// mkRatioProgressFixture builds a 2-member [engine, decoder] RatioBalanced
// group with per-Component (original, new-revision, serving) counts so the
// surge-path new-revision lockstep can be exercised end-to-end through
// EvaluateUpdateGate. Serving is held at the original for both Components —
// the SurgeThenDrain steady state — so the serving-capacity band alone
// would authorize every surge; only the new-revision lockstep can pace it.
func mkRatioProgressFixture(tol, origE, newE, origD, newD int32) *v1beta1.InferenceService {
	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: &tol},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  origE,
							v1beta1.DecoderComponent: origD,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{
					Replicas: origE, ServingReplicas: origE, UpdatedReplicas: newE,
				}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{
					Replicas: origD, ServingReplicas: origD, UpdatedReplicas: newD,
				}},
			},
		},
	})
}

// TestEvaluateUpdateGate_RatioBalancedHoldsLeadingComponent is the
// leading-Component regression: a
// multinode SurgeThenDrain RatioBalanced roll let the engine rotate its
// Instances onto the new revision while the decoder lagged, holding the
// live ready-capacity ratio off band for the whole gap. The serving
// counts stay at the original N for both Components throughout a
// SurgeThenDrain roll (surge rotates IN before the source drains OUT), so
// the serving-capacity band — and its deadlock-breaking tiebreaker —
// authorize every surge and never pace the leader. The new-revision
// lockstep is what holds it.
//
// Each row reads "engine at newE/origE rolled, decoder at newD/origD; may
// `comp` start its next surge?" through the production decision site
// (EvaluateUpdateGate with SurgeThenDrain).
func TestEvaluateUpdateGate_RatioBalancedHoldsLeadingComponent(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	cases := []struct {
		name                     string
		tol                      int32
		origE, newE, origD, newD int32
		comp                     v1beta1.ComponentType
		wantAllowed              bool
	}{
		{ // Start of roll: nobody is ahead → opening surge proceeds.
			name: "N=2 tol=25 both at 0 new — first surge allowed", tol: 25,
			origE: 2, newE: 0, origD: 2, newD: 0, comp: eng, wantAllowed: true,
		},
		{ // The leading-Component bug: engine already 1/2 (50%), decoder 0/2 (0%);
			// engine's NEXT surge would extend a 50pp lead > 25% → HELD.
			name: "N=2 tol=25 engine 1 ahead of decoder 0 — engine held", tol: 25,
			origE: 2, newE: 1, origD: 2, newD: 0, comp: eng, wantAllowed: false,
		},
		{ // …while the lagging decoder (0/2, behind engine's 50%) proceeds,
			// so the pair converges instead of deadlocking.
			name: "N=2 tol=25 decoder behind — decoder catches up", tol: 25,
			origE: 2, newE: 1, origD: 2, newD: 0, comp: dec, wantAllowed: true,
		},
		{ // Both level at 1/2 → neither leads → both may take the next step.
			name: "N=2 tol=25 both at 1 new — level, engine proceeds", tol: 25,
			origE: 2, newE: 1, origD: 2, newD: 1, comp: eng, wantAllowed: true,
		},
		{ // Tight band at N=4: engine 1/4 (25%) vs decoder 0 (0%) is a
			// 25pp lead > 10% tol → engine held one step earlier than at 25%.
			name: "N=4 tol=10 engine 1 ahead — engine held", tol: 10,
			origE: 4, newE: 1, origD: 4, newD: 0, comp: eng, wantAllowed: false,
		},
		{ // Asymmetric 4:2 rolls in FRACTION lockstep: engine 2/4 (50%) and
			// decoder 1/2 (50%) are level, so engine's next surge proceeds —
			// the guard is ratio-correct, not a raw equal-count rule.
			name: "asymmetric 4:2 both at 50% — engine proceeds", tol: 25,
			origE: 4, newE: 2, origD: 2, newD: 1, comp: eng, wantAllowed: true,
		},
		{ // Asymmetric 4:2: engine 2/4 (50%) ahead of decoder 0/2 (0%) →
			// 50pp lead > 25% → engine held.
			name: "asymmetric 4:2 engine 50% ahead of decoder 0% — held", tol: 25,
			origE: 4, newE: 2, origD: 2, newD: 0, comp: eng, wantAllowed: false,
		},
		{ // No false hold at the finish: once the decoder is fully rolled it
			// stops pacing, so the engine's trailing surges complete.
			name: "decoder done — engine final surge allowed", tol: 25,
			origE: 2, newE: 1, origD: 2, newD: 2, comp: eng, wantAllowed: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := mkRatioProgressFixture(tc.tol, tc.origE, tc.newE, tc.origD, tc.newD)
			client := fakeClientForISVC(isvc)
			allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, tc.comp, nil, GroupDefaults{},
				workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
			if allowed != tc.wantAllowed {
				t.Errorf("allowed=%v want %v (reason=%q)", allowed, tc.wantAllowed, reason)
			}
		})
	}
}

// TestEvaluateSurge_SurgeTiebreaker pins the surge-side tiebreaker. When the
// band is narrower than one whole pod, a surge-first roll deadlocks because the
// discrete (N+1):N step exceeds the band. A surge only ADDS capacity (it can
// NEVER starve a peer) and SurgeThenDrain drains it back, so ONE minimal +1
// surge from a balanced full-original baseline is allowed through regardless of
// overshoot magnitude. Only mid-roll / already-skewed / zero-peer baselines
// stay denied (guards (a) full-original + (b) balanced). The overshoot is NOT
// capped: a 2x-band cap would permanently wedge the unavoidable minimal
// surge at small N / tight band (N=1 2:1, tol 10% at N=4 5:4) — a roll
// that can only proceed by surging.
func TestEvaluateSurge_SurgeTiebreaker(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	cases := []struct {
		name        string
		tol         int32
		original    map[v1beta1.ComponentType]int32
		serving     map[v1beta1.ComponentType]int32
		comp        v1beta1.ComponentType
		wantAllowed bool
	}{
		// Allowed — rounding-error overshoot within 2x band:
		{ // 3:3 -> 4:3 = 1.333 <= 2x band 1.5
			name: "N=3 symmetric (ratio_n3)", tol: 25, comp: eng, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 3, dec: 3},
			serving:  map[v1beta1.ComponentType]int32{eng: 3, dec: 3},
		},
		{ // decoder 2->3, 4:3 = 1.333 within orig-2.0 2x band [1.0,3.0]
			name: "asymmetric 4:2, small decoder surges", tol: 25, comp: dec, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
			serving:  map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
		},
		{ // decoder 2->3, 8:3 = 2.667 within orig-4.0 2x band [2.0,6.0]
			name: "asymmetric 8:2, small decoder surges", tol: 25, comp: dec, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 8, dec: 2},
			serving:  map[v1beta1.ComponentType]int32{eng: 8, dec: 2},
		},
		// A minimal +1 surge on a balanced full-original baseline is
		// allowed regardless of overshoot magnitude: a surge can't starve
		// a peer and SurgeThenDrain drains it back. A 2x-band cap would
		// leave these as permanent deadlocks, wedging a roll that can ONLY
		// proceed by surging, on both single-pod and gang shapes.
		{ // 1:1 -> 2:1 — N=1 can only roll by surging
			name: "N=1 minimal surge", tol: 25, comp: eng, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
			serving:  map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
		},
		{ // 4:4 tol=10 -> 5:4 — band narrower than one pod
			name: "tight tol=10 at N=4 minimal surge", tol: 10, comp: eng, wantAllowed: true,
			original: map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			serving:  map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pacing := v1beta1.CoordinationPacing{
				Type:                  v1beta1.CoordinationPacingRatioBalanced,
				RatioTolerancePercent: int32Ptr(tc.tol),
			}
			d := EvaluateSurge(pacing, RatioState{Original: tc.original, Serving: tc.serving}, tc.comp, +1)
			got := d.AllowedSurgeDelta != 0
			if got != tc.wantAllowed {
				t.Errorf("surge allowed=%v want %v (delta=%d, reason=%q)", got, tc.wantAllowed, d.AllowedSurgeDelta, d.Reason)
			}
		})
	}
}

// TestEvaluateSurge_DrainStaysStrictAfterSurgeTiebreaker guards that the
// surge tiebreaker did NOT loosen the DRAIN direction: a -1 drain that
// exceeds 2x band on a small asymmetric side stays denied — a drain is the
// real capacity-shortfall risk the band exists for. (4:2, decoder drains to
// 1 → 4:1 = 4.0 > 2x-band ceiling 3.0.)
func TestEvaluateSurge_DrainStaysStrictAfterSurgeTiebreaker(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(25),
	}
	d := EvaluateSurge(pacing, RatioState{
		Original: map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
		Serving:  map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
	}, dec, -1)
	if d.AllowedSurgeDelta != 0 {
		t.Errorf("a drain exceeding 2x band (4:1 from orig 2:1) must stay DENIED; the surge tiebreaker must not loosen drains; got allowed (reason=%q)", d.Reason)
	}
}

// TestEvaluateSurge_DrainTiebreaker pins the Part-2 discrete-granularity
// tiebreaker: a drain-first roll (RecreatePod / in-place) gets exactly ONE
// rounding-error pod through when a tight band otherwise permits zero
// whole-pod moves, but large asymmetric skews stay denied and a second
// pod (from an already-skewed baseline) is refused.
func TestEvaluateSurge_DrainTiebreaker(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(25), // band [0.75, 1.25]; 2x band [0.5, 1.5]
	}
	cases := []struct {
		name        string
		original    map[v1beta1.ComponentType]int32
		serving     map[v1beta1.ComponentType]int32
		comp        v1beta1.ComponentType
		wantAllowed bool
	}{
		{
			// 4/3 = 1.333 > 1.25 (strict) but <= 1.5 (2x band) from a
			// balanced 4:4 → tiebreaker allows the one pod.
			name:        "symmetric 4:4 drain — tiebreaker allows one pod",
			original:    map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			serving:     map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			comp:        eng,
			wantAllowed: true,
		},
		{
			// 4:1 = 4.0 > 3.0 (2x band on orig 2.0) → too large an overshoot,
			// stays an honest deadlock (operator widens tolerance).
			name:        "asymmetric 4:2 decoder drain — large skew denied",
			original:    map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
			serving:     map[v1beta1.ComponentType]int32{eng: 4, dec: 2},
			comp:        dec,
			wantAllowed: false,
		},
		{
			// Baseline already 3:4 (one pod out) → not in band → tiebreaker
			// refuses, bounding in-flight drains to ONE pod at a time.
			name:        "bounded — second pod from skewed 3:4 baseline denied",
			original:    map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
			serving:     map[v1beta1.ComponentType]int32{eng: 3, dec: 4},
			comp:        eng,
			wantAllowed: false,
		},
		{
			// Draining the last pod would zero a side → always refused
			// (mass-drain guard), even from a balanced 1:1 baseline.
			name:        "zero-guard — draining the last pod denied",
			original:    map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
			serving:     map[v1beta1.ComponentType]int32{eng: 1, dec: 1},
			comp:        eng,
			wantAllowed: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := EvaluateSurge(pacing, RatioState{Original: tc.original, Serving: tc.serving}, tc.comp, -1)
			gotAllowed := d.AllowedSurgeDelta != 0
			if gotAllowed != tc.wantAllowed {
				t.Errorf("allowed=%v want %v (delta=%d, skewRejected=%v, reason=%q)",
					gotAllowed, tc.wantAllowed, d.AllowedSurgeDelta, d.SkewRejected, d.Reason)
			}
		})
	}
}

// TestEvaluateUpdateGate_RecreatePodSymmetricCleared is the end-to-end
// tiebreaker regression: a RecreatePod (drain-first) roll on a
// symmetric 4:4 RatioBalanced group, with a MaxUnavailable budget so the
// unavailability gate permits the pod, must be allowed — the ratio
// tiebreaker clears the drain that would otherwise deadlock.
func TestEvaluateUpdateGate_RecreatePodSymmetricCleared(t *testing.T) {
	isvc := mkSymmetricRatioFixture(25)
	mu := intstr.FromInt(3)
	isvc.Spec.Rollout.Groups[0].RollingUpdate.MaxUnavailable = &mu
	pinActiveRun(isvc) // re-pin: the budget edit must be part of the pinned plan
	client := fakeClientForISVC(isvc)

	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategyRecreatePod, 0, 0); !allowed {
		t.Errorf("RecreatePod on symmetric 4:4 (MaxUnavailable=3) must be allowed via the tiebreaker; got denied: %s", reason)
	}
}

// TestEvaluateSurge_DrainTiebreaker_InFlightOpBoundsAcrossWakeups pins the
// cross-wake-up bound: when a prior recreate's drain is still in flight
// (operation-tracked InFlightUnavail=1) but the cache-derived Serving still
// over-reports a full 4:4 (the watch lag), the tiebreaker must NOT fire — it
// would let a SECOND pod drain (2:4, past the band) during a recreate burst.
// The fresh op count is what catches it; stale Serving alone does not.
func TestEvaluateSurge_DrainTiebreaker_InFlightOpBoundsAcrossWakeups(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(25),
	}
	// Serving STALE-high at 4:4 (cache hasn't observed the prior drain), but
	// InFlightUnavail=1 (op-tracked: one engine pod is already mid-recreate).
	state := RatioState{
		Original:        map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
		Serving:         map[v1beta1.ComponentType]int32{eng: 4, dec: 4},
		InFlightUnavail: map[v1beta1.ComponentType]int32{eng: 1},
	}
	if d := EvaluateSurge(pacing, state, eng, -1); d.AllowedSurgeDelta != 0 {
		t.Errorf("a second drain must be denied while a prior drain is in flight "+
			"(InFlightUnavail=1), even though stale Serving shows a full 4:4; got allowed (reason=%q)", d.Reason)
	}

	// Sanity: with InFlightUnavail=0 (no prior drain), the FIRST pod IS allowed.
	state.InFlightUnavail = map[v1beta1.ComponentType]int32{}
	if d := EvaluateSurge(pacing, state, eng, -1); d.AllowedSurgeDelta == 0 {
		t.Errorf("the first drain (no in-flight op) must be allowed by the tiebreaker; got denied (reason=%q)", d.Reason)
	}
}

// Decoder surge-budget pins: a bumped SurgeThenDrain maxSurge=1 decoder
// must peak at N+1, not be clamped to N.
//
// Shape (both cases): a PD [engine, decoder] coordination group, engine
// maxSurge=50% (per-Component) and decoder SurgeThenDrain maxSurge=1, with
// the group's pacing.maxSurge left unset → defaulted to "25%". Both
// Components are bumped in the same spec update, so each
// rolls off its OWN revision and surges per its OWN budget — the documented
// per-Component surge model ("each component paces on its OWN budget, no
// cross-ratio"; a bumped SurgeThenDrain maxSurge=1 decoder surges to N+1=5).
//
// The correct decoder peak is therefore N(4) + maxSurge(1) = 5. These tests
// lock that in at the gate level: the decoder is permitted EXACTLY ONE surge
// (one extra pod alive → peak 5) and is gated from a SECOND (peak never
// reaches 6). A future change that wrongly clamped the decoder to peak 4
// (surge 0) would make
// the "allowed" assertions fail; a change that let it surge twice would make
// the "denied" assertions fail.

// mkPDGroupSurge builds the group shape for the DECODER's surge
// gate: a 2-member [engine, decoder] group with the given pacing, the group's
// pacing.MaxSurge defaulted to "25%" (= budget 1 at N=4), and a 4:4
// ObservedRatio anchor (needed by RatioBalanced; harmless for PerComponent).
// decoderSurgesInFlight controls how many decoder Instances are already on a
// surge step (Surge/SurgeDrain) from a prior wake-up.
func mkPDGroupSurge(pacingType v1beta1.CoordinationPacingType, decoderSurgesInFlight int) (*v1beta1.InferenceService, []v1beta1.OMENativeInstanceStatus) {
	groupSurge := intstr.FromString("25%") // the webhook default when maxSurge is unset
	group := v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{MaxSurge: &groupSurge},
	}
	// RatioBalanced pacing is expressed as MaintainRatio on the
	// rollingUpdate group; PerComponent is the absence of MaintainRatio.
	if pacingType == v1beta1.CoordinationPacingRatioBalanced {
		group.MaintainRatio = &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(25))}
	}

	decoderInstances := make([]v1beta1.OMENativeInstanceStatus, 0, decoderSurgesInFlight)
	for i := 0; i < decoderSurgesInFlight; i++ {
		decoderInstances = append(decoderInstances, v1beta1.OMENativeInstanceStatus{
			Index:     int32(i),
			Phase:     v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "Surge"},
		})
	}

	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{group},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{
					Replicas:        4,
					ServingReplicas: 4,
				}},
			},
		},
	}), decoderInstances
}

// TestDecoderSurgeBudget_RatioBalanced pins the shape: a bumped
// SurgeThenDrain maxSurge=1 decoder under RatioBalanced pacing surges EXACTLY
// once (peak N+1 = 5) and no more.
func TestDecoderSurgeBudget_RatioBalanced(t *testing.T) {
	// First surge: no decoder surge in flight → the gate stack (CheckRatio
	// +1 projects 5:4 = 1.25, in band [0.75,1.25]; CheckSurge 0+0+1 <=
	// budget 1) must ALLOW. This is the surge that takes the decoder to
	// peak 5.
	isvc, decInsts := mkPDGroupSurge(v1beta1.CoordinationPacingRatioBalanced, 0)
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.DecoderComponent: decInsts})
	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.DecoderComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("RatioBalanced: decoder's FIRST surge (→ peak N+1=5) must be allowed; got denied: %s", reason)
	}

	// Second surge: one decoder Instance already surging → CheckSurge
	// projects 1+0+1 = 2 > budget 1 → must DENY. This is what keeps the
	// decoder peak at 5 and not 6. (If the decoder were wrongly clamped to
	// peak 4, the first-surge assertion above would already have failed.)
	isvc, decInsts = mkPDGroupSurge(v1beta1.CoordinationPacingRatioBalanced, 1)
	client = fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.DecoderComponent: decInsts})
	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.DecoderComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); allowed {
		t.Fatalf("RatioBalanced: decoder's SECOND surge (would be peak 6 > maxSurge=1 budget) must be denied; got allowed (reason=%s)", reason)
	}
}

// TestDecoderSurgeBudget_PerComponent pins the identical shape under
// PerComponent pacing (no cross-ratio gate). The decoder still
// surges exactly once to peak 5 — governed solely by the group MaxSurge
// budget (defaulted "25%" = 1 at N=4).
func TestDecoderSurgeBudget_PerComponent(t *testing.T) {
	isvc, decInsts := mkPDGroupSurge(v1beta1.CoordinationPacingPerComponent, 0)
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.DecoderComponent: decInsts})
	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.DecoderComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("PerComponent: decoder's FIRST surge (→ peak N+1=5) must be allowed; got denied: %s", reason)
	}

	isvc, decInsts = mkPDGroupSurge(v1beta1.CoordinationPacingPerComponent, 1)
	client = fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.DecoderComponent: decInsts})
	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.DecoderComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); allowed {
		t.Fatalf("PerComponent: decoder's SECOND surge (would be peak 6 > maxSurge=1 budget) must be denied; got allowed (reason=%s)", reason)
	}
}

// TestDecoderSurgeBudget_InWakeUpDeltaGatesSecondSurge guards the
// within-wake-up tally path: even before status reflects the first surge, the
// dispatcher threads inFlightSurge into the gate, so a second surge in the
// SAME wake-up (inFlightSurge=1) is denied. This is the other half of
// "decoder peak never exceeds 5" — the stale-snapshot hole that would
// otherwise let the dispatcher fire two decoder surges in one pass.
func TestDecoderSurgeBudget_InWakeUpDeltaGatesSecondSurge(t *testing.T) {
	for _, pt := range []v1beta1.CoordinationPacingType{
		v1beta1.CoordinationPacingRatioBalanced,
		v1beta1.CoordinationPacingPerComponent,
	} {
		isvc, decInsts := mkPDGroupSurge(pt, 0)
		client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.DecoderComponent: decInsts})
		// inFlightSurge=1 → CheckSurge projects 0+1+1 = 2 > budget 1.
		if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.DecoderComponent, nil, GroupDefaults{},
			workloadtypes.UpdateStrategySurgeThenDrain, 1, 0); allowed {
			t.Errorf("pacing=%s: a second decoder surge in one wake-up (inFlightSurge=1, budget=1) must be denied; got allowed (reason=%s)", pt, reason)
		}
	}
}

// mkGangSurgeFixture builds a symmetric engine 2 : decoder 2 RatioBalanced
// group (tol 25%, band [0.75, 1.25]) where ENGINE is mid multi-node (gang)
// SurgeThenDrain: its surge-target Instance has a full, serving gang, so the
// status aggregator counts it and engine ServingReplicas reads N+1=3 — the
// transient surge PEAK. Decoder is at steady N=2.
//
// For a single-pod Component the surge flip is atomic within one reconcile,
// so ServingReplicas never reports N+1 across a status write; for a gang the
// surge gang lingers serving alongside the source, so the gate genuinely
// observes the peak. The gate must treat that peak as the transient surge
// headroom it is — NOT durable serving capacity a drain may eat into.
func mkGangSurgeFixture() (*v1beta1.InferenceService, []v1beta1.OMENativeInstanceStatus) {
	engineInstances := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
			PodCount: 2, ServingPodCount: 2,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate, Step: "Surge",
				SurgeIndex: int32Ptr(2)}},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 2, ServingPodCount: 2},
		// Surge-target gang: full pod set, all serving →
		// counted in ServingReplicas (the N+1 peak).
		{Index: 2, Phase: v1beta1.OMENativeInstanceCreating,
			PodCount: 2, ServingPodCount: 2,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate,
				Step: workloadtypes.UpdateStepGangSurgeTarget}},
	}
	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(25))},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 3, // N=2 source + 1 surge gang = peak
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  2,
							v1beta1.DecoderComponent: 2,
						},
					},
				}},
			},
		},
	}), engineInstances
}

// TestCheckRatioGate_GangSurgePeakDoesNotAuthorizeDrain pins multi-node
// RatioBalanced pacing during a gang surge. Engine is mid gang
// SurgeThenDrain so its ServingReplicas reads the transient PEAK N+1=3
// against decoder's steady N=2. A drain of one engine Instance would, once
// the surge settles back to N, leave engine at 1 : decoder 2 = ratio 2.0 vs
// the 1:1 anchor — far past the 25% band. The gate must REJECT the drain.
//
// Counting the surge peak as durable serving would read 3:2, project 2:2
// for a -1 drain (in band) and ALLOW it. The gate nets the in-flight
// gang-surge peak out, sees the steady 2:2, and a -1 drain projects 1:2
// (out of band) → DENIED. Single-pod is unaffected: it has no
// GangSurgeTarget Instance, so nothing is netted.
func TestCheckRatioGate_GangSurgePeakDoesNotAuthorizeDrain(t *testing.T) {
	isvc, engInsts := mkGangSurgeFixture()
	allowed, reason := CheckRatioGate(isvc, v1beta1.EngineComponent, 0, engInsts...)
	if allowed {
		t.Fatalf("a drain on an engine whose serving is inflated by an in-flight "+
			"gang surge (peak 3 over steady 2) must be DENIED — once the surge "+
			"settles the drain leaves 1:2 (ratio 2.0 vs 1:1 anchor, >25%% band); "+
			"got allowed (reason=%q)", reason)
	}
}

// TestUnavailableInFlight_GangSurgeTargetNotCountedUnavailable pins that a
// gang surge TARGET Instance (Step=GangSurgeTarget) is NOT counted as
// unavailable-in-flight — it ADDS a replacement gang, it does not pull one
// from rotation. Counting it would poison the drain-side tiebreaker's
// "no pod already out of rotation" guard during every gang surge.
func TestUnavailableInFlight_GangSurgeTargetNotCountedUnavailable(t *testing.T) {
	statuses := []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Operation: &v1beta1.InstanceOperation{Step: "Surge"}},
		{Index: 1, Operation: &v1beta1.InstanceOperation{Step: workloadtypes.UpdateStepGangSurgeTarget}},
		{Index: 2, Operation: &v1beta1.InstanceOperation{Step: "Drain"}},
	}
	if got := unavailableInFlight(statuses); got != 1 {
		t.Fatalf("only the Drain Instance is unavailable-in-flight; Surge and "+
			"GangSurgeTarget add/keep capacity: want 1, got %d", got)
	}
}

// mkGangSurgeDrainedFixture is mkGangSurgeFixture advanced one step: the
// engine surge source (idx 0) has been DRAINED from serving (gangSurgeUpdate
// flipped it serving=False before deletion), so engine ServingReplicas reads
// the STEADY N=2 (idx1 serving + the surge target idx2 serving). The target
// still carries its GangSurgeTarget marker — it isn't promoted/removed until a
// later reconcile. Decoder is at steady N=2. This is the post-drain / pre-
// promote window where there is NO peak left to net out.
func mkGangSurgeDrainedFixture() (*v1beta1.InferenceService, []v1beta1.OMENativeInstanceStatus) {
	engineInstances := []v1beta1.OMENativeInstanceStatus{
		// Source: drained from serving (0/2), delete pending.
		{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating,
			PodCount: 2, ServingPodCount: 0,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate, Step: "Surge",
				SurgeIndex: int32Ptr(2)}},
		{Index: 1, Phase: v1beta1.OMENativeInstanceReady,
			PodCount: 2, ServingPodCount: 2},
		// Surge target still serving + still marked, but its
		// source has drained → it IS the steady serving slot.
		{Index: 2, Phase: v1beta1.OMENativeInstanceCreating,
			PodCount: 2, ServingPodCount: 2,
			Operation: &v1beta1.InstanceOperation{
				Type: v1beta1.InstanceOperationUpdate,
				Step: workloadtypes.UpdateStepGangSurgeTarget}},
	}
	return pinActiveRun(&v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					RollingUpdate: &v1beta1.GroupRollingUpdate{},
					MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(25))},
				}},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2, // source drained → steady N, NOT a peak
					},
				},
				v1beta1.DecoderComponent: {
					Lifecycle: &v1beta1.LifecycleStatus{
						Replicas:        2,
						ServingReplicas: 2,
					},
				},
			},
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  2,
							v1beta1.DecoderComponent: 2,
						},
					},
				}},
			},
		},
	}), engineInstances
}

// TestCheckRatioGate_DrainedGangSurgeNotANetTrough: the gate nets the
// gang-surge PEAK (replacement serving beside the not-yet-drained
// source) out of its serving view. Netting it UNCONDITIONALLY — still
// subtracting the target after gangSurgeUpdate drained the source from
// serving — makes the gate see N-1 in the post-drain / pre-promote
// window: a phantom TROUGH, even though ServingReplicas has already
// collapsed to the steady N. With engine and decoder gangs flipping at
// staggered times, that N-1 read swings the live ratio to 2:1 / 1:2.
//
// Here engine is in that window: source drained (0/2), target serving (2/2) →
// engine's STEADY serving is N=2, equal to decoder. A DECODER drain would take
// the true 2:2 to engine 2 : decoder 1 = ratio 2.0 vs the 1:1 anchor — past the
// 25% band — and MUST be denied.
//
// Netting the still-marked target out of engine's serving would read
// engine=1, and a decoder -1 would project 1:1 (in band) → ALLOWED, driving
// the very trough this proves. Because the target's source is not serving,
// nothing is netted: engine reads the steady 2, and the decoder drain
// projects 2:1 (out of band) → DENIED.
func TestCheckRatioGate_DrainedGangSurgeNotANetTrough(t *testing.T) {
	isvc, engInsts := mkGangSurgeDrainedFixture()
	if got := servingSurgePeakInFlight(engInsts); got != 0 {
		t.Fatalf("once the surge source is drained the target is the steady "+
			"serving slot, not a peak: want servingSurgePeakInFlight=0, got %d", got)
	}
	// The gang-surge instance detail lives on the engine IR; overlay it there
	// (not the queried decoder) so the gate nets engine's serving correctly.
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{v1beta1.EngineComponent: engInsts})
	allowed, reason := ResolveGateContext(context.Background(), client, isvc, v1beta1.DecoderComponent).CheckRatio(0, 0, -1)
	if allowed {
		t.Fatalf("a decoder drain against an engine at its STEADY N=2 (surge "+
			"source already drained) projects 2:1 (ratio 2.0 vs 1:1 anchor, "+
			">25%% band) and must be DENIED; the gate wrongly netted the still-"+
			"marked surge target out of engine serving (phantom N-1 trough) and "+
			"allowed it (reason=%q)", reason)
	}
}

// CheckRatioGate, CheckUnavailabilityGate, and CheckSurgeGate are
// test-only shims preserving the (isvc, component, inFlightDelta) call
// shape the gate unit tests exercise. The production package-level
// wrappers were removed (zero production callers — the dispatcher uses
// the GateContext-aware methods via EvaluateUpdateGate); these helpers
// keep the per-gate band-math coverage by delegating to the same
// GateContext methods the wrappers did.
func CheckRatioGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32, insts ...v1beta1.OMENativeInstanceStatus) (bool, string) {
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{component: insts})
	return ResolveGateContext(context.Background(), client, isvc, component).CheckRatio(0, inFlightDelta, -1)
}

func CheckUnavailabilityGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32, insts ...v1beta1.OMENativeInstanceStatus) (bool, string) {
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{component: insts})
	return ResolveGateContext(context.Background(), client, isvc, component).CheckUnavailability(inFlightDelta)
}

func CheckSurgeGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, inFlightDelta int32, insts ...v1beta1.OMENativeInstanceStatus) (bool, string) {
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{component: insts})
	return ResolveGateContext(context.Background(), client, isvc, component).CheckSurge(inFlightDelta)
}

// The fail-closed trio: a transient IR read error must DENY, not read as "no
// observation yet". RatioBalanced also denies a missing member IR because its
// cross-Component decision requires a complete view. The single-Component
// surge and unavailability gates retain their first-observation behavior.

func TestCheckRatio_ReadErrorFailsClosed(t *testing.T) {
	isvc := mkPDFixture(nil)
	reader := &failingIRClient{Client: fakeClientForISVC(isvc)}
	allowed, reason := ResolveGateContext(context.Background(), reader, isvc, v1beta1.EngineComponent).CheckRatio(0, 0, -1)
	if allowed {
		t.Fatalf("ratio gate must fail closed on IR read error: %s", reason)
	}
	if !strings.Contains(reason, "failing closed") {
		t.Errorf("denial reason should mark the degraded read: %s", reason)
	}
}

func TestCheckUnavailability_ReadErrorFailsClosed(t *testing.T) {
	mu := intstr.FromInt32(2)
	isvc := mkUnavailFixture(&mu)
	reader := &failingIRClient{Client: fakeClientForISVC(isvc)}
	allowed, reason := ResolveGateContext(context.Background(), reader, isvc, v1beta1.EngineComponent).CheckUnavailability(0)
	if allowed {
		t.Fatalf("unavailability gate must fail closed on IR read error: %s", reason)
	}
	if !strings.Contains(reason, "failing closed") {
		t.Errorf("denial reason should mark the degraded read: %s", reason)
	}
}

func TestCheckSurge_ReadErrorFailsClosed(t *testing.T) {
	ms := intstr.FromInt32(2)
	isvc := mkSurgeFixture(&ms)
	reader := &failingIRClient{Client: fakeClientForISVC(isvc)}
	allowed, reason := ResolveGateContext(context.Background(), reader, isvc, v1beta1.EngineComponent).CheckSurge(0)
	if allowed {
		t.Fatalf("surge gate must fail closed on IR read error: %s", reason)
	}
	if !strings.Contains(reason, "failing closed") {
		t.Errorf("denial reason should mark the degraded read: %s", reason)
	}
}

// TestEvaluateUpdateGate_PlanGateHoldsWithoutRun pins the run-model ordering
// invariant: a Component in a declared rollout group takes no updates until a
// run pins the effective plan — the gate denies with RolloutHoldGatePlan.
// Pinning the run restores the normal allowed path.
func TestEvaluateUpdateGate_PlanGateHoldsWithoutRun(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 5, ReadyReplicas: 5, ServingReplicas: 5},
		},
	}
	isvc.Status.Rollout = nil // no run pinned yet
	client := fakeClientForISVC(isvc)

	allowed, gate, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if allowed {
		t.Fatalf("a grouped Component without an active run must be denied, got allowed (reason=%q)", reason)
	}
	if gate != v1beta1.RolloutHoldGatePlan {
		t.Fatalf("denial must name the plan gate, got gate=%q reason=%q", gate, reason)
	}

	pinActiveRun(isvc)
	allowed, gate, reason = EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if !allowed {
		t.Fatalf("with the run pinned the normal gate path must allow, got denied: gate=%q reason=%q", gate, reason)
	}
}

// TestResolveGateContext_HoldsWithoutActiveRun pins the GateContext surface of
// the same invariant: group membership with no active run resolves to a Hold
// (not a ShortCircuit, not a resolved group).
func TestResolveGateContext_HoldsWithoutActiveRun(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Rollout = nil
	ctx := ResolveGateContext(context.Background(), nil, isvc, v1beta1.EngineComponent)
	if !ctx.Hold || ctx.ShortCircuit {
		t.Fatalf("grouped Component without a run: want Hold=true ShortCircuit=false, got %+v", ctx)
	}
	if ctx.HoldReason == "" {
		t.Fatal("Hold must carry an operator-facing reason")
	}
	// A Component outside every group is not held — it is outside the run.
	ctx = ResolveGateContext(context.Background(), nil, isvc, v1beta1.RouterComponent)
	if ctx.Hold || !ctx.ShortCircuit {
		t.Fatalf("ungrouped Component: want ShortCircuit (not Hold), got %+v", ctx)
	}
}
