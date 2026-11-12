package coordination

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// pairingScheme registers both the OME types (InferenceReplica) and appsv1
// (ControllerRevision) the pairing gate reads.
func pairingScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1: %v", err)
	}
	// The unavailability gate lists the Component's pods, so a client with
	// no pods must still answer the list with none.
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return scheme
}

// pairingCR builds the ControllerRevision `<isvc>-<component>-<hash>` carrying
// the pairing annotation the way EnsureControllerRevisionFromHash stamps it
// (absent when the protocol is empty).
func pairingCR(namespace, isvcName string, component v1beta1.ComponentType, hash, protocol string) *appsv1.ControllerRevision {
	cr := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      isvcName + "-" + string(component) + "-" + hash,
		},
	}
	if protocol != "" {
		cr.Annotations = map[string]string{query.LabelPairingProtocol: protocol}
	}
	return cr
}

// servingInstances builds one serving instance per named revision hash. The
// RunningRevision is recorded in the full `<isvc>-<component>-<hash>` form the
// controller writes.
func servingInstances(isvcName string, component v1beta1.ComponentType, hashes ...string) []v1beta1.OMENativeInstanceStatus {
	out := make([]v1beta1.OMENativeInstanceStatus, 0, len(hashes))
	for i, h := range hashes {
		inst := v1beta1.OMENativeInstanceStatus{Index: int32(i), ServingPodCount: 1}
		if h != "" {
			inst.RunningRevision = isvcName + "-" + string(component) + "-" + h
		}
		out = append(out, inst)
	}
	return out
}

func pairingIR(namespace, isvcName string, component v1beta1.ComponentType, instances []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      isvcName + "-" + string(component),
		},
		Status: v1beta1.InferenceReplicaStatus{InstanceStatuses: instances},
	}
}

// pairingISVC declares engine+decoder in one blueGreen group with the given
// target protocol ("" leaves the field unset).
func pairingISVC(protocol string) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"},
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					BlueGreen:  &v1beta1.GroupBlueGreen{},
				}},
			},
		},
	}
	if protocol != "" {
		isvc.Spec.Rollout.PairingProtocol = ptr.To(protocol)
	}
	return pinActiveRun(isvc)
}

// forwardTarget is the roll target of an ordinary forward roll: a revision of
// the Component carrying the protocol the spec declares ("" leaves the
// revision unannotated).
func forwardTarget(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *appsv1.ControllerRevision {
	return pairingCR(isvc.Namespace, isvc.Name, component, "target", isvc.Spec.RolloutPairingProtocol())
}

// pairingGate builds the gate for a forward-roll step of component.
func pairingGate(t *testing.T, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, objs ...client.Object) GateContext {
	t.Helper()
	return pairingGateTo(t, isvc, component, forwardTarget(isvc, component), objs...)
}

// pairingGateTo builds the gate for a step of component whose roll target is
// the given revision.
func pairingGateTo(t *testing.T, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, target *appsv1.ControllerRevision, objs ...client.Object) GateContext {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).Build()
	gate := ResolveGateContext(context.Background(), c, isvc, component)
	gate.TargetRevision = target
	return gate
}

func TestCheckPairing_InactivePaths(t *testing.T) {
	// No rollout block at all: the shared prelude short-circuits.
	gate := pairingGate(t, &v1beta1.InferenceService{}, v1beta1.EngineComponent)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("no rollout block: denied (%s)", reason)
	}

	// Engine-only group never pairs.
	soloISVC := pairingISVC("nixl-v2")
	soloISVC.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
	pinActiveRun(soloISVC) // re-pin: the membership edit must be part of the pinned plan
	gate = pairingGate(t, soloISVC, v1beta1.EngineComponent)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("engine-only group: denied (%s)", reason)
	}

	// Group pairs but no protocol declared.
	gate = pairingGate(t, pairingISVC(""), v1beta1.EngineComponent)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("no protocol: denied (%s)", reason)
	}

	// Router in a pairing group never constrains the pair.
	routerISVC := pairingISVC("nixl-v2")
	routerISVC.Spec.Rollout.Groups[0].Components = append(routerISVC.Spec.Rollout.Groups[0].Components, v1beta1.RouterComponent)
	pinActiveRun(routerISVC) // re-pin: the membership edit must be part of the pinned plan
	gate = pairingGate(t, routerISVC, v1beta1.RouterComponent)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("router step: denied (%s)", reason)
	}

	// Everything already serving the target protocol: no transition.
	isvc := pairingISVC("nixl-v2")
	gate = pairingGate(t, isvc, v1beta1.EngineComponent,
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "e1")),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "d1")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "e1", "nixl-v2"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "d1", "nixl-v2"),
	)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("converged fleet: denied (%s)", reason)
	}
}

// TestCheckPairing_DeniesLastOldEngineUntilTargetDecoderServes is the core
// pair floor: draining the last old-cohort engine before the target cohort
// has a serving decoder is denied naming the dying cohort; once a target
// decoder serves, the same step is allowed.
func TestCheckPairing_DeniesLastOldEngineUntilTargetDecoderServes(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		// Engine: one serving instance on old cohort A, one already on B.
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		// Decoder: still entirely on old cohort A.
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("draining the last proto-a engine with no proto-b decoder must be denied")
	}
	if !strings.Contains(reason, `"proto-a"`) || !strings.Contains(reason, `"proto-b"`) {
		t.Errorf("denial must name the dying cohort and the target: %s", reason)
	}

	// The decoder CAN step: a proto-b engine already serves, so the decoder's
	// surge replacement (proto-b) pairs with it the moment the old decoder
	// drains — this asymmetry is what lets the transition make progress while
	// the engine's last old instance is held.
	gateDec := pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
	if ok, reason := gateDec.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("decoder surge pairs its replacement with the serving proto-b engine; want allow, got deny (%s)", reason)
	}

	// A serving proto-b decoder unblocks the engine step.
	objsUnblocked := []client.Object{
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da", "db")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
	}
	gate = pairingGate(t, isvc, v1beta1.EngineComponent, objsUnblocked...)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("proto-b serves on both Components; step should be allowed: %s", reason)
	}
}

// TestCheckPairing_CreditDistinguishesSurgeFromRecreate pins the replacement
// credit and the last-hop escape on the engine-side asymmetric shape: with
// the old cohort down to its last engine and the target decoder already
// serving, a surge step passes the ordinary simulation (its replacement
// serves before the drain — no escape involved), while a drain-first
// recreate of the same instance fails the simulation and is admitted only by
// the last-hop escape: the hop is unavoidable, its gap equals a
// single-replica recreate without pairing, and the incoming instance pairs
// with the decoder's target capacity the moment it serves.
func TestCheckPairing_CreditDistinguishesSurgeFromRecreate(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da", "db")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
	}
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if !ok {
		t.Errorf("surge swaps the last engine to proto-b, pairable with db: %s", reason)
	}
	if reason != "" {
		t.Errorf("surge must pass the ordinary simulation, not an escape: %s", reason)
	}
	ok, reason = gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if !ok {
		t.Errorf("recreate of the engine's last instance is its final hop; must be allowed: %s", reason)
	}
	if !strings.Contains(reason, "last serving instance") {
		t.Errorf("recreate must be admitted by the last-hop escape, not the simulation: %s", reason)
	}
}

// TestCheckPairing_WildcardPairsWithAnything: instances with no recorded
// revision (or a pre-pairing revision) pair with any cohort and keep steps
// unblocked.
func TestCheckPairing_WildcardPairsWithAnything(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		// Engines: one legacy (no RunningRevision → wildcard), one old cohort A.
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "", "ea")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		// Decoder: old cohort A only.
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	// Removing the proto-a engine leaves the wildcard engine, which pairs with
	// the proto-a decoder; removing the wildcard leaves the proto-a pair.
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Errorf("wildcard engine keeps every step pairable: %s", reason)
	}
}

// TestCheckPairing_MutualWallAllows: both Components at a single serving
// old-cohort instance with no target capacity anywhere is the 1×1 wall —
// denying both first movers would deadlock the transition, so the step is
// allowed.
func TestCheckPairing_MutualWallAllows(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		gate := pairingGate(t, isvc, comp, objs...)
		if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
			t.Errorf("%s: 1x1 mutual wall must allow rather than deadlock: %s", comp, reason)
		}
	}
}

// TestCheckPairing_NoPairToProtectAllows: when the serving state already has
// no pairable pair, denial protects nothing and would wedge recovery.
func TestCheckPairing_NoPairToProtectAllows(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		// Engine already on B; decoder still on A; nothing pairs.
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "eb")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	gate := pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0); !ok {
		t.Errorf("no existing pair to protect; the decoder's move to proto-b is the only exit: %s", reason)
	}
}

// TestCheckPairing_InFlightStartsCharge: fresh starts from the same wake-up
// count against the cohort under simulation before IR status reflects them.
func TestCheckPairing_InFlightStartsCharge(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		// Two proto-a engines serving; decoders have both cohorts.
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "ea2")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea2", "proto-a"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da", "db")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
	}
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	// Recreate with one drain already in flight this wake-up: both proto-a
	// engines are gone in the projection and nothing replaces them — denied.
	if ok, _ := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 1); ok {
		t.Errorf("second same-pass recreate would empty the engine pool; must be denied")
	}
	// Surge in-flight carries its own replacement credit — allowed.
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 1, 0); !ok {
		t.Errorf("surge in-flight replaces what it drains: %s", reason)
	}
}

// TestCheckPairing_FailsClosed covers unreadable protocol data: a serving
// instance whose ControllerRevision is missing, and a reader that errors.
func TestCheckPairing_FailsClosed(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		// ea's CR is deliberately absent; eb resolves.
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-a"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("missing CR behind a serving instance must fail closed")
	}
	if !strings.Contains(reason, "not found") {
		t.Errorf("reason should say the revision is missing: %s", reason)
	}

	failing := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isCR := obj.(*appsv1.ControllerRevision); isCR {
				return errors.New("apiserver unavailable")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()
	gate = ResolveGateContext(context.Background(), failing, isvc, v1beta1.EngineComponent)
	gate.TargetRevision = forwardTarget(isvc, v1beta1.EngineComponent)
	ok, reason = gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("CR read error must fail closed")
	}
	if !strings.Contains(reason, "failing closed") || !strings.Contains(reason, "cannot read revision") {
		t.Errorf("reason should mark the CR read as the fail-closed path: %s", reason)
	}

	// No roll target at all: the gate cannot orient the simulation.
	gate = pairingGateTo(t, isvc, v1beta1.EngineComponent, nil, objs...)
	ok, reason = gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("a step with no target revision must fail closed")
	}
	if !strings.Contains(reason, "failing closed") || !strings.Contains(reason, "target revision") {
		t.Errorf("reason should name the missing target revision: %s", reason)
	}
}

// TestCheckPairing_RevertMovesTowardTargetRevision: mid-swap, the live spec
// field names the new protocol while a rollback pins the stable revision as
// the roll target. The engine's revert step moves its remaining new-cohort
// instance back to the stable cohort, where the fully reverted decoder
// already serves, and must be admitted for every strategy. The same fleet
// read as a forward roll (target = the new protocol) is the last-old-engine
// hold: the gate's direction follows the target revision, not the live
// field.
func TestCheckPairing_RevertMovesTowardTargetRevision(t *testing.T) {
	isvc := pairingISVC("proto-b")
	stable := pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a")
	objs := []client.Object{
		// Engine: one instance still on the stable cohort A, one on the rejected B.
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		stable,
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		// Decoder: fully reverted to A.
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	for _, strategy := range []workloadtypes.UpdateStrategyType{workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategyInPlaceIfPossible} {
		gate := pairingGateTo(t, isvc, v1beta1.EngineComponent, stable, objs...)
		if ok, reason := gate.CheckPairing("", strategy, 0, 0); !ok {
			t.Errorf("%s: reverting the engine's proto-b instance toward the reverted decoder must be admitted: %s", strategy, reason)
		}
	}
	// The peer, already entirely on the stable cohort, has nothing to move
	// and is never held by the engine's transition.
	gate := pairingGateTo(t, isvc, v1beta1.DecoderComponent, pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"), objs...)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0); !ok {
		t.Errorf("a fully reverted decoder step must be admitted: %s", reason)
	}

	// Mirror: the same fleet rolling forward to proto-b holds the last
	// proto-a engine until a proto-b decoder serves.
	forward := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	ok, reason := forward.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("forward roll: the last proto-a engine must be held until a proto-b decoder serves")
	}
	if !strings.Contains(reason, `transition to "proto-b"`) {
		t.Errorf("forward denial must name the forward target: %s", reason)
	}
}

func TestPairingProtocolForRevision(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(
		pairingCR("prod", "llama", v1beta1.EngineComponent, "abc", "nixl-v2"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "old", ""),
	).Build()

	proto, found, err := PairingProtocolForRevision(context.Background(), c, "prod", "llama", v1beta1.EngineComponent, "abc")
	if err != nil || !found || proto != "nixl-v2" {
		t.Errorf("annotated CR: got (%q,%v,%v)", proto, found, err)
	}
	proto, found, err = PairingProtocolForRevision(context.Background(), c, "prod", "llama", v1beta1.EngineComponent, "old")
	if err != nil || !found || proto != "" {
		t.Errorf("pre-pairing CR: got (%q,%v,%v)", proto, found, err)
	}
	proto, found, err = PairingProtocolForRevision(context.Background(), c, "prod", "llama", v1beta1.EngineComponent, "gone")
	if err != nil || found || proto != "" {
		t.Errorf("missing CR: got (%q,%v,%v)", proto, found, err)
	}
	if _, _, err = PairingProtocolForRevision(context.Background(), c, "prod", "llama", v1beta1.EngineComponent, ""); err != nil {
		t.Errorf("empty hash must be a quiet no-op: %v", err)
	}
}

func TestAttachPairingProtocols(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(
		pairingCR("prod", "llama", v1beta1.EngineComponent, "new", "proto-b"),
	).Build()
	weights := []RevisionWeight{
		{RevisionHash: "new", Percent: 30},
		{RevisionHash: "swept", Percent: 70},
	}
	if err := AttachPairingProtocols(context.Background(), c, "prod", "llama", v1beta1.EngineComponent, weights); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if weights[0].PairingProtocol != "proto-b" {
		t.Errorf("annotated revision: got %q", weights[0].PairingProtocol)
	}
	if weights[1].PairingProtocol != "" {
		t.Errorf("swept revision degrades to wildcard: got %q", weights[1].PairingProtocol)
	}
	targets := BuildTrafficTargets("llama", v1beta1.EngineComponent, weights)
	if len(targets) != 2 || targets[1].PairingProtocol != "proto-b" && targets[0].PairingProtocol != "proto-b" {
		t.Errorf("BuildTrafficTargets must carry the protocol: %+v", targets)
	}
}

// TestTrafficComparators_PairingProtocol: a protocol flip on an existing
// target is a meaningful diff and is never deadband-suppressed — it is the
// value routing consumers pair on.
func TestTrafficComparators_PairingProtocol(t *testing.T) {
	observed := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "svc-a", Percent: 50, PairingProtocol: ""},
		{RevisionName: "svc-b", Percent: 50, PairingProtocol: "proto-b"},
	}
	desired := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "svc-a", Percent: 50, PairingProtocol: "proto-a"},
		{RevisionName: "svc-b", Percent: 50, PairingProtocol: "proto-b"},
	}
	if !TrafficDiffersMeaningfully(desired, observed) {
		t.Errorf("protocol flip must be a meaningful diff")
	}
	if TrafficWithinDeadband(desired, observed, 100) {
		t.Errorf("protocol flip must never be deadband-suppressed")
	}
	same := []v1beta1.ComponentTrafficTarget{
		{RevisionName: "svc-a", Percent: 50, PairingProtocol: ""},
		{RevisionName: "svc-b", Percent: 50, PairingProtocol: "proto-b"},
	}
	if TrafficDiffersMeaningfully(same, observed) {
		t.Errorf("identical targets must not diff")
	}
}

// TestEnsurePerRevisionServices_PairingLabel: the routing Service carries the
// revision's protocol as a metadata label, never in the selector; a
// protocol-free revision gets no label.
func TestEnsurePerRevisionServices_PairingLabel(t *testing.T) {
	isvc := testISVC()
	c := fakeClient(pairingCR(isvc.Namespace, isvc.Name, v1beta1.EngineComponent, "hash1", "nixl-v2"))
	out, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash1", RevisionRoutingSelector{}, runnerPorts(), nil)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	routing := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out.RoutingName}, routing); err != nil {
		t.Fatalf("get routing: %v", err)
	}
	if routing.Labels[query.LabelPairingProtocol] != "nixl-v2" {
		t.Errorf("routing Service pairing label: got %+v", routing.Labels)
	}
	if _, inSelector := routing.Spec.Selector[query.LabelPairingProtocol]; inSelector {
		t.Errorf("pairing protocol must never enter the selector: %+v", routing.Spec.Selector)
	}

	// No CR (pre-pairing / swept) → no label, no error.
	out2, err := EnsurePerRevisionServices(context.Background(), c, isvc, v1beta1.EngineComponent, "hash2", RevisionRoutingSelector{}, runnerPorts(), nil)
	if err != nil {
		t.Fatalf("ensure hash2: %v", err)
	}
	routing2 := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: out2.RoutingName}, routing2); err != nil {
		t.Fatalf("get routing2: %v", err)
	}
	if _, has := routing2.Labels[query.LabelPairingProtocol]; has {
		t.Errorf("protocol-free revision must not be labeled: %+v", routing2.Labels)
	}
}

// TestCheckPairing_LastHopEscape_AsymmetricDecoder is the asymmetric wedge
// shape: engine already staged the target cohort while the single-replica
// drain-first decoder still runs the old one. The decoder's step empties its
// side in simulation, but it is the decoder's final unavoidable hop and the
// engine's target capacity re-pairs the incoming instance — allowed via the
// last-hop escape (the reason pins the escape, not an accidental pass).
func TestCheckPairing_LastHopEscape_AsymmetricDecoder(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	gate := pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
	ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if !ok {
		t.Fatalf("last decoder hop with engine target capacity must be allowed: %s", reason)
	}
	if !strings.Contains(reason, "last serving instance") {
		t.Errorf("allow must come from the last-hop escape: %s", reason)
	}
}

// TestCheckPairing_LastHopEscape_RequiresPeerTargetCapacity: the escape never
// fires before the peer stages target capacity — the single-replica decoder
// is held until then (its later hop is covered by the escape once the peer
// flips), and a multi-serving acting Component is held regardless. This is
// the boundary that keeps the escape from draining a cohort early.
func TestCheckPairing_LastHopEscape_RequiresPeerTargetCapacity(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "ea2")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea2", "proto-a"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	gate := pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
	ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if ok {
		t.Fatalf("single-replica decoder must be held until the engine stages target capacity")
	}
	if !strings.Contains(reason, `"proto-a"`) {
		t.Errorf("denial must name the dying cohort: %s", reason)
	}
}

// TestCheckPairing_AsymmetricTransitionReachability walks every state of the
// engine{old:2} x decoder{old:1} drain-first transition and asserts each step
// the convergence path needs is admitted — and that the one hold that orders
// the walk (the engine's last old instance before the decoder flips) stays a
// hold. Every step uses RecreatePod, the strategy with no replacement credit.
func TestCheckPairing_AsymmetricTransitionReachability(t *testing.T) {
	isvc := pairingISVC("proto-b")
	transitionCRs := func() []client.Object {
		return []client.Object{
			pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
			pairingCR("prod", "llama", v1beta1.EngineComponent, "ea2", "proto-a"),
			pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
			pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
			pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
		}
	}

	// State 1 — engine{a,a} decoder{a}: the engine's first step keeps an
	// old pair serving.
	objs := append(transitionCRs(),
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "ea2")),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
	)
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0); !ok {
		t.Fatalf("state 1: engine's first old step must be allowed: %s", reason)
	}

	// State 2 — engine{a,b} decoder{a}: the engine's remaining old step is
	// HELD (no decoder target yet; this hold is what orders the walk) while
	// the decoder's step goes through the last-hop escape.
	objs = append(transitionCRs(),
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
	)
	gate = pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	if ok, _ := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0); ok {
		t.Fatalf("state 2: engine's last old step must be held until the decoder flips")
	}
	gate = pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
	ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if !ok {
		t.Fatalf("state 2: decoder's last hop must be allowed: %s", reason)
	}
	if !strings.Contains(reason, "last serving instance") {
		t.Errorf("state 2: decoder allow must come from the last-hop escape: %s", reason)
	}

	// State 3 — engine{a,b} decoder{b}: the engine's last old step now has a
	// target pair to land on; the ordinary simulation admits it.
	objs = append(transitionCRs(),
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "db")),
	)
	gate = pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	if ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategyRecreatePod, 0, 0); !ok {
		t.Fatalf("state 3: engine's final old step must be allowed: %s", reason)
	}
}

// strandedIntermediateObjects is the fleet after a protocol bumped v1→v2 was
// bumped again to v3 once one engine had moved: the engines run {v2, v1} and
// the decoders {v1, v1}, all serving.
func strandedIntermediateObjects() []client.Object {
	return []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "e1", "nixl-v1"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "e2", "nixl-v2"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "d1", "nixl-v1"),
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "e2", "e1")),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "d1", "d1")),
	}
}

// TestCheckPairing_StrandedIntermediateCohortMoves pins the re-target over a
// half-landed cohort. The engine on v2 has nothing to pair with and its move
// to v3 keeps the (v1, v1) pair, so its step is allowed. The engine on v1 is
// the last pairable engine: its step is held, and stays held once the v2
// start is charged in flight. A start whose row records no revision, or
// whose revision is gone, keeps the pessimistic reading: every old cohort is
// simulated losing the instance and the step is held. A revision the gate
// cannot read fails closed.
func TestCheckPairing_StrandedIntermediateCohortMoves(t *testing.T) {
	isvc := pairingISVC("nixl-v3")
	objs := strandedIntermediateObjects()
	fromV2, fromV1 := "llama-engine-e2", "llama-engine-e1"

	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	if ok, reason := gate.CheckPairing(fromV2, workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Fatalf("the engine on the intermediate cohort must be allowed to move: %s", reason)
	}
	ok, reason := gate.CheckPairing(fromV1, workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("the last pairable engine must be held until cohort nixl-v3 serves on both Components")
	}
	if !strings.Contains(reason, `cohort "nixl-v1"`) || !strings.Contains(reason, `cohort "nixl-v3" serves on both`) {
		t.Errorf("the hold must name the cohort the instance leaves and the cohort it waits for: %s", reason)
	}
	if ok, _ := gate.CheckPairing(fromV1, workloadtypes.UpdateStrategySurgeThenDrain, 1, 0); ok {
		t.Fatalf("the last pairable engine must stay held with the intermediate engine's start charged in flight")
	}
	if ok, _ := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); ok {
		t.Fatalf("a start with no recorded revision must keep the pessimistic reading and be held")
	}
	if ok, _ := gate.CheckPairing("llama-engine-gone", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); ok {
		t.Fatalf("a start whose revision is gone must keep the pessimistic reading and be held")
	}

	gate = pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
	if ok, reason := gate.CheckPairing("llama-decoder-d1", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Fatalf("a decoder step keeps a (nixl-v1, nixl-v1) pair and must be allowed: %s", reason)
	}

	failing := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isCR := obj.(*appsv1.ControllerRevision); isCR && key.Name == "llama-engine-e3" {
				return errors.New("apiserver unavailable")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()
	gate = ResolveGateContext(context.Background(), failing, isvc, v1beta1.EngineComponent)
	gate.TargetRevision = forwardTarget(isvc, v1beta1.EngineComponent)
	ok, reason = gate.CheckPairing("llama-engine-e3", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("a starting instance whose revision cannot be read must fail closed")
	}
	if !strings.Contains(reason, "failing closed") || !strings.Contains(reason, "starting engine instance") {
		t.Errorf("reason should mark the starting instance's revision read as the fail-closed path: %s", reason)
	}
}

// TestEvaluateUpdateGate_StrandedIntermediateCohortMoves walks the same
// re-target through the full gate stack: the engine on the intermediate
// cohort is admitted and the engine on the last pairable cohort is held by
// the pairing gate.
func TestEvaluateUpdateGate_StrandedIntermediateCohortMoves(t *testing.T) {
	isvc := pairingISVC("nixl-v3")
	c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(strandedIntermediateObjects()...).Build()
	target := forwardTarget(isvc, v1beta1.EngineComponent)

	allowed, gate, reason := EvaluateUpdateGate(context.Background(), c, isvc, v1beta1.EngineComponent, "llama-engine-e2", target, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if !allowed {
		t.Fatalf("the engine on the intermediate cohort must be admitted, held by %s: %s", gate, reason)
	}
	allowed, gate, reason = EvaluateUpdateGate(context.Background(), c, isvc, v1beta1.EngineComponent, "llama-engine-e1", target, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if allowed || gate != v1beta1.RolloutHoldGatePairing {
		t.Fatalf("the last pairable engine = (%v, %q, %q), want the pairing hold", allowed, gate, reason)
	}
}

// sizedPairingIR is pairingIR with the desired count, update strategy and
// maxUnavailable budget the pairable floor is read from.
func sizedPairingIR(namespace, isvcName string, component v1beta1.ComponentType, instances []v1beta1.OMENativeInstanceStatus, strategy v1beta1.UpdateStrategyType, replicas, maxUnavailable int) *v1beta1.InferenceReplica {
	ir := pairingIR(namespace, isvcName, component, instances)
	ir.Spec.Replicas = ptr.To(int32(replicas))
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
		Type:          strategy,
		RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: ptr.To(intstr.FromInt(maxUnavailable))},
	}}
	return ir
}

// Two surging engines, none allowed out of pairing, and two decoders
// recreated one at a time, halfway through a move from protocol a to b: one
// engine and one decoder are on b, one of each still on a. Recreating the
// last a decoder would leave the a engine without a partner, 1 of 2 under a
// floor of 2, so the start is held until that engine has moved; the
// engine's own surge start is admitted, because its source stays in
// rotation until the drain-time gate lets it go.
func TestCheckPairing_DrainFirstStartKeepsThePeerAboveItsFloor(t *testing.T) {
	isvc := pairingISVC("proto-b")
	crs := []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
	}
	decoders := sizedPairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "db", "da"),
		v1beta1.UpdateStrategyRecreatePod, 2, 1)
	halfway := append(crs,
		sizedPairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "eb", "ea"),
			v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
		decoders)

	gate := pairingGate(t, isvc, v1beta1.DecoderComponent, halfway...)
	ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if ok {
		t.Fatalf("recreating the last proto-a decoder must wait while an engine still speaks only proto-a")
	}
	if !strings.Contains(reason, "below its floor") || !strings.Contains(reason, "engine with 1 pairable") {
		t.Errorf("the hold must name the engine's shortfall against its floor: %s", reason)
	}
	gate = pairingGate(t, isvc, v1beta1.EngineComponent, halfway...)
	if ok, reason := gate.CheckPairing("llama-engine-ea", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !ok {
		t.Fatalf("the engine's surge start keeps its source in rotation and must be admitted: %s", reason)
	}

	// With one engine allowed out of pairing the decoder's step goes.
	budgeted := append(crs,
		sizedPairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "eb", "ea"),
			v1beta1.UpdateStrategySurgeThenDrain, 2, 1),
		decoders)
	gate = pairingGate(t, isvc, v1beta1.DecoderComponent, budgeted...)
	if ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0); !ok {
		t.Fatalf("one engine may be out of pairing under a budget of one: %s", reason)
	}

	// Both engines on b: nothing loses a partner and the last decoder moves.
	moved := append(crs,
		sizedPairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "eb", "eb"),
			v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
		decoders)
	gate = pairingGate(t, isvc, v1beta1.DecoderComponent, moved...)
	if ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0); !ok {
		t.Fatalf("with every engine on proto-b the last decoder's recreate must be admitted: %s", reason)
	}
}

// Six engines relabelled in place one at a time and two surging decoders
// none of which may be out of pairing. One engine already carries b while
// both decoders still serve a: relabelling a second engine would leave the
// engine with 4 of 6 pairable, under its floor of 5, so the step waits for
// a b decoder; once one serves, the relabel is admitted.
func TestCheckPairing_InPlaceStartKeepsItsOwnFloor(t *testing.T) {
	isvc := pairingISVC("proto-b")
	crs := []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
	}
	engines := sizedPairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "eb", "ea", "ea", "ea", "ea", "ea"),
		v1beta1.UpdateStrategyInPlaceIfPossible, 6, 1)

	gate := pairingGate(t, isvc, v1beta1.EngineComponent, append(crs, engines,
		sizedPairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da", "da"),
			v1beta1.UpdateStrategySurgeThenDrain, 2, 0))...)
	ok, reason := gate.CheckPairing("llama-engine-ea", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 0)
	if ok {
		t.Fatalf("a second in-place relabel with no proto-b decoder must wait")
	}
	if !strings.Contains(reason, "engine with 4 pairable") || !strings.Contains(reason, "floor of 5") {
		t.Errorf("the hold must name the engine's shortfall against its floor: %s", reason)
	}

	gate = pairingGate(t, isvc, v1beta1.EngineComponent, append(crs, engines,
		sizedPairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da", "db"),
			v1beta1.UpdateStrategySurgeThenDrain, 2, 0))...)
	if ok, reason := gate.CheckPairing("llama-engine-ea", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 0); !ok {
		t.Fatalf("with a proto-b decoder serving the relabelled engines pair and the step is admitted: %s", reason)
	}
}

// settledRow is a serving Instance row with no attempt, running hash.
func settledRow(isvcName string, component v1beta1.ComponentType, index int, hash string) v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index:           int32(index),
		Phase:           v1beta1.OMENativeInstanceReady,
		RunningRevision: isvcName + "-" + string(component) + "-" + hash,
		ServingPodCount: 1,
	}
}

// surgingRow is the row of a single-pod Instance mid-surge: it runs
// runningHash, its surge is pinned to targetHash at the given step, and
// serving counts its pods in rotation (two once the replacement serves
// beside the source).
func surgingRow(isvcName string, component v1beta1.ComponentType, index int, runningHash, targetHash, step string, serving int32) v1beta1.OMENativeInstanceStatus {
	target := isvcName + "-" + string(component) + "-" + targetHash
	return v1beta1.OMENativeInstanceStatus{
		Index:           int32(index),
		Phase:           v1beta1.OMENativeInstanceUpdating,
		RunningRevision: isvcName + "-" + string(component) + "-" + runningHash,
		TargetRevision:  target,
		ServingPodCount: serving,
		Operation:       &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: step, TargetRevision: target},
	}
}

// Two surging engines and one drain-first decoder. The first engine's
// proto-b replacement serves beside its proto-a source, not yet promoted.
// The decoder's only hop needs proto-b capacity on the engine: it is
// admitted on the serving replacement, through the last-hop escape, while a
// replacement not yet serving admits nothing. Past the hand-over the
// replacement is the row's serving pod and counts the same way. A pinned
// revision that is gone fails the gate closed, like any serving revision.
func TestCheckPairing_ServingSurgeReplacementCountsForItsCohort(t *testing.T) {
	isvc := pairingISVC("proto-b")
	crs := []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	decoders := pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da"))
	engines := func(targetHash, step string, serving int32) *v1beta1.InferenceReplica {
		return pairingIR("prod", "llama", v1beta1.EngineComponent, []v1beta1.OMENativeInstanceStatus{
			surgingRow("llama", v1beta1.EngineComponent, 0, "ea", targetHash, step, serving),
			settledRow("llama", v1beta1.EngineComponent, 1, "ea"),
		})
	}
	decoderHop := func(engineRows *v1beta1.InferenceReplica) (bool, string) {
		gate := pairingGate(t, isvc, v1beta1.DecoderComponent, append(crs, engineRows, decoders)...)
		return gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	}

	ok, reason := decoderHop(engines("eb", workloadtypes.UpdateStepSurge, 2))
	if !ok || !strings.Contains(reason, "last serving instance") {
		t.Fatalf("a serving proto-b replacement must admit the decoder's last hop through the last-hop escape: ok=%v %s", ok, reason)
	}
	ok, reason = decoderHop(engines("eb", workloadtypes.UpdateStepSurge, 1))
	if ok {
		t.Fatalf("a replacement not yet serving stages no proto-b capacity; the decoder's hop must wait")
	}
	if !strings.Contains(reason, `"proto-a"`) {
		t.Errorf("denial must name the dying cohort: %s", reason)
	}
	ok, reason = decoderHop(engines("eb", workloadtypes.UpdateStepSurgeDrain, 1))
	if !ok || !strings.Contains(reason, "last serving instance") {
		t.Fatalf("past the hand-over the replacement is the serving pod and counts for proto-b: ok=%v %s", ok, reason)
	}
	ok, reason = decoderHop(engines("gone", workloadtypes.UpdateStepSurge, 2))
	if ok || !strings.Contains(reason, "not found, failing closed") {
		t.Fatalf("a pinned revision that is gone must fail closed: ok=%v %s", ok, reason)
	}
}

// The source of a surge in flight stays in rotation and keeps pairing. Two
// surging engines, none allowed out of pairing, the first mid-surge from
// proto-a to proto-b with both pods serving, the second still on proto-a;
// two decoders, one on each cohort. Recreating the proto-a decoder would
// leave both proto-a engine pods without a partner, one of two engines
// pairable under a floor of two, so the step is held.
func TestCheckPairing_SurgeSourceKeepsItsCohortBesideItsReplacement(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
		sizedPairingIR("prod", "llama", v1beta1.EngineComponent, []v1beta1.OMENativeInstanceStatus{
			surgingRow("llama", v1beta1.EngineComponent, 0, "ea", "eb", workloadtypes.UpdateStepSurge, 2),
			settledRow("llama", v1beta1.EngineComponent, 1, "ea"),
		}, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
		sizedPairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "db", "da"),
			v1beta1.UpdateStrategyRecreatePod, 2, 1),
	}
	gate := pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
	ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if ok {
		t.Fatalf("recreating the last proto-a decoder must wait while every proto-a engine pod would lose its partner")
	}
	if !strings.Contains(reason, "below its floor") || !strings.Contains(reason, "engine with 1 pairable") {
		t.Errorf("the hold must name the engine's shortfall against its floor: %s", reason)
	}
}

// A gang surge's replacement is its own row, running nothing yet. While it
// serves it counts for the cohort its surge is pinned to, so the lone
// drain-first decoder's hop is admitted through the last-hop escape; a
// replacement gang not yet serving leaves the 1x1 mutual wall, where the
// hop waits for it: the engine's surge keeps its old gang in rotation, so
// the decoder loses nothing by waiting and rebuilds with a partner.
func TestCheckPairing_GangSurgeReplacementCountsForItsPinnedCohort(t *testing.T) {
	isvc := pairingISVC("proto-b")
	crs := []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	engines := func(replacementServing int32) *v1beta1.InferenceReplica {
		surgeIdx := int32(1)
		source := surgingRow("llama", v1beta1.EngineComponent, 0, "ea", "eb", workloadtypes.UpdateStepSurge, 8)
		source.Operation.SurgeIndex = &surgeIdx
		replacement := v1beta1.OMENativeInstanceStatus{
			Index: 1, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: "llama-engine-eb", ServingPodCount: replacementServing,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepGangSurgeTarget, TargetRevision: "llama-engine-eb"},
		}
		return pairingIR("prod", "llama", v1beta1.EngineComponent, []v1beta1.OMENativeInstanceStatus{source, replacement})
	}
	decoders := pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da"))

	gate := pairingGate(t, isvc, v1beta1.DecoderComponent, append(crs, engines(8), decoders)...)
	ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if !ok || !strings.Contains(reason, "last serving instance") {
		t.Fatalf("a serving replacement gang stages proto-b capacity; the decoder's hop goes through the last-hop escape: ok=%v %s", ok, reason)
	}
	gate = pairingGate(t, isvc, v1beta1.DecoderComponent, append(crs, engines(0), decoders)...)
	ok, reason = gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if ok || !strings.Contains(reason, "the engine surges") {
		t.Fatalf("with the replacement gang not serving the decoder's hop waits for it: ok=%v %s", ok, reason)
	}
}

// One surging engine beside one drain-first decoder, no side named first:
// the decoder's hop is admitted through the last-hop escape once the
// engine's replacement serves, and waits for that replacement before, since
// the engine's surge keeps its old pod in rotation while the hop would
// leave no pair at all. The engine's drain waits on the decoder's new pod,
// which waits on nothing of the engine's but a replacement that is already
// on its way, so neither step waits on a step that waits on it.
func TestCheckPairing_OneByOneSurgeBesideDrainFirst_PeerStepWaitsForTheServingReplacement(t *testing.T) {
	isvc := pairingISVC("proto-b")
	crs := []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	decoders := pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da"))
	for _, tc := range []struct {
		serving int32
		allowed bool
		reason  string
	}{{2, true, "last serving instance"}, {1, false, "the engine surges"}} {
		engines := pairingIR("prod", "llama", v1beta1.EngineComponent, []v1beta1.OMENativeInstanceStatus{
			surgingRow("llama", v1beta1.EngineComponent, 0, "ea", "eb", workloadtypes.UpdateStepSurge, tc.serving),
		})
		gate := pairingGate(t, isvc, v1beta1.DecoderComponent, append(crs, engines, decoders)...)
		ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
		if ok != tc.allowed || !strings.Contains(reason, tc.reason) {
			t.Fatalf("serving=%d: the decoder's hop must read allowed=%v (%s): ok=%v %s", tc.serving, tc.allowed, tc.reason, ok, reason)
		}
	}
}

// Six engines relabelled in place one at a time and two surging decoders
// none of which may be out of pairing. One engine already carries proto-b;
// the first decoder's proto-b replacement serves beside its source, not yet
// promoted. The relabelled engines pair with that replacement, so the
// second relabel is admitted where it waits while no proto-b decoder pod
// serves.
func TestCheckPairing_InPlaceStartSeesTheSurgingPeersServingReplacement(t *testing.T) {
	isvc := pairingISVC("proto-b")
	objs := []client.Object{
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
		sizedPairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "eb", "ea", "ea", "ea", "ea", "ea"),
			v1beta1.UpdateStrategyInPlaceIfPossible, 6, 1),
		sizedPairingIR("prod", "llama", v1beta1.DecoderComponent, []v1beta1.OMENativeInstanceStatus{
			surgingRow("llama", v1beta1.DecoderComponent, 0, "da", "db", workloadtypes.UpdateStepSurge, 2),
			settledRow("llama", v1beta1.DecoderComponent, 1, "da"),
		}, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
	}
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	if ok, reason := gate.CheckPairing("llama-engine-ea", workloadtypes.UpdateStrategyInPlaceIfPossible, 0, 0); !ok {
		t.Fatalf("with a proto-b decoder replacement serving the relabelled engines pair and the step is admitted: %s", reason)
	}
}
