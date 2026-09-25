package inferencereplica

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// pdParent is a PD parent ISVC with one blueGreen group spanning engine and
// decoder, so peer env is declared and both Components are serving peers.
func pdParent() *v1beta1.InferenceService {
	parent := migrationParent(nil, true)
	parent.Spec.Engine = &v1beta1.EngineSpec{}
	parent.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	}}}
	return parent
}

// projectedIR is baselineIR for component, stamped with the parent
// generation the projector writes, with status reporting the given
// current/update revisions as already observed for its generation.
func projectedIR(component v1beta1.ComponentType, parentGeneration, currentHash, updateHash string) *v1beta1.InferenceReplica {
	ir := baselineIR("llama-"+string(component), "default", 1)
	ir.Spec.Component = component
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = parentGeneration
	ir.Generation = 3
	ir.Status.ObservedGeneration = 3
	if currentHash != "" {
		ir.Status.CurrentRevision = ir.Name + "-" + currentHash
	}
	if updateHash != "" {
		ir.Status.UpdateRevision = ir.Name + "-" + updateHash
	}
	return ir
}

func ownTargetCR(name string) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
}

// TestResolvePeerRevisions_PairsTargetWithTargetAndOthersWithCurrent pins
// the pairing rule: a pod rendered for the engine's roll target pairs with
// the decoder's roll target; a pod rendered for any other engine revision
// pairs with the decoder's last fully rolled revision.
func TestResolvePeerRevisions_PairsTargetWithTargetAndOthersWithCurrent(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	r, _ := newReconciler(t, engine, decoder, pdParent())

	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2"))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if hold != "" {
		t.Fatalf("fresh peer must not hold, got %q", hold)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); got != "d2" {
		t.Errorf("target-revision engine pod must pair with the decoder target d2, got %q", got)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d1" {
		t.Errorf("old-revision engine pod must pair with the decoder's current d1, got %q", got)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, ""); got != "d2" {
		t.Errorf("empty pod hash pairs as target, got %q", got)
	}
	if got := peers.hashFor(v1beta1.RouterComponent, "e2"); got != "" {
		t.Errorf("undeclared peer must resolve to no revision, got %q", got)
	}
	var fn coordination.PeerRevisionFunc = peers.hashFor
	if fn == nil {
		t.Error("hashFor must satisfy coordination.PeerRevisionFunc")
	}
}

// TestResolvePeerRevisions_HoldsWhilePeerLags pins the two lag guards: a
// peer whose projection reflects another parent generation, and a peer whose
// status has not observed its own generation, both hold rendering.
func TestResolvePeerRevisions_HoldsWhilePeerLags(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "8", "e1", "e2")

	stale := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
	r, _ := newReconciler(t, engine, stale, pdParent())
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2")); err != nil || hold == "" {
		t.Errorf("projection lag must hold: hold=%q err=%v", hold, err)
	}

	unobserved := projectedIR(v1beta1.DecoderComponent, "8", "d1", "d1")
	unobserved.Status.ObservedGeneration = unobserved.Generation - 1
	r, _ = newReconciler(t, engine, unobserved, pdParent())
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2")); err != nil || hold == "" {
		t.Errorf("status lag must hold: hold=%q err=%v", hold, err)
	}

	noTarget := projectedIR(v1beta1.DecoderComponent, "8", "", "")
	r, _ = newReconciler(t, engine, noTarget, pdParent())
	if _, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2")); err != nil || hold == "" {
		t.Errorf("peer without a reported target must hold: hold=%q err=%v", hold, err)
	}
}

// TestResolvePeerRevisions_MissingPeerIRResolvesNothing pins that a declared
// peer with no IR (not IR-managed) neither holds nor yields a revision.
func TestResolvePeerRevisions_MissingPeerIRResolvesNothing(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
	r, _ := newReconciler(t, engine, pdParent())
	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e2"))
	if err != nil || hold != "" {
		t.Fatalf("missing peer IR must not hold: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); got != "" {
		t.Errorf("missing peer IR resolves to no revision, got %q", got)
	}
}

// TestResolvePeerRevisions_RollbackPinsPeerTarget pins that a peer rolling
// back pairs on its rollback revision while that revision exists, and on
// its spec target once the rollback revision is gone.
func TestResolvePeerRevisions_RollbackPinsPeerTarget(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e1")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	decoder.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To("llama-decoder-d1")}

	r, _ := newReconciler(t, engine, decoder, pdParent(), ownTargetCR("llama-decoder-d1"))
	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e1"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d1" {
		t.Errorf("rollback in flight pairs with the rollback revision d1, got %q", got)
	}

	r, _ = newReconciler(t, engine, decoder, pdParent())
	peers, hold, err = r.resolvePeerRevisions(context.Background(), engine, pdParent(), ownTargetCR("llama-engine-e1"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if got := peers.hashFor(v1beta1.DecoderComponent, "e1"); got != "d2" {
		t.Errorf("swept rollback revision falls through to the spec target d2, got %q", got)
	}
}

// TestEnsurePeerRevisionServices_CreatesPeerTargetServicesBeforePods pins
// that the engine pass creates the decoder target revision's Services —
// selected by that revision hash only — with no decoder pod in existence.
func TestEnsurePeerRevisionServices_CreatesPeerTargetServicesBeforePods(t *testing.T) {
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	decoder.Spec.Runners[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}}
	parent := pdParent()
	r, c := newReconciler(t, engine, decoder, parent)

	peers, hold, err := r.resolvePeerRevisions(context.Background(), engine, parent, ownTargetCR("llama-engine-e2"))
	if err != nil || hold != "" {
		t.Fatalf("resolve: hold=%q err=%v", hold, err)
	}
	if err := r.ensurePeerRevisionServices(context.Background(), parent, peers); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	names := coordination.PerRevisionServiceNames(parent.Name, v1beta1.DecoderComponent, "d2")
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		svc := &corev1.Service{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: parent.Namespace, Name: name}, svc); err != nil {
			t.Fatalf("decoder target Service %s must exist before any decoder pod: %v", name, err)
		}
		if svc.Spec.Selector[query.LabelRevisionHash] != "d2" || svc.Spec.Selector[constants.OMEComponentLabel] != string(v1beta1.DecoderComponent) {
			t.Errorf("Service %s must select only decoders of revision d2, got %v", name, svc.Spec.Selector)
		}
	}
	// The engine's own endpoint for the rendered pod names exactly that Service.
	if got := peers.hashFor(v1beta1.DecoderComponent, "e2"); coordination.PerRevisionServiceName(parent.Name, v1beta1.DecoderComponent, got) != names.RoutingName {
		t.Errorf("rendered engine pod pairs with %q, Service created for d2", got)
	}
	if err := r.ensurePeerRevisionServices(context.Background(), parent, nil); err != nil {
		t.Errorf("nil peers must be a no-op, got %v", err)
	}
}
