package inferencereplica

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func requestFor(namespace, name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}
}

// TestPeerReplicasOf_EnqueuesTheServingPeersOfTheParent pins the mapping a
// peer's transition takes back to the replicas that pair on it: every
// serving peer of the same parent, never the replica itself, and nothing
// for a replica outside a rollout group.
func TestPeerReplicasOf_EnqueuesTheServingPeersOfTheParent(t *testing.T) {
	ctx := context.Background()
	engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e1")
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")

	t.Run("engine and decoder wake each other", func(t *testing.T) {
		r, _ := newReconciler(t, engine, decoder, pdParent())
		if got, want := r.peerReplicasOf(ctx, decoder), []reconcile.Request{requestFor("default", "llama-engine")}; !reflect.DeepEqual(got, want) {
			t.Errorf("decoder event maps to %v, want %v", got, want)
		}
		if got, want := r.peerReplicasOf(ctx, engine), []reconcile.Request{requestFor("default", "llama-decoder")}; !reflect.DeepEqual(got, want) {
			t.Errorf("engine event maps to %v, want %v", got, want)
		}
	})

	t.Run("a declared router is a peer too", func(t *testing.T) {
		parent := pdParent()
		parent.Spec.Router = &v1beta1.RouterSpec{}
		r, _ := newReconciler(t, engine, decoder, parent)
		want := []reconcile.Request{requestFor("default", "llama-decoder"), requestFor("default", "llama-router")}
		if got := r.peerReplicasOf(ctx, engine); !reflect.DeepEqual(got, want) {
			t.Errorf("engine event maps to %v, want %v", got, want)
		}
	})

	t.Run("no rollout group declares no peers", func(t *testing.T) {
		parent := pdParent()
		parent.Spec.Rollout = nil
		r, _ := newReconciler(t, engine, decoder, parent)
		if got := r.peerReplicasOf(ctx, engine); got != nil {
			t.Errorf("a parent without rollout groups maps to %v, want nothing", got)
		}
	})

	t.Run("a standalone replica and a missing parent map to nothing", func(t *testing.T) {
		standalone := projectedIR(v1beta1.EngineComponent, "7", "e1", "e1")
		standalone.Name = "solo-engine"
		standalone.Spec.ParentRef = nil
		r, _ := newReconciler(t, standalone, engine)
		if got := r.peerReplicasOf(ctx, standalone); got != nil {
			t.Errorf("standalone replica maps to %v, want nothing", got)
		}
		if got := r.peerReplicasOf(ctx, engine); got != nil {
			t.Errorf("replica with an unresolvable parent maps to %v, want nothing", got)
		}
		if got := r.peerReplicasOf(ctx, &corev1.Pod{}); got != nil {
			t.Errorf("non-replica object maps to %v, want nothing", got)
		}
	})
}

// TestPeerRevisionPredicate_AdmitsOnlyPeerRevisionInputs pins which peer
// transitions wake a sibling: the ones its peer-revision resolution reads.
// Counter churn on a busy peer must not.
func TestPeerRevisionPredicate_AdmitsOnlyPeerRevisionInputs(t *testing.T) {
	base := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	base.Status.ReadyReplicas = 1
	p := peerRevisionPredicate()

	cases := []struct {
		name   string
		mutate func(ir *v1beta1.InferenceReplica)
		want   bool
	}{
		{"unchanged", func(*v1beta1.InferenceReplica) {}, false},
		{"counter churn", func(ir *v1beta1.InferenceReplica) { ir.Status.ReadyReplicas = 2; ir.Status.ServingReplicas = 2 }, false},
		{"status observes the generation", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration++ }, true},
		{"generation bumps", func(ir *v1beta1.InferenceReplica) { ir.Generation++ }, true},
		{"projection reflects a new parent generation", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "8"
		}, true},
		{"update revision moves", func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = ir.Name + "-d3" }, true},
		{"current revision promotes", func(ir *v1beta1.InferenceReplica) { ir.Status.CurrentRevision = ir.Status.UpdateRevision }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updated := base.DeepCopy()
			tc.mutate(updated)
			if got := p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: updated}); got != tc.want {
				t.Errorf("Update admitted=%v, want %v", got, tc.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: base}) || !p.Delete(event.DeleteEvent{Object: base}) {
		t.Error("a peer appearing or vanishing must wake its siblings")
	}
}

// TestReconcile_PeerStatusCatchUpReleasesAHeldReplacement replays the
// stall the peer watch exists to prevent. A Component mid-surge holds its
// pass while a serving peer's status lags the peer's generation, and the
// replacement it already has ContainersReady stays out of rotation for as
// long as nothing reconciles it again. The peer's catch-up is the event
// that must do so: it maps back to the held replica, whose next pass admits
// the replacement to serving.
func TestReconcile_PeerStatusCatchUpReleasesAHeldReplacement(t *testing.T) {
	ctx := context.Background()
	// Control: with a fresh peer the same pass admits the replacement at
	// once, so a replacement left unadmitted below is the peer hold's doing.
	{
		engine, source, replacement := midSurgeEngine(t)
		decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
		r, c := newReconciler(t, engine, decoder, pdParent(), source, replacement)
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(engine)}); err != nil {
			t.Fatalf("fresh-peer reconcile: %v", err)
		}
		if !podIsServing(t, c, replacement) {
			t.Fatal("with a fresh peer the first pass must admit the ContainersReady replacement")
		}
	}

	engine, source, replacement := midSurgeEngine(t)
	// The decoder's spec moved and its reconciler has not observed it yet.
	decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
	decoder.Generation = decoder.Status.ObservedGeneration + 1

	r, c := newReconciler(t, engine, decoder, pdParent(), source, replacement)
	engineReq := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(engine)}

	if _, err := r.Reconcile(ctx, engineReq); err != nil {
		t.Fatalf("held reconcile: %v", err)
	}
	if podIsServing(t, c, replacement) {
		t.Fatal("the pass must hold while the peer lags; the replacement was admitted against an unresolved pairing")
	}

	// The decoder's reconciler catches up. That transition, and nothing
	// else, has to bring the engine back.
	fresh := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(decoder), fresh); err != nil {
		t.Fatalf("get decoder: %v", err)
	}
	fresh.Status.ObservedGeneration = fresh.Generation
	if err := c.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("decoder status catch-up: %v", err)
	}
	woken := r.peerReplicasOf(ctx, fresh)
	if len(woken) != 1 || woken[0] != engineReq {
		t.Fatalf("the peer's catch-up maps to %v, want the held engine %v", woken, engineReq)
	}

	if _, err := r.Reconcile(ctx, engineReq); err != nil {
		t.Fatalf("released reconcile: %v", err)
	}
	if !podIsServing(t, c, replacement) {
		t.Fatal("once the peer is fresh the pass must admit the ContainersReady replacement to serving")
	}
}

// midSurgeEngine is a projected engine replica one pass into a
// SurgeThenDrain update of its only Instance: the source pod serves at
// ordinal 0 on the previous revision and the replacement at ordinal 1 is
// ContainersReady on the target, waiting to be admitted to serving.
func midSurgeEngine(t *testing.T) (*v1beta1.InferenceReplica, *corev1.Pod, *corev1.Pod) {
	t.Helper()
	engine := baselineIR("llama-engine", "default", 1)
	engine.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "7"
	target := targetRevisionNameFor(t, engine)
	engine.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceUpdating,
		RunningRevision: "llama-engine-a1b2c3d4", TargetRevision: target,
		Operation: &v1beta1.InstanceOperation{
			ID: "update-0", Type: v1beta1.InstanceOperationUpdate,
			Step: workloadtypes.UpdateStepSurge, TargetRevision: target,
		},
	}}
	source := podForIR(engine, 0, string(v1beta1.RunnerNameDefault), 0, true, true)
	source.Labels[query.LabelRevisionHash] = "a1b2c3d4"
	replacement := podForIR(engine, 0, string(v1beta1.RunnerNameDefault), 1, true, false)
	replacement.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target)
	return engine, source, replacement
}

func podIsServing(t *testing.T, c client.Client, pod *corev1.Pod) bool {
	t.Helper()
	fresh := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), fresh); err != nil {
		t.Fatalf("get pod %s: %v", pod.Name, err)
	}
	return podreadiness.IsServing(fresh)
}

// peerWatchRequests replays one InferenceReplica update through the peer
// watches SetupWithManager registers: each predicate decides, each mapper
// enqueues, and the union is what the controller's queue receives.
func peerWatchRequests(r *Reconciler, oldIR, newIR *v1beta1.InferenceReplica) []reconcile.Request {
	ctx := context.Background()
	e := event.UpdateEvent{ObjectOld: oldIR, ObjectNew: newIR}
	var reqs []reconcile.Request
	if peerRevisionPredicate().Update(e) {
		reqs = append(reqs, r.peerReplicasOf(ctx, newIR)...)
	}
	if peerCounterPredicate().Update(e) {
		reqs = append(reqs, r.heldPeerReplicasOf(ctx, newIR)...)
	}
	return reqs
}

// TestPeerCounterPredicate_AdmitsOnlyCounterTransitions pins which peer
// transitions the held-peer watch considers: the serving counters and the
// per-Instance rows a pairing or ratio simulation reads. The revision
// inputs, conditions and the peer's own hold are not its concern, and a
// peer appearing or vanishing is the peer-revision watch's event.
func TestPeerCounterPredicate_AdmitsOnlyCounterTransitions(t *testing.T) {
	base := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
	base.Status.Replicas = 2
	base.Status.ServingReplicas = 1
	base.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "llama-decoder-d1", PodCount: 1, ServingPodCount: 1},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: "llama-decoder-d1", PodCount: 1},
	}
	p := peerCounterPredicate()

	cases := []struct {
		name   string
		mutate func(ir *v1beta1.InferenceReplica)
		want   bool
	}{
		{"unchanged", func(*v1beta1.InferenceReplica) {}, false},
		{"serving count rises", func(ir *v1beta1.InferenceReplica) { ir.Status.ServingReplicas = 2 }, true},
		{"replica count moves", func(ir *v1beta1.InferenceReplica) { ir.Status.Replicas = 3 }, true},
		{"updated count moves", func(ir *v1beta1.InferenceReplica) { ir.Status.UpdatedReplicas = 1 }, true},
		{"an instance starts serving", func(ir *v1beta1.InferenceReplica) { ir.Status.InstanceStatuses[1].ServingPodCount = 1 }, true},
		{"an instance promotes its running revision", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses[1].RunningRevision = "llama-decoder-d2"
		}, true},
		{"columnar rows move", func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{Members: "1"}
		}, true},
		{"status observes the generation", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration++ }, false},
		{"revisions move", func(ir *v1beta1.InferenceReplica) { ir.Status.CurrentRevision = ir.Status.UpdateRevision }, false},
		{"generation bumps", func(ir *v1beta1.InferenceReplica) { ir.Generation++ }, false},
		{"the peer's own hold changes", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RolloutHold = &v1beta1.RolloutHold{Gate: v1beta1.RolloutHoldGateBudget, Reason: "budget exhausted"}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updated := base.DeepCopy()
			tc.mutate(updated)
			if got := p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: updated}); got != tc.want {
				t.Errorf("Update admitted=%v, want %v", got, tc.want)
			}
		})
	}
	if p.Create(event.CreateEvent{Object: base}) || p.Delete(event.DeleteEvent{Object: base}) {
		t.Error("a peer appearing or vanishing is not a counter transition")
	}
}

// TestHeldPeerReplicasOf_WakesOnlySiblingsHeldOnPeerCounters pins the bound
// on the held-peer watch: a peer's counter transition reaches a sibling
// only while that sibling's recorded hold waits on peer serving counters.
// A sibling with no hold, or one held on its own budget, its retry state, a
// revision or the plan pin, is not enqueued, so a busy peer's churn wakes
// no one unless someone is waiting on it.
func TestHeldPeerReplicasOf_WakesOnlySiblingsHeldOnPeerCounters(t *testing.T) {
	ctx := context.Background()
	holdOn := func(gate v1beta1.RolloutHoldGate) *v1beta1.RolloutHold {
		return &v1beta1.RolloutHold{Gate: gate, Reason: "waiting", Target: "llama-engine-e2"}
	}
	wakes := map[v1beta1.RolloutHoldGate]bool{
		v1beta1.RolloutHoldGateRatio:      true,
		v1beta1.RolloutHoldGatePairing:    true,
		v1beta1.RolloutHoldGateSequential: false,
		v1beta1.RolloutHoldGatePlan:       false,
		v1beta1.RolloutHoldGateBudget:     false,
		v1beta1.RolloutHoldGateRetryBlock: false,
		v1beta1.RolloutHoldGateHeld:       false,
	}
	for gate, want := range wakes {
		t.Run("engine held on "+string(gate), func(t *testing.T) {
			engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
			engine.Status.RolloutHold = holdOn(gate)
			decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
			r, _ := newReconciler(t, engine, decoder, pdParent())
			got := r.heldPeerReplicasOf(ctx, decoder)
			if want {
				if !reflect.DeepEqual(got, []reconcile.Request{requestFor("default", "llama-engine")}) {
					t.Errorf("decoder counters map to %v, want the held engine", got)
				}
			} else if got != nil {
				t.Errorf("decoder counters map to %v, want nothing: a %s hold does not wait on a peer's counters", got, gate)
			}
		})
	}

	t.Run("no hold recorded enqueues nothing", func(t *testing.T) {
		engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
		decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
		r, _ := newReconciler(t, engine, decoder, pdParent())
		if got := r.heldPeerReplicasOf(ctx, decoder); got != nil {
			t.Errorf("decoder counters map to %v, want nothing while no sibling is held", got)
		}
		if got := r.heldPeerReplicasOf(ctx, engine); got != nil {
			t.Errorf("engine counters map to %v, want nothing while no sibling is held", got)
		}
	})

	t.Run("only the held sibling is enqueued", func(t *testing.T) {
		parent := pdParent()
		parent.Spec.Router = &v1beta1.RouterSpec{}
		engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
		decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d2")
		decoder.Status.RolloutHold = holdOn(v1beta1.RolloutHoldGatePairing)
		router := projectedIR(v1beta1.RouterComponent, "7", "r1", "r1")
		r, _ := newReconciler(t, engine, decoder, router, parent)
		if got, want := r.heldPeerReplicasOf(ctx, engine), []reconcile.Request{requestFor("default", "llama-decoder")}; !reflect.DeepEqual(got, want) {
			t.Errorf("engine counters map to %v, want only the held decoder %v", got, want)
		}
	})

	t.Run("no rollout group, missing parent and foreign objects map to nothing", func(t *testing.T) {
		engine := projectedIR(v1beta1.EngineComponent, "7", "e1", "e2")
		engine.Status.RolloutHold = holdOn(v1beta1.RolloutHoldGateRatio)
		decoder := projectedIR(v1beta1.DecoderComponent, "7", "d1", "d1")
		parent := pdParent()
		parent.Spec.Rollout = nil
		r, _ := newReconciler(t, engine, decoder, parent)
		if got := r.heldPeerReplicasOf(ctx, decoder); got != nil {
			t.Errorf("a parent without rollout groups maps to %v, want nothing", got)
		}
		orphaned, _ := newReconciler(t, engine, decoder)
		if got := orphaned.heldPeerReplicasOf(ctx, decoder); got != nil {
			t.Errorf("an unresolvable parent maps to %v, want nothing", got)
		}
		if got := r.heldPeerReplicasOf(ctx, &corev1.Pod{}); got != nil {
			t.Errorf("non-replica object maps to %v, want nothing", got)
		}
	})
}

// ratioPeersParent is mkRatioParent with both Components declared, so the
// serving peers of each are resolvable, and with the owner identity the
// projected replicas carry.
func ratioPeersParent(tol, engServing, decServing int32) *v1beta1.InferenceService {
	parent := mkRatioParent(tol, engServing, decServing)
	parent.UID = "llama-isvc-uid"
	parent.Spec.Engine = &v1beta1.EngineSpec{}
	parent.Spec.Decoder = &v1beta1.DecoderSpec{}
	return parent
}

// steadyEngine is a projected engine replica fully serving on a revision
// other than the one its spec renders: every Instance is Ready on the old
// revision with its pod in rotation, so the next pass starts an update of each.
func steadyEngine(replicas int32) (*v1beta1.InferenceReplica, []client.Object) {
	engine := baselineIR("llama-engine", "default", replicas)
	setParentGenStamp(engine, 1)
	engine.Status.ObservedGeneration = engine.Generation
	const oldHash = "a1b2c3d4"
	old := engine.Name + "-" + oldHash
	engine.Status.CurrentRevision = old
	engine.Status.UpdateRevision = old
	engine.Status.Replicas = replicas
	engine.Status.ReadyReplicas = replicas
	engine.Status.ServingReplicas = replicas
	pods := make([]client.Object, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		engine.Status.InstanceStatuses = append(engine.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
			RunningRevision: old, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1,
		})
		pod := podForIR(engine, i, string(v1beta1.RunnerNameDefault), 0, true, true)
		pod.Labels[query.LabelRevisionHash] = oldHash
		pods = append(pods, pod)
	}
	return engine, pods
}

// TestReconcile_PeerServingCountReleasesARatioHold pins the release of a
// coordination denial that waits on a peer's serving count. The engine's
// update start is denied by the RatioBalanced gate while the decoder
// serves 3 of 4, and with no gate cadence configured the pass asks only
// for the rate-limited backoff, which a burst of churn can push out by
// minutes. The decoder's serving count then rises, and nothing else about
// it changes. That transition must map back to the held engine, whose next
// pass admits the start.
func TestReconcile_PeerServingCountReleasesARatioHold(t *testing.T) {
	ctx := context.Background()
	engine, pods := steadyEngine(4)
	decoder := projectedIR(v1beta1.DecoderComponent, "1", "d1", "d1")
	decoder.Spec.Replicas = ptr.To(int32(4))
	decoder.Status.Replicas = 4
	decoder.Status.ServingReplicas = 3
	r, c := newReconciler(t, append([]client.Object{engine, decoder, ratioPeersParent(25, 4, 3)}, pods...)...)
	engineReq := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(engine)}

	res, err := r.Reconcile(ctx, engineReq)
	if err != nil {
		t.Fatalf("denied reconcile: %v", err)
	}
	held := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, engineReq.NamespacedName, held); err != nil {
		t.Fatalf("get engine: %v", err)
	}
	if held.Status.RolloutHold == nil || held.Status.RolloutHold.Gate != v1beta1.RolloutHoldGateRatio {
		t.Fatalf("the engine must record a Ratio hold while the decoder serves 3 of 4, got %+v", held.Status.RolloutHold)
	}
	if !res.Requeue || res.RequeueAfter != 0 { //nolint:staticcheck // the rate-limited backoff is the only spelling of an unconfigured cadence
		t.Fatalf("with no gate cadence configured the denied pass rides the rate-limited backoff, got %+v", res)
	}

	// The decoder's fourth instance comes back into rotation: its serving
	// count rises and nothing else about it changes.
	before := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(decoder), before); err != nil {
		t.Fatalf("get decoder: %v", err)
	}
	after := before.DeepCopy()
	after.Status.ServingReplicas = 4
	if err := c.Status().Update(ctx, after); err != nil {
		t.Fatalf("decoder serving count rises: %v", err)
	}
	woken := peerWatchRequests(r, before, after)
	if len(woken) != 1 || woken[0] != engineReq {
		t.Fatalf("the peer's serving count rising maps to %v, want the held engine %v", woken, engineReq)
	}

	if _, err := r.Reconcile(ctx, engineReq); err != nil {
		t.Fatalf("released reconcile: %v", err)
	}
	released := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, engineReq.NamespacedName, released); err != nil {
		t.Fatalf("get engine: %v", err)
	}
	if released.Status.RolloutHold != nil {
		t.Fatalf("once the decoder serves 4 of 4 the engine's start must be admitted, still held: %+v", released.Status.RolloutHold)
	}
	updating := 0
	for _, inst := range released.Status.InstanceStatuses {
		if inst.Phase == v1beta1.OMENativeInstanceUpdating {
			updating++
		}
	}
	if updating == 0 {
		t.Fatal("the released pass must start an update on at least one Instance")
	}

	// With the hold cleared, the decoder's further counter churn is nobody's
	// release and must enqueue nothing.
	churned := after.DeepCopy()
	churned.Status.ReadyReplicas = 4
	if got := peerWatchRequests(r, after, churned); got != nil {
		t.Fatalf("counter churn on a peer nothing is held on maps to %v, want nothing", got)
	}
}
