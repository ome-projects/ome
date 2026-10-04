package coordination

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// drainPod builds a routable OMENative pod of one cohort: the labels Render
// stamps, a revision hash (which is what makes it routable), and PodReady
// when `ready`. runner "worker" makes it a gang worker, which never routes.
func drainPod(isvcName string, comp v1beta1.ComponentType, name, runner, proto string, ready bool) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: name,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvcName,
			constants.OMEComponentLabel:           string(comp),
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelRunner:                     runner,
			query.LabelRevisionHash:               "rev" + strings.ReplaceAll(proto, "-", ""),
		},
	}}
	if proto != "" {
		pod.Labels[query.LabelPairingProtocol] = proto
	}
	if ready {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return pod
}

func drainScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := pairingScheme(t)
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return scheme
}

func drainGate(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, sources []string, objs ...client.Object) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(drainScheme(t)).WithObjects(objs...).Build()
	return EvaluatePairingDrain(context.Background(), c, isvc, comp, forwardTarget(isvc, comp), GroupDefaults{}, sources)
}

// The shape under test: a 1×1 surge transition whose decoder replacement is
// Ready first. Draining the old decoder would leave the old engine unpaired,
// so the drain is held; once the engine's replacement serves it is allowed.
func TestEvaluatePairingDrain_HoldsTheLastPartnerUntilThePeerServesTarget(t *testing.T) {
	isvc := pairingISVC("v2")
	oldEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-0", "default", "v1", true)
	oldDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-0", "default", "v1", true)
	newDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-1", "default", "v2", true)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldDecoder, newDecoder)
	if allowed {
		t.Fatalf("draining the last v1 decoder while the engine serves only v1 must be held")
	}
	if gate != v1beta1.RolloutHoldGatePairing {
		t.Errorf("gate = %q, want Pairing", gate)
	}
	if !strings.Contains(reason, "holding the drain") || !strings.Contains(reason, `"v1"`) {
		t.Errorf("reason must name the hold and the dying cohort: %s", reason)
	}

	newEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-1", "default", "v2", true)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldDecoder, newDecoder, newEngine); !allowed {
		t.Fatalf("with a v2 engine serving the v2 decoder pairs; the drain must proceed: %s", reason)
	}
	// The mirror: the engine's drain with both decoder cohorts serving.
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{oldEngine.Name}, oldEngine, oldDecoder, newDecoder, newEngine); !allowed {
		t.Fatalf("the v2 engine pairs with the v2 decoder; the engine drain must proceed: %s", reason)
	}
}

// A referenced peer's pods carry the peer replica's own name prefix; the
// gate finds them there, so the same hold-then-allow sequence plays out
// with both roles served by referenced replicas.
func TestEvaluatePairingDrain_ReferencedPeerPodsAreFoundUnderTheirOwnPrefix(t *testing.T) {
	isvc := pairingISVC("v2")
	isvc.Spec.ReplicaRefs = &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}}
	oldEngine := drainPod("pool-a", v1beta1.EngineComponent, "pool-a-engine-0-default-0", "default", "v1", true)
	oldDecoder := drainPod("pool-d", v1beta1.DecoderComponent, "pool-d-decoder-0-default-0", "default", "v1", true)
	newDecoder := drainPod("pool-d", v1beta1.DecoderComponent, "pool-d-decoder-0-default-1", "default", "v2", true)
	// Pods labelled with the service name are not the referenced roles' pods.
	strayEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-1", "default", "v2", true)

	allowed, gate, _ := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldDecoder, newDecoder, strayEngine)
	if allowed || gate != v1beta1.RolloutHoldGatePairing {
		t.Fatalf("draining the last v1 decoder of the referenced replica while the referenced engine serves only v1 must be held: allowed=%v gate=%q", allowed, gate)
	}
	newEngine := drainPod("pool-a", v1beta1.EngineComponent, "pool-a-engine-0-default-1", "default", "v2", true)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldDecoder, newDecoder, newEngine); !allowed {
		t.Fatalf("with a v2 engine serving the v2 decoder pairs; the drain must proceed: %s", reason)
	}
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{oldEngine.Name}, oldEngine, oldDecoder, newDecoder, newEngine); !allowed {
		t.Fatalf("the v2 engine pairs with the referenced v2 decoder; the engine drain must proceed: %s", reason)
	}
}

func TestEvaluatePairingDrain_ReplacementNotYetReadyIsNotCapacity(t *testing.T) {
	isvc := pairingISVC("v2")
	oldEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-0", "default", "v1", true)
	pendingEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-1", "default", "v2", false)
	oldDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-0", "default", "v1", true)
	newDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-1", "default", "v2", true)
	if allowed, _, _ := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, pendingEngine, oldDecoder, newDecoder); allowed {
		t.Fatalf("an engine replacement that is not PodReady is not serving capacity; the drain must be held")
	}
}

func TestEvaluatePairingDrain_GangWorkersDoNotCount(t *testing.T) {
	isvc := pairingISVC("v2")
	oldEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-leader-0", "leader", "v1", true)
	oldEngineWorker := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-worker-0", "worker", "v1", true)
	newEngineWorker := drainPod("llama", v1beta1.EngineComponent, "llama-engine-1-worker-0", "worker", "v2", true)
	oldDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-leader-0", "leader", "v1", true)
	newDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-1-leader-0", "leader", "v2", true)
	// Only a v2 engine WORKER is up: no routable v2 engine serves yet.
	if allowed, _, _ := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldEngineWorker, newEngineWorker, oldDecoder, newDecoder); allowed {
		t.Fatalf("a worker is never routed; the drain must be held until the v2 engine leader serves")
	}
	newEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-1-leader-0", "leader", "v2", true)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldEngineWorker, newEngineWorker, newEngine, oldDecoder, newDecoder); !allowed {
		t.Fatalf("with the v2 engine leader serving the drain must proceed: %s", reason)
	}
}

func TestEvaluatePairingDrain_InactivePaths(t *testing.T) {
	oldEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-0", "default", "v1", true)
	oldDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-0", "default", "v1", true)
	newDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-1", "default", "v2", true)

	// No rollout block: the shared prelude short-circuits.
	if allowed, _, _ := drainGate(t, &v1beta1.InferenceService{}, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldDecoder, newDecoder); !allowed {
		t.Errorf("no rollout block: held")
	}
	// Engine-only group never pairs.
	solo := pairingISVC("v2")
	solo.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
	pinActiveRun(solo)
	if allowed, _, _ := drainGate(t, solo, v1beta1.EngineComponent, []string{oldEngine.Name}, oldEngine, oldDecoder, newDecoder); !allowed {
		t.Errorf("engine-only group: held")
	}
	// No protocol declared.
	if allowed, _, _ := drainGate(t, pairingISVC(""), v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldEngine, oldDecoder, newDecoder); !allowed {
		t.Errorf("no protocol: held")
	}
	// Everything already on the target: no transition.
	isvc := pairingISVC("v2")
	e2 := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-0", "default", "v2", true)
	d2 := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-0", "default", "v2", true)
	if allowed, _, _ := drainGate(t, isvc, v1beta1.DecoderComponent, []string{d2.Name}, e2, d2); !allowed {
		t.Errorf("converged fleet: held")
	}
	// No pair to protect: the engine has nothing serving at all.
	if allowed, _, _ := drainGate(t, isvc, v1beta1.DecoderComponent, []string{oldDecoder.Name}, oldDecoder, newDecoder); !allowed {
		t.Errorf("no serving engine: held instead of allowing progress")
	}
	// A source that is not serving leaves nothing behind.
	drainedDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-0", "default", "v1", false)
	if allowed, _, _ := drainGate(t, isvc, v1beta1.DecoderComponent, []string{drainedDecoder.Name}, oldEngine, drainedDecoder, newDecoder); !allowed {
		t.Errorf("a source already out of rotation: held")
	}
}

func TestEvaluatePairingDrain_FailsClosedWhenPodsCannotBeListed(t *testing.T) {
	isvc := pairingISVC("v2")
	c := fake.NewClientBuilder().WithScheme(drainScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return errors.New("apiserver unavailable")
			}
			return cl.List(ctx, list, opts...)
		},
	}).Build()
	allowed, gate, reason := EvaluatePairingDrain(context.Background(), c, isvc, v1beta1.DecoderComponent, forwardTarget(isvc, v1beta1.DecoderComponent), GroupDefaults{}, []string{"x"})
	if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "failing closed") {
		t.Fatalf("a list error must fail closed with a retryable Pairing hold: allowed=%v gate=%q reason=%s", allowed, gate, reason)
	}
}
