package core

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// coordinatedISVC builds a coordination-enabled, multi-component ISVC: a
// single BlueGreen group binding engine + decoder. Used to exercise the
// peer-env injection hook end-to-end.
func coordinatedISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine:  &v1beta1.EngineSpec{},
			Decoder: &v1beta1.DecoderSpec{},
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{
						Components: []v1beta1.ComponentType{
							v1beta1.EngineComponent,
							v1beta1.DecoderComponent,
						},
						BlueGreen: &v1beta1.GroupBlueGreen{},
					},
				},
			},
		},
	}
}

// decoupledISVC builds a PD ISVC whose engine and decoder are in SEPARATE
// single-Component blueGreen groups — the v2 spelling of "roll engine, then
// decoder, NOT as a coupled unit." They roll independently, but they are still
// SERVING peers (a PD engine needs the decoder's address regardless of how the
// rollout groups them), so peer-env must still be injected.
func decoupledISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"},
		Spec: v1beta1.InferenceServiceSpec{
			Engine:  &v1beta1.EngineSpec{},
			Decoder: &v1beta1.DecoderSpec{},
			Rollout: &v1beta1.RolloutSpec{
				Groups: []v1beta1.RolloutGroup{
					{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
					{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
				},
			},
		},
	}
}

// renderedPod mimics what workload/ops.Render produces: a pod stamped
// with the canonical component label (constants.OMEComponentLabel, whose
// value is "component") for the given component.
func renderedPod(component v1beta1.ComponentType) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				constants.OMEComponentLabel: string(component),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main"},
				{Name: "sidecar"},
			},
		},
	}
}

// pairedWithDecoder is a resolver that pairs every engine revision with
// one fixed decoder revision and records the pod hash it was asked about.
func pairedWithDecoder(decoderHash string, asked *[]string) coordination.PeerRevisionFunc {
	return func(peer v1beta1.ComponentType, podRevisionHash string) string {
		*asked = append(*asked, podRevisionHash)
		if peer == v1beta1.DecoderComponent {
			return decoderHash
		}
		return ""
	}
}

// TestISVCRenderHook_InjectsPeerEnvForCoordinatedComponent pins that the
// hook recovers the component from constants.OMEComponentLabel
// ("component"); reading any other key yields "", an empty peer lookup,
// and no InjectPeerEnv call. This drives a rendered engine pod through the
// live hook and asserts both decoder peer endpoints land on every
// container: the generic one, and the per-revision one naming the decoder
// revision the resolver paired this pod with — NOT the pod's own hash
// (each Component hashes its own template, so llama-decoder-rev-<engineHash>
// never exists).
func TestISVCRenderHook_InjectsPeerEnvForCoordinatedComponent(t *testing.T) {
	isvc := coordinatedISVC()
	var asked []string
	hook := ISVCRenderHook(isvc, pairedWithDecoder("dec456", &asked))
	if hook == nil {
		t.Fatal("ISVCRenderHook returned nil for a coordination-enabled ISVC")
	}

	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "eng123")

	for _, ctr := range pod.Spec.Containers {
		if got := envValue(ctr.Env, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_ENDPOINT = %q, want llama-decoder.prod.svc.cluster.local",
				ctr.Name, got)
		}
		want := coordination.PerRevisionServiceName("llama", v1beta1.DecoderComponent, "dec456") + ".prod.svc.cluster.local"
		if got := envValue(ctr.Env, "OME_DECODER_REVISION_ENDPOINT"); got != want {
			t.Errorf("container %q: OME_DECODER_REVISION_ENDPOINT = %q, want the paired decoder revision Service %q",
				ctr.Name, got, want)
		}
		// The engine never names itself as a peer.
		if containsEnvNamed(ctr.Env, "OME_ENGINE_ENDPOINT") {
			t.Errorf("container %q: pod should not carry its own component as a peer", ctr.Name)
		}
	}
	for _, h := range asked {
		if h != "eng123" {
			t.Errorf("resolver must be asked about the rendered pod's own revision hash, got %q", h)
		}
	}
	if len(asked) == 0 {
		t.Error("resolver was never consulted")
	}
}

// TestISVCRenderHook_NoResolverEmitsGenericOnly pins the degraded shape: with
// no resolver the hook must inject only the revision-agnostic endpoint, never
// a per-revision DNS name derived from the pod's own hash.
func TestISVCRenderHook_NoResolverEmitsGenericOnly(t *testing.T) {
	hook := ISVCRenderHook(coordinatedISVC(), nil)
	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "eng123")
	for _, ctr := range pod.Spec.Containers {
		if got := envValue(ctr.Env, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_ENDPOINT = %q", ctr.Name, got)
		}
		if got := envValue(ctr.Env, "OME_DECODER_REVISION_ENDPOINT"); got != "" {
			t.Errorf("container %q: OME_DECODER_REVISION_ENDPOINT = %q, want it absent without a resolver", ctr.Name, got)
		}
	}
}

// TestISVCRenderHook_NilForNonCoordinatedISVC confirms the hook is a
// no-op (nil) when the ISVC declares no rollout coordination, so
// single-component / non-coordinated boxes are unaffected by the wiring.
func TestISVCRenderHook_NilForNonCoordinatedISVC(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "solo", Namespace: "prod"}}
	if ISVCRenderHook(isvc, nil) != nil {
		t.Error("ISVCRenderHook should return nil when the ISVC has no rolloutCoordination")
	}
	if ISVCRenderHook(nil, nil) != nil {
		t.Error("ISVCRenderHook(nil) should return nil")
	}
}

// TestISVCRenderHook_EmptyHashStillPairs confirms the empty-hash render
// path still asks the resolver, which pairs it as a target-revision pod.
func TestISVCRenderHook_EmptyHashStillPairs(t *testing.T) {
	var asked []string
	hook := ISVCRenderHook(coordinatedISVC(), pairedWithDecoder("dec456", &asked))
	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "")

	env := pod.Spec.Containers[0].Env
	if got := envValue(env, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
		t.Errorf("generic endpoint missing/wrong: %q", got)
	}
	if got := envValue(env, "OME_DECODER_REVISION_ENDPOINT"); got != "llama-decoder-rev-dec456.prod.svc.cluster.local" {
		t.Errorf("revision endpoint = %q, want the paired decoder revision", got)
	}
}

// TestISVCRenderHook_SeparateGroupsStillServingPeers verifies that a PD pair
// placed in SEPARATE rollout groups (rolled one-at-a-time) is STILL wired as
// serving peers. Peer endpoints reflect serving topology — the ISVC's declared
// Components — not rollout grouping, so the engine still gets the decoder's
// endpoint regardless of how the rollout sequences them.
func TestISVCRenderHook_SeparateGroupsStillServingPeers(t *testing.T) {
	var asked []string
	hook := ISVCRenderHook(decoupledISVC(), pairedWithDecoder("dec456", &asked))
	if hook == nil {
		t.Fatal("hook nil for an ISVC with rollout groups")
	}
	pod := renderedPod(v1beta1.EngineComponent)
	hook(pod, "runner-0", 0, "eng123")
	for _, ctr := range pod.Spec.Containers {
		if got := envValue(ctr.Env, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_ENDPOINT = %q, want the decoder serving peer (rollout grouping must not matter)", ctr.Name, got)
		}
		if got := envValue(ctr.Env, "OME_DECODER_REVISION_ENDPOINT"); got != "llama-decoder-rev-dec456.prod.svc.cluster.local" {
			t.Errorf("container %q: OME_DECODER_REVISION_ENDPOINT = %q, want the paired decoder revision", ctr.Name, got)
		}
	}
}

func containsEnvNamed(env []corev1.EnvVar, name string) bool {
	for _, e := range env {
		if e.Name == name {
			return true
		}
	}
	return false
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
