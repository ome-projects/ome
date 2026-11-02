package coordination

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// peerISVC is an InferenceService whose roles are all projected, so every
// peer prefix is the service name.
func peerISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "prod"}}
}

func TestBuildPeerEndpointEnv(t *testing.T) {
	env := BuildPeerEndpointEnv("llama", "llama", "prod", v1beta1.DecoderComponent, "abcd1234")
	if env.GenericName != "OME_DECODER_ENDPOINT" {
		t.Errorf("GenericName: got %q want OME_DECODER_ENDPOINT", env.GenericName)
	}
	if env.GenericValue != "llama-decoder.prod.svc.cluster.local" {
		t.Errorf("GenericValue: got %q want llama-decoder.prod.svc.cluster.local", env.GenericValue)
	}
	if env.RevisionName != "OME_DECODER_REVISION_ENDPOINT" {
		t.Errorf("RevisionName: got %q want OME_DECODER_REVISION_ENDPOINT", env.RevisionName)
	}
	if env.RevisionValue != "llama-decoder-rev-abcd1234.prod.svc.cluster.local" {
		t.Errorf("RevisionValue: got %q want llama-decoder-rev-abcd1234.prod.svc.cluster.local", env.RevisionValue)
	}
}

// The generic endpoint names the service's stable Service; the revision
// endpoint names the peer replica's per-revision Service, so a referenced
// peer is addressed under its own prefix.
func TestBuildPeerEndpointEnv_ReferencedPeerPrefix(t *testing.T) {
	env := BuildPeerEndpointEnv("svc", "pool-d", "team-a", v1beta1.DecoderComponent, "abcd1234")
	if env.GenericValue != "svc-decoder.team-a.svc.cluster.local" {
		t.Errorf("GenericValue: got %q want svc-decoder.team-a.svc.cluster.local", env.GenericValue)
	}
	if env.RevisionValue != "pool-d-decoder-rev-abcd1234.team-a.svc.cluster.local" {
		t.Errorf("RevisionValue: got %q want pool-d-decoder-rev-abcd1234.team-a.svc.cluster.local", env.RevisionValue)
	}
}

func TestBuildPeerEndpointEnv_UpperCasesComponent(t *testing.T) {
	env := BuildPeerEndpointEnv("llama", "llama", "prod", v1beta1.EngineComponent, "abcd")
	if env.GenericName != "OME_ENGINE_ENDPOINT" {
		t.Errorf("uppercases component: got %q", env.GenericName)
	}
}

func TestInjectPeerEnv_AddsGenericAndRevisionEnv(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main"},
				{Name: "sidecar"},
			},
		},
	}
	InjectPeerEnv(pod, peerISVC(),
		[]v1beta1.ComponentType{v1beta1.DecoderComponent},
		func(v1beta1.ComponentType) string { return "rev1" },
	)
	for _, ctr := range pod.Spec.Containers {
		if !containsEnvNamed(ctr.Env, "OME_DECODER_ENDPOINT") {
			t.Errorf("container %q missing OME_DECODER_ENDPOINT", ctr.Name)
		}
		if !containsEnvNamed(ctr.Env, "OME_DECODER_REVISION_ENDPOINT") {
			t.Errorf("container %q missing OME_DECODER_REVISION_ENDPOINT", ctr.Name)
		}
	}
}

// A referenced peer's revision endpoint carries the peer replica's own
// prefix; the generic endpoint still names the service's stable Service.
func TestInjectPeerEnv_ReferencedPeer(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"},
		Spec: v1beta1.InferenceServiceSpec{
			ReplicaRefs: &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}},
		},
	}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main"}},
		},
	}
	InjectPeerEnv(pod, isvc,
		[]v1beta1.ComponentType{v1beta1.DecoderComponent},
		func(v1beta1.ComponentType) string { return "rev1" },
	)
	env := pod.Spec.Containers[0].Env
	if got := envValue(env, "OME_DECODER_ENDPOINT"); got != "svc-decoder.team-a.svc.cluster.local" {
		t.Errorf("OME_DECODER_ENDPOINT: got %q want the service's stable Service", got)
	}
	if got := envValue(env, "OME_DECODER_REVISION_ENDPOINT"); got != "pool-d-decoder-rev-rev1.team-a.svc.cluster.local" {
		t.Errorf("OME_DECODER_REVISION_ENDPOINT: got %q want the replica's per-revision Service", got)
	}
}

func TestInjectPeerEnv_EmptyHashOmitsRevisionEnv(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main"}},
		},
	}
	InjectPeerEnv(pod, peerISVC(),
		[]v1beta1.ComponentType{v1beta1.DecoderComponent},
		nil,
	)
	if !containsEnvNamed(pod.Spec.Containers[0].Env, "OME_DECODER_ENDPOINT") {
		t.Errorf("generic env missing")
	}
	if containsEnvNamed(pod.Spec.Containers[0].Env, "OME_DECODER_REVISION_ENDPOINT") {
		t.Errorf("revision env should be omitted when no hash is known")
	}
}

func TestInjectPeerEnv_OverridesUserSuppliedSameNameVar(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "main",
					Env: []corev1.EnvVar{
						{Name: "OME_DECODER_ENDPOINT", Value: "userset"},
						{Name: "KEEP_ME", Value: "untouched"},
					},
				},
			},
		},
	}
	InjectPeerEnv(pod, peerISVC(),
		[]v1beta1.ComponentType{v1beta1.DecoderComponent},
		func(v1beta1.ComponentType) string { return "rev1" },
	)
	env := pod.Spec.Containers[0].Env
	if got := envValue(env, "OME_DECODER_ENDPOINT"); got != "llama-decoder.prod.svc.cluster.local" {
		t.Errorf("override OME_DECODER_ENDPOINT: got %q want canonical service DNS", got)
	}
	if got := envValue(env, "KEEP_ME"); got != "untouched" {
		t.Errorf("unrelated env should survive: got %q", got)
	}
}

func TestInjectPeerEnv_NilPodNoOp(t *testing.T) {
	InjectPeerEnv(nil, peerISVC(), []v1beta1.ComponentType{v1beta1.DecoderComponent}, nil)
}

func TestInjectPeerEnv_EmptyPeersNoOp(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Env: []corev1.EnvVar{{Name: "USER", Value: "x"}}}},
		},
	}
	InjectPeerEnv(pod, peerISVC(), nil, nil)
	if len(pod.Spec.Containers[0].Env) != 1 || pod.Spec.Containers[0].Env[0].Name != "USER" {
		t.Errorf("empty peers should not touch env: got %+v", pod.Spec.Containers[0].Env)
	}
	InjectPeerEnv(pod, nil, []v1beta1.ComponentType{v1beta1.DecoderComponent}, nil)
	if len(pod.Spec.Containers[0].Env) != 1 {
		t.Errorf("nil service should not touch env: got %+v", pod.Spec.Containers[0].Env)
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

func TestPeerEnvDeclared(t *testing.T) {
	if PeerEnvDeclared(nil) {
		t.Error("nil ISVC must not declare peer env")
	}
	bare := &v1beta1.InferenceService{}
	if PeerEnvDeclared(bare) {
		t.Error("ISVC without rollout groups must not declare peer env")
	}
	grouped := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		}}},
	}}
	if !PeerEnvDeclared(grouped) {
		t.Error("ISVC with a rollout group must declare peer env")
	}
}

// Every declared role learns every other declared role's address, in a
// stable order, whatever the shape: P, P+D, E+P, E+P+D, each with and
// without a router.
func TestServingPeersNamesEveryOtherDeclaredRole(t *testing.T) {
	shape := func(encoder, decoder, router bool) *v1beta1.InferenceService {
		isvc := peerISVC()
		isvc.Spec.Engine = &v1beta1.EngineSpec{}
		if encoder {
			isvc.Spec.Encoder = &v1beta1.EncoderSpec{}
		}
		if decoder {
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
		}
		if router {
			isvc.Spec.Router = &v1beta1.RouterSpec{}
		}
		return isvc
	}
	const (
		E = v1beta1.EncoderComponent
		P = v1beta1.EngineComponent
		D = v1beta1.DecoderComponent
		R = v1beta1.RouterComponent
	)
	cases := []struct {
		name                     string
		encoder, decoder, router bool
		caller                   v1beta1.ComponentType
		want                     []v1beta1.ComponentType
	}{
		{name: "P engine", caller: P, want: nil},
		{name: "P+R engine", router: true, caller: P, want: []v1beta1.ComponentType{R}},
		{name: "P+D engine", decoder: true, caller: P, want: []v1beta1.ComponentType{D}},
		{name: "P+D+R decoder", decoder: true, router: true, caller: D, want: []v1beta1.ComponentType{P, R}},
		{name: "E+P engine", encoder: true, caller: P, want: []v1beta1.ComponentType{E}},
		{name: "E+P encoder", encoder: true, caller: E, want: []v1beta1.ComponentType{P}},
		{name: "E+P+R encoder", encoder: true, router: true, caller: E, want: []v1beta1.ComponentType{P, R}},
		{name: "E+P+D engine", encoder: true, decoder: true, caller: P, want: []v1beta1.ComponentType{D, E}},
		{name: "E+P+D encoder", encoder: true, decoder: true, caller: E, want: []v1beta1.ComponentType{D, P}},
		{name: "E+P+D+R router", encoder: true, decoder: true, router: true, caller: R, want: []v1beta1.ComponentType{D, E, P}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ServingPeers(shape(tc.encoder, tc.decoder, tc.router), tc.caller)
			if len(got) != len(tc.want) {
				t.Fatalf("ServingPeers = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ServingPeers = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
