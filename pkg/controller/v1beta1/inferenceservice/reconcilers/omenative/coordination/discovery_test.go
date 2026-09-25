package coordination

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestBuildPeerEndpointEnv(t *testing.T) {
	env := BuildPeerEndpointEnv("llama", "prod", v1beta1.DecoderComponent, "abcd1234")
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

func TestBuildPeerEndpointEnv_UpperCasesComponent(t *testing.T) {
	env := BuildPeerEndpointEnv("llama", "prod", v1beta1.EngineComponent, "abcd")
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
	InjectPeerEnv(pod, "llama", "prod",
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

func TestInjectPeerEnv_EmptyHashOmitsRevisionEnv(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main"}},
		},
	}
	InjectPeerEnv(pod, "llama", "prod",
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
	InjectPeerEnv(pod, "llama", "prod",
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
	InjectPeerEnv(nil, "llama", "prod", []v1beta1.ComponentType{v1beta1.DecoderComponent}, nil)
}

func TestInjectPeerEnv_EmptyPeersNoOp(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Env: []corev1.EnvVar{{Name: "USER", Value: "x"}}}},
		},
	}
	InjectPeerEnv(pod, "llama", "prod", nil, nil)
	if len(pod.Spec.Containers[0].Env) != 1 || pod.Spec.Containers[0].Env[0].Name != "USER" {
		t.Errorf("empty peers should not touch env: got %+v", pod.Spec.Containers[0].Env)
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
