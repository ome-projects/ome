package render

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func nodeAffinityIn(key, value string) *corev1.NodeAffinity {
	return &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{value}}}}}}}
}

// nearEnginePodAffinity is a user-written rule that keeps the encoder in the
// fabric domain of the service's engine pods.
func nearEnginePodAffinity() *corev1.PodAffinity {
	return &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{constants.OMEComponentLabel: "engine", constants.InferenceServicePodLabelKey: "svc"}},
		TopologyKey:   "example.com/fabric-domain",
	}}}
}

func encoderTestPiece(t *testing.T, class *v1beta1.AcceleratorClassSpec) *Piece {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return &Piece{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), Log: logr.Discard(),
		InferenceServiceConfig: &controllerconfig.InferenceServicesConfig{AcceleratorResources: []string{"example.com/gpu"}},
		AcceleratorClass:       class, AcceleratorClassName: "class-a"}
}

// A block on spec.encoder that sets only podAffinity keeps the accelerator
// class's node affinity; a nodeAffinity of its own replaces it. The class's
// node selector applies either way. Engine and decoder keep their rule: any
// affinity on the block keeps the class's node affinity out.
func TestEncoderKeepsClassNodeAffinityBesidePodAffinity(t *testing.T) {
	byAffinity := &v1beta1.AcceleratorClassSpec{Discovery: v1beta1.AcceleratorDiscovery{
		Affinity: &corev1.Affinity{NodeAffinity: nodeAffinityIn("accelerator", "small")}}}
	bySelector := &v1beta1.AcceleratorClassSpec{Discovery: v1beta1.AcceleratorDiscovery{
		NodeSelector: map[string]string{"accelerator": "small"}}}
	runner := func() *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: constants.MainContainerName, Image: "example.com/serving:v1"}}
	}
	tests := []struct {
		name             string
		class            *v1beta1.AcceleratorClassSpec
		user             *corev1.Affinity
		wantAffinity     *corev1.Affinity
		wantNodeSelector map[string]string
	}{
		{name: "podAffinity only, class finds nodes by affinity", class: byAffinity,
			user:         &corev1.Affinity{PodAffinity: nearEnginePodAffinity()},
			wantAffinity: &corev1.Affinity{PodAffinity: nearEnginePodAffinity(), NodeAffinity: nodeAffinityIn("accelerator", "small")}},
		{name: "podAffinity only, class finds nodes by node selector", class: bySelector,
			user:             &corev1.Affinity{PodAffinity: nearEnginePodAffinity()},
			wantAffinity:     &corev1.Affinity{PodAffinity: nearEnginePodAffinity()},
			wantNodeSelector: map[string]string{"accelerator": "small"}},
		{name: "own nodeAffinity replaces the class's", class: byAffinity,
			user:         &corev1.Affinity{NodeAffinity: nodeAffinityIn("zone", "a")},
			wantAffinity: &corev1.Affinity{NodeAffinity: nodeAffinityIn("zone", "a")}},
		{name: "no affinity takes the class's", class: byAffinity,
			wantAffinity: &corev1.Affinity{NodeAffinity: nodeAffinityIn("accelerator", "small")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"},
				Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Encoder: &v1beta1.EncoderSpec{PodSpec: v1beta1.PodSpec{Affinity: tt.user.DeepCopy()}}}}
			merged := &v1beta1.EncoderSpec{PodSpec: v1beta1.PodSpec{Affinity: tt.user.DeepCopy()}, Runner: runner()}
			got, err := RenderEncoder(t.Context(), encoderTestPiece(t, tt.class), svc, merged)
			if err != nil {
				t.Fatal(err)
			}
			if d := cmp.Diff(tt.wantAffinity, got.Primary.Affinity); d != "" {
				t.Errorf("encoder affinity (-want +got):\n%s", d)
			}
			if d := cmp.Diff(tt.wantNodeSelector, got.Primary.NodeSelector); tt.wantNodeSelector != nil && d != "" {
				t.Errorf("encoder node selector (-want +got):\n%s", d)
			}
		})
	}

	// The decoder's rule is unchanged: a podAffinity-only block drops the
	// class's node affinity.
	svc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"},
		Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{PodSpec: v1beta1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: nearEnginePodAffinity()}}}}}
	merged := &v1beta1.DecoderSpec{PodSpec: v1beta1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: nearEnginePodAffinity()}}, Runner: runner()}
	got, err := RenderDecoder(t.Context(), encoderTestPiece(t, byAffinity), svc, merged)
	if err != nil {
		t.Fatal(err)
	}
	if got.Primary.Affinity.NodeAffinity != nil {
		t.Errorf("decoder took the class's node affinity beside its own podAffinity: %+v", got.Primary.Affinity.NodeAffinity)
	}
}
