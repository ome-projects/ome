package runtimepreset

import (
	"context"
	"encoding/json"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func newDecoder(t *testing.T) admission.Decoder {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return admission.NewDecoder(scheme)
}

func encodeSR(t *testing.T, sr *v1beta1.ServingRuntime) []byte {
	t.Helper()
	raw, err := json.Marshal(sr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func encodeCSR(t *testing.T, csr *v1beta1.ClusterServingRuntime) []byte {
	t.Helper()
	raw, err := json.Marshal(csr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func applyAndDecode(t *testing.T, sr *v1beta1.ServingRuntime) (*v1beta1.ServingRuntime, admission.Response) {
	t.Helper()
	raw := encodeSR(t, sr)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: raw}}}
	m := &ServingRuntimeMutator{Decoder: newDecoder(t), Clientset: fake.NewClientset(shippedConfigMap(t))}
	resp := m.Handle(context.Background(), req)
	if !resp.Allowed || len(resp.Patches) == 0 {
		return sr, resp
	}
	patched := applyPatchToSR(t, raw, resp)
	return patched, resp
}

func applyPatchToSR(t *testing.T, original []byte, resp admission.Response) *v1beta1.ServingRuntime {
	t.Helper()
	patchJSON, err := json.Marshal(resp.Patches)
	if err != nil {
		t.Fatalf("marshal patches: %v", err)
	}
	patch, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	out, err := patch.Apply(original)
	if err != nil {
		t.Fatalf("apply patch: %v", err)
	}
	sr := &v1beta1.ServingRuntime{}
	if err := json.Unmarshal(out, sr); err != nil {
		t.Fatalf("unmarshal patched: %v", err)
	}
	return sr
}

func TestMutator_NoAnnotation_PassesThrough(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "no-preset", Namespace: "ns"},
		Spec:       v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)},
	}
	_, resp := applyAndDecode(t, sr)
	g.Expect(resp.Allowed).To(gomega.BeTrue())
	g.Expect(resp.Patches).To(gomega.BeEmpty())
}

func TestMutator_UnknownEngine_Rejected(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bad", Namespace: "ns",
			Annotations: map[string]string{constants.RuntimeEngineAnnotationKey: "does-not-exist"},
		},
	}
	_, resp := applyAndDecode(t, sr)
	g.Expect(resp.Allowed).To(gomega.BeFalse())
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("unknown"))
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("sglang-pd"))
}

func TestMutator_SglangPD_InjectsEngineDefaults(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rt", Namespace: "ns",
			Annotations: map[string]string{constants.RuntimeEngineAnnotationKey: "sglang-pd"},
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{{
				ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
				ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
			}},
		},
	}
	patched, resp := applyAndDecode(t, sr)
	g.Expect(resp.Allowed).To(gomega.BeTrue())
	g.Expect(patched.Spec.EngineConfig.Runner.Image).To(gomega.Equal(shippedPresets(t)["sglang-pd"].EngineConfig.Runner.Image))
	g.Expect(patched.Spec.DecoderConfig.Runner.Command).To(gomega.ContainElement("decode"))
	g.Expect(patched.Spec.RouterConfig.Runner.Image).To(gomega.Equal(shippedPresets(t)["sglang-pd"].RouterConfig.Runner.Image))
}

func TestMutator_SglangPD_RuntimeWinsOnCollision(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rt-override", Namespace: "ns",
			Annotations: map[string]string{constants.RuntimeEngineAnnotationKey: "sglang-pd"},
		},
		Spec: v1beta1.ServingRuntimeSpec{
			EngineConfig: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
					Name:  "ome-container",
					Image: "internal.example.com/sglang:custom",
				}},
			},
		},
	}
	patched, resp := applyAndDecode(t, sr)
	g.Expect(resp.Allowed).To(gomega.BeTrue())
	g.Expect(patched.Spec.EngineConfig.Runner.Image).To(gomega.Equal("internal.example.com/sglang:custom"))
	g.Expect(patched.Spec.DecoderConfig).NotTo(gomega.BeNil())
	g.Expect(patched.Spec.RouterConfig).NotTo(gomega.BeNil())
}

func TestClusterMutator_SglangPD_InjectsEngineDefaults(t *testing.T) {
	g := gomega.NewWithT(t)
	csr := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "csr",
			Annotations: map[string]string{constants.RuntimeEngineAnnotationKey: "sglang-pd"},
		},
	}
	raw := encodeCSR(t, csr)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: raw}}}
	m := &ClusterServingRuntimeMutator{Decoder: newDecoder(t), Clientset: fake.NewClientset(shippedConfigMap(t))}
	resp := m.Handle(context.Background(), req)
	g.Expect(resp.Allowed).To(gomega.BeTrue())
	g.Expect(resp.Patches).NotTo(gomega.BeEmpty())
}

func TestPresets_NamesAreSorted(t *testing.T) {
	g := gomega.NewWithT(t)
	presets := Presets{"vllm": {}, "sglang-pd": {}, "trt": {}}
	g.Expect(presets.names()).To(gomega.Equal([]string{"sglang-pd", "trt", "vllm"}))
}

// TestMutator_PreservesUnknownFields verifies the patch never removes
// spec fields this build's Go types don't know about (e.g. a newer CRD
// schema during version skew): the patch base is the decoded object, so
// decode-dropped fields stay out of the diff.
func TestMutator_PreservesUnknownFields(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rt-future", Namespace: "ns",
			Annotations: map[string]string{constants.RuntimeEngineAnnotationKey: "sglang-pd"},
		},
	}
	var doc map[string]any
	g.Expect(json.Unmarshal(encodeSR(t, sr), &doc)).To(gomega.Succeed())
	doc["spec"].(map[string]any)["futureField"] = "keep-me"
	raw, err := json.Marshal(doc)
	g.Expect(err).To(gomega.BeNil())

	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: raw}}}
	m := &ServingRuntimeMutator{Decoder: newDecoder(t), Clientset: fake.NewClientset(shippedConfigMap(t))}
	resp := m.Handle(context.Background(), req)
	g.Expect(resp.Allowed).To(gomega.BeTrue())
	g.Expect(resp.Patches).NotTo(gomega.BeEmpty())

	patchJSON, err := json.Marshal(resp.Patches)
	g.Expect(err).To(gomega.BeNil())
	patch, err := jsonpatch.DecodePatch(patchJSON)
	g.Expect(err).To(gomega.BeNil())
	out, err := patch.Apply(raw)
	g.Expect(err).To(gomega.BeNil())

	var patchedDoc map[string]any
	g.Expect(json.Unmarshal(out, &patchedDoc)).To(gomega.Succeed())
	g.Expect(patchedDoc["spec"].(map[string]any)).To(gomega.HaveKeyWithValue("futureField", "keep-me"))
	// The preset merge itself must still land.
	g.Expect(patchedDoc["spec"].(map[string]any)).To(gomega.HaveKey("engineConfig"))
}
