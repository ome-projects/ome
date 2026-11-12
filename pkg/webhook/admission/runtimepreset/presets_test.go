package runtimepreset

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func configMapWithPresets(block string) *corev1.ConfigMap {
	data := map[string]string{}
	if block != "" {
		data[PresetsConfigKeyName] = block
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       data,
	}
}

func TestParsePresets_AbsentBlockIsAnEmptyTable(t *testing.T) {
	g := gomega.NewWithT(t)
	presets, err := parsePresets(configMapWithPresets(""))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(presets).To(gomega.BeEmpty())
}

func TestParsePresets_MalformedBlockIsRefused(t *testing.T) {
	g := gomega.NewWithT(t)
	_, err := parsePresets(configMapWithPresets(`{"sglang-pd": [}`))
	g.Expect(err).To(gomega.HaveOccurred())
	g.Expect(err.Error()).To(gomega.ContainSubstring(PresetsConfigKeyName))
}

func TestApplyPreset_EmptyTableRefusesEveryEngineAndNamesTheBlock(t *testing.T) {
	g := gomega.NewWithT(t)
	merged, resp := applyPreset("sglang-pd", &v1beta1.ServingRuntimeSpec{}, Presets{})
	g.Expect(merged).To(gomega.BeNil())
	g.Expect(resp).NotTo(gomega.BeNil())
	g.Expect(resp.Allowed).To(gomega.BeFalse())
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring(PresetsConfigKeyName))
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("names no engine"))
}

// TestMutate_PresetsComeFromTheConfigMap: the table the webhook applies is
// the ConfigMap's, so an engine the ConfigMap names with its own image is
// filled in with that image.
func TestMutate_PresetsComeFromTheConfigMap(t *testing.T) {
	g := gomega.NewWithT(t)
	configMap := configMapWithPresets(`{"engine-a": {"engineConfig": {"runner": {"name": "ome-container", "image": "example.com/engine:1"}}}}`)
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rt", Namespace: "ns",
			Annotations: map[string]string{constants.RuntimeEngineAnnotationKey: "engine-a"},
		},
	}
	raw := encodeSR(t, sr)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: raw}}}
	m := &ServingRuntimeMutator{Decoder: newDecoder(t), Clientset: fake.NewClientset(configMap)}

	resp := m.Handle(context.Background(), req)

	g.Expect(resp.Allowed).To(gomega.BeTrue())
	patched := applyPatchToSR(t, raw, resp)
	g.Expect(patched.Spec.EngineConfig.Runner.Image).To(gomega.Equal("example.com/engine:1"))
	g.Expect(patched.Spec.DecoderConfig).To(gomega.BeNil(), "the preset fills only what it names")
}

func TestMutate_MissingConfigMapIsAnError(t *testing.T) {
	g := gomega.NewWithT(t)
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rt", Namespace: "ns",
			Annotations: map[string]string{constants.RuntimeEngineAnnotationKey: "sglang-pd"},
		},
	}
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{Object: runtime.RawExtension{Raw: encodeSR(t, sr)}}}
	m := &ServingRuntimeMutator{Decoder: newDecoder(t), Clientset: fake.NewClientset()}

	resp := m.Handle(context.Background(), req)

	g.Expect(resp.Allowed).To(gomega.BeFalse())
	g.Expect(resp.Result.Code).To(gomega.BeEquivalentTo(500))
}

// TestShippedPresets_SglangPDMatchesTheGolden keeps the shipped sglang-pd
// preset at testdata/sglang_pd.golden.json: the images, ports, commands,
// probes and selectors a runtime that names the engine is filled with.
func TestShippedPresets_SglangPDMatchesTheGolden(t *testing.T) {
	g := gomega.NewWithT(t)
	preset, ok := shippedPresets(t)["sglang-pd"]
	g.Expect(ok).To(gomega.BeTrue())
	rendered, err := json.MarshalIndent(preset, "", "  ")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	want, err := os.ReadFile(filepath.Join("testdata", "sglang_pd.golden.json"))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(string(rendered) + "\n").To(gomega.Equal(string(want)))
}

func TestShippedPresets_SglangPD_Engine(t *testing.T) {
	g := gomega.NewWithT(t)
	preset := shippedPresets(t)["sglang-pd"]

	g.Expect(preset.EngineConfig).NotTo(gomega.BeNil())
	g.Expect(preset.EngineConfig.Runner).NotTo(gomega.BeNil())

	engine := preset.EngineConfig.Runner.Container
	g.Expect(engine.Name).To(gomega.Equal("ome-container"))
	g.Expect(engine.Image).NotTo(gomega.BeEmpty())
	g.Expect(engine.Command).To(gomega.ContainElement("--disaggregation-mode"))
	g.Expect(engine.Command).To(gomega.ContainElement("prefill"))
	g.Expect(engine.Command).To(gomega.ContainElement("$(MODEL_PATH)"))
	g.Expect(engine.Command).To(gomega.ContainElement("--disaggregation-bootstrap-port"))

	g.Expect(containerPortNames(engine.Ports)).To(gomega.ConsistOf("http", "bootstrap"))
}

func TestShippedPresets_SglangPD_Decoder(t *testing.T) {
	g := gomega.NewWithT(t)
	preset := shippedPresets(t)["sglang-pd"]

	decoder := preset.DecoderConfig.Runner.Container
	g.Expect(decoder.Command).To(gomega.ContainElement("decode"))
	// Decoder must not expose the bootstrap port — engine-only in sglang-pd.
	g.Expect(decoder.Command).NotTo(gomega.ContainElement("--disaggregation-bootstrap-port"))
	g.Expect(containerPortNames(decoder.Ports)).To(gomega.ConsistOf("http"))
}

func TestShippedPresets_SglangPD_Router(t *testing.T) {
	g := gomega.NewWithT(t)
	preset := shippedPresets(t)["sglang-pd"]

	router := preset.RouterConfig.Runner.Container
	g.Expect(router.Name).To(gomega.Equal("router"))
	g.Expect(router.Image).NotTo(gomega.BeEmpty())
	// Router discovers engine/decoder pods via labels OME already sets
	// on ISVC-owned pods; selector drift here breaks routing.
	g.Expect(router.Args).To(gomega.ContainElement(
		"component=engine,ome.io/inferenceservice=$(INFERENCESERVICE_NAME)"))
	g.Expect(router.Args).To(gomega.ContainElement(
		"component=decoder,ome.io/inferenceservice=$(INFERENCESERVICE_NAME)"))
}

func containerPortNames(ports []corev1.ContainerPort) []string {
	out := make([]string, 0, len(ports))
	for _, p := range ports {
		out = append(out, p.Name)
	}
	return out
}
