package servingruntime

import (
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// runtimeOption edits the metadata and the spec that a ServingRuntime and a
// ClusterServingRuntime share.
type runtimeOption func(meta *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec)

// servingRuntime is an enabled runtime that auto-selects the "vllm" format,
// version "1", at priority 1 over the OpenAI protocol, with one main
// container; the options edit it in order.
func servingRuntime(name, namespace string, opts ...runtimeOption) *v1beta1.ServingRuntime {
	sr := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       baseRuntimeSpec(),
	}
	for _, opt := range opts {
		opt(&sr.ObjectMeta, &sr.Spec)
	}
	return sr
}

// clusterServingRuntime is servingRuntime at cluster scope.
func clusterServingRuntime(name string, opts ...runtimeOption) *v1beta1.ClusterServingRuntime {
	csr := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       baseRuntimeSpec(),
	}
	for _, opt := range opts {
		opt(&csr.ObjectMeta, &csr.Spec)
	}
	return csr
}

func baseRuntimeSpec() v1beta1.ServingRuntimeSpec {
	format := autoSelectedFormat("vllm", 1)
	format.Version = ptr.To("1")
	return v1beta1.ServingRuntimeSpec{
		SupportedModelFormats: []v1beta1.SupportedModelFormat{format},
		Disabled:              ptr.To(false),
		ProtocolVersions:      []constants.InferenceServiceProtocol{constants.OpenAIProtocol},
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			Containers: []corev1.Container{{
				Name:  constants.MainContainerName,
				Image: "ome/vllm:latest",
				Args:  []string{"--model_name={{.Name}}"},
			}},
		},
	}
}

// autoSelectedFormat is a model format the runtime selector may pick on its
// own, at the given priority and without a version.
func autoSelectedFormat(name string, priority int32) v1beta1.SupportedModelFormat {
	return v1beta1.SupportedModelFormat{Name: name, AutoSelect: ptr.To(true), Priority: ptr.To(priority)}
}

// firstFormat is the format the per-format options edit.
func firstFormat(spec *v1beta1.ServingRuntimeSpec) *v1beta1.SupportedModelFormat {
	return &spec.SupportedModelFormats[0]
}

func withFormat(name string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) { firstFormat(spec).Name = name }
}

// withFormats replaces the supported formats.
func withFormats(formats ...v1beta1.SupportedModelFormat) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) { spec.SupportedModelFormats = formats }
}

func withPriority(priority int32) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		firstFormat(spec).Priority = ptr.To(priority)
	}
}

func withoutPriority() runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) { firstFormat(spec).Priority = nil }
}

func withVersion(version string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		firstFormat(spec).Version = ptr.To(version)
	}
}

func withoutVersion() runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) { firstFormat(spec).Version = nil }
}

func withAutoSelect(on bool) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		firstFormat(spec).AutoSelect = ptr.To(on)
	}
}

// withoutAutoSelect leaves the format's autoSelect unset.
func withoutAutoSelect() runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) { firstFormat(spec).AutoSelect = nil }
}

func withArchitecture(architecture string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		firstFormat(spec).ModelArchitecture = ptr.To(architecture)
	}
}

func withFramework(name, version string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		firstFormat(spec).ModelFramework = &v1beta1.ModelFrameworkSpec{Name: name, Version: ptr.To(version)}
	}
}

func withModelFormat(name, version string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		firstFormat(spec).ModelFormat = &v1beta1.ModelFormat{Name: name, Version: ptr.To(version)}
	}
}

func withSizeRange(minSize, maxSize string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		spec.ModelSizeRange = &v1beta1.ModelSizeRangeSpec{Min: ptr.To(minSize), Max: ptr.To(maxSize)}
	}
}

func withDisabled(disabled bool) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) { spec.Disabled = ptr.To(disabled) }
}

func withImage(image string) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) { spec.Containers[0].Image = image }
}

// withScalingPolicy sets the policy; a proportional policy also declares the
// engine and decoder components its ratio ties together.
func withScalingPolicy(policy *v1beta1.ScalingPolicy) runtimeOption {
	return func(_ *metav1.ObjectMeta, spec *v1beta1.ServingRuntimeSpec) {
		spec.ScalingPolicy = policy
		if policy != nil && policy.Mode == v1beta1.ScalingProportional {
			spec.EngineConfig = &v1beta1.EngineSpec{}
			spec.DecoderConfig = &v1beta1.DecoderSpec{}
		}
	}
}

// withParent names the runtime this one inherits from; an empty name leaves
// it without a parent.
func withParent(name string) runtimeOption {
	return func(meta *metav1.ObjectMeta, _ *v1beta1.ServingRuntimeSpec) {
		if name == "" {
			return
		}
		if meta.Annotations == nil {
			meta.Annotations = map[string]string{}
		}
		meta.Annotations[constants.RuntimeInheritFromAnnotationKey] = name
	}
}

// withProfileAnnotation declares the runtime a profile other runtimes
// inherit from.
func withProfileAnnotation() runtimeOption {
	return func(meta *metav1.ObjectMeta, _ *v1beta1.ServingRuntimeSpec) {
		if meta.Annotations == nil {
			meta.Annotations = map[string]string{}
		}
		meta.Annotations[constants.RuntimeProfileAnnotationKey] = "true"
	}
}

func newDecoder(t *testing.T) admission.Decoder {
	t.Helper()
	return admission.NewDecoder(fakeclient.Scheme(t))
}

// newServingRuntimeValidator reads the given objects as the cluster's state.
func newServingRuntimeValidator(t *testing.T, existing ...client.Object) *ServingRuntimeValidator {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(existing...))
	return &ServingRuntimeValidator{Client: c, Decoder: newDecoder(t)}
}

// newClusterServingRuntimeValidator reads the given objects as the cluster's state.
func newClusterServingRuntimeValidator(t *testing.T, existing ...client.Object) *ClusterServingRuntimeValidator {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(existing...))
	return &ClusterServingRuntimeValidator{Client: c, Decoder: newDecoder(t)}
}

func encode(t *testing.T, obj any) []byte {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func createReq(t *testing.T, obj any) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Object:    runtime.RawExtension{Raw: encode(t, obj)},
	}}
}

func updateReq(t *testing.T, oldObj, newObj any) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update,
		Object:    runtime.RawExtension{Raw: encode(t, newObj)},
		OldObject: runtime.RawExtension{Raw: encode(t, oldObj)},
	}}
}
