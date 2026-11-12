// Package runtimepreset implements a mutating webhook that injects engine
// defaults from the preset the runtimePresets block of the
// inferenceservice-config ConfigMap holds for the engine ome.io/engine: <name>
// names. The runtime-inheritance resolver composes on top at consumer call
// sites; $(VAR) placeholders are substituted by Kubernetes at pod-create time.
package runtimepreset

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
)

// +kubebuilder:webhook:verbs=create;update,path=/mutate-ome-io-v1beta1-servingruntime-preset,mutating=true,failurePolicy=fail,groups=ome.io,resources=servingruntimes,versions=v1beta1,name=servingruntime.ome-webhook-server.preset,sideEffects=None,admissionReviewVersions=v1
// +kubebuilder:webhook:verbs=create;update,path=/mutate-ome-io-v1beta1-clusterservingruntime-preset,mutating=true,failurePolicy=fail,groups=ome.io,resources=clusterservingruntimes,versions=v1beta1,name=clusterservingruntime.ome-webhook-server.preset,sideEffects=None,admissionReviewVersions=v1

var log = logf.Log.WithName("runtimepreset-mutator")

type ServingRuntimeMutator struct {
	Decoder   admission.Decoder
	Clientset kubernetes.Interface
}

func (m *ServingRuntimeMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	sr := &v1beta1.ServingRuntime{}
	return mutate(ctx, m.Decoder, m.Clientset, req, sr, &sr.Spec)
}

type ClusterServingRuntimeMutator struct {
	Decoder   admission.Decoder
	Clientset kubernetes.Interface
}

func (m *ClusterServingRuntimeMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	csr := &v1beta1.ClusterServingRuntime{}
	return mutate(ctx, m.Decoder, m.Clientset, req, csr, &csr.Spec)
}

// mutate decodes the request into runtime, merges the preset its engine
// annotation names into spec (the field inside runtime) and answers with
// the patch from the decoded object to the merged one. The preset table is
// read from the ConfigMap only when the runtime names an engine.
func mutate(ctx context.Context, decoder admission.Decoder, clientset kubernetes.Interface, req admission.Request, runtime client.Object, spec *v1beta1.ServingRuntimeSpec) admission.Response {
	if err := decoder.Decode(req, runtime); err != nil {
		log.Error(err, "decode "+req.Kind.Kind, "name", runtime.GetName(), "namespace", runtime.GetNamespace())
		return admission.Errored(http.StatusBadRequest, err)
	}
	engine, ok := runtime.GetAnnotations()[constants.RuntimeEngineAnnotationKey]
	if !ok || engine == "" {
		return admission.Allowed("no engine preset annotation")
	}
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(ctx,
		constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "Failed to find config map", "name", constants.InferenceServiceConfigMapName)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	presets, err := parsePresets(configMap)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	merged, resp := applyPreset(engine, spec, presets)
	if resp != nil {
		return *resp
	}
	original, err := encodeWithoutNulls(runtime)
	if err != nil {
		log.Error(err, "encode runtime for patch base")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	*spec = *merged
	mutated, err := encodeWithoutNulls(runtime)
	if err != nil {
		log.Error(err, "encode mutated runtime")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(original, mutated)
}

// applyPreset returns (merged, nil) on success and (nil, &resp) when the
// engine is refused: one the table does not name, or any engine while the
// table is empty.
func applyPreset(engine string, spec *v1beta1.ServingRuntimeSpec, presets Presets) (*v1beta1.ServingRuntimeSpec, *admission.Response) {
	preset, known := presets[engine]
	if !known {
		names := strings.Join(presets.names(), ", ")
		if names == "" {
			names = fmt.Sprintf("none; the %s block of %s names no engine", PresetsConfigKeyName, constants.InferenceServiceConfigMapName)
		}
		resp := admission.Denied(fmt.Sprintf(
			"unknown %s value %q (known: %s)",
			constants.RuntimeEngineAnnotationKey, engine, names,
		))
		return nil, &resp
	}
	// Runtime (child) wins on collision; preset (parent) fills unset
	// positions. Same contract as the runtime-inheritance resolver, so
	// preset + inheritance compose commutatively for any field set by
	// exactly one source.
	merged, err := runtimeinheritance.Merge(preset, spec)
	if err != nil {
		resp := admission.Errored(http.StatusInternalServerError, fmt.Errorf("merge %s preset: %w", engine, err))
		return nil, &resp
	}
	return merged, nil
}

// encodeWithoutNulls serializes the runtime as one side of the patch.
// Both sides come from the decoded object, not the raw request, so fields
// this build's API types do not know stay out of the patch (a raw-based
// diff would emit remove ops that destroy them). ServingRuntimePodSpec's
// embedded Containers lacks omitempty, so a nil slice marshals as "null",
// which CRD validation rejects; the nulls go so both sides are symmetric.
func encodeWithoutNulls(runtime any) ([]byte, error) {
	out, err := json.Marshal(runtime)
	if err != nil {
		return nil, err
	}
	return runtimeinheritance.StripJSONNulls(out)
}
