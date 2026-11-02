// Package runtimepreset implements a mutating webhook that injects
// engine defaults from a Go-defined preset when ome.io/engine: <name>
// names a known engine. The runtime-inheritance resolver composes on
// top at consumer call sites; $(VAR) placeholders are substituted by
// Kubernetes at pod-create time.
package runtimepreset

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

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

// presets is the registry — adding a new engine is one preset file +
// one map entry.
var presets = map[string]func() *v1beta1.ServingRuntimeSpec{
	"sglang-pd": sglangPDPreset,
}

type ServingRuntimeMutator struct {
	Decoder admission.Decoder
}

func (m *ServingRuntimeMutator) Handle(_ context.Context, req admission.Request) admission.Response {
	sr := &v1beta1.ServingRuntime{}
	return mutate(m.Decoder, req, sr, &sr.Spec)
}

type ClusterServingRuntimeMutator struct {
	Decoder admission.Decoder
}

func (m *ClusterServingRuntimeMutator) Handle(_ context.Context, req admission.Request) admission.Response {
	csr := &v1beta1.ClusterServingRuntime{}
	return mutate(m.Decoder, req, csr, &csr.Spec)
}

// mutate decodes the request into runtime, merges the preset its engine
// annotation names into spec (the field inside runtime) and answers with
// the patch from the decoded object to the merged one.
func mutate(decoder admission.Decoder, req admission.Request, runtime client.Object, spec *v1beta1.ServingRuntimeSpec) admission.Response {
	if err := decoder.Decode(req, runtime); err != nil {
		log.Error(err, "decode "+req.Kind.Kind, "name", runtime.GetName(), "namespace", runtime.GetNamespace())
		return admission.Errored(http.StatusBadRequest, err)
	}
	merged, resp := applyPreset(runtime.GetAnnotations(), spec)
	if resp != nil {
		return *resp
	}
	if merged == nil {
		return admission.Allowed("no engine preset annotation")
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

// applyPreset returns (merged, nil) on success, (nil, &resp) on
// passthrough or rejection. merged==nil && resp==nil means no
// annotation set — caller should pass through.
func applyPreset(annotations map[string]string, spec *v1beta1.ServingRuntimeSpec) (*v1beta1.ServingRuntimeSpec, *admission.Response) {
	engine, ok := annotations[constants.RuntimeEngineAnnotationKey]
	if !ok || engine == "" {
		return nil, nil
	}
	factory, known := presets[engine]
	if !known {
		resp := admission.Denied(fmt.Sprintf(
			"unknown %s value %q (known: %s)",
			constants.RuntimeEngineAnnotationKey, engine, strings.Join(knownPresetNames(), ", "),
		))
		return nil, &resp
	}
	// Runtime (child) wins on collision; preset (parent) fills unset
	// positions. Same contract as the runtime-inheritance resolver, so
	// preset + inheritance compose commutatively for any field set by
	// exactly one source.
	merged, err := runtimeinheritance.Merge(factory(), spec)
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

func knownPresetNames() []string {
	out := make([]string, 0, len(presets))
	for k := range presets {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
