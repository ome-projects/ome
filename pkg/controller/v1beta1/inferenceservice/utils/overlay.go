package utils

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	OverlayMountPathPrefix = "/opt/ml/model-overlays"
	OverlayEnvVarPrefix    = "OVERLAY_"
	OverlayEnvVarSuffix    = "_MODEL_PATH"
)

// ResolvedOverlay is one entry of ResolveOverlays' output. Spec is nil
// iff the overlay was skipped (NotFound, disabled, Sharded-NotReady);
// SkipReason carries the human-readable cause for status reporting.
type ResolvedOverlay struct {
	Ref        v1beta1.ModelOverlayRef
	Spec       *v1beta1.BaseModelSpec
	Meta       *metav1.ObjectMeta
	Status     *v1beta1.ModelStatusSpec
	SkipReason string
}

func (r ResolvedOverlay) Skipped() bool { return r.Spec == nil }

// ResolveOverlays fetches every overlay declared on the ISVC, in order.
// Resolution failures become Skipped entries (the primary is the
// load-bearing model); transient client errors bubble up so reconcile
// requeues.
func ResolveOverlays(cl client.Client, isvc *v1beta1.InferenceService) ([]ResolvedOverlay, error) {
	if isvc == nil || isvc.Spec.Model == nil || len(isvc.Spec.Model.Overlays) == 0 {
		return nil, nil
	}
	out := make([]ResolvedOverlay, 0, len(isvc.Spec.Model.Overlays))
	for _, ref := range isvc.Spec.Model.Overlays {
		resolved := ResolvedOverlay{Ref: ref}

		spec, modelMeta, status, err := GetReferencedModel(context.Background(), cl, isvc.Namespace, ref.Name, ref.Kind, ref.APIGroup)
		if err != nil {
			if isModelNotFoundError(err) {
				resolved.SkipReason = fmt.Sprintf("overlay %q not found", ref.Name)
				out = append(out, resolved)
				continue
			}
			return nil, fmt.Errorf("resolve overlay %q: %w", ref.Name, err)
		}
		if spec.Disabled != nil && *spec.Disabled {
			resolved.SkipReason = fmt.Sprintf("overlay %q is disabled", ref.Name)
			out = append(out, resolved)
			continue
		}
		if IsShardedBaseModel(spec) {
			if ready, msg := ShardedBaseModelReady(status, modelMeta.Generation); !ready {
				resolved.SkipReason = fmt.Sprintf("overlay %q is sharded but not ready: %s", ref.Name, msg)
				out = append(out, resolved)
				continue
			}
		}
		resolved.Spec = spec
		resolved.Meta = modelMeta
		resolved.Status = status
		out = append(out, resolved)
	}
	return out, nil
}

func OverlayEnvVarName(modelName string) string {
	return OverlayEnvVarPrefix + sanitizeOverlayName(modelName) + OverlayEnvVarSuffix
}

func OverlayMountPath(modelName string) string {
	return OverlayMountPathPrefix + "/" + modelName
}

func sanitizeOverlayName(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

func isModelNotFoundError(err error) bool {
	return apierrors.IsNotFound(err) || (err != nil && strings.HasPrefix(err.Error(), "No BaseModel or ClusterBaseModel with the name:"))
}
