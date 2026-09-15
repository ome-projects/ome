// Package mutate implements CLI-local guarded action safety, never controllers.
package mutate

import (
	"errors"
	"regexp"
	"strings"

	validation "k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	ErrUnsafeTarget   = errors.New("action refused: target identity is missing or unsafe")
	ErrPlacement      = errors.New("action refused: placement sources and derived services cannot be mutated")
	ErrBounds         = errors.New("action refused: safety inputs exceed inspection bounds")
	ErrDeleting       = errors.New("action refused: target is being deleted")
	scalarPattern     = regexp.MustCompile(`^[A-Za-z0-9_.:/@+-]+$`)
	credentialPattern = regexp.MustCompile(`(?:^|[^A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}|^[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}$`)
)

// SafeScalar allows bounded, non-control identities without hiding a value
// subsequently used by a mutation. It is a shape guard, not a secret scanner.
func SafeScalar(value string) bool {
	if len(value) == 0 || len(value) > 256 || !scalarPattern.MatchString(value) || credentialPattern.MatchString(value) {
		return false
	}
	// Unmistakable credential userinfo must not be previewed or silently
	// redacted before mutating its exact logical value. Ordinary user@host
	// and colon-only protocol identities remain valid.
	if at := strings.LastIndexByte(value, '@'); at >= 0 {
		userinfo := value[:at]
		if scheme := strings.Index(userinfo, "://"); scheme >= 0 {
			userinfo = userinfo[scheme+3:]
		}
		if strings.ContainsRune(userinfo, ':') {
			return false
		}
	}
	return true
}

// ValidateTarget checks exact identity and disallows unsafe placement ownership.
func ValidateTarget(v *v1beta1.InferenceService) error {
	// Generated typed clients clear TypeMeta after decoding. Reject a conflicting
	// nonempty GVK without requiring fields the transport deliberately removes.
	if v == nil || v.Kind != "" && v.Kind != "InferenceService" || v.APIVersion != "" && v.APIVersion != "ome.io/v1beta1" || len(validation.IsDNS1123Subdomain(v.Name)) != 0 ||
		len(validation.IsDNS1123Label(v.Namespace)) != 0 || !SafeScalar(v.Name) ||
		!SafeScalar(v.Namespace) || !SafeScalar(string(v.UID)) ||
		!SafeScalar(v.ResourceVersion) || v.Generation <= 0 {
		return ErrUnsafeTarget
	}
	if v.DeletionTimestamp != nil {
		return ErrDeleting
	}
	if len(v.Annotations) > 256 || len(v.Labels) > 256 || len(v.Finalizers) > 64 || len(v.Status.Conditions) > 64 || len(v.Status.Components) > 3 {
		return ErrBounds
	}
	if !boundedPrivatePayload(v) {
		return ErrBounds
	}
	bytes := 0
	for k, value := range v.Annotations {
		bytes += len(k) + len(value)
		if bytes > 65536 {
			return ErrBounds
		}
	}
	for k, value := range v.Labels {
		bytes += len(k) + len(value)
		if bytes > 65536 {
			return ErrBounds
		}
	}
	for _, value := range v.Finalizers {
		bytes += len(value)
		if bytes > 65536 {
			return ErrBounds
		}
		if value == "ome.io/placement" {
			return ErrPlacement
		}
	}
	if v.Spec.Placement != nil || v.Status.Placement != nil {
		return ErrPlacement
	}
	if v.Status.Rollout != nil && v.Status.Rollout.ActiveRun != nil {
		run := v.Status.Rollout.ActiveRun
		if len(run.Plan.Groups) > 3 || len(run.TargetRevisions) > 3 || len(run.RunID) > 300 {
			return ErrBounds
		}
		for _, pinned := range run.Plan.Groups {
			group := pinned.Group
			if len(group.Components) > 3 || len(group.Order) > 3 {
				return ErrBounds
			}
			if group.Canary == nil {
				continue
			}
			if len(group.Canary.Steps) > 20 {
				return ErrBounds
			}
			for _, step := range group.Canary.Steps {
				if step.Analysis != nil && len(step.Analysis.Metrics) > 10 {
					return ErrBounds
				}
			}
		}
	}
	if v.Status.Canary != nil && len(v.Status.Canary.MetricResults) > 10 {
		return ErrBounds
	}
	if v.Status.RolloutCoordination != nil {
		if len(v.Status.RolloutCoordination.Groups) > 3 {
			return ErrBounds
		}
		for _, group := range v.Status.RolloutCoordination.Groups {
			if len(group.Components) > 3 || len(group.Order) > 3 {
				return ErrBounds
			}
		}
	}
	for _, component := range v.Status.Components {
		if len(component.Traffic) > 8 {
			return ErrBounds
		}
	}
	for _, k := range []string{"ome.io/accelerator-requirements", "ome.io/cluster-selector", constants.PlacementOrigin, constants.PlacementOriginUID, constants.PlacementControlPlane} {
		if _, exists := v.Annotations[k]; exists {
			return ErrPlacement
		}
		if _, exists := v.Labels[k]; exists {
			return ErrPlacement
		}
	}
	return nil
}
