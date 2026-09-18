package autoscaleprojection

import (
	"context"
	"errors"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

var ErrLiveScalerInput = errors.New("invalid live autoscaler status input")

// LiveScalerReader is restricted to exact named reads. Implementations must
// bound response bytes and request duration; LIST is never needed.
type LiveScalerReader interface {
	GetInferenceReplica(context.Context, string, string, metav1.GetOptions) (*ome.InferenceReplica, error)
	GetHPA(context.Context, string, string, metav1.GetOptions) (*autoscalingv2.HorizontalPodAutoscaler, error)
	GetScaledObject(context.Context, string, string, metav1.GetOptions) (*kedav1.ScaledObject, error)
}

// EnrichLiveScaler adds opt-in, message-free scaler evidence. It only trusts
// the exact parent-selected target and a verified controller-owner chain.
func EnrichLiveScaler(ctx context.Context, parent *ome.InferenceService, base reportv1alpha1.AutoscaleStatusReport, reader LiveScalerReader, clock reportv1alpha1.Clock) (reportv1alpha1.AutoscaleStatusReport, error) {
	if ctx == nil || parent == nil || reader == nil || parent.Namespace != base.Metadata.Namespace || parent.Name != base.Metadata.Name || parent.UID == "" {
		return reportv1alpha1.AutoscaleStatusReport{}, ErrLiveScalerInput
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	result := base.Canonical()
	for i := range result.Content.Components {
		component := &result.Content.Components[i]
		live := &reportv1alpha1.AutoscaleLiveScaler{
			Kind: component.Class, Evidence: reportv1alpha1.AutoscaleLiveScalerNotSelected,
			GenerationState: reportv1alpha1.AutoscaleScalerGenerationUnproven,
			Conditions:      []reportv1alpha1.AutoscaleScalerCondition{},
		}
		component.LiveScaler = live
		if component.ManagedBy != reportv1alpha1.AutoscaleManagedByOME ||
			(component.Class != reportv1alpha1.AutoscaleClassHPA && component.Class != reportv1alpha1.AutoscaleClassKEDA) {
			continue
		}
		status, found := parent.Status.Components[ome.ComponentType(component.Type)]
		if !found || status.ScaleTargetRef == nil {
			continue
		}
		ref := status.ScaleTargetRef
		if len(validation.IsDNS1123Subdomain(ref.Name)) > 0 {
			live.Evidence = reportv1alpha1.AutoscaleLiveScalerInvalid
			continue
		}
		if !((ref.APIVersion == "ome.io/v1beta1" && ref.Kind == "InferenceReplica") ||
			(ref.APIVersion == "apps/v1" && ref.Kind == "Deployment")) {
			live.Evidence = reportv1alpha1.AutoscaleLiveScalerUnsupported
			continue
		}
		if component.Target.State != reportv1alpha1.AutoscaleTargetReported ||
			component.Target.Name != ref.Name || component.Target.Namespace != parent.Namespace || component.Target.APIVersion != ref.APIVersion || string(component.Target.Kind) != ref.Kind {
			live.Evidence = reportv1alpha1.AutoscaleLiveScalerInvalid
			continue
		}
		ownerKind, ownerName, ownerUID := "InferenceService", parent.Name, parent.UID
		switch {
		case ref.APIVersion == "ome.io/v1beta1" && ref.Kind == "InferenceReplica":
			if err := ctx.Err(); err != nil {
				return reportv1alpha1.AutoscaleStatusReport{}, err
			}
			ir, err := reader.GetInferenceReplica(ctx, parent.Namespace, ref.Name, metav1.GetOptions{})
			if err := ctx.Err(); err != nil {
				return reportv1alpha1.AutoscaleStatusReport{}, err
			}
			source := reportv1alpha1.AutoscaleSourceReference{Kind: reportv1alpha1.AutoscaleSourceInferenceReplica, Namespace: parent.Namespace, Name: ref.Name, CollectedAt: clock.Now().UTC()}
			if err != nil {
				live.Evidence = scalerReadEvidence(err)
				source.Evidence = reportv1alpha1.EvidenceUnavailable
				result.Sources = append(result.Sources, source)
				continue
			}
			if !validSelectedIR(ir, parent, ome.ComponentType(component.Type), ref.Name) {
				live.Evidence = reportv1alpha1.AutoscaleLiveScalerInvalid
				source.Evidence = reportv1alpha1.EvidenceUnavailable
				result.Sources = append(result.Sources, source)
				continue
			}
			source.UID, source.Generation, source.Evidence = string(ir.UID), ir.Generation, reportv1alpha1.EvidenceObserved
			result.Sources = append(result.Sources, source)
			if ir.DeletionTimestamp != nil {
				live.Evidence = reportv1alpha1.AutoscaleLiveScalerDeleting
				continue
			}
			ownerKind, ownerName, ownerUID = "InferenceReplica", ir.Name, ir.UID
		case ref.APIVersion == "apps/v1" && ref.Kind == "Deployment":
			// Parent status selects the Raw Deployment; owner proof comes from
			// the scaler's controller reference to this exact parent UID.
		default:
			live.Evidence = reportv1alpha1.AutoscaleLiveScalerUnsupported
			continue
		}
		if err := ctx.Err(); err != nil {
			return reportv1alpha1.AutoscaleStatusReport{}, err
		}
		var source reportv1alpha1.AutoscaleSourceReference
		source.Namespace, source.CollectedAt = parent.Namespace, clock.Now().UTC()
		if component.Class == reportv1alpha1.AutoscaleClassHPA {
			source.Kind, source.Name = reportv1alpha1.AutoscaleSourceHPA, ref.Name
			hpa, err := reader.GetHPA(ctx, parent.Namespace, ref.Name, metav1.GetOptions{})
			if err := ctx.Err(); err != nil {
				return reportv1alpha1.AutoscaleStatusReport{}, err
			}
			if err != nil {
				live.Evidence = scalerReadEvidence(err)
			} else if !validScalerHPA(hpa, parent.Namespace, ref, ownerKind, ownerName, ownerUID) {
				live.Evidence = reportv1alpha1.AutoscaleLiveScalerInvalid
			} else {
				source.UID, source.Generation, source.Evidence = string(hpa.UID), hpa.Generation, reportv1alpha1.EvidenceObserved
				if hpa.DeletionTimestamp != nil {
					live.Evidence = reportv1alpha1.AutoscaleLiveScalerDeleting
				} else if hpa.Status.ObservedGeneration != nil && *hpa.Status.ObservedGeneration != hpa.Generation {
					live.Evidence = reportv1alpha1.AutoscaleLiveScalerStale
				} else {
					conditions, valid := scalerHPAConditions(hpa.Status.Conditions)
					if !valid {
						live.Evidence = reportv1alpha1.AutoscaleLiveScalerInvalid
					} else {
						live.Evidence = reportv1alpha1.AutoscaleLiveScalerReported
						if hpa.Status.ObservedGeneration != nil {
							live.GenerationState = reportv1alpha1.AutoscaleScalerGenerationMatched
						}
						current, desired := hpa.Status.CurrentReplicas, hpa.Status.DesiredReplicas
						live.CurrentReplicas, live.DesiredReplicas = &current, &desired
						live.Conditions = conditions
					}
				}
			}
		} else {
			source.Kind, source.Name = reportv1alpha1.AutoscaleSourceScaledObject, isvcutils.GetScaledObjectName(ref.Name)
			so, err := reader.GetScaledObject(ctx, parent.Namespace, source.Name, metav1.GetOptions{})
			if err := ctx.Err(); err != nil {
				return reportv1alpha1.AutoscaleStatusReport{}, err
			}
			if err != nil {
				live.Evidence = scalerReadEvidence(err)
			} else if !validScalerSO(so, parent.Namespace, source.Name, ref, ownerKind, ownerName, ownerUID) {
				live.Evidence = reportv1alpha1.AutoscaleLiveScalerInvalid
			} else {
				source.UID, source.Generation, source.Evidence = string(so.UID), so.Generation, reportv1alpha1.EvidenceObserved
				if so.DeletionTimestamp != nil {
					live.Evidence = reportv1alpha1.AutoscaleLiveScalerDeleting
				} else {
					conditions, valid := scalerSOConditions(so.Status.Conditions)
					if !valid {
						live.Evidence = reportv1alpha1.AutoscaleLiveScalerInvalid
					} else {
						live.Evidence = reportv1alpha1.AutoscaleLiveScalerReported
						live.Conditions = conditions
					}
				}
			}
		}
		if source.Evidence == "" {
			source.Evidence = reportv1alpha1.EvidenceUnavailable
		}
		result.Sources = append(result.Sources, source)
	}
	for _, component := range result.Content.Components {
		if component.ManagedBy == reportv1alpha1.AutoscaleManagedByOME &&
			(component.Class == reportv1alpha1.AutoscaleClassHPA || component.Class == reportv1alpha1.AutoscaleClassKEDA) &&
			component.LiveScaler != nil && component.LiveScaler.Evidence != reportv1alpha1.AutoscaleLiveScalerReported {
			result.Warnings = append(result.Warnings, reportv1alpha1.AutoscaleWarning{Code: reportv1alpha1.AutoscaleWarningPartialData})
			break
		}
	}
	return result.Canonical(), nil
}

func scalerReadEvidence(err error) reportv1alpha1.AutoscaleLiveScalerEvidence {
	switch {
	case apierrors.IsForbidden(err):
		return reportv1alpha1.AutoscaleLiveScalerForbidden
	case apierrors.IsNotFound(err):
		return reportv1alpha1.AutoscaleLiveScalerNotFound
	default:
		return reportv1alpha1.AutoscaleLiveScalerUnavailable
	}
}

func scalerOwnerMatches(refs []metav1.OwnerReference, kind, name string, uid types.UID) bool {
	if len(refs) == 0 || len(refs) > 16 {
		return false
	}
	controllers := 0
	for _, ref := range refs {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if ref.APIVersion != "ome.io/v1beta1" || ref.Kind != kind || ref.Name != name || ref.UID != uid {
			return false
		}
		controllers++
	}
	return controllers == 1
}

func validScalerHPA(hpa *autoscalingv2.HorizontalPodAutoscaler, namespace string, target *ome.ScaleTargetRef, ownerKind, ownerName string, ownerUID types.UID) bool {
	return hpa != nil && hpa.APIVersion == "autoscaling/v2" && hpa.Kind == "HorizontalPodAutoscaler" &&
		hpa.Namespace == namespace && hpa.Name == target.Name && safeScaleIdentity(string(hpa.UID)) && hpa.Generation > 0 &&
		hpa.Spec.ScaleTargetRef.APIVersion == target.APIVersion && hpa.Spec.ScaleTargetRef.Kind == target.Kind && hpa.Spec.ScaleTargetRef.Name == target.Name &&
		scalerOwnerMatches(hpa.OwnerReferences, ownerKind, ownerName, ownerUID) && hpa.Status.CurrentReplicas >= 0 && hpa.Status.DesiredReplicas >= 0
}

func validScalerSO(so *kedav1.ScaledObject, namespace, name string, target *ome.ScaleTargetRef, ownerKind, ownerName string, ownerUID types.UID) bool {
	return so != nil && so.APIVersion == "keda.sh/v1alpha1" && so.Kind == "ScaledObject" &&
		so.Namespace == namespace && so.Name == name && safeScaleIdentity(string(so.UID)) && so.Generation > 0 && so.Spec.ScaleTargetRef != nil &&
		so.Spec.ScaleTargetRef.APIVersion == target.APIVersion && so.Spec.ScaleTargetRef.Kind == target.Kind && so.Spec.ScaleTargetRef.Name == target.Name &&
		scalerOwnerMatches(so.OwnerReferences, ownerKind, ownerName, ownerUID)
}

func scalerHPAConditions(conditions []autoscalingv2.HorizontalPodAutoscalerCondition) ([]reportv1alpha1.AutoscaleScalerCondition, bool) {
	result := []reportv1alpha1.AutoscaleScalerCondition{}
	for _, condition := range conditions {
		switch condition.Type {
		case autoscalingv2.AbleToScale, autoscalingv2.ScalingActive, autoscalingv2.ScalingLimited:
			if !validScalerConditionStatus(metav1.ConditionStatus(condition.Status)) {
				return nil, false
			}
			var valid bool
			result, valid = appendScalerCondition(result, reportv1alpha1.AutoscaleConditionType(condition.Type), reportv1alpha1.AutoscaleConditionStatus(condition.Status))
			if !valid {
				return nil, false
			}
		}
	}
	return result, true
}

func scalerSOConditions(conditions kedav1.Conditions) ([]reportv1alpha1.AutoscaleScalerCondition, bool) {
	result := []reportv1alpha1.AutoscaleScalerCondition{}
	for _, condition := range conditions {
		switch condition.Type {
		case kedav1.ConditionReady, kedav1.ConditionActive, kedav1.ConditionFallback, kedav1.ConditionPaused:
			if !validScalerConditionStatus(condition.Status) {
				return nil, false
			}
			var valid bool
			result, valid = appendScalerCondition(result, reportv1alpha1.AutoscaleConditionType(condition.Type), reportv1alpha1.AutoscaleConditionStatus(condition.Status))
			if !valid {
				return nil, false
			}
		}
	}
	return result, true
}

func appendScalerCondition(items []reportv1alpha1.AutoscaleScalerCondition, kind reportv1alpha1.AutoscaleConditionType, status reportv1alpha1.AutoscaleConditionStatus) ([]reportv1alpha1.AutoscaleScalerCondition, bool) {
	for _, item := range items {
		if item.Type == kind {
			return items, item.Status == status
		}
	}
	return append(items, reportv1alpha1.AutoscaleScalerCondition{Type: kind, Status: status}), true
}

func validScalerConditionStatus(status metav1.ConditionStatus) bool {
	return status == metav1.ConditionTrue || status == metav1.ConditionFalse || status == metav1.ConditionUnknown
}
