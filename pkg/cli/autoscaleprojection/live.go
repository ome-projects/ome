package autoscaleprojection

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

var ErrLiveScaleInput = errors.New("invalid live autoscale status input")

// LiveScaleReader is restricted to exact IR and IR /scale GETs.
type LiveScaleReader interface {
	GetInferenceReplica(context.Context, string, string, metav1.GetOptions) (*ome.InferenceReplica, error)
	GetInferenceReplicaScale(context.Context, string, string, metav1.GetOptions) (*autoscalingv1.Scale, error)
}

// EnrichLiveScale adds optional count-only evidence for parent-selected IRs.
// It never discovers scalers or reads sibling objects.
func EnrichLiveScale(ctx context.Context, parent *ome.InferenceService, base reportv1alpha1.AutoscaleStatusReport, reader LiveScaleReader, clock reportv1alpha1.Clock) (reportv1alpha1.AutoscaleStatusReport, error) {
	if ctx == nil || parent == nil || reader == nil || parent.Namespace != base.Metadata.Namespace || parent.Name != base.Metadata.Name || parent.UID == "" {
		return reportv1alpha1.AutoscaleStatusReport{}, ErrLiveScaleInput
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	result := base.Canonical()
	for i := range result.Content.Components {
		component := &result.Content.Components[i]
		live := &reportv1alpha1.AutoscaleLiveScale{Evidence: reportv1alpha1.AutoscaleLiveScaleNotSelected, CountComparison: reportv1alpha1.AutoscaleLiveScaleUnknown}
		component.LiveScale = live
		status, found := parent.Status.Components[ome.ComponentType(component.Type)]
		if !found || status.ScaleTargetRef == nil {
			continue
		}
		ref := status.ScaleTargetRef
		if len(utilvalidation.IsDNS1123Subdomain(ref.Name)) != 0 || component.Target.State == reportv1alpha1.AutoscaleTargetInvalid {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleInvalid
			continue
		}
		if ref.APIVersion != "ome.io/v1beta1" || ref.Kind != "InferenceReplica" {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleUnsupported
			continue
		}
		if component.Target.State != reportv1alpha1.AutoscaleTargetReported || component.Target.Name != ref.Name || component.Target.Namespace != parent.Namespace || component.Target.Kind != reportv1alpha1.AutoscaleTargetInferenceReplica {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleInvalid
			continue
		}
		if err := ctx.Err(); err != nil {
			return reportv1alpha1.AutoscaleStatusReport{}, err
		}
		ir, err := reader.GetInferenceReplica(ctx, parent.Namespace, ref.Name, metav1.GetOptions{})
		if cancelErr := ctx.Err(); cancelErr != nil {
			return reportv1alpha1.AutoscaleStatusReport{}, cancelErr
		}
		irSource := reportv1alpha1.AutoscaleSourceReference{Kind: reportv1alpha1.AutoscaleSourceInferenceReplica,
			Namespace: parent.Namespace, Name: ref.Name, CollectedAt: clock.Now().UTC()}
		if err != nil {
			live.Evidence = unavailableLiveScaleEvidence(err)
			irSource.Evidence = reportv1alpha1.EvidenceUnavailable
			result.Sources = append(result.Sources, irSource)
			continue
		}
		if !validSelectedIR(ir, parent, ome.ComponentType(component.Type), ref.Name) {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleInvalid
			irSource.Evidence = reportv1alpha1.EvidenceUnavailable
			result.Sources = append(result.Sources, irSource)
			continue
		}
		irSource.UID, irSource.Generation = string(ir.UID), ir.Generation
		irSource.Evidence = reportv1alpha1.EvidenceObserved
		result.Sources = append(result.Sources, irSource)
		if ir.DeletionTimestamp != nil {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleDeleting
			continue
		}
		if ir.Status.ObservedGeneration != ir.Generation {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleStale
			continue
		}
		scale, err := reader.GetInferenceReplicaScale(ctx, parent.Namespace, ref.Name, metav1.GetOptions{})
		if cancelErr := ctx.Err(); cancelErr != nil {
			return reportv1alpha1.AutoscaleStatusReport{}, cancelErr
		}
		scaleSource := reportv1alpha1.AutoscaleSourceReference{Kind: reportv1alpha1.AutoscaleSourceInferenceReplicaScale,
			Namespace: parent.Namespace, Name: ref.Name, CollectedAt: clock.Now().UTC()}
		if err != nil {
			live.Evidence = unavailableLiveScaleEvidence(err)
			scaleSource.Evidence = reportv1alpha1.EvidenceUnavailable
			result.Sources = append(result.Sources, scaleSource)
			continue
		}
		if !validLiveScale(scale, parent.Namespace, ref.Name) {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleInvalid
			scaleSource.Evidence = reportv1alpha1.EvidenceUnavailable
			result.Sources = append(result.Sources, scaleSource)
			continue
		}
		scaleSource.UID = string(scale.UID)
		scaleSource.Evidence = reportv1alpha1.EvidenceObserved
		result.Sources = append(result.Sources, scaleSource)
		if scale.DeletionTimestamp != nil {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleDeleting
			continue
		}
		if scale.UID != ir.UID || scale.ResourceVersion != ir.ResourceVersion || scale.Spec.Replicas != *ir.Spec.Replicas || scale.Status.Replicas != ir.Status.Replicas {
			live.Evidence = reportv1alpha1.AutoscaleLiveScaleChanged
			continue
		}
		live.Evidence = reportv1alpha1.AutoscaleLiveScaleReported
		spec, current := scale.Spec.Replicas, scale.Status.Replicas
		live.SpecReplicas, live.CurrentReplicas = &spec, &current
		if component.Replicas.State == reportv1alpha1.AutoscaleReplicasReported && component.Replicas.DesiredReplicas != nil && component.Replicas.CurrentReplicas != nil {
			live.CountComparison = reportv1alpha1.AutoscaleLiveScaleEqual
			if *component.Replicas.DesiredReplicas != spec || *component.Replicas.CurrentReplicas != current {
				live.CountComparison = reportv1alpha1.AutoscaleLiveScaleDrift
			}
		}
	}
	for _, component := range result.Content.Components {
		if component.LiveScale != nil && (component.LiveScale.Evidence != reportv1alpha1.AutoscaleLiveScaleReported || component.LiveScale.CountComparison != reportv1alpha1.AutoscaleLiveScaleEqual) {
			result.Warnings = append(result.Warnings, reportv1alpha1.AutoscaleWarning{Code: reportv1alpha1.AutoscaleWarningPartialData})
			break
		}
	}
	return result.Canonical(), nil
}

func unavailableLiveScaleEvidence(err error) reportv1alpha1.AutoscaleLiveScaleEvidence {
	switch {
	case apierrors.IsForbidden(err):
		return reportv1alpha1.AutoscaleLiveScaleForbidden
	case apierrors.IsNotFound(err):
		return reportv1alpha1.AutoscaleLiveScaleNotFound
	default:
		return reportv1alpha1.AutoscaleLiveScaleUnavailable
	}
}

func validSelectedIR(ir *ome.InferenceReplica, parent *ome.InferenceService, component ome.ComponentType, name string) bool {
	if ir == nil || ir.Name != name || ir.Namespace != parent.Namespace || !safeScaleIdentity(string(ir.UID)) || !safeScaleIdentity(ir.ResourceVersion) || ir.Generation <= 0 ||
		ir.Kind != "" && ir.Kind != "InferenceReplica" || ir.APIVersion != "" && ir.APIVersion != "ome.io/v1beta1" ||
		ir.Spec.ParentRef.Name != parent.Name || ir.Spec.Component != component || ir.Spec.Replicas == nil || *ir.Spec.Replicas < 0 || ir.Status.Replicas < 0 ||
		ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] != strconv.FormatInt(parent.Generation, 10) ||
		len(ir.OwnerReferences) == 0 || len(ir.OwnerReferences) > 16 {
		return false
	}
	controllers := 0
	for _, owner := range ir.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.APIVersion != "ome.io/v1beta1" || owner.Kind != "InferenceService" || owner.Name != parent.Name || owner.UID != parent.UID {
			return false
		}
		controllers++
	}
	return controllers == 1
}

func validLiveScale(scale *autoscalingv1.Scale, namespace, name string) bool {
	return scale != nil && scale.APIVersion == "autoscaling/v1" && scale.Kind == "Scale" &&
		scale.Namespace == namespace && scale.Name == name && safeScaleIdentity(string(scale.UID)) && safeScaleIdentity(scale.ResourceVersion) &&
		scale.Spec.Replicas >= 0 && scale.Status.Replicas >= 0
}

func safeScaleIdentity(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsFunc(value, unicode.IsControl)
}
