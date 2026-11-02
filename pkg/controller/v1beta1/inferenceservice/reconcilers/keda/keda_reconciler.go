package keda

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ownedscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// defaultMinReplicas is the minimum applied when the component spec does not
// declare MinReplicas > 0.
const defaultMinReplicas int32 = 1

// KEDAReconciler brings a Component's ScaledObject to the desired shape.
type KEDAReconciler = ownedscaler.Reconciler[*kedav1.ScaledObject]

// kind is the ScaledObject as an owned scaler. Its one rule of its own: a
// ScaledObject with no trigger is never created, because KEDA rejects it.
var kind = ownedscaler.Kind[*kedav1.ScaledObject]{
	Name:      "ScaledObject",
	Class:     constants.AutoscalerClassKEDA,
	Log:       logf.Log.WithName("KEDAReconciler"),
	New:       func() *kedav1.ScaledObject { return &kedav1.ScaledObject{} },
	Creatable: func(desired *kedav1.ScaledObject) bool { return len(desired.Spec.Triggers) > 0 },
	SpecEqual: func(desired, existing *kedav1.ScaledObject) bool {
		return equality.Semantic.DeepEqual(desired.Spec, existing.Spec)
	},
	CopySpec: func(from, to *kedav1.ScaledObject) { to.Spec = from.Spec },
}

// NewKEDAReconcilerForTarget builds a KEDAReconciler whose ScaledObject
// targets scaleTargetRef, a Deployment or an InferenceReplica. kedaSpec is the
// resolved KedaAutoscaler block; its Triggers, Advanced, PollingInterval,
// CooldownPeriod, IdleReplicaCount and Fallback go onto the ScaledObject spec
// as they are. A nil or empty block gives a ScaledObject with no trigger,
// which Reconcile never creates.
func NewKEDAReconcilerForTarget(
	client client.Client,
	componentMeta metav1.ObjectMeta,
	scaleTargetRef kedav1.ScaleTarget,
	kedaSpec *v1beta1.KedaAutoscaler,
	minReplicas int32,
	maxReplicas int32,
) *KEDAReconciler {
	return ownedscaler.New(client, kind, createScaledObject(componentMeta, scaleTargetRef, kedaSpec, minReplicas, maxReplicas))
}

// createScaledObject builds the desired ScaledObject from explicit inputs.
//
// scaleTargetRef is forwarded verbatim to the ScaledObject spec — callers
// supply `{apps/v1, Deployment, <name>}` for Raw Deployment dispatch and
// `{ome.io/v1beta1, InferenceReplica, <ir-name>}` for the IR-managed path.
//
// kedaSpec / minReplicas / maxReplicas come from the resolver or the Raw
// Deployment shim (createScaledObjectFromComponentExt). When kedaSpec is
// non-nil the Triggers, Advanced, PollingInterval, CooldownPeriod,
// IdleReplicaCount, and Fallback fields are all forwarded verbatim. Shared
// dispatch validates that at least one trigger is present; a nil or empty
// block gives a ScaledObject with no trigger, which Reconcile never creates.
func createScaledObject(
	componentMeta metav1.ObjectMeta,
	scaleTargetRef kedav1.ScaleTarget,
	kedaSpec *v1beta1.KedaAutoscaler,
	minReplicas int32,
	maxReplicas int32,
) *kedav1.ScaledObject {
	filteredLabels := make(map[string]string)
	for key, value := range componentMeta.Labels {
		// Exclude the label that could prevent opening the edit window through lens
		if key != "k8slens-edit-resource-version" {
			filteredLabels[key] = value
		}
	}

	spec := kedav1.ScaledObjectSpec{
		ScaleTargetRef:  &scaleTargetRef,
		MinReplicaCount: &minReplicas,
		MaxReplicaCount: &maxReplicas,
	}

	if kedaSpec != nil {
		spec.Triggers = kedaSpec.Triggers
		spec.Advanced = kedaSpec.Advanced
		spec.PollingInterval = kedaSpec.PollingInterval
		spec.CooldownPeriod = kedaSpec.CooldownPeriod
		spec.IdleReplicaCount = kedaSpec.IdleReplicaCount
		spec.Fallback = kedaSpec.Fallback
	}

	return &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:        utils.GetScaledObjectName(componentMeta.Name),
			Namespace:   componentMeta.Namespace,
			Labels:      filteredLabels,
			Annotations: componentMeta.Annotations,
			// The controller owner arrives on componentMeta.
			OwnerReferences: componentMeta.OwnerReferences,
		},
		Spec: spec,
	}
}

// createScaledObjectFromComponentExt is the Raw Deployment bridge from the
// (componentMeta, componentExt) caller to the parameterized createScaledObject.
// Reads Autoscaler.Keda when present (the canonical authoring location) and
// otherwise leaves Triggers empty. The lower-level Reconcile() gate
// defensively skips applying that invalid object.
func createScaledObjectFromComponentExt(
	componentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
) *kedav1.ScaledObject {
	scaleTargetRef := kedav1.ScaleTarget{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       componentMeta.Name,
	}
	minReplicas := calculateMinReplicas(componentExt)
	maxReplicas := calculateMaxReplicas(componentExt, minReplicas)

	var kedaSpec *v1beta1.KedaAutoscaler
	if componentExt.Autoscaler != nil && componentExt.Autoscaler.Keda != nil {
		kedaSpec = componentExt.Autoscaler.Keda
	}

	return createScaledObject(componentMeta, scaleTargetRef, kedaSpec, minReplicas, maxReplicas)
}

// calculateMinReplicas calculates the minimum replicas
func calculateMinReplicas(componentExt *v1beta1.ComponentExtensionSpec) int32 {
	if componentExt.MinReplicas != nil && *componentExt.MinReplicas > 0 {
		return int32(*componentExt.MinReplicas)
	}
	return defaultMinReplicas
}

// calculateMaxReplicas calculates the maximum replicas
func calculateMaxReplicas(componentExt *v1beta1.ComponentExtensionSpec, minReplicas int32) int32 {
	if componentExt.MaxReplicas > int(minReplicas) {
		return int32(componentExt.MaxReplicas)
	}
	return minReplicas
}

// ConfiguredHPAName returns advanced.horizontalPodAutoscalerConfig.name, the
// HPA name a ScaledObject asks KEDA to use, or "" when KEDA picks the name.
// KEDA deletes its HPA and creates a new one whenever this name changes.
func ConfiguredHPAName(advanced *kedav1.AdvancedConfig) string {
	if advanced == nil || advanced.HorizontalPodAutoscalerConfig == nil {
		return ""
	}
	return advanced.HorizontalPodAutoscalerConfig.Name
}

// kedaHPANamePrefix is the prefix KEDA puts before the ScaledObject name when
// the ScaledObject does not name its HPA.
const kedaHPANamePrefix = "keda-hpa-"

// ShortenedHPAName returns the HPA name to set on a ScaledObject that does not
// name its HPA, or "" when KEDA's own name fits. KEDA's admission webhook
// rejects a ScaledObject whose HPA name is longer than a DNS label, so a longer
// name is cut to a DNS label with a hash of the full name, keeping names unique.
func ShortenedHPAName(scaledObjectName string) string {
	name := kedaHPANamePrefix + scaledObjectName
	if len(name) <= validation.DNS1123LabelMaxLength {
		return ""
	}
	return constants.TruncateNameWithMaxLength(name, validation.DNS1123LabelMaxLength)
}

// WithHPAName returns kedaSpec with advanced.horizontalPodAutoscalerConfig.name
// set to name. It copies the block instead of changing the caller's object, and
// returns kedaSpec itself when the name already matches.
func WithHPAName(kedaSpec *v1beta1.KedaAutoscaler, name string) *v1beta1.KedaAutoscaler {
	if kedaSpec == nil || ConfiguredHPAName(kedaSpec.Advanced) == name {
		return kedaSpec
	}
	out := kedaSpec.DeepCopy()
	if out.Advanced == nil {
		out.Advanced = &kedav1.AdvancedConfig{}
	}
	if out.Advanced.HorizontalPodAutoscalerConfig == nil {
		out.Advanced.HorizontalPodAutoscalerConfig = &kedav1.HorizontalPodAutoscalerConfig{}
	}
	out.Advanced.HorizontalPodAutoscalerConfig.Name = name
	return out
}
