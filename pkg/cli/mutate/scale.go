package mutate

import (
	"encoding/json"
	"errors"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	omevalidation "sigs.k8s.io/ome/pkg/validation"
)

var ErrScaleOwnership = errors.New("manual scale is transient and requires --override-autoscaler --yes for every ownership class")
var ErrScaleBounds = errors.New("manual scale replicas must be positive and within verified bounds and partitions")

// ScalePlan is an immutable one-shot /scale CAS plan, not a rollout plan.
type ScalePlan struct {
	patch   []byte
	target  reportv1alpha1.ActionTarget
	details reportv1alpha1.ScaleActionDetails
}

func (ScalePlan) MarshalJSON() ([]byte, error)          { return nil, ErrScaleEvidence }
func (ScalePlan) MarshalYAML() (any, error)             { return nil, ErrScaleEvidence }
func (ScalePlan) String() string                        { return "<mutate.ScalePlan redacted>" }
func (ScalePlan) GoString() string                      { return "<mutate.ScalePlan redacted>" }
func (p ScalePlan) Patch() []byte                       { return append([]byte(nil), p.patch...) }
func (p ScalePlan) Target() reportv1alpha1.ActionTarget { return p.target }
func (p ScalePlan) Details() reportv1alpha1.ScaleActionDetails {
	return *(reportv1alpha1.ActionResult{Scale: &p.details}).Canonical().Scale
}

func PrepareScale(parent *v1beta1.InferenceService, state *effective.RuntimeState, evidence ScaleEvidence, component v1beta1.ComponentType, replicas int32, override, yes bool, clock reportv1alpha1.Clock) (ScalePlan, error) {
	if err := ValidateTarget(parent); err != nil {
		return ScalePlan{}, err
	}
	if evidence.replica == nil || !evidence.source.MatchesParent(parent) || evidence.parent.UID != parent.UID || evidence.parent.ResourceVersion != parent.ResourceVersion {
		return ScalePlan{}, ErrScaleEvidence
	}
	components, err := RequireNativeRuntime(parent, state)
	if err != nil {
		return ScalePlan{}, err
	}
	if !containsScaleComponent(components, string(component)) || evidence.replica.Spec.Component != component {
		return ScalePlan{}, ErrScaleEvidence
	}
	if !override || !yes {
		return ScalePlan{}, ErrScaleOwnership
	}
	summary := evidence.source.Summary()
	if replicas < 1 || replicas < summary.Minimum || replicas > summary.Maximum || !scalePartitionAllows(evidence.replica, replicas) {
		return ScalePlan{}, ErrScaleBounds
	}
	for _, key := range []string{constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation} {
		if _, exists := parent.Annotations[key]; exists {
			return ScalePlan{}, ErrPending
		}
	}
	if err := requireScaleStable(parent, evidence, component, clock); err != nil {
		return ScalePlan{}, err
	}
	details := reportv1alpha1.ScaleActionDetails{Component: string(component), Subresource: "/scale", Field: "spec.replicas",
		PriorReplicas: *evidence.replica.Spec.Replicas, RequestedReplicas: replicas, MinReplicas: summary.Minimum, MaxReplicas: summary.Maximum,
		Class: string(summary.Class), ManagedBy: string(summary.ManagedBy), SpecSource: reportv1alpha1.ScaleSpecSource(summary.SpecSource), Override: true, Transient: true,
		Parent: scaleSourceIdentity(summary.Parent), Sources: []reportv1alpha1.ScaleSourceIdentity{}, ReplicaGeneration: evidence.replica.Generation, ParentGenerationStamp: evidence.parentStamp(), ParentFreshness: "Unverifiable",
		Instances: reportv1alpha1.ScaleInstanceCounts{Replicas: evidence.replica.Status.Replicas, Ready: evidence.replica.Status.ReadyReplicas, Serving: evidence.replica.Status.ServingReplicas, Available: evidence.replica.Status.AvailableReplicas},
		Issues:    []string{}, Warnings: []string{"AlphaAction", "TransientReplicaRequest", "AutoscalerCanOverwrite", "ScaleDownCanDrain", "NotMultiObjectTransaction"}}
	if details.Instances.Replicas != details.PriorReplicas {
		details.Issues = append(details.Issues, "ReportedDesiredCountDiscrepancy")
	}
	for _, source := range summary.Sources {
		if !SafeScalar(source.Kind) || (source.Namespace != "" && !SafeScalar(source.Namespace)) || !SafeScalar(source.Name) || !SafeScalar(source.UID) {
			return ScalePlan{}, ErrUnsafeValue
		}
		details.Sources = append(details.Sources, scaleSourceIdentity(source))
	}
	for component, replica := range evidence.replicas {
		if component == evidence.replica.Spec.Component {
			continue
		}
		details.Sources = append(details.Sources, reportv1alpha1.ScaleSourceIdentity{Kind: "InferenceReplica", Namespace: replica.Namespace, Name: replica.Name, UID: string(replica.UID), Generation: replica.Generation})
	}
	if len(evidence.replicas) > 1 {
		details.Warnings = append(details.Warnings, "ConditionalPinnedIRReads")
	}
	patch, err := json.Marshal([]patchOperation{{Op: "test", Path: "/metadata/uid", Value: string(evidence.replica.UID)}, {Op: "test", Path: "/metadata/resourceVersion", Value: evidence.replica.ResourceVersion}, {Op: "replace", Path: "/spec/replicas", Value: replicas}})
	if err != nil {
		return ScalePlan{}, ErrScaleEvidence
	}
	return ScalePlan{patch: patch, target: reportv1alpha1.ActionTarget{Kind: "InferenceReplica", Namespace: evidence.replica.Namespace, Name: evidence.replica.Name, UID: string(evidence.replica.UID), ResourceVersion: evidence.replica.ResourceVersion}, details: details}, nil
}

func scaleSourceIdentity(source effective.ManualScaleIdentity) reportv1alpha1.ScaleSourceIdentity {
	return reportv1alpha1.ScaleSourceIdentity{Kind: source.Kind, Namespace: source.Namespace, Name: source.Name, UID: source.UID, Generation: source.Generation}
}

func containsScaleComponent(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func scalePartitionAllows(replica *v1beta1.InferenceReplica, replicas int32) bool {
	var partitions []*int32
	if replica.Spec.Pacing != nil {
		partitions = append(partitions, replica.Spec.Pacing.Partition)
	}
	if lifecycle := replica.Spec.Lifecycle; lifecycle != nil {
		if strategy := lifecycle.UpdateStrategy; strategy != nil {
			switch strategy.Type {
			case "", v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategyInPlaceOnly:
			default:
				return false
			}
		}
		if omevalidation.ValidateLifecycle(&v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Lifecycle: lifecycle}}}) != nil {
			return false
		}
		if lifecycle.UpdateStrategy != nil && lifecycle.UpdateStrategy.RollingUpdate != nil {
			partitions = append(partitions, lifecycle.UpdateStrategy.RollingUpdate.Partition)
		}
	}
	for _, partition := range partitions {
		if partition != nil && (*partition < 0 || workload.HeldByPartition(&workload.RollingUpdate{Partition: partition}, replicas)) {
			return false
		}
	}
	return true
}

func requireScaleStable(parent *v1beta1.InferenceService, evidence ScaleEvidence, component v1beta1.ComponentType, clock reportv1alpha1.Clock) error {
	replica := evidence.replica
	projection, err := rolloutprojection.Project(parent, clock)
	if err != nil {
		return ErrStale
	}
	for _, issue := range projection.Content.Issues {
		if issue.Component != "" && string(issue.Component) != string(component) {
			continue
		}
		// The exact-current selected IR supplies independent lifecycle evidence;
		// a missing parent mirror is not an acknowledgment in that domain.
		if issue.Code == reportv1alpha1.RolloutIssueComponentStatusMissing && issue.Group == nil && string(issue.Component) == string(component) &&
			(parent.Status.Rollout == nil || parent.Status.Rollout.ActiveRun == nil) && replica.Status.UpdateRevision != "" && replica.Status.CurrentRevision == replica.Status.UpdateRevision {
			continue
		}
		if issue.Code != reportv1alpha1.RolloutIssueEpochUnverifiable && issue.Code != reportv1alpha1.RolloutIssueAnalysisInconclusive {
			return ErrStale
		}
	}
	for _, observed := range projection.Content.Components {
		if string(observed.Type) == string(component) && observed.Phase != reportv1alpha1.RolloutPhaseStable && observed.Phase != reportv1alpha1.RolloutPhaseUnknown {
			return ErrScaleWork
		}
	}
	for _, group := range projection.Content.Groups {
		selected := false
		for _, member := range group.Components {
			if string(member) == string(component) {
				selected = true
			}
		}
		if selected && group.Phase != reportv1alpha1.RolloutPhaseStable {
			return ErrScaleWork
		}
	}
	if parent.Status.Rollout != nil && parent.Status.Rollout.ActiveRun != nil {
		_, err := pinnedWorkActive(parent, evidence.pinnedSources(), clock)
		if err != nil {
			return err
		}
	}
	return nil
}

// ValidateResponse binds only API acceptance, never controller convergence.
func (p ScalePlan) ValidateResponse(scale *autoscalingv1.Scale) error {
	if len(p.patch) == 0 || scale == nil || (scale.APIVersion != "" && scale.APIVersion != "autoscaling/v1") || (scale.Kind != "" && scale.Kind != "Scale") ||
		scale.Name != p.target.Name || scale.Namespace != p.target.Namespace || string(scale.UID) != p.target.UID || !SafeScalar(scale.ResourceVersion) || scale.Spec.Replicas != p.details.RequestedReplicas {
		return ErrScaleEvidence
	}
	return nil
}
