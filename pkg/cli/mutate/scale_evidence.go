package mutate

import (
	"errors"
	"strconv"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

var ErrScaleEvidence = errors.New("manual scale requires complete current selected replica and Scale evidence")
var ErrScaleWork = errors.New("manual scale refused: selected lifecycle, migration or rollout work is active")

// ScaleEvidence can only be minted after full selected-replica and Scale
// inspection. It carries no externally settable safety booleans.
type ScaleEvidence struct {
	parent   *v1beta1.InferenceService
	replica  *v1beta1.InferenceReplica
	source   effective.ManualScaleSource
	replicas map[v1beta1.ComponentType]*v1beta1.InferenceReplica
}

func (ScaleEvidence) MarshalJSON() ([]byte, error) { return nil, ErrScaleEvidence }
func (ScaleEvidence) MarshalYAML() (any, error)    { return nil, ErrScaleEvidence }
func (ScaleEvidence) String() string               { return "<mutate.ScaleEvidence redacted>" }
func (ScaleEvidence) GoString() string             { return "<mutate.ScaleEvidence redacted>" }

// InspectScaleEvidence binds the exact Scale snapshot to a fully inspected
// current owned IR. Count differences are observations, not lifecycle proof.
func InspectScaleEvidence(parent *v1beta1.InferenceService, replica *v1beta1.InferenceReplica, scale *autoscalingv1.Scale, source effective.ManualScaleSource, clock reportv1alpha1.Clock) (ScaleEvidence, error) {
	if err := ValidateTarget(parent); err != nil {
		return ScaleEvidence{}, err
	}
	if !source.MatchesParent(parent) || source.ValidateReplica(replica) != nil || replica == nil || scale == nil {
		return ScaleEvidence{}, ErrScaleEvidence
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	row, err := inspectReplica(replica, parent, []string{string(replica.Spec.Component)}, clock.Now())
	if err != nil {
		return ScaleEvidence{}, err
	}
	if !row.complete || replica.Spec.Replicas == nil || *replica.Spec.Replicas < 1 || replica.Status.Replicas < 0 {
		return ScaleEvidence{}, ErrScaleEvidence
	}
	if (scale.APIVersion != "" && scale.APIVersion != "autoscaling/v1") || (scale.Kind != "" && scale.Kind != "Scale") ||
		scale.Name != replica.Name || scale.Namespace != replica.Namespace || scale.UID != replica.UID || scale.ResourceVersion != replica.ResourceVersion ||
		!SafeScalar(string(scale.UID)) || !SafeScalar(scale.ResourceVersion) || scale.Spec.Replicas < 0 || scale.Status.Replicas < 0 ||
		scale.Spec.Replicas != *replica.Spec.Replicas || scale.Status.Replicas != replica.Status.Replicas {
		return ScaleEvidence{}, ErrScaleEvidence
	}
	if row.active || scaleLifecycleWork(row.logicalReplica) {
		return ScaleEvidence{}, ErrScaleWork
	}
	return ScaleEvidence{parent: parent.DeepCopy(), replica: replica.DeepCopy(), source: source, replicas: map[v1beta1.ComponentType]*v1beta1.InferenceReplica{replica.Spec.Component: replica.DeepCopy()}}, nil
}

func scaleLifecycleWork(replica *v1beta1.InferenceReplica) bool {
	if replica.Spec.Pacing != nil && replica.Spec.Pacing.RollbackToRevision != nil {
		return true
	}
	for _, instance := range replica.Status.InstanceStatuses {
		if instance.Phase == v1beta1.OMENativeInstanceDeleting || instance.Operation != nil && instance.Operation.Type == v1beta1.InstanceOperationDelete {
			return true
		}
		// A transient phase without its operation is incomplete lifecycle
		// evidence for scaling, even though pause does not require that mirror.
		if instance.Operation == nil {
			switch instance.Phase {
			case v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating:
				return true
			}
		}
	}
	return false
}

func (e ScaleEvidence) parentStamp() int64 {
	if e.replica == nil {
		return 0
	}
	value, _ := strconv.ParseInt(e.replica.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey], 10, 64)
	return value
}
