package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// isvcWithPhases builds a derived ISVC declaring `component` with an empty
// Lifecycle summary. Used by the controller reconcile tests that seed a derived
// ISVC on a worker cluster; the per-instance data lives on the paired
// InferenceReplica (see irWithPhases), which the predicate reads via
// ComponentIRStatus. The variadic arg exists only for call-site symmetry with
// irWithPhases; it does not populate the ISVC.
func isvcWithPhases(component v1beta1.ComponentType, _ ...v1beta1.OMENativeInstancePhase) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				component: {Lifecycle: &v1beta1.LifecycleStatus{}},
			},
		},
	}
	declareComponent(isvc, component)
	return isvc
}

// phaseStatusMap builds the authoritative per-component IR status map the
// IsTerminallyFailed predicate now consumes.
func phaseStatusMap(component v1beta1.ComponentType, phases ...v1beta1.OMENativeInstancePhase) map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(phases))
	for i, p := range phases {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: p}
	}
	return map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		component: {InstanceStatuses: insts},
	}
}

// irWithPhases builds an InferenceReplica named "svc-<component>" in "prod"
// whose status carries instances at the given phases, for seeding on a worker
// fake client so the reconcile reads it via ComponentIRStatus.
func irWithPhases(component v1beta1.ComponentType, phases ...v1beta1.OMENativeInstancePhase) *v1beta1.InferenceReplica {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(phases))
	for i, p := range phases {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: p}
	}
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc-" + string(component)},
		Status:     v1beta1.InferenceReplicaStatus{InstanceStatuses: insts},
	}
}

func TestIsTerminallyFailed(t *testing.T) {
	assert.True(t, IsTerminallyFailed(&v1beta1.InferenceService{},
		phaseStatusMap(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceFailed)))
	assert.False(t, IsTerminallyFailed(&v1beta1.InferenceService{},
		phaseStatusMap(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceCreating)))
	assert.False(t, IsTerminallyFailed(&v1beta1.InferenceService{}, nil))

	// Non-OMENative failures surface via the model-status transition, with no
	// failed Instance phase. These must also be terminal so the placement does
	// not re-fan-out into the same failure forever.
	blockedLoad := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{
		ModelStatus: v1beta1.ModelStatus{TransitionStatus: v1beta1.BlockedByFailedLoad},
	}}
	assert.True(t, IsTerminallyFailed(blockedLoad, nil), "model-load failure is terminal")

	invalidSpec := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{
		ModelStatus: v1beta1.ModelStatus{TransitionStatus: v1beta1.InvalidSpec},
	}}
	assert.True(t, IsTerminallyFailed(invalidSpec, nil), "runtime-selection/spec failure is terminal")

	// In-progress model status is NOT terminal.
	inProgress := &v1beta1.InferenceService{Status: v1beta1.InferenceServiceStatus{
		ModelStatus: v1beta1.ModelStatus{TransitionStatus: v1beta1.InProgress},
	}}
	assert.False(t, IsTerminallyFailed(inProgress, nil))
}
