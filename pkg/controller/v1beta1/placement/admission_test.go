package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// isvcWithInstances builds a derived ISVC declaring `component` with an empty
// Lifecycle summary. Used by the controller reconcile tests that seed a derived
// ISVC on a worker cluster; the per-instance data lives on the paired
// InferenceReplica (see irWithInstances), which the predicates read via
// ComponentIRStatus. The variadic arg exists only for call-site symmetry with
// irWithInstances; it does not populate the ISVC.
func isvcWithInstances(component v1beta1.ComponentType, _ ...bool) *v1beta1.InferenceService {
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

// admittedStatusMap builds the authoritative per-component IR status map the
// placement predicates now consume, mirroring what the reconcile assembles from
// the workload cluster's InferenceReplicas.
func admittedStatusMap(component v1beta1.ComponentType, admitted ...bool) map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(admitted))
	for i, a := range admitted {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Admitted: a}
	}
	return map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		component: {InstanceStatuses: insts},
	}
}

// irWithInstances builds an InferenceReplica named "svc-<component>" in "prod"
// (matching the derived ISVC the reconcile tests use) whose status carries the
// given admitted instances, for seeding on a worker fake client so the reconcile
// reads it via ComponentIRStatus.
func irWithInstances(component v1beta1.ComponentType, admitted ...bool) *v1beta1.InferenceReplica {
	insts := make([]v1beta1.OMENativeInstanceStatus, len(admitted))
	for i, a := range admitted {
		insts[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Admitted: a}
	}
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc-" + string(component)},
		Status:     v1beta1.InferenceReplicaStatus{InstanceStatuses: insts},
	}
}

// declareComponent adds a minimal spec block for component so AllComponentsAdmitted
// (which reads declared components off the spec) sees it as declared.
func declareComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) {
	switch component {
	case v1beta1.EngineComponent:
		isvc.Spec.Engine = &v1beta1.EngineSpec{}
	case v1beta1.DecoderComponent:
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	case v1beta1.RouterComponent:
		isvc.Spec.Router = &v1beta1.RouterSpec{}
	}
}

func TestAllComponentsAdmitted(t *testing.T) {
	engine := &v1beta1.InferenceService{}
	declareComponent(engine, v1beta1.EngineComponent)

	// single declared component, admitted -> true.
	assert.True(t, AllComponentsAdmitted(engine, admittedStatusMap(v1beta1.EngineComponent, false, true)))
	// single declared component, all gated -> false.
	assert.False(t, AllComponentsAdmitted(engine, admittedStatusMap(v1beta1.EngineComponent, false, false)))
	// no declared components -> never a winner.
	assert.False(t, AllComponentsAdmitted(&v1beta1.InferenceService{}, nil))

	// PD: engine admitted but decoder still gated -> NOT a winner.
	pd := &v1beta1.InferenceService{}
	declareComponent(pd, v1beta1.EngineComponent)
	pd.Spec.Decoder = &v1beta1.DecoderSpec{}
	mixed := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		v1beta1.EngineComponent:  {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: true}}},
		v1beta1.DecoderComponent: {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: false}}},
	}
	assert.False(t, AllComponentsAdmitted(pd, mixed), "engine admitted, decoder gated must not win")

	// PD: both admitted -> winner.
	both := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		v1beta1.EngineComponent:  {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: true}}},
		v1beta1.DecoderComponent: {InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Admitted: true}}},
	}
	assert.True(t, AllComponentsAdmitted(pd, both), "both components admitted must win")

	// declared component with no status entry yet -> false (no panic).
	missingStatus := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
	assert.False(t, AllComponentsAdmitted(missingStatus, nil))
}

func TestAnyInstanceAdmitted(t *testing.T) {
	// at least one admitted instance -> true.
	assert.True(t, AnyInstanceAdmitted(admittedStatusMap(v1beta1.EngineComponent, false, true)))
	// all gated -> false.
	assert.False(t, AnyInstanceAdmitted(admittedStatusMap(v1beta1.EngineComponent, false, false)))
	// no statuses -> false.
	assert.False(t, AnyInstanceAdmitted(nil))
	// component present but nil status -> false (no panic).
	assert.False(t, AnyInstanceAdmitted(map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{v1beta1.EngineComponent: nil}))
}
