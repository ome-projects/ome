package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// modeChangeRunID names the open run of every fixture here; a refusal must
// repeat it so the operator can find the run.
const modeChangeRunID = "run-0001"

// modeChangeReason ends every refusal of a Component leaving OMENative while
// a run is open on it.
const modeChangeReason = "DeploymentModeChangeUnderOpenRun"

// modeChangeISVC is a service whose engine runs under OMENative and rolls
// in a blueGreen group of its own. mutate shapes the rest.
func modeChangeISVC(mutate func(*v1beta1.InferenceService)) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
			Engine:  omeNativeComponentEngine(),
			Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{groupOf(v1beta1.EngineComponent)}}},
	}
	if mutate != nil {
		mutate(isvc)
	}
	return isvc
}

func groupOf(components ...v1beta1.ComponentType) v1beta1.RolloutGroup {
	return v1beta1.RolloutGroup{Components: components, BlueGreen: &v1beta1.GroupBlueGreen{}}
}

func omeNativeExtension() v1beta1.ComponentExtensionSpec {
	return v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)}}
}

func modeChangeRunner() *v1beta1.RunnerSpec {
	return &v1beta1.RunnerSpec{Container: corev1.Container{Image: "runtime-image:v1"}}
}

func omeNativeComponentEngine() *v1beta1.EngineSpec {
	return &v1beta1.EngineSpec{ComponentExtensionSpec: omeNativeExtension(), Runner: modeChangeRunner()}
}

func omeNativeComponentDecoder() *v1beta1.DecoderSpec {
	return &v1beta1.DecoderSpec{ComponentExtensionSpec: omeNativeExtension(), Runner: modeChangeRunner()}
}

func omeNativeComponentEncoder() *v1beta1.EncoderSpec {
	return &v1beta1.EncoderSpec{ComponentExtensionSpec: omeNativeExtension(), Runner: modeChangeRunner()}
}

func omeNativeComponentRouter() *v1beta1.RouterSpec {
	return &v1beta1.RouterSpec{ComponentExtensionSpec: omeNativeExtension(), Runner: modeChangeRunner()}
}

// gangEngine is an engine whose backend is OMENative by its shape alone: a
// leader and a worker, no annotation and no spec.deploymentMode.
func gangEngine() *v1beta1.EngineSpec {
	return &v1beta1.EngineSpec{Runner: modeChangeRunner(), Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}}
}

// openRunOn is the status of a service whose run is open on the given
// Components: the plan the run holds names them in one blueGreen group.
func openRunOn(components ...v1beta1.ComponentType) *v1beta1.RolloutStatus {
	now := metav1.Now()
	return &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID:    modeChangeRunID,
		OpenedAt: now,
		PinnedAt: now,
		Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
			Source: v1beta1.RolloutPlanSourceInline,
			Group:  groupOf(components...),
		}}},
	}}
}

// closedRun is the status of a service whose last run finished: no run is
// open, only the record of the closed one remains.
func closedRun() *v1beta1.RolloutStatus {
	return &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunCompleted}}
}

// setComponentMode moves every declared role to the given backend through
// its annotation and takes the service out of every rollout group, which
// the group rules require of a member that is no longer OMENative.
func setComponentMode(isvc *v1beta1.InferenceService, mode constants.DeploymentModeType) {
	for _, view := range v1beta1.ServiceRoles(&isvc.Spec).Declared() {
		if view.Extension.Annotations == nil {
			view.Extension.Annotations = map[string]string{}
		}
		view.Extension.Annotations[constants.DeploymentMode] = string(mode)
	}
	isvc.Spec.Rollout = nil
}

// TestInferenceService_ComponentCannotLeaveOMENativeWhileItsRunIsOpen: an
// engine of 4 Instances rolls to version 2 in a blueGreen group, so a run is
// open on it. The write that moves the engine to another backend is
// refused, whether the mode comes from the engine's annotation, from
// spec.deploymentMode or from a leader and worker shape the write removes,
// and the refusal names the Component, the run, the backend the write asked
// for and what the operator can do instead. A decoder or an encoder the run
// is open on is refused by its own name, not the engine's beside it.
func TestInferenceService_ComponentCannotLeaveOMENativeWhileItsRunIsOpen(t *testing.T) {
	tests := []struct {
		name  string
		old   *v1beta1.InferenceService
		leave func(*v1beta1.InferenceService)
		named string
	}{
		{
			name:  "engine annotation moves to RawDeployment",
			old:   modeChangeISVC(func(isvc *v1beta1.InferenceService) { isvc.Status.Rollout = openRunOn(v1beta1.EngineComponent) }),
			leave: func(isvc *v1beta1.InferenceService) { setComponentMode(isvc, constants.RawDeployment) },
			named: "engine",
		},
		{
			name: "spec.deploymentMode moves to RawDeployment",
			old: modeChangeISVC(func(isvc *v1beta1.InferenceService) {
				mode := constants.OMENative
				isvc.Spec.DeploymentMode = &mode
				isvc.Spec.Engine.Annotations = nil
				isvc.Status.Rollout = openRunOn(v1beta1.EngineComponent)
			}),
			leave: func(isvc *v1beta1.InferenceService) {
				mode := constants.RawDeployment
				isvc.Spec.DeploymentMode = &mode
				isvc.Spec.Rollout = nil
			},
			named: "engine",
		},
		{
			name: "the leader and worker pair that made the engine OMENative is removed",
			old: modeChangeISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine = gangEngine()
				isvc.Status.Rollout = openRunOn(v1beta1.EngineComponent)
			}),
			leave: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine.Leader = nil
				isvc.Spec.Engine.Worker = nil
				isvc.Spec.Rollout = nil
			},
			named: "engine",
		},
		{
			name: "the decoder the run is open on leaves with the engine",
			old: modeChangeISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Decoder = omeNativeComponentDecoder()
				isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{groupOf(v1beta1.DecoderComponent)}
				isvc.Status.Rollout = openRunOn(v1beta1.DecoderComponent)
			}),
			leave: func(isvc *v1beta1.InferenceService) { setComponentMode(isvc, constants.RawDeployment) },
			named: "decoder",
		},
		{
			name: "the encoder the run is open on leaves with the engine",
			old: modeChangeISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Encoder = omeNativeComponentEncoder()
				isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{groupOf(v1beta1.EncoderComponent)}
				isvc.Status.Rollout = openRunOn(v1beta1.EncoderComponent)
			}),
			leave: func(isvc *v1beta1.InferenceService) { setComponentMode(isvc, constants.RawDeployment) },
			named: "encoder",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated := tt.old.DeepCopy()
			tt.leave(updated)
			_, err := placementValidator(t).ValidateUpdate(context.Background(), tt.old, updated)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.named+" cannot leave OMENative while rollout run "+modeChangeRunID+" is open on it")
			assert.Contains(t, err.Error(), "as "+string(constants.RawDeployment))
			assert.Contains(t, err.Error(), "wait for the run to finish")
			assert.Contains(t, err.Error(), "("+modeChangeReason+")")
		})
	}
}

// TestInferenceService_ComponentLeavesOMENativeWhenNoRunIsOpen: the same
// write on a service whose last run finished, or that never had one, is
// admitted as it is today.
func TestInferenceService_ComponentLeavesOMENativeWhenNoRunIsOpen(t *testing.T) {
	tests := []struct {
		name   string
		status *v1beta1.RolloutStatus
	}{
		{name: "the last run finished", status: closedRun()},
		{name: "no run was ever open", status: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := modeChangeISVC(func(isvc *v1beta1.InferenceService) { isvc.Status.Rollout = tt.status })
			updated := old.DeepCopy()
			setComponentMode(updated, constants.RawDeployment)
			_, err := placementValidator(t).ValidateUpdate(context.Background(), old, updated)
			require.NoError(t, err)
		})
	}
}

// TestInferenceService_OtherWritesAreAdmittedWhileARunIsOpen: with a run
// open on the engine, a router the run does not name leaves OMENative, and
// an engine edit that keeps the backend lands.
func TestInferenceService_OtherWritesAreAdmittedWhileARunIsOpen(t *testing.T) {
	tests := []struct {
		name string
		old  *v1beta1.InferenceService
		edit func(*v1beta1.InferenceService)
	}{
		{
			name: "a router the run does not name leaves OMENative",
			old: modeChangeISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Router = omeNativeComponentRouter()
				isvc.Status.Rollout = openRunOn(v1beta1.EngineComponent)
			}),
			edit: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Router.Annotations[constants.DeploymentMode] = string(constants.RawDeployment)
			},
		},
		{
			name: "the engine keeps OMENative and changes its image",
			old:  modeChangeISVC(func(isvc *v1beta1.InferenceService) { isvc.Status.Rollout = openRunOn(v1beta1.EngineComponent) }),
			edit: func(isvc *v1beta1.InferenceService) { isvc.Spec.Engine.Runner.Image = "runtime-image:v2" },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated := tt.old.DeepCopy()
			tt.edit(updated)
			_, err := placementValidator(t).ValidateUpdate(context.Background(), tt.old, updated)
			require.NoError(t, err)
		})
	}
}
