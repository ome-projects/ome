package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func readyPolicyPtr(p v1beta1.InstanceReadyPolicy) *v1beta1.InstanceReadyPolicy {
	return &p
}

// readyPolicyISVC builds a minimal ISVC that passes every unrelated
// validator: lowercase name, an explicit runtime pin with autoSync=false
// (skips live runtime resolution), and a mutating callback for the
// per-case shape.
func readyPolicyISVC(mutate func(*v1beta1.InferenceService)) *v1beta1.InferenceService {
	autoSync := false
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime", AutoSync: &autoSync},
		},
	}
	if mutate != nil {
		mutate(isvc)
	}
	return isvc
}

func multiPodEngine(size int, policy *v1beta1.InstanceReadyPolicy) *v1beta1.EngineSpec {
	runner := &v1beta1.RunnerSpec{Container: v1.Container{Image: "runtime-image:v1"}}
	engine := &v1beta1.EngineSpec{
		Leader: &v1beta1.LeaderSpec{Runner: runner.DeepCopy()},
		Worker: &v1beta1.WorkerSpec{Size: &size, Runner: runner.DeepCopy()},
	}
	if policy != nil {
		engine.Lifecycle = &v1beta1.LifecycleSpec{ReadyPolicy: policy}
	}
	return engine
}

func singlePodEngine(policy *v1beta1.InstanceReadyPolicy) *v1beta1.EngineSpec {
	engine := &v1beta1.EngineSpec{
		Runner: &v1beta1.RunnerSpec{Container: v1.Container{Image: "runtime-image:v1"}},
	}
	if policy != nil {
		engine.Lifecycle = &v1beta1.LifecycleSpec{ReadyPolicy: policy}
	}
	return engine
}

func TestValidateCreate_MultiPodReadyPolicyNone(t *testing.T) {
	none := readyPolicyPtr(v1beta1.InstanceReadyPolicyNone)
	allPodReady := readyPolicyPtr(v1beta1.InstanceReadyPolicyAllPodReady)

	tests := []struct {
		name    string
		isvc    *v1beta1.InferenceService
		wantErr bool
		errSub  string
	}{
		{
			name: "multi-pod engine with None, top-level OMENative annotation - rejected",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			wantErr: true,
			errSub:  "engine.lifecycle.readyPolicy",
		},
		{
			name: "multi-pod engine with None, no annotation (shape heuristic) - rejected",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			wantErr: true,
			errSub:  "per-pod readiness reporting is not yet supported",
		},
		{
			name: "multi-pod engine with None, spec.deploymentMode OMENative - rejected",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				mode := constants.OMENative
				isvc.Spec.DeploymentMode = &mode
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			wantErr: true,
			errSub:  "engine.lifecycle.readyPolicy",
		},
		{
			name: "multi-pod engine with AllPodReady - accepted",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
				isvc.Spec.Engine = multiPodEngine(2, allPodReady)
			}),
		},
		{
			name: "multi-pod engine with no lifecycle block - accepted",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
				isvc.Spec.Engine = multiPodEngine(2, nil)
			}),
		},
		{
			name: "single-pod engine with None - accepted",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
				isvc.Spec.Engine = singlePodEngine(none)
			}),
		},
		{
			name: "multi-pod engine with None but per-component RawDeployment annotation - accepted",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				engine := multiPodEngine(2, none)
				engine.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
				isvc.Spec.Engine = engine
			}),
		},
		{
			name: "multi-pod decoder with None, per-component OMENative annotations - rejected",
			isvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				engine := singlePodEngine(nil)
				engine.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
				isvc.Spec.Engine = engine
				runner := &v1beta1.RunnerSpec{Container: v1.Container{Image: "runtime-image:v1"}}
				size := 2
				isvc.Spec.Decoder = &v1beta1.DecoderSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
						Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
						Lifecycle:   &v1beta1.LifecycleSpec{ReadyPolicy: none},
					},
					Leader: &v1beta1.LeaderSpec{Runner: runner.DeepCopy()},
					Worker: &v1beta1.WorkerSpec{Size: &size, Runner: runner.DeepCopy()},
				}
			}),
			wantErr: true,
			errSub:  "decoder.lifecycle.readyPolicy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			_, err := v.ValidateCreate(context.Background(), tt.isvc)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSub)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateUpdate_MultiPodReadyPolicyNoneRatchet(t *testing.T) {
	none := readyPolicyPtr(v1beta1.InstanceReadyPolicyNone)
	allPodReady := readyPolicyPtr(v1beta1.InstanceReadyPolicyAllPodReady)
	omeNative := func(isvc *v1beta1.InferenceService) {
		if isvc.Annotations == nil {
			isvc.Annotations = map[string]string{}
		}
		isvc.Annotations[constants.DeploymentMode] = string(constants.OMENative)
	}

	tests := []struct {
		name    string
		oldIsvc *v1beta1.InferenceService
		newIsvc *v1beta1.InferenceService
		wantErr bool
		errSub  string
	}{
		{
			name: "stored multi-pod None unchanged, unrelated update - tolerated",
			oldIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			newIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Labels = map[string]string{"team": "serving"}
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
		},
		{
			name: "stored multi-pod None, worker size change - tolerated",
			oldIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			newIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(3, none)
			}),
		},
		{
			name: "update introduces None on multi-pod engine - rejected",
			oldIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, allPodReady)
			}),
			newIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			wantErr: true,
			errSub:  "engine.lifecycle.readyPolicy",
		},
		{
			name: "update sets None where lifecycle was absent - rejected",
			oldIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, nil)
			}),
			newIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			wantErr: true,
			errSub:  "per-pod readiness reporting is not yet supported",
		},
		{
			name: "shape becomes multi-pod while retaining None - rejected",
			oldIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = singlePodEngine(none)
			}),
			newIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			wantErr: true,
			errSub:  "engine.lifecycle.readyPolicy",
		},
		{
			name: "update replaces stored None with AllPodReady - accepted",
			oldIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, none)
			}),
			newIsvc: readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
				omeNative(isvc)
				isvc.Spec.Engine = multiPodEngine(2, allPodReady)
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &InferenceServiceValidator{}
			_, err := v.ValidateUpdate(context.Background(), tt.oldIsvc, tt.newIsvc)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSub)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestMultiPodReadyPolicyNoneComponents_ReadsEveryDeclaredRole locks the
// walk over the role table: every declared role is read in the order of
// v1beta1.Roles, and only a role with a declared Leader+Worker pair is
// named. The encoder and the router declare none, so None on them is
// admitted: at one pod per replica None and AllPodReady agree.
func TestMultiPodReadyPolicyNoneComponents_ReadsEveryDeclaredRole(t *testing.T) {
	none := readyPolicyPtr(v1beta1.InstanceReadyPolicyNone)
	runner := &v1beta1.RunnerSpec{Container: v1.Container{Image: "runtime-image:v1"}}
	lifecycleNone := v1beta1.ComponentExtensionSpec{Lifecycle: &v1beta1.LifecycleSpec{ReadyPolicy: none}}
	size := 2
	isvc := readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
		isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
		isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: lifecycleNone, Runner: runner.DeepCopy()}
		isvc.Spec.Encoder = &v1beta1.EncoderSpec{ComponentExtensionSpec: lifecycleNone, Runner: runner.DeepCopy()}
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{
			ComponentExtensionSpec: lifecycleNone,
			Leader:                 &v1beta1.LeaderSpec{Runner: runner.DeepCopy()},
			Worker:                 &v1beta1.WorkerSpec{Size: &size, Runner: runner.DeepCopy()},
		}
		isvc.Spec.Engine = multiPodEngine(2, none)
	})

	assert.Equal(t, []string{"engine", "decoder"}, multiPodReadyPolicyNoneComponents(isvc))
}
