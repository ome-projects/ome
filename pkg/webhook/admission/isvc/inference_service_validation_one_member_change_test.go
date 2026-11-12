package isvc

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

// groupedPDISVC is an OMENative prefill/decode pair under SurgeThenDrain in
// one rollingUpdate group, with a router beside it when withRouter is set
// and inside the group when routerInGroup is set as well.
func groupedPDISVC(withRouter, routerInGroup bool) *v1beta1.InferenceService {
	return readyPolicyISVC(func(isvc *v1beta1.InferenceService) {
		omeNative := map[string]string{constants.DeploymentMode: string(constants.OMENative)}
		runner := &v1beta1.RunnerSpec{Container: v1.Container{Image: "runtime-image:v1"}}
		surge := &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: v1beta1.UpdateStrategySurgeThenDrain}}
		isvc.Spec.Engine = &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeNative,
				MinReplicas: ptr.To(6), MaxReplicas: 6, Lifecycle: surge.DeepCopy()},
			Runner: runner.DeepCopy(),
		}
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeNative,
				MinReplicas: ptr.To(2), MaxReplicas: 2, Lifecycle: surge.DeepCopy()},
			Runner: runner.DeepCopy(),
		}
		members := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
		if withRouter {
			isvc.Spec.Router = &v1beta1.RouterSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeNative,
					MinReplicas: ptr.To(2), MaxReplicas: 2, Lifecycle: surge.DeepCopy()},
				Runner: runner.DeepCopy(),
			}
			if routerInGroup {
				members = append(members, v1beta1.RouterComponent)
			}
		}
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components:    members,
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
		}}}
	})
}

// runnerOf is the runner block of one member of the pair.
func runnerOf(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) *v1beta1.RunnerSpec {
	switch c {
	case v1beta1.EngineComponent:
		return isvc.Spec.Engine.Runner
	case v1beta1.DecoderComponent:
		return isvc.Spec.Decoder.Runner
	case v1beta1.RouterComponent:
		return isvc.Spec.Router.Runner
	}
	return nil
}

// podTemplateChanges is every kind of change an operator makes to one
// member's pod template, each a change that rolls that member.
var podTemplateChanges = map[string]func(isvc *v1beta1.InferenceService, c v1beta1.ComponentType){
	"cpu and memory resources": func(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) {
		runnerOf(isvc, c).Resources = v1.ResourceRequirements{Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("2"),
			v1.ResourceMemory: resource.MustParse("4Gi"),
		}}
	},
	"env": func(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) {
		runnerOf(isvc, c).Env = []v1.EnvVar{{Name: "CONFIG_VERSION", Value: "2"}}
	},
	"image": func(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) {
		runnerOf(isvc, c).Image = "runtime-image:v2"
	},
	"args": func(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) {
		runnerOf(isvc, c).Args = []string{"--fast"}
	},
	"pod-template label": func(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) {
		labels := map[string]string{"tier": "canary"}
		switch c {
		case v1beta1.EngineComponent:
			isvc.Spec.Engine.Labels = labels
		case v1beta1.DecoderComponent:
			isvc.Spec.Decoder.Labels = labels
		}
	},
	"volume": func(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) {
		volumes := []v1.Volume{{Name: "scratch"}}
		switch c {
		case v1beta1.EngineComponent:
			isvc.Spec.Engine.PodSpec.Volumes = volumes
		case v1beta1.DecoderComponent:
			isvc.Spec.Decoder.PodSpec.Volumes = volumes
		}
	},
}

// partialChangeWarning is the one warning an update that changes some
// members of a rollingUpdate group and not others carries.
func partialChangeWarning(t *testing.T, warnings []string) string {
	t.Helper()
	var found []string
	for _, w := range warnings {
		if strings.HasPrefix(w, "spec.rollout.groups[") && strings.Contains(w, "keep") {
			found = append(found, w)
		}
	}
	require.Len(t, found, 1, "warnings = %v", warnings)
	return found[0]
}

// A change to one member's pod template alone is admitted and warned about:
// the warning names the member that changes and the one that keeps its
// revision, for every kind of change, on the engine alone and on the
// decoder alone.
func TestValidateUpdate_OneMemberChangeIsAdmittedWithAWarning(t *testing.T) {
	want := map[v1beta1.ComponentType]string{
		v1beta1.EngineComponent:  "spec.rollout.groups[0]: only the engine changes; the decoder keeps its revision and rolls nothing (RollingUpdatePartialChange)",
		v1beta1.DecoderComponent: "spec.rollout.groups[0]: only the decoder changes; the engine keeps its revision and rolls nothing (RollingUpdatePartialChange)",
	}
	for _, member := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		for name, change := range podTemplateChanges {
			t.Run(string(member)+"/"+name, func(t *testing.T) {
				v := &InferenceServiceValidator{}
				old := groupedPDISVC(false, false)
				updated := groupedPDISVC(false, false)
				change(updated, member)
				warnings, err := v.ValidateUpdate(context.Background(), old, updated)
				require.NoError(t, err, "a %s change on the %s alone must be admitted", name, member)
				assert.Equal(t, want[member], partialChangeWarning(t, warnings))
			})
		}
	}
}

// The same change on both members is admitted with no such warning: every
// member of the group rolls.
func TestValidateUpdate_BothMembersChangedIsAdmittedSilently(t *testing.T) {
	v := &InferenceServiceValidator{}
	old := groupedPDISVC(false, false)
	updated := groupedPDISVC(false, false)
	podTemplateChanges["env"](updated, v1beta1.EngineComponent)
	podTemplateChanges["env"](updated, v1beta1.DecoderComponent)
	warnings, err := v.ValidateUpdate(context.Background(), old, updated)
	require.NoError(t, err)
	for _, w := range warnings {
		assert.NotContains(t, w, "keeps its revision", "warnings = %v", warnings)
	}
}

// With the router inside the group, an engine-only change names both the
// decoder and the router as the members that keep their revisions; a change
// to the engine and the decoder together names the router alone.
func TestValidateUpdate_OneMemberChangeNamesEveryMemberThatKeepsItsRevision(t *testing.T) {
	v := &InferenceServiceValidator{}
	old := groupedPDISVC(true, true)
	updated := groupedPDISVC(true, true)
	podTemplateChanges["env"](updated, v1beta1.EngineComponent)
	warnings, err := v.ValidateUpdate(context.Background(), old, updated)
	require.NoError(t, err)
	assert.Equal(t, "spec.rollout.groups[0]: only the engine changes; the decoder and the router keep their revisions and roll nothing (RollingUpdatePartialChange)",
		partialChangeWarning(t, warnings))

	updated = groupedPDISVC(true, true)
	podTemplateChanges["env"](updated, v1beta1.EngineComponent)
	podTemplateChanges["env"](updated, v1beta1.DecoderComponent)
	warnings, err = v.ValidateUpdate(context.Background(), old, updated)
	require.NoError(t, err)
	assert.Equal(t, "spec.rollout.groups[0]: only the engine and the decoder change; the router keeps its revision and rolls nothing (RollingUpdatePartialChange)",
		partialChangeWarning(t, warnings))
}

// A router outside the group changing alone is nobody's business in the
// group: admitted, no warning.
func TestValidateUpdate_UngroupedRouterChangeIsAdmittedSilently(t *testing.T) {
	v := &InferenceServiceValidator{}
	old := groupedPDISVC(true, false)
	updated := groupedPDISVC(true, false)
	podTemplateChanges["env"](updated, v1beta1.RouterComponent)
	warnings, err := v.ValidateUpdate(context.Background(), old, updated)
	require.NoError(t, err)
	for _, w := range warnings {
		assert.NotContains(t, w, "keeps its revision", "warnings = %v", warnings)
	}
}

// A one-member change that also moves spec.rollout.pairingProtocol is judged
// by the protocol rule, whatever the pod templates do: one rollingUpdate
// group over engine and decoder admits the protocol change, and a group over
// the engine alone refuses it with the protocol rule's own reason.
func TestValidateUpdate_OneMemberChangeWithProtocolChangeFollowsTheProtocolRule(t *testing.T) {
	v := &InferenceServiceValidator{}
	old := groupedPDISVC(false, false)
	old.Spec.Rollout.PairingProtocol = ptr.To("kv-v1")
	updated := groupedPDISVC(false, false)
	updated.Spec.Rollout.PairingProtocol = ptr.To("kv-v2")
	podTemplateChanges["env"](updated, v1beta1.EngineComponent)
	_, err := v.ValidateUpdate(context.Background(), old, updated)
	require.NoError(t, err, "a protocol change under one rollingUpdate group over both members is admitted")

	engineOnly := []v1beta1.ComponentType{v1beta1.EngineComponent}
	old.Spec.Rollout.Groups[0].Components = engineOnly
	updated.Spec.Rollout.Groups[0].Components = engineOnly
	_, err = v.ValidateUpdate(context.Background(), old, updated)
	require.Error(t, err)
	assert.Contains(t, err.Error(), validation.ReasonPairingProtocolChangeUncoordinated)
}
