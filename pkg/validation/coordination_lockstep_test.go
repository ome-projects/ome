package validation

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// rollingUpdateGroupSpec attaches a 2-Component rollingUpdate group
// (engine + decoder) to the spec so the update-time rules see a group.
func rollingUpdateGroupSpec(spec *v1beta1.InferenceServiceSpec) *v1beta1.InferenceServiceSpec {
	spec.Rollout = &v1beta1.RolloutSpec{
		Groups: []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
		},
	}
	return spec
}

// memberParts returns the pieces of one member's spec a pod-template change
// lands on: its runner, its pod spec and its extension block.
func memberParts(spec *v1beta1.InferenceServiceSpec, c v1beta1.ComponentType) (*v1beta1.RunnerSpec, *v1beta1.PodSpec, *v1beta1.ComponentExtensionSpec) {
	switch c {
	case v1beta1.EngineComponent:
		return spec.Engine.Runner, &spec.Engine.PodSpec, &spec.Engine.ComponentExtensionSpec
	case v1beta1.DecoderComponent:
		return spec.Decoder.Runner, &spec.Decoder.PodSpec, &spec.Decoder.ComponentExtensionSpec
	}
	return nil, nil, nil
}

// oneMemberChanges is every kind of change an operator makes to one
// member's pod template: it re-renders that member and rolls it.
var oneMemberChanges = map[string]func(runner *v1beta1.RunnerSpec, pod *v1beta1.PodSpec, ext *v1beta1.ComponentExtensionSpec){
	"cpu and memory resources": func(runner *v1beta1.RunnerSpec, _ *v1beta1.PodSpec, _ *v1beta1.ComponentExtensionSpec) {
		runner.Resources = corev1.ResourceRequirements{Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		}}
	},
	"env": func(runner *v1beta1.RunnerSpec, _ *v1beta1.PodSpec, _ *v1beta1.ComponentExtensionSpec) {
		runner.Env = []corev1.EnvVar{{Name: "CONFIG_VERSION", Value: "2"}}
	},
	"image": func(runner *v1beta1.RunnerSpec, _ *v1beta1.PodSpec, _ *v1beta1.ComponentExtensionSpec) {
		runner.Image = "v2"
	},
	"args": func(runner *v1beta1.RunnerSpec, _ *v1beta1.PodSpec, _ *v1beta1.ComponentExtensionSpec) {
		runner.Args = []string{"--fast"}
	},
	"command": func(runner *v1beta1.RunnerSpec, _ *v1beta1.PodSpec, _ *v1beta1.ComponentExtensionSpec) {
		runner.Command = []string{"serve", "--fast"}
	},
	"volume": func(_ *v1beta1.RunnerSpec, pod *v1beta1.PodSpec, _ *v1beta1.ComponentExtensionSpec) {
		pod.Volumes = []corev1.Volume{{Name: "scratch"}}
	},
	"pod-template label": func(_ *v1beta1.RunnerSpec, _ *v1beta1.PodSpec, ext *v1beta1.ComponentExtensionSpec) {
		ext.Labels = map[string]string{"tier": "canary"}
	},
	"pod-template annotation": func(_ *v1beta1.RunnerSpec, _ *v1beta1.PodSpec, ext *v1beta1.ComponentExtensionSpec) {
		ann := make(map[string]string, len(ext.Annotations)+1)
		for k, v := range ext.Annotations {
			ann[k] = v
		}
		ann["rollout-trigger"] = "1"
		ext.Annotations = ann
	},
}

// oneMemberWarning is the warning a change to one member of the engine +
// decoder group carries: the changed member rolls, the other keeps its
// revision.
func oneMemberWarning(changed, kept v1beta1.ComponentType) string {
	return "spec.rollout.groups[0]: only the " + string(changed) + " changes; the " + string(kept) +
		" keeps its revision and rolls nothing (" + ReasonRollingUpdatePartialChange + ")"
}

func wantWarnings(t *testing.T, scenario string, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: warnings = %q, want %q", scenario, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: warning %d = %q, want %q", scenario, i, got[i], want[i])
		}
	}
}

// A change to one member's pod template alone, of any kind, on the engine
// alone or on the decoder alone, warns once: the warning names the member
// that changes and the member that keeps its revision.
func TestCoordinationUpdateWarnings_OneMemberChangeNamesWhoMovesAndWhoStays(t *testing.T) {
	pairs := map[v1beta1.ComponentType]v1beta1.ComponentType{
		v1beta1.EngineComponent:  v1beta1.DecoderComponent,
		v1beta1.DecoderComponent: v1beta1.EngineComponent,
	}
	for member, kept := range pairs {
		for name, mutate := range oneMemberChanges {
			oldSpec := omeNativePDSpecWithImages("v1", "v1")
			newSpec := rollingUpdateGroupSpec(omeNativePDSpecWithImages("v1", "v1"))
			mutate(memberParts(newSpec, member))
			wantWarnings(t, name+" change on the "+string(member)+" alone",
				CoordinationUpdateWarnings(oldSpec, newSpec), oneMemberWarning(member, kept))
		}
	}
}

// The same change applied to BOTH grouped Components warns about nothing:
// every member rolls.
func TestCoordinationUpdateWarnings_BothMembersChanged_NoWarning(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := rollingUpdateGroupSpec(omeNativePDSpecWithImages("v1", "v1"))
	newSpec.Engine.Runner.Env = []corev1.EnvVar{{Name: "CONFIG_VERSION", Value: "2"}}
	newSpec.Decoder.Runner.Env = []corev1.EnvVar{{Name: "CONFIG_VERSION", Value: "2"}}
	wantWarnings(t, "env change on both grouped Components", CoordinationUpdateWarnings(oldSpec, newSpec))
}

// With the router inside the group, an engine-only change names the decoder
// and the router as the members that keep their revisions; a change to the
// engine and the decoder together names the router alone.
func TestCoordinationUpdateWarnings_NamesEveryMemberThatKeepsItsRevision(t *testing.T) {
	threeWay := func(engineEnv, decoderEnv bool) *v1beta1.InferenceServiceSpec {
		spec := omeNativePDSpecWithImages("v1", "v1")
		spec.Router = &v1beta1.RouterSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: spec.Engine.Annotations},
			Runner:                 &v1beta1.RunnerSpec{Container: corev1.Container{Image: "router:v1"}},
		}
		spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent},
			RollingUpdate: &v1beta1.GroupRollingUpdate{},
		}}}
		if engineEnv {
			spec.Engine.Runner.Env = []corev1.EnvVar{{Name: "CONFIG_VERSION", Value: "2"}}
		}
		if decoderEnv {
			spec.Decoder.Runner.Env = []corev1.EnvVar{{Name: "CONFIG_VERSION", Value: "2"}}
		}
		return spec
	}
	wantWarnings(t, "engine alone in a three-member group", CoordinationUpdateWarnings(threeWay(false, false), threeWay(true, false)),
		"spec.rollout.groups[0]: only the engine changes; the decoder and the router keep their revisions and roll nothing ("+ReasonRollingUpdatePartialChange+")")
	wantWarnings(t, "engine and decoder in a three-member group", CoordinationUpdateWarnings(threeWay(false, false), threeWay(true, true)),
		"spec.rollout.groups[0]: only the engine and the decoder change; the router keeps its revision and rolls nothing ("+ReasonRollingUpdatePartialChange+")")
}

// A one-member change that also moves spec.rollout.pairingProtocol is the
// protocol rule's to judge, whatever the pod templates do: the member warning
// stands on its own, one rollingUpdate group over both members admits the
// protocol change, and a group over the engine alone refuses it.
func TestCoordinationUpdateWarnings_MemberAndProtocolChange_ProtocolRuleDecides(t *testing.T) {
	oldSpec := rollingUpdateGroupSpec(omeNativePDSpecWithImages("v1", "v1"))
	oldSpec.Rollout.PairingProtocol = ptr.To("kv-v1")
	newSpec := rollingUpdateGroupSpec(omeNativePDSpecWithImages("v2", "v1"))
	newSpec.Rollout.PairingProtocol = ptr.To("kv-v2")
	wantWarnings(t, "engine-only image change beside a protocol change", CoordinationUpdateWarnings(oldSpec, newSpec),
		oneMemberWarning(v1beta1.EngineComponent, v1beta1.DecoderComponent))
	if err := ValidatePairingProtocolUpdate(oldSpec, newSpec); err != nil {
		t.Errorf("protocol change under one rollingUpdate group over both members: got %v want admitted", err)
	}
	newSpec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
	err := ValidatePairingProtocolUpdate(oldSpec, newSpec)
	if err == nil || !strings.Contains(err.Error(), ReasonPairingProtocolChangeUncoordinated) {
		t.Errorf("protocol change with the decoder outside the group: got %v want %s", err, ReasonPairingProtocolChangeUncoordinated)
	}
}

// Scaling knobs do not re-render a pod template: a one-sided MinReplicas
// change scales the Component without rolling it, so nothing is warned about.
func TestCoordinationUpdateWarnings_ScaleOnlyChange_NoWarning(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := rollingUpdateGroupSpec(omeNativePDSpecWithImages("v1", "v1"))
	three := 3
	newSpec.Engine.MinReplicas = &three
	wantWarnings(t, "engine-only MinReplicas change", CoordinationUpdateWarnings(oldSpec, newSpec))
}

// Lifecycle (pacing) does not re-render a pod template either: a one-sided
// partition change stages the rollout without minting a new revision.
func TestCoordinationUpdateWarnings_LifecycleOnlyChange_NoWarning(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	newSpec := rollingUpdateGroupSpec(omeNativePDSpecWithImages("v1", "v1"))
	partition := int32(1)
	newSpec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			RollingUpdate: &v1beta1.RollingUpdate{Partition: &partition},
		},
	}
	wantWarnings(t, "engine-only lifecycle change", CoordinationUpdateWarnings(oldSpec, newSpec))
}

// A change to a Component OUTSIDE the group is nobody's business in the
// group; only grouped Components are compared.
func TestCoordinationUpdateWarnings_IgnoresUngroupedComponent(t *testing.T) {
	oldSpec := omeNativePDSpecWithImages("v1", "v1")
	oldSpec.Router = &v1beta1.RouterSpec{}
	newSpec := rollingUpdateGroupSpec(omeNativePDSpecWithImages("v1", "v1"))
	newSpec.Router = &v1beta1.RouterSpec{
		Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "router:v2"}},
	}
	wantWarnings(t, "router change outside the group", CoordinationUpdateWarnings(oldSpec, newSpec))
}
