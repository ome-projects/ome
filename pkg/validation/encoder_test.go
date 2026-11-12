package validation

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// encoderSpec builds a service with an engine and an encoder, every role
// running OMENative through spec.deploymentMode and one blueGreen group over
// every role, plus a decoder when asked.
func encoderSpec(withDecoder bool) *v1beta1.InferenceServiceSpec {
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: ptr.To(constants.OMENative),
		Engine:         &v1beta1.EngineSpec{},
		Encoder:        &v1beta1.EncoderSpec{},
	}
	group := v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EncoderComponent, v1beta1.EngineComponent}}
	if withDecoder {
		spec.Decoder = &v1beta1.DecoderSpec{}
		group.Components = append(group.Components, v1beta1.DecoderComponent)
	}
	spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{group}}
	return spec
}

func modeAnnotation(mode constants.DeploymentModeType) v1beta1.ComponentExtensionSpec {
	return v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(mode)}}
}

func TestValidateEncoder_Shapes(t *testing.T) {
	cases := []struct {
		name       string
		spec       func() *v1beta1.InferenceServiceSpec
		wantReason string
	}{
		{name: "no encoder is not this rule's business", spec: func() *v1beta1.InferenceServiceSpec {
			return &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}
		}},
		{name: "encoder and engine", spec: func() *v1beta1.InferenceServiceSpec { return encoderSpec(false) }},
		{name: "encoder, engine and decoder", spec: func() *v1beta1.InferenceServiceSpec { return encoderSpec(true) }},
		{name: "encoder, engine, decoder and an OMENative router", spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Router = &v1beta1.RouterSpec{}
			return spec
		}},
		{name: "a RawDeployment router beside an encoder", wantReason: ReasonEncoderRequiresOMENative, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: modeAnnotation(constants.RawDeployment)}
			return spec
		}},
		{name: "no rollout groups", wantReason: ReasonEncoderRequiresRolloutGroups, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Rollout = nil
			return spec
		}},
		{name: "a rollout block with a token and no groups", wantReason: ReasonEncoderRequiresRolloutGroups, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: ptr.To("v1")}
			return spec
		}},
		{name: "encoder alone", wantReason: ReasonEncoderRequiresEngine, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(false)
			spec.Engine = nil
			return spec
		}},
		{name: "encoder and decoder without engine", wantReason: ReasonEncoderRequiresEngine, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Engine = nil
			return spec
		}},
		{name: "no deployment mode resolves the roles to RawDeployment", wantReason: ReasonEncoderRequiresOMENative, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.DeploymentMode = nil
			return spec
		}},
		{name: "per-role OMENative annotations", spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(false)
			spec.DeploymentMode = nil
			spec.Engine.ComponentExtensionSpec = modeAnnotation(constants.OMENative)
			spec.Encoder.ComponentExtensionSpec = modeAnnotation(constants.OMENative)
			return spec
		}},
		{name: "an engine gang without a mode dispatches as OMENative", spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(false)
			spec.DeploymentMode = nil
			spec.Engine = &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}}
			spec.Encoder.ComponentExtensionSpec = modeAnnotation(constants.OMENative)
			return spec
		}},
		{name: "a RawDeployment decoder beside an encoder", wantReason: ReasonEncoderRequiresOMENative, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Decoder.ComponentExtensionSpec = modeAnnotation(constants.RawDeployment)
			return spec
		}},
		{name: "a RawDeployment encoder", wantReason: ReasonEncoderRequiresOMENative, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(false)
			spec.Encoder.ComponentExtensionSpec = modeAnnotation(constants.RawDeployment)
			return spec
		}},
		{name: "maintainRatio over the encoder, the engine and the decoder", wantReason: ReasonEncoderRatioGroup, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Rollout.Groups[0].MaintainRatio = &v1beta1.MaintainRatio{}
			return spec
		}},
		{name: "maintainRatio over the encoder and the engine alone", spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(false)
			spec.Rollout.Groups[0].MaintainRatio = &v1beta1.MaintainRatio{}
			return spec
		}},
		{name: "beside placement", wantReason: ReasonEncoderNotPlaceable, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Placement = &v1beta1.PlacementSpec{}
			return spec
		}},
		{name: "beside routing", wantReason: ReasonEncoderNotPlaceable, spec: func() *v1beta1.InferenceServiceSpec {
			spec := encoderSpec(true)
			spec.Routing = &v1beta1.RoutingSpec{}
			return spec
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEncoder(tc.spec())
			if tc.wantReason == "" {
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("got %v, want a refusal naming %s", err, tc.wantReason)
			}
		})
	}
}

func TestValidateEncoder_RawDeploymentNamesTheRole(t *testing.T) {
	spec := encoderSpec(true)
	spec.Decoder.ComponentExtensionSpec = modeAnnotation(constants.RawDeployment)
	err := ValidateEncoder(spec)
	if err == nil || !strings.Contains(err.Error(), `spec.decoder resolves to deploymentMode "RawDeployment"`) {
		t.Fatalf("got %v, want the refusal to name spec.decoder and its mode", err)
	}
}

func TestValidatePairingProtocolUpdate_Encoder(t *testing.T) {
	withProtocol := func(spec *v1beta1.InferenceServiceSpec, protocol *string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceServiceSpec {
		spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: protocol, Groups: groups}
		return spec
	}
	chain := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EncoderComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	}
	pd := func() *v1beta1.InferenceServiceSpec {
		return &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{}}
	}
	cases := []struct {
		name string
		old  *v1beta1.InferenceServiceSpec
		new  *v1beta1.InferenceServiceSpec
		// reason names the refusal; empty means the edit is admitted.
		reason string
	}{
		{
			name:   "a change between two values is refused even under one group over the chain",
			old:    withProtocol(encoderSpec(true), ptr.To("v1"), chain),
			new:    withProtocol(encoderSpec(true), ptr.To("v2"), chain),
			reason: ReasonPairingProtocolChangeWithEncoder,
		},
		{
			name:   "a change between two values is refused with no decoder",
			old:    withProtocol(encoderSpec(false), ptr.To("v1")),
			new:    withProtocol(encoderSpec(false), ptr.To("v2")),
			reason: ReasonPairingProtocolChangeWithEncoder,
		},
		{
			name:   "adding the encoder in the same edit as the change is refused",
			old:    withProtocol(pd(), ptr.To("v1"), pairedGroup("blueGreen")),
			new:    withProtocol(encoderSpec(true), ptr.To("v2"), pairedGroup("blueGreen")),
			reason: ReasonEncoderAddedWithPairingProtocol,
		},
		{
			name:   "adding the encoder while the value stays set is refused: an earlier change may still be rolling",
			old:    withProtocol(pd(), ptr.To("v1"), pairedGroup("blueGreen")),
			new:    withProtocol(encoderSpec(true), ptr.To("v1"), pairedGroup("blueGreen")),
			reason: ReasonEncoderAddedWithPairingProtocol,
		},
		{
			name:   "adding the encoder while setting a value from empty is refused as an encoder added with a value",
			old:    withProtocol(pd(), nil, pairedGroup("blueGreen")),
			new:    withProtocol(encoderSpec(true), ptr.To("v1"), pairedGroup("blueGreen")),
			reason: ReasonEncoderAddedWithPairingProtocol,
		},
		{
			name: "adding the encoder while clearing the value is admitted",
			old:  withProtocol(pd(), ptr.To("v1"), pairedGroup("blueGreen")),
			new:  withProtocol(encoderSpec(true), nil, pairedGroup("blueGreen")),
		},
		{
			name: "adding the encoder to a service with no value is admitted",
			old:  withProtocol(pd(), nil, pairedGroup("blueGreen")),
			new:  withProtocol(encoderSpec(true), nil, pairedGroup("blueGreen")),
		},
		{
			name: "clearing the value is admitted",
			old:  withProtocol(encoderSpec(true), ptr.To("v1")),
			new:  withProtocol(encoderSpec(true), nil),
		},
		{
			name:   "setting a value from empty is refused: an earlier value may still serve",
			old:    withProtocol(encoderSpec(true), nil),
			new:    withProtocol(encoderSpec(true), ptr.To("v2")),
			reason: ReasonPairingProtocolChangeWithEncoder,
		},
		{
			name: "keeping the value is admitted",
			old:  withProtocol(encoderSpec(true), ptr.To("v1")),
			new:  withProtocol(encoderSpec(true), ptr.To("v1")),
		},
		{
			name:   "removing the encoder in the same edit as the change is refused",
			old:    withProtocol(encoderSpec(true), ptr.To("v1"), pairedGroup("blueGreen")),
			new:    withProtocol(pd(), ptr.To("v2"), pairedGroup("blueGreen")),
			reason: ReasonPairingProtocolChangeWithEncoder,
		},
		{
			name: "after the encoder is gone the PD rule applies again",
			old:  withProtocol(pd(), ptr.To("v1"), pairedGroup("blueGreen")),
			new:  withProtocol(pd(), ptr.To("v2"), pairedGroup("blueGreen")),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePairingProtocolUpdate(tc.old, tc.new)
			if tc.reason != "" {
				if err == nil || !strings.Contains(err.Error(), tc.reason) {
					t.Fatalf("got %v, want a refusal naming %s", err, tc.reason)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected refusal: %v", err)
			}
		})
	}
}

func TestValidateCanary_EncoderJoinsTheEngineUnit(t *testing.T) {
	canaryOver := func(components ...v1beta1.ComponentType) *v1beta1.InferenceServiceSpec {
		spec := encoderSpec(true)
		spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{Components: components, Canary: oneStepCanary()}}}
		return spec
	}
	cases := []struct {
		name    string
		spec    *v1beta1.InferenceServiceSpec
		wantErr string
	}{
		{
			name: "a group over every pairing role",
			spec: canaryOver(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.EncoderComponent),
		},
		{
			name:    "a group that leaves the encoder out",
			spec:    canaryOver(v1beta1.EngineComponent, v1beta1.DecoderComponent),
			wantErr: "declares engine, decoder and encoder, so a canary group naming any of them must name all of them",
		},
		{
			name:    "an encoder-only group misses the unit's entrypoint",
			spec:    canaryOver(v1beta1.EncoderComponent),
			wantErr: `must include its entrypoint Component "engine"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCanary(tc.spec)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want a refusal containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateCanary_EngineAndDecoderRefusalSaysBoth(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{
		DeploymentMode: ptr.To(constants.OMENative),
		Engine:         &v1beta1.EngineSpec{},
		Decoder:        &v1beta1.DecoderSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			Canary:     oneStepCanary(),
		}}},
	}
	err := ValidateCanary(spec)
	want := "this InferenceService declares both engine and decoder, so a canary group naming either must name both"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want a refusal containing %q", err, want)
	}
}

// oneStepCanary is a canary plan with its one, final step.
func oneStepCanary() *v1beta1.GroupCanary {
	return &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}}
}

func TestRolloutRefusalsListEveryRole(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{},
		Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{"sidecar"},
		}}},
	}
	err := ValidateCoordination(spec)
	if err == nil || !strings.Contains(err.Error(), "is not one of router/engine/decoder/encoder") {
		t.Fatalf("got %v, want the refusal to list every role", err)
	}
}

// The refusal of a value changed beside an encoder says what the value may
// do: stay empty when it was empty, else stay as it was or be cleared.
func TestValidatePairingProtocolUpdate_EncoderRefusalSaysWhatTheValueMayDo(t *testing.T) {
	withProtocol := func(protocol *string) *v1beta1.InferenceServiceSpec {
		return &v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Encoder: &v1beta1.EncoderSpec{}, Rollout: &v1beta1.RolloutSpec{PairingProtocol: protocol}}
	}
	for _, tc := range []struct {
		name     string
		old, new *string
		wantText string
	}{
		{name: "from empty", old: nil, new: ptr.To("v2"), wantText: "the value may only stay empty, since"},
		{name: "from a value", old: ptr.To("v1"), new: ptr.To("v2"), wantText: `the value may only stay "v1" or be cleared, since`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePairingProtocolUpdate(withProtocol(tc.old), withProtocol(tc.new))
			if err == nil || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("got %v, want a refusal saying %q", err, tc.wantText)
			}
		})
	}
}
