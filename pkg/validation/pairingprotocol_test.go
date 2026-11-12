package validation

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// pairingSpec builds an ISVC spec with engine+decoder declared, the given
// pairing protocol (nil = unset), and the given rollout groups.
func pairingSpec(protocol *string, groups ...v1beta1.RolloutGroup) *v1beta1.InferenceServiceSpec {
	spec := &v1beta1.InferenceServiceSpec{
		Engine:  &v1beta1.EngineSpec{},
		Decoder: &v1beta1.DecoderSpec{},
	}
	if protocol != nil || len(groups) > 0 {
		spec.Rollout = &v1beta1.RolloutSpec{
			PairingProtocol: protocol,
			Groups:          groups,
		}
	}
	return spec
}

func pairedGroup(progression string) v1beta1.RolloutGroup {
	g := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
	}
	switch progression {
	case "blueGreen":
		g.BlueGreen = &v1beta1.GroupBlueGreen{}
	case "canary":
		g.Canary = &v1beta1.GroupCanary{}
	case "rollingUpdate":
		g.RollingUpdate = &v1beta1.GroupRollingUpdate{}
	case "policyRef-rollingUpdate":
		g.PolicyRef = &v1beta1.RolloutPolicyRef{Name: "paced", Progression: v1beta1.RolloutProgressionRollingUpdate}
	}
	return g
}

func TestValidatePairingProtocolUpdate(t *testing.T) {
	cases := []struct {
		name    string
		old     *v1beta1.InferenceServiceSpec
		new     *v1beta1.InferenceServiceSpec
		wantErr bool
	}{
		{
			name: "unchanged protocol always admitted",
			old:  pairingSpec(ptr.To("nixl-v1")),
			new:  pairingSpec(ptr.To("nixl-v1")),
		},
		{
			name: "setting a protocol from unset is upgrade-neutral",
			old:  pairingSpec(nil),
			new:  pairingSpec(ptr.To("nixl-v1")),
		},
		{
			name: "clearing a protocol is upgrade-neutral",
			old:  pairingSpec(ptr.To("nixl-v1")),
			new:  pairingSpec(nil),
		},
		{
			name: "change under a shared blueGreen group admitted",
			old:  pairingSpec(ptr.To("nixl-v1"), pairedGroup("blueGreen")),
			new:  pairingSpec(ptr.To("nixl-v2"), pairedGroup("blueGreen")),
		},
		{
			name: "change under a shared defaulted (progression-less) group admitted",
			old:  pairingSpec(ptr.To("nixl-v1"), pairedGroup("")),
			new:  pairingSpec(ptr.To("nixl-v2"), pairedGroup("")),
		},
		{
			name: "change under a shared canary group admitted",
			old:  pairingSpec(ptr.To("nixl-v1"), pairedGroup("canary")),
			new:  pairingSpec(ptr.To("nixl-v2"), pairedGroup("canary")),
		},
		{
			name: "change under a shared rollingUpdate group admitted",
			old:  pairingSpec(ptr.To("nixl-v1"), pairedGroup("rollingUpdate")),
			new:  pairingSpec(ptr.To("nixl-v2"), pairedGroup("rollingUpdate")),
		},
		{
			name: "change under a shared rollingUpdate group declared by policyRef admitted",
			old:  pairingSpec(ptr.To("nixl-v1"), pairedGroup("policyRef-rollingUpdate")),
			new:  pairingSpec(ptr.To("nixl-v2"), pairedGroup("policyRef-rollingUpdate")),
		},
		{
			name: "change with engine and decoder in separate groups denied",
			old: pairingSpec(ptr.To("nixl-v1"),
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
			),
			new: pairingSpec(ptr.To("nixl-v2"),
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}},
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}},
			),
			wantErr: true,
		},
		{
			name: "change with engine and decoder in separate rollingUpdate groups denied",
			old: pairingSpec(ptr.To("nixl-v1"),
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
			),
			new: pairingSpec(ptr.To("nixl-v2"),
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
			),
			wantErr: true,
		},
		{
			name: "change with a rollingUpdate group over the engine alone denied",
			old: pairingSpec(ptr.To("nixl-v1"),
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
			),
			new: pairingSpec(ptr.To("nixl-v2"),
				v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
			),
			wantErr: true,
		},
		{
			name:    "change with no rollout groups (independent rollouts) denied",
			old:     pairingSpec(ptr.To("nixl-v1")),
			new:     pairingSpec(ptr.To("nixl-v2")),
			wantErr: true,
		},
		{
			name: "change without a decoder admitted (no pair to break)",
			old: func() *v1beta1.InferenceServiceSpec {
				s := pairingSpec(ptr.To("nixl-v1"))
				s.Decoder = nil
				return s
			}(),
			new: func() *v1beta1.InferenceServiceSpec {
				s := pairingSpec(ptr.To("nixl-v2"))
				s.Decoder = nil
				return s
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePairingProtocolUpdate(tc.old, tc.new)
			if tc.wantErr && err == nil {
				t.Fatalf("want rejection, got admit")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("want admit, got: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), ReasonPairingProtocolChangeUncoordinated) {
				t.Errorf("rejection must carry the reason constant: %v", err)
			}
		})
	}
}
