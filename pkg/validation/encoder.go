package validation

import (
	"fmt"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Encoder admission reasons. Operators and the envtest suites match these by
// name, so they are exported and stable.
const (
	// ReasonEncoderRequiresEngine refuses an encoder on a service with no
	// engine: an encoder makes embeddings for a prefill, and with none it
	// serves nothing.
	ReasonEncoderRequiresEngine = "EncoderRequiresEngine"
	// ReasonEncoderRequiresOMENative refuses an encoder unless every declared
	// role runs as an OMENative pod set. Only OMENative gives a role its
	// peers' addresses and its per-revision Services; a RawDeployment role
	// beside an encoder could not reach it, or be reached.
	ReasonEncoderRequiresOMENative = "EncoderRequiresOMENative"
	// ReasonEncoderRequiresRolloutGroups refuses an encoder on a service
	// that declares no rollout groups. The peer address variables, which
	// tell the encoder and its peers where the others are, exist only when
	// the service declares groups.
	ReasonEncoderRequiresRolloutGroups = "EncoderRequiresRolloutGroups"
	// ReasonEncoderNotPlaceable refuses an encoder beside spec.placement or
	// spec.routing: multi-cluster placement does not place an encoder.
	ReasonEncoderNotPlaceable = "EncoderNotPlaceable"
	// ReasonEncoderRatioGroup refuses maintainRatio on a group that names the
	// encoder and two or more other roles: the ratio check compares members
	// two at a time, and with three members the checks can hold each other
	// so the group never finishes its roll.
	ReasonEncoderRatioGroup = "EncoderRatioGroup"
	// ReasonPairingProtocolChangeWithEncoder refuses every pairing-protocol
	// change except clearing it, while the old or the new spec declares an
	// encoder. The rollout gate keeps only the engine and the decoder
	// pairable through a change, not the encoder and the engine.
	ReasonPairingProtocolChangeWithEncoder = "PairingProtocolChangeWithEncoder"
	// ReasonEncoderAddedWithPairingProtocol refuses an edit that adds an
	// encoder while the pairing protocol is set. An earlier change of the
	// value may still be rolling, and the new encoder would serve beside two
	// values with nothing keeping it pairable.
	ReasonEncoderAddedWithPairingProtocol = "EncoderAddedWithPairingProtocol"
)

// ValidateEncoder enforces the shape an encoder needs: an engine beside it, a
// single-cluster service, rollout groups (which turn on the peer address
// variables), and OMENative dispatch for every declared role.
func ValidateEncoder(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.Encoder == nil {
		return nil
	}
	if spec.Engine == nil && ReferencedReplica(spec, v1beta1.EngineComponent) == "" {
		return fmt.Errorf("encoder cannot be specified without engine (%s)", ReasonEncoderRequiresEngine)
	}
	if spec.Placement != nil || spec.Routing != nil {
		return fmt.Errorf("spec.encoder cannot be combined with spec.placement or spec.routing: multi-cluster placement does not place an encoder; serve the service from one cluster (%s)",
			ReasonEncoderNotPlaceable)
	}
	if len(spec.GetRolloutGroups()) == 0 {
		return fmt.Errorf("spec.encoder requires spec.rollout.groups: the encoder and its peers learn each other's addresses from variables OME sets only on a service that declares rollout groups; declare at least one group, for example one blueGreen group over every role (%s)",
			ReasonEncoderRequiresRolloutGroups)
	}
	for i, g := range spec.GetRolloutGroups() {
		if g.MaintainRatio != nil && len(g.Components) >= 3 && slices.Contains(g.Components, v1beta1.EncoderComponent) {
			return fmt.Errorf("spec.rollout.groups[%d]: maintainRatio is refused on a group that names the encoder and two or more other roles, because the ratio check compares members two at a time and three members can hold each other so the roll never finishes; keep the ratio on a two-role group, or drop maintainRatio (%s)",
				i, ReasonEncoderRatioGroup)
		}
	}
	for _, role := range declaredRoles(spec) {
		if mode := roleDispatchMode(&role, spec.DeploymentMode); mode != string(constants.OMENative) {
			return fmt.Errorf("%s resolves to deploymentMode %q on the InferenceService; a service with an encoder runs every role as OMENative, the only mode that gives the roles each other's addresses — set spec.deploymentMode: OMENative (%s)",
				role.field, mode, ReasonEncoderRequiresOMENative)
		}
	}
	return nil
}

// roleDispatchMode is the mode a role declares on the InferenceService
// itself, in the controller's order: its own ome.io/deploymentMode
// annotation, else spec.deploymentMode, else OMENative for a Leader/Worker
// gang on the service, else RawDeployment. A mode that only the runtime's
// piece supplies does not count; the controller checks the merged block and
// holds the service if a role resolves to another mode there.
func roleDispatchMode(role *declaredRole, specMode *constants.DeploymentModeType) string {
	if mode := resolveComponentDeploymentMode(role.extension.Annotations, specMode); mode != "" {
		return mode
	}
	if role.leader != nil || role.worker != nil {
		return string(constants.OMENative)
	}
	return string(constants.RawDeployment)
}
