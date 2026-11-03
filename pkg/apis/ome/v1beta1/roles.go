package v1beta1

import (
	"sort"

	"knative.dev/pkg/apis"
)

// Role describes one role an InferenceService may declare: the condition its
// readiness sets and the facts the rules that walk the roles read. A rule
// that needs every role ranges over Roles instead of naming them one by one,
// so a new role is one more row here.
//
// +kubebuilder:object:generate=false
// +k8s:openapi-gen=false
type Role struct {
	// Type is the role's ComponentType.
	Type ComponentType
	// ReadyCondition is the condition the role's readiness sets.
	ReadyCondition apis.ConditionType
	// Pairs reports whether the role carries spec.rollout.pairingProtocol:
	// its pods pass per-request state to, or take it from, another role's
	// pods over a runtime-owned protocol.
	Pairs bool
	// RunsModel reports whether the role's pods run the model rather than
	// route to it: the selector picks an accelerator class for it, the
	// runtime's model format applies, its own node selector joins the
	// class's, and runtime selection matches its deployment mode.
	RunsModel bool
	// PrimaryRank ranks the role when a rollout group picks its primary, the
	// Component its canary step machine runs through: the member with the
	// lowest rank is the primary. It is not the ingress entrypoint order.
	PrimaryRank int
	// ServiceField is the path of the role's block on an InferenceService,
	// as a refusal names it.
	ServiceField string
	// RuntimeField is the path of the role's piece on a ServingRuntime.
	RuntimeField string
}

// Roles lists every role in the order the rules walk them and refusals name
// them: the engine first, since every service declares one, then the decoder,
// the encoder and the router.
var Roles = []Role{
	{Type: EngineComponent, ReadyCondition: EngineReady, Pairs: true, RunsModel: true, PrimaryRank: 1,
		ServiceField: "spec.engine", RuntimeField: "engineConfig"},
	{Type: DecoderComponent, ReadyCondition: DecoderReady, Pairs: true, RunsModel: true, PrimaryRank: 2,
		ServiceField: "spec.decoder", RuntimeField: "decoderConfig"},
	{Type: EncoderComponent, ReadyCondition: EncoderReady, Pairs: true, RunsModel: true, PrimaryRank: 3,
		ServiceField: "spec.encoder", RuntimeField: "encoderConfig"},
	{Type: RouterComponent, ReadyCondition: RouterReady, PrimaryRank: 0,
		ServiceField: "spec.router", RuntimeField: "routerConfig"},
}

// RoleOf returns the role whose type is c, and false when c names no role.
func RoleOf(c ComponentType) (Role, bool) {
	for _, r := range Roles {
		if r.Type == c {
			return r, true
		}
	}
	return Role{}, false
}

// ReadyConditionOf returns the condition role c's readiness sets, and false
// when c names no role.
func ReadyConditionOf(c ComponentType) (apis.ConditionType, bool) {
	r, ok := RoleOf(c)
	return r.ReadyCondition, ok
}

// RoleTypes returns the type of every role, in the order of Roles.
func RoleTypes() []ComponentType {
	types := make([]ComponentType, len(Roles))
	for i, r := range Roles {
		types[i] = r.Type
	}
	return types
}

// RoleView is one role's block read through pointers into the object that
// declares it: an InferenceService spec, a runtime, or their merge. A field
// the role does not have is nil. Writing through a pointer writes the object;
// reassigning a field of the view does not.
//
// +kubebuilder:object:generate=false
// +k8s:openapi-gen=false
type RoleView struct {
	Type                ComponentType
	PodSpec             *PodSpec
	Extension           *ComponentExtensionSpec
	Runner              *RunnerSpec
	Leader              *LeaderSpec
	Worker              *WorkerSpec
	TopologyKey         *string
	TopologySpread      *TopologySpreadPolicy
	TopologySpreadKey   *string
	AcceleratorOverride *AcceleratorSelector
	RouterConfig        map[string]string
}

// RolesByPrimaryRank returns the type of every role in PrimaryRank order,
// the order a rollout group prefers its primary in.
func RolesByPrimaryRank() []ComponentType {
	types := RoleTypes()
	sort.SliceStable(types, func(i, j int) bool {
		ri, _ := RoleOf(types[i])
		rj, _ := RoleOf(types[j])
		return ri.PrimaryRank < rj.PrimaryRank
	})
	return types
}

// RoleSpecs holds one typed block per role, nil for a role that is not
// declared. Role-generic code reads a role through View; role-specific code
// (typed merge and render, ingress, placement, the prefill-decode pair gate)
// names its field directly.
//
// +kubebuilder:object:generate=false
// +k8s:openapi-gen=false
type RoleSpecs struct {
	Engine  *EngineSpec
	Decoder *DecoderSpec
	Encoder *EncoderSpec
	Router  *RouterSpec
}

// View returns the block of role c, nil when the role is not declared or c
// names no role.
func (s RoleSpecs) View(c ComponentType) *RoleView {
	switch c {
	case EngineComponent:
		if e := s.Engine; e != nil {
			return &RoleView{Type: c, PodSpec: &e.PodSpec, Extension: &e.ComponentExtensionSpec, Runner: e.Runner,
				Leader: e.Leader, Worker: e.Worker, TopologyKey: e.TopologyKey, TopologySpread: e.TopologySpread,
				TopologySpreadKey: e.TopologySpreadKey, AcceleratorOverride: e.AcceleratorOverride}
		}
	case DecoderComponent:
		if d := s.Decoder; d != nil {
			return &RoleView{Type: c, PodSpec: &d.PodSpec, Extension: &d.ComponentExtensionSpec, Runner: d.Runner,
				Leader: d.Leader, Worker: d.Worker, TopologyKey: d.TopologyKey, TopologySpread: d.TopologySpread,
				TopologySpreadKey: d.TopologySpreadKey, AcceleratorOverride: d.AcceleratorOverride}
		}
	case EncoderComponent:
		if e := s.Encoder; e != nil {
			return &RoleView{Type: c, PodSpec: &e.PodSpec, Extension: &e.ComponentExtensionSpec, Runner: e.Runner,
				TopologySpread: e.TopologySpread, TopologySpreadKey: e.TopologySpreadKey, AcceleratorOverride: e.AcceleratorOverride}
		}
	case RouterComponent:
		if r := s.Router; r != nil {
			return &RoleView{Type: c, PodSpec: &r.PodSpec, Extension: &r.ComponentExtensionSpec, Runner: r.Runner,
				RouterConfig: r.Config}
		}
	}
	return nil
}

// Declared returns the block of every declared role, in the order of Roles.
func (s RoleSpecs) Declared() []RoleView {
	var views []RoleView
	for _, r := range Roles {
		if v := s.View(r.Type); v != nil {
			views = append(views, *v)
		}
	}
	return views
}

// ServiceRoles returns the role blocks an InferenceService spec declares.
func ServiceRoles(spec *InferenceServiceSpec) RoleSpecs {
	if spec == nil {
		return RoleSpecs{}
	}
	return RoleSpecs{Engine: spec.Engine, Decoder: spec.Decoder, Encoder: spec.Encoder, Router: spec.Router}
}

// RuntimeRoles returns the role pieces a ServingRuntime spec declares.
func RuntimeRoles(spec *ServingRuntimeSpec) RoleSpecs {
	if spec == nil {
		return RoleSpecs{}
	}
	return RoleSpecs{Engine: spec.EngineConfig, Decoder: spec.DecoderConfig, Encoder: spec.EncoderConfig, Router: spec.RouterConfig}
}

// SpecOf returns the block of role c on an InferenceService spec, nil when
// the spec does not declare the role.
func SpecOf(spec *InferenceServiceSpec, c ComponentType) *RoleView {
	return ServiceRoles(spec).View(c)
}

// PieceOf returns the piece of role c on a ServingRuntime spec, nil when the
// runtime carries no such piece.
func PieceOf(spec *ServingRuntimeSpec, c ComponentType) *RoleView {
	return RuntimeRoles(spec).View(c)
}

// DeclareRole gives the spec an empty block for role c, keeping a block it
// already has, and reports whether c names a role. Rendering one role from a
// runtime piece alone needs it: a role is rendered only when the service
// declares it.
func DeclareRole(spec *InferenceServiceSpec, c ComponentType) bool {
	switch c {
	case EngineComponent:
		if spec.Engine == nil {
			spec.Engine = &EngineSpec{}
		}
	case DecoderComponent:
		if spec.Decoder == nil {
			spec.Decoder = &DecoderSpec{}
		}
	case EncoderComponent:
		if spec.Encoder == nil {
			spec.Encoder = &EncoderSpec{}
		}
	case RouterComponent:
		if spec.Router == nil {
			spec.Router = &RouterSpec{}
		}
	default:
		return false
	}
	return true
}
