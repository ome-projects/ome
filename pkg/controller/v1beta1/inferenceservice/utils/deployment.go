package utils

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// DetermineDeploymentModes determines the engine, decoder and router modes,
// for callers that name those three roles; a role that is not declared
// resolves to RawDeployment, and an engine is required. Callers that name
// the three roles link against this signature. Code that walks every role
// uses DetermineRoleDeploymentModes.
func DetermineDeploymentModes(engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec, _ *v1beta1.ServingRuntimeSpec, specMode *constants.DeploymentModeType) (engineMode, decoderMode, routerMode constants.DeploymentModeType, err error) {
	if engine == nil {
		return "", "", "", fmt.Errorf("engine component is required")
	}
	specs := v1beta1.RoleSpecs{Engine: engine, Decoder: decoder, Router: router}
	return RoleDeploymentMode(specs.View(v1beta1.EngineComponent), specMode),
		RoleDeploymentMode(specs.View(v1beta1.DecoderComponent), specMode),
		RoleDeploymentMode(specs.View(v1beta1.RouterComponent), specMode), nil
}

// DetermineRoleDeploymentModes determines the deployment mode of every
// declared role, keyed by role; an undeclared role has no entry. An engine
// is required. See RoleDeploymentMode for the precedence chain.
func DetermineRoleDeploymentModes(specs v1beta1.RoleSpecs, specMode *constants.DeploymentModeType) (map[v1beta1.ComponentType]constants.DeploymentModeType, error) {
	if specs.Engine == nil {
		return nil, fmt.Errorf("engine component is required")
	}
	modes := make(map[v1beta1.ComponentType]constants.DeploymentModeType, len(v1beta1.Roles))
	for _, view := range specs.Declared() {
		modes[view.Type] = RoleDeploymentMode(&view, specMode)
	}
	return modes, nil
}

// RoleDeploymentMode determines the deployment mode of one role's block.
//
// Resolution order:
//  1. Per-Component annotation (ome.io/deploymentMode) — highest priority,
//     preserved as the operator escape hatch for mixed-mode experiments.
//  2. specMode — the typed top-level spec.deploymentMode field. When set,
//     propagates to every Component without mutating per-Component
//     annotations (kept clean for `kubectl get -o yaml`).
//  3. Leader/Worker shape inference → OMENative (native multi-node).
//     LeaderWorkerSet-backed MultiNode is selected only by an explicit
//     deployment-mode annotation or config default, never by shape.
//  4. Default → RawDeployment.
//
// Every role resolves the same way, so a PD-disaggregated ISVC can opt each
// role into OMENative independently. A nil block resolves to RawDeployment.
func RoleDeploymentMode(view *v1beta1.RoleView, specMode *constants.DeploymentModeType) constants.DeploymentModeType {
	if view == nil {
		return constants.RawDeployment
	}
	if mode, found := GetDeploymentModeFromAnnotations(view.Extension.Annotations); found {
		return mode
	}
	if mode, found := deploymentModeFromSpecField(specMode); found {
		return mode
	}
	if view.Leader != nil || view.Worker != nil {
		return constants.OMENative
	}
	return constants.RawDeployment
}

// deploymentModeFromSpecField returns (mode, true) when the typed
// spec.deploymentMode field is set to a valid value. Invalid values are
// ignored here; the CRD enum marker rejects them at admission time so
// reaching this code with an invalid value would indicate a CRD/schema
// mismatch — quietly falling through to the next precedence rung is the
// safer behavior.
func deploymentModeFromSpecField(specMode *constants.DeploymentModeType) (constants.DeploymentModeType, bool) {
	if specMode == nil {
		return "", false
	}
	if !specMode.IsValid() {
		return "", false
	}
	return *specMode, true
}

// DetermineEntrypointComponent determines which component should be the main entrypoint for the InferenceService.
// Priority: Router (if present) > Engine (always present)
// This function implements the automatic routing logic: router if present, else engine.
func DetermineEntrypointComponent(isvc *v1beta1.InferenceService) v1beta1.ComponentType {
	// Auto-determine: Router takes precedence if present
	if isvc.Spec.Router != nil {
		return v1beta1.RouterComponent
	}

	// Default to engine
	return v1beta1.EngineComponent
}

// InferenceServiceDeploymentMode resolves the mode of the InferenceService
// as a whole: the top-level ome.io/deploymentMode annotation, then
// spec.deploymentMode, then the declared shape (Engine+Decoder is
// PDDisaggregated, an Engine Leader+Worker pair is OMENative), then the
// operator default. It gates the InferenceService-level paths
// (VirtualDeployment, status); per-Component dispatch uses
// DetermineDeploymentModes.
func InferenceServiceDeploymentMode(isvc *v1beta1.InferenceService, defaultMode constants.DeploymentModeType) constants.DeploymentModeType {
	if isvc == nil {
		return defaultMode
	}
	if mode, found := GetDeploymentModeFromAnnotations(isvc.Annotations); found {
		return mode
	}
	if mode, found := deploymentModeFromSpecField(isvc.Spec.DeploymentMode); found {
		return mode
	}
	if isvc.Spec.Engine != nil && isvc.Spec.Decoder != nil {
		return constants.PDDisaggregated
	}
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.Leader != nil && isvc.Spec.Engine.Worker != nil {
		return constants.OMENative
	}
	return defaultMode
}

// workerSpawnsPods reports whether a declared Worker spawns at least one pod.
// An unset Worker.Size resolves to one worker at reconcile time, so a
// declared Worker is multi-pod unless its size is explicitly non-positive.
func workerSpawnsPods(worker *v1beta1.WorkerSpec) bool {
	return worker != nil && (worker.Size == nil || *worker.Size > 0)
}
