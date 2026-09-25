package utils

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// DetermineEngineDeploymentMode determines the deployment mode for the engine based on its configuration.
//
// Resolution order:
//  1. Per-Component annotation (ome.io/deploymentMode) — highest priority,
//     preserved as the operator escape hatch for mixed-mode experiments.
//  2. specMode — the typed top-level spec.deploymentMode field. When set,
//     propagates to every Component without mutating per-Component
//     annotations (kept clean for `kubectl get -o yaml`).
//  3. Leader/Worker shape inference → OMENative (native multi-node).
//  4. Default → RawDeployment.
func DetermineEngineDeploymentMode(engine *v1beta1.EngineSpec, specMode *constants.DeploymentModeType) constants.DeploymentModeType {
	if engine == nil {
		return constants.RawDeployment
	}

	if mode, found := GetDeploymentModeFromAnnotations(engine.Annotations); found {
		return mode
	}

	if mode, found := deploymentModeFromSpecField(specMode); found {
		return mode
	}

	// Multi-node (leader/worker) resolves to OMENative, which handles
	// multi-node serving natively. LeaderWorkerSet-backed MultiNode is
	// selected only by an explicit deployment-mode annotation or config
	// default, never by shape inference.
	if engine.Leader != nil || engine.Worker != nil {
		return constants.OMENative
	}

	// Default to raw deployment
	return constants.RawDeployment
}

// DetermineDeploymentModes determines the deployment modes for all components based on their specs.
// See DetermineEngineDeploymentMode for the precedence chain that also governs Decoder and Router.
func DetermineDeploymentModes(engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec, runtime *v1beta1.ServingRuntimeSpec, specMode *constants.DeploymentModeType) (engineMode, decoderMode, routerMode constants.DeploymentModeType, err error) {
	engineMode = determineComponentDeploymentMode(engine, runtime, specMode)
	decoderMode = determineComponentDeploymentMode(decoder, runtime, specMode)
	routerMode = determineComponentDeploymentMode(router, runtime, specMode)

	if engine == nil {
		return "", "", "", fmt.Errorf("engine component is required")
	}

	return engineMode, decoderMode, routerMode, nil
}

// determineComponentDeploymentMode determines deployment mode for a generic component.
// Per-Component annotation > spec.deploymentMode (specMode) > Leader/Worker shape (OMENative) >
// RawDeployment default. Symmetric for Engine, Decoder, and Router so PD-disaggregated
// ISVCs can opt each component into OMENative independently.
func determineComponentDeploymentMode(spec interface{}, runtime *v1beta1.ServingRuntimeSpec, specMode *constants.DeploymentModeType) constants.DeploymentModeType {
	switch s := spec.(type) {
	case *v1beta1.EngineSpec:
		return DetermineEngineDeploymentMode(s, specMode)
	case *v1beta1.DecoderSpec:
		if s == nil {
			return constants.RawDeployment
		}
		if mode, found := GetDeploymentModeFromAnnotations(s.Annotations); found {
			return mode
		}
		if mode, found := deploymentModeFromSpecField(specMode); found {
			return mode
		}
		if s.Leader != nil || s.Worker != nil {
			return constants.OMENative
		}
		return constants.RawDeployment
	case *v1beta1.RouterSpec:
		if s == nil {
			return constants.RawDeployment
		}
		if mode, found := GetDeploymentModeFromAnnotations(s.Annotations); found {
			return mode
		}
		if mode, found := deploymentModeFromSpecField(specMode); found {
			return mode
		}
		return constants.RawDeployment
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

// IsMultiPodComponent reports whether a Component declares a Worker that
// spawns at least one pod. Router is always single-pod.
func IsMultiPodComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) bool {
	if isvc == nil {
		return false
	}
	switch component {
	case v1beta1.EngineComponent:
		return engineSpawnsMultiplePods(isvc.Spec.Engine)
	case v1beta1.DecoderComponent:
		return decoderSpawnsMultiplePods(isvc.Spec.Decoder)
	default:
		// Router and any future single-pod-only Component types.
		return false
	}
}

// engineSpawnsMultiplePods is the Engine-specific tail of
// IsMultiPodComponent. An unset Worker.Size resolves to one worker at
// reconcile time, so a declared Worker is multi-pod unless its size is
// explicitly non-positive; selector tightening then applies only when at
// least two pods exist per Instance.
func engineSpawnsMultiplePods(engine *v1beta1.EngineSpec) bool {
	if engine == nil {
		return false
	}
	return workerSpawnsPods(engine.Worker)
}

// decoderSpawnsMultiplePods is the Decoder-specific tail of
// IsMultiPodComponent. See engineSpawnsMultiplePods for rationale.
func decoderSpawnsMultiplePods(decoder *v1beta1.DecoderSpec) bool {
	if decoder == nil {
		return false
	}
	return workerSpawnsPods(decoder.Worker)
}

func workerSpawnsPods(worker *v1beta1.WorkerSpec) bool {
	return worker != nil && (worker.Size == nil || *worker.Size > 0)
}
