package snapshot

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

type workloadComponentSpec struct {
	ctype   v1beta1.ComponentType
	present bool
	mode    constants.DeploymentModeType
}

// workloadComponentSpecs observes the public InferenceService deployment-mode
// contract. Alfred does not call the controller's workload-generation helpers.
func workloadComponentSpecs(isvc *v1beta1.InferenceService) []workloadComponentSpec {
	engine, decoder, router := isvc.Spec.Engine, isvc.Spec.Decoder, isvc.Spec.Router
	components := []workloadComponentSpec{
		{v1beta1.EngineComponent, engine != nil, constants.RawDeployment},
		{v1beta1.DecoderComponent, decoder != nil, constants.RawDeployment},
		{v1beta1.RouterComponent, router != nil, constants.RawDeployment},
	}
	if engine == nil {
		// Preserve the existing fallback for an invalid/transient service:
		// keep all components raw so their pods remain visible.
		return components
	}
	mode := isvc.Spec.DeploymentMode
	components[0].mode = observedDeploymentMode(engine.Annotations, mode, engine.Leader != nil || engine.Worker != nil)
	if decoder != nil {
		components[1].mode = observedDeploymentMode(decoder.Annotations, mode, decoder.Leader != nil || decoder.Worker != nil)
	}
	if router != nil {
		components[2].mode = observedDeploymentMode(router.Annotations, mode, false)
	}
	return components
}

// observedDeploymentMode follows the API's precedence: component annotation,
// typed service field, leader/worker shape, then RawDeployment. Invalid explicit
// values fall through, matching observation of older objects and version skew.
func observedDeploymentMode(annotations map[string]string, specMode *constants.DeploymentModeType, hasLeaderOrWorker bool) constants.DeploymentModeType {
	if mode := constants.DeploymentModeType(annotations[constants.DeploymentMode]); mode.IsValid() {
		return mode
	}
	if specMode != nil && specMode.IsValid() {
		return *specMode
	}
	if hasLeaderOrWorker {
		return constants.OMENative
	}
	return constants.RawDeployment
}
