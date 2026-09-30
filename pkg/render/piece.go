package render

import (
	"fmt"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// Piece is everything rendering one component's pods needs beyond the
// service and the merged role spec: the resolved model, runtime and
// accelerator class, the operator's rendering config, and the fine-tuned
// weights loaded for the service.
type Piece struct {
	// Client reads FineTunedWeight objects named by the service's model.
	Client client.Client
	Log    logr.Logger
	// InferenceServiceConfig carries the accelerator resource names and the
	// model cache settings that shape env vars and parallelism.
	InferenceServiceConfig *controllerconfig.InferenceServicesConfig
	DeploymentMode         constants.DeploymentModeType
	BaseModel              *v1beta1.BaseModelSpec
	BaseModelMeta          *metav1.ObjectMeta
	Runtime                *v1beta1.ServingRuntimeSpec
	RuntimeName            string
	SupportedModelFormat   *v1beta1.SupportedModelFormat
	// AcceleratorClass is the class resolved for this component, nil when the
	// service declares no accelerator preference or the runtime names no class.
	AcceleratorClass     *v1beta1.AcceleratorClassSpec
	AcceleratorClassName string
	// Overlays are the resolved spec.model.overlays; nil when none is declared.
	Overlays []isvcutils.ResolvedOverlay

	// The fine-tuned serving fields are filled by ReconcileFineTunedWeights.
	FineTunedServing                  bool
	FineTunedServingWithMergedWeights bool
	FineTunedWeights                  []*v1beta1.FineTunedWeight
}

// ReconcileFineTunedWeights reconciles fine-tuned weights for any component
func ReconcileFineTunedWeights(p *Piece, isvc *v1beta1.InferenceService) error {
	if isvc.Spec.Model == nil {
		return nil
	}
	numOfFineTunedWeights := len(isvc.Spec.Model.FineTunedWeights)
	if numOfFineTunedWeights == 0 {
		return nil
	}

	p.Log.Info("FT serving mode", "Number of fine-tuned weights", numOfFineTunedWeights)
	p.FineTunedServing = true

	// TODO: lift here when start supporting stacked FT serving
	if numOfFineTunedWeights > 1 {
		return fmt.Errorf("stacked fine-tuned serving is not supported yet")
	}

	allFineTunedWeights := make([]*v1beta1.FineTunedWeight, 0)

	for _, fineTunedWeightName := range isvc.Spec.Model.FineTunedWeights {
		fineTunedWeight, err := isvcutils.GetFineTunedWeight(p.Client, fineTunedWeightName)
		if err != nil {
			return err
		}
		allFineTunedWeights = append(allFineTunedWeights, fineTunedWeight)
	}

	// Determine if loading merged fine-tuned weights
	loadingMergedFineTunedWeights, err := isvcutils.LoadingMergedFineTunedWeight(allFineTunedWeights)
	if err != nil {
		p.Log.Error(err, "Failed to determine if loading merged fine-tuned weights")
		return err
	}
	p.FineTunedServingWithMergedWeights = loadingMergedFineTunedWeights
	p.FineTunedWeights = allFineTunedWeights

	return nil
}
