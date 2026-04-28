package components

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	knservingv1 "knative.dev/serving/pkg/apis/serving/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/status"
	isvcutils "github.com/sgl-project/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/sgl-project/ome/pkg/utils"
)

var _ Component = &Engine{}
var _ ComponentConfig = &Engine{}

// Engine reconciles resources for the engine component
type Engine struct {
	BaseComponentFields
	engineSpec           *v1beta1.EngineSpec
	deploymentReconciler *common.DeploymentReconciler
	podSpecReconciler    *common.PodSpecReconciler
}

// NewEngine creates a new Engine component instance
func NewEngine(
	client client.Client,
	clientset kubernetes.Interface,
	scheme *runtime.Scheme,
	inferenceServiceConfig *controllerconfig.InferenceServicesConfig,
	deploymentMode constants.DeploymentModeType,
	baseModel *v1beta1.BaseModelSpec,
	baseModelMeta *metav1.ObjectMeta,
	engineSpec *v1beta1.EngineSpec,
	runtime *v1beta1.ServingRuntimeSpec,
	runtimeName string,
	supportedModelFormat *v1beta1.SupportedModelFormat,
	acceleratorClass *v1beta1.AcceleratorClassSpec,
	acceleratorClassName string,
) Component {
	logger := ctrl.Log.WithName("EngineReconciler")
	statusManager := status.NewStatusReconciler(clientset).WithLogger(logger.WithName("InferenceServiceStatus"))
	base := BaseComponentFields{
		Client:                 client,
		Clientset:              clientset,
		Scheme:                 scheme,
		InferenceServiceConfig: inferenceServiceConfig,
		DeploymentMode:         deploymentMode,
		BaseModel:              baseModel,
		BaseModelMeta:          baseModelMeta,
		Runtime:                runtime,
		RuntimeName:            runtimeName,
		StatusManager:          statusManager,
		Log:                    logger,
		AcceleratorClass:       acceleratorClass,
		AcceleratorClassName:   acceleratorClassName,
		SupportedModelFormat:   supportedModelFormat,
	}

	return &Engine{
		BaseComponentFields: base,
		engineSpec:          engineSpec,
		deploymentReconciler: &common.DeploymentReconciler{
			Client:        client,
			Clientset:     clientset,
			Scheme:        scheme,
			StatusManager: base.StatusManager,
			Log:           base.Log,
		},
		podSpecReconciler: &common.PodSpecReconciler{
			Log: base.Log,
		},
	}
}

// Reconcile implements the Component interface for Engine
func (e *Engine) Reconcile(isvc *v1beta1.InferenceService) (ctrl.Result, error) {
	e.Log.Info("Reconciling engine component", "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Validate engine spec
	if e.engineSpec == nil {
		return ctrl.Result{}, errors.New("engine spec is nil")
	}

	// Reconcile fine-tuned weights if specified
	if isvc.Spec.Model != nil && len(isvc.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&e.BaseComponentFields, isvc); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to reconcile fine-tuned weights")
		}
	}

	// Reconcile object metadata
	objectMeta, err := e.reconcileObjectMeta(isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile object metadata")
	}

	// Reconcile pod spec
	podSpec, err := e.reconcilePodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile pod spec")
	}

	// Reconcile worker pod spec if needed
	workerPodSpec, err := e.reconcileWorkerPodSpec(isvc, &objectMeta)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile worker pod spec")
	}

	// Get worker size
	size := e.getWorkerSize()

	// Reconcile deployment based on deployment mode
	if result, err := e.reconcileDeployment(isvc, objectMeta, podSpec, size, workerPodSpec); err != nil {
		return result, err
	}

	// Update engine status
	if err := e.updateEngineStatus(isvc, objectMeta); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// getWorkerSize returns the worker size for multi-node deployments
func (e *Engine) getWorkerSize() int {
	var size int

	// Prioritize sizes in order: Engine.Worker -> default
	switch {
	case e.engineSpec.Worker != nil && e.engineSpec.Worker.Size != nil:
		size = *e.engineSpec.Worker.Size
	default:
		size = 0 // Default value
	}

	return size
}

// reconcileDeployment manages the deployment logic for different deployment modes
func (e *Engine) reconcileDeployment(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, workerSize int, workerPodSpec *v1.PodSpec) (ctrl.Result, error) {
	switch e.DeploymentMode {
	case constants.RawDeployment:
		return e.deploymentReconciler.ReconcileRawDeployment(isvc, objectMeta, podSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent)
	case constants.MultiNode:
		return e.deploymentReconciler.ReconcileMultiNodeDeployment(isvc, objectMeta, podSpec, workerSize, workerPodSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent)
	case constants.MultiNodeRayVLLM:
		return e.deploymentReconciler.ReconcileMultiNodeRayVLLMDeployment(isvc, objectMeta, podSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent)
	case constants.Serverless:
		return e.deploymentReconciler.ReconcileKnativeDeployment(isvc, objectMeta, podSpec, &e.engineSpec.ComponentExtensionSpec, v1beta1.EngineComponent)
	default:
		return ctrl.Result{}, errors.New("invalid deployment mode for engine")
	}
}

// updateEngineStatus updates the status of the engine
func (e *Engine) updateEngineStatus(isvc *v1beta1.InferenceService, objectMeta metav1.ObjectMeta) error {
	return UpdateComponentStatus(&e.BaseComponentFields, isvc, v1beta1.EngineComponent, objectMeta, e.getPodLabelInfo)
}

// getPodLabelInfo returns the pod label key and value based on the deployment mode
func (e *Engine) getPodLabelInfo(rawDeployment bool, objectMeta metav1.ObjectMeta, statusSpec v1beta1.ComponentStatusSpec) (string, string) {
	if rawDeployment {
		return constants.RawDeploymentAppLabel, constants.GetRawServiceLabel(objectMeta.Name)
	}
	return constants.RevisionLabel, statusSpec.LatestCreatedRevision
}

// reconcileObjectMeta creates the object metadata for the engine component
func (e *Engine) reconcileObjectMeta(isvc *v1beta1.InferenceService) (metav1.ObjectMeta, error) {
	engineName, err := e.determineEngineName(isvc)
	if err != nil {
		return metav1.ObjectMeta{}, err
	}

	annotations, err := e.processAnnotations(isvc)
	if err != nil {
		return metav1.ObjectMeta{
			Name:      engineName,
			Namespace: isvc.Namespace,
		}, err
	}

	labels, err := e.processLabels(isvc)
	if err != nil {
		return metav1.ObjectMeta{
			Name:        engineName,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
		}, err
	}

	return metav1.ObjectMeta{
		Name:        engineName,
		Namespace:   isvc.Namespace,
		Labels:      labels,
		Annotations: annotations,
	}, nil
}

// processAnnotations processes the annotations for the engine
func (e *Engine) processAnnotations(isvc *v1beta1.InferenceService) (map[string]string, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})

	// Merge with engine annotations
	mergedAnnotations := annotations
	if e.engineSpec != nil {
		engineAnnotations := e.engineSpec.Annotations
		mergedAnnotations = utils.Union(annotations, engineAnnotations)
	}

	// Use common function for base annotations processing
	processedAnnotations, err := ProcessBaseAnnotations(&e.BaseComponentFields, isvc, mergedAnnotations)
	if err != nil {
		return nil, err
	}

	return processedAnnotations, nil
}

// processLabels processes the labels for the engine
func (e *Engine) processLabels(isvc *v1beta1.InferenceService) (map[string]string, error) {
	mergedLabels := isvc.Labels
	if e.engineSpec != nil {
		engineLabels := e.engineSpec.Labels
		mergedLabels = utils.Union(isvc.Labels, engineLabels)
	}

	// Use common function for base labels processing
	return ProcessBaseLabels(&e.BaseComponentFields, isvc, v1beta1.EngineComponent, mergedLabels)
}

// determineEngineName determines the name of the engine service
func (e *Engine) determineEngineName(isvc *v1beta1.InferenceService) (string, error) {
	// For engine, we'll use a pattern similar to predictor but with "-engine" suffix
	defaultEngineName := isvc.Name + "-engine"
	existingName := defaultEngineName

	if e.DeploymentMode == constants.RawDeployment {
		existing := &v1.Service{}
		if err := e.Client.Get(context.TODO(), types.NamespacedName{Name: defaultEngineName, Namespace: isvc.Namespace}, existing); err == nil {
			return existingName, nil
		}
	} else {
		existing := &knservingv1.Service{}
		if err := e.Client.Get(context.TODO(), types.NamespacedName{Name: defaultEngineName, Namespace: isvc.Namespace}, existing); err == nil {
			return existingName, nil
		}
	}

	// If the default name doesn't exist, use it
	return defaultEngineName, nil
}

// reconcilePodSpec creates the pod spec for the engine component
func (e *Engine) reconcilePodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Get the appropriate pod spec and runner based on deployment mode
	deploymentMode := isvcutils.DetermineEngineDeploymentMode(e.engineSpec)

	var basePodSpec v1beta1.PodSpec
	var runnerSpec *v1beta1.RunnerSpec

	switch deploymentMode {
	case constants.MultiNode, constants.MultiNodeRayVLLM:
		// For multi-node, use leader spec
		if e.engineSpec.Leader != nil {
			basePodSpec = e.engineSpec.Leader.PodSpec
			runnerSpec = e.engineSpec.Leader.Runner
		} else {
			// Fallback to engine spec if leader is not defined
			basePodSpec = e.engineSpec.PodSpec
			runnerSpec = e.engineSpec.Runner
		}
	default:
		// For raw deployment and serverless, use engine spec
		basePodSpec = e.engineSpec.PodSpec
		runnerSpec = e.engineSpec.Runner
	}
	if runnerSpec != nil {
		UpdateEnvVariables(&e.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		UpdateVolumeMounts(&e.BaseComponentFields, isvc, &runnerSpec.Container, objectMeta)
		MergeEngineResources(&e.BaseComponentFields, isvc, &runnerSpec.Container)
		MergeRuntimeArgumentsOverride(&e.BaseComponentFields, &runnerSpec.Container)
		if e.AcceleratorClass == nil {
			e.setParallelismEnvVarForEngine(&runnerSpec.Container, e.getWorkerSize())
		}
	}

	// Use common pod spec reconciler for base logic
	podSpec, err := e.podSpecReconciler.ReconcilePodSpec(isvc, objectMeta, &basePodSpec, runnerSpec)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&e.BaseComponentFields, isvc, podSpec, objectMeta)
	UpdatePodSpecNodeSelector(&e.BaseComponentFields, isvc, podSpec, v1beta1.EngineComponent)
	UpdateEngineAffinity(&e.BaseComponentFields, isvc, podSpec)

	e.Log.Info("Engine PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return podSpec, nil
}

// reconcileWorkerPodSpec reconciles the worker pod spec for multi-node deployments
func (e *Engine) reconcileWorkerPodSpec(isvc *v1beta1.InferenceService, objectMeta *metav1.ObjectMeta) (*v1.PodSpec, error) {
	// Return nil if no worker spec is defined
	if e.engineSpec.Worker == nil {
		return nil, nil
	}

	// Get worker runner spec if available
	var workerRunner *v1beta1.RunnerSpec
	if e.engineSpec.Worker != nil {
		workerRunner = e.engineSpec.Worker.Runner
		if workerRunner != nil {
			UpdateVolumeMounts(&e.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			UpdateEnvVariables(&e.BaseComponentFields, isvc, &workerRunner.Container, objectMeta)
			MergeEngineResources(&e.BaseComponentFields, isvc, &workerRunner.Container)
			MergeRuntimeArgumentsOverride(&e.BaseComponentFields, &workerRunner.Container)
			if e.AcceleratorClass == nil {
				e.setParallelismEnvVarForEngine(&workerRunner.Container, e.getWorkerSize())
			}
		}
	}

	// Use common reconciler for worker pod spec
	workerPodSpec, err := e.podSpecReconciler.ReconcileWorkerPodSpec(isvc, objectMeta, &e.engineSpec.Worker.PodSpec, workerRunner)
	if err != nil {
		return nil, err
	}
	UpdatePodSpecVolumes(&e.BaseComponentFields, isvc, workerPodSpec, objectMeta)
	UpdatePodSpecNodeSelector(&e.BaseComponentFields, isvc, workerPodSpec, v1beta1.EngineComponent)
	UpdateEngineAffinity(&e.BaseComponentFields, isvc, workerPodSpec)
	e.Log.Info("Engine Worker PodSpec updated", "inference service", isvc.Name, "namespace", isvc.Namespace)
	return workerPodSpec, nil
}

// setParallelismEnvVarForEngine calculates and sets the PARALLELISM_SIZE environment variable for the engine's container.
func (e *Engine) setParallelismEnvVarForEngine(container *v1.Container, workerReplicas int) {
	if container == nil || e.engineSpec == nil {
		e.Log.Info("Cannot set parallelism: container or engineSpec is nil")
		return
	}

	numGPUsPerPod := int64(isvcutils.GetGpuCountFromContainer(container))
	numLeaders := int64(1) // at least one leader/pod
	numWorkers := int64(workerReplicas)

	// Only proceed if there are GPUs
	if numGPUsPerPod > 0 {
		parallelismSize := numGPUsPerPod * (numLeaders + numWorkers)
		if parallelismSize > 0 {
			envVar := v1.EnvVar{Name: constants.ParallelismSizeEnvVarKey, Value: strconv.FormatInt(parallelismSize, 10)}
			isvcutils.UpdateEnvVars(container, &envVar)
			e.Log.Info("Added parallelism env variable to engine container", "value", parallelismSize, "containerName", container.Name)
		} else {
			e.Log.Info("Calculated parallelism is zero, not adding env var", "containerName", container.Name)
		}
	} else {
		e.Log.Info("Conditions not met for parallelism (no GPUs or no leaders/workers)", "containerName", container.Name, "gpus", numGPUsPerPod, "leaders", numLeaders, "workers", numWorkers)
	}
}

// GetComponentType implements ComponentConfig interface
func (e *Engine) GetComponentType() v1beta1.ComponentType {
	return v1beta1.EngineComponent
}

// GetComponentSpec implements ComponentConfig interface
func (e *Engine) GetComponentSpec() *v1beta1.ComponentExtensionSpec {
	if e.engineSpec == nil {
		return nil
	}
	return &e.engineSpec.ComponentExtensionSpec
}

// GetServiceSuffix implements ComponentConfig interface
func (e *Engine) GetServiceSuffix() string {
	return "-engine"
}

// ValidateSpec implements ComponentConfig interface
func (e *Engine) ValidateSpec() error {
	if e.engineSpec == nil {
		return errors.New("engine spec is nil")
	}
	// Add more validation logic as needed
	return nil
}
