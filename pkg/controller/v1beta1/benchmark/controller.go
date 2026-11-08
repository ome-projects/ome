package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/benchmark/reconcilers/job"
	benchmarkutils "sigs.k8s.io/ome/pkg/controller/v1beta1/benchmark/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

const (
	finalizerName = "benchmarkjob.finalizers"

	// Container and volume names
	benchmarkCommand        = "genai-bench"
	benchmarkSubcommand     = "benchmark"
	outputStorageVolumeName = "benchmark-output-storage"

	// Environment variable names
	envEnableUI          = "ENABLE_UI"
	envHuggingFaceAPIKey = "HUGGINGFACE_API_KEY"

	// Benchmark job states
	statePending   = "Pending"
	stateRunning   = "Running"
	stateCompleted = "Completed"
	stateFailed    = "Failed"

	// Requeue duration when waiting for dependencies
	requeueAfterNotReady = time.Minute
)

// +kubebuilder:rbac:groups=ome.io,resources=benchmarkjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=benchmarkjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=benchmarkjobs/finalizers,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs/finalizers,verbs=get;update;patch

// BenchmarkJobReconciler reconciles a BenchmarkJob object.
type BenchmarkJobReconciler struct {
	client.Client
	Clientset kubernetes.Interface
	Log       logr.Logger
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
}

// Reconcile is the entry point for the reconciliation logic.
func (r *BenchmarkJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("benchmarkjob", req.NamespacedName)

	benchmarkJob, err := r.fetchBenchmarkJob(ctx, req)
	if err != nil {
		if apierr.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling BenchmarkJob", "name", benchmarkJob.Name, "namespace", benchmarkJob.Namespace)

	// Finalizer handling
	if !benchmarkJob.DeletionTimestamp.IsZero() {
		// Object is being deleted
		return r.handleDeletion(ctx, benchmarkJob)
	}

	// Ensure finalizer is present
	if !controllerutil.ContainsFinalizer(benchmarkJob, finalizerName) {
		controllerutil.AddFinalizer(benchmarkJob, finalizerName)
		if err := r.Update(ctx, benchmarkJob); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	// Update status
	if err := r.updateStatus(ctx, benchmarkJob); err != nil {
		r.Recorder.Eventf(benchmarkJob, v1.EventTypeWarning, "StatusUpdateFailed", err.Error())
		return ctrl.Result{}, err
	}

	if benchmarkJob.Spec.Endpoint.InferenceService != nil {
		isvc, err := benchmarkutils.GetInferenceService(ctx, r.Client, benchmarkJob.Spec.Endpoint.InferenceService)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !isvc.Status.IsReady() {
			log.Info("InferenceService is not ready, re-queuing")
			return ctrl.Result{RequeueAfter: requeueAfterNotReady}, nil
		}
	}

	// Build config and pod spec
	config, err := controllerconfig.NewBenchmarkJobConfig(r.Clientset)
	if err != nil {
		return ctrl.Result{}, err
	}

	podSpec, err := r.createPodSpec(ctx, benchmarkJob, config)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Reconcile Job
	meta := r.buildMetadata(benchmarkJob)
	if err := r.reconcileJob(ctx, benchmarkJob, podSpec, meta); err != nil {
		// Attempt status update on failure
		if uErr := r.updateStatus(ctx, benchmarkJob); uErr != nil {
			return ctrl.Result{}, uErr
		}
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// fetchBenchmarkJob retrieves the BenchmarkJob resource from the cluster.
func (r *BenchmarkJobReconciler) fetchBenchmarkJob(ctx context.Context, req ctrl.Request) (*v1beta1.BenchmarkJob, error) {
	benchmarkJob := &v1beta1.BenchmarkJob{}
	if err := r.Get(ctx, req.NamespacedName, benchmarkJob); err != nil {
		return nil, err
	}
	return benchmarkJob, nil
}

// handleDeletion performs cleanup steps when the BenchmarkJob is being deleted.
func (r *BenchmarkJobReconciler) handleDeletion(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob) (ctrl.Result, error) {
	if controllerutil.ContainsFinalizer(benchmarkJob, finalizerName) {
		// Perform cleanup logic here
		controllerutil.RemoveFinalizer(benchmarkJob, finalizerName)
		if err := r.Update(ctx, benchmarkJob); err != nil {
			r.Log.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// reconcileJob creates the Job resource associated with the BenchmarkJob.
func (r *BenchmarkJobReconciler) reconcileJob(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob, podSpec *v1.PodSpec, meta metav1.ObjectMeta) error {
	jobReconciler := job.NewJobReconciler(r.Client, r.Scheme, meta, podSpec)
	if err := controllerutil.SetControllerReference(benchmarkJob, jobReconciler.Job, r.Scheme); err != nil {
		return err
	}

	if err := jobReconciler.Reconcile(ctx); err != nil {
		return errors.Wrapf(err, "failed to reconcile benchmark job")
	}
	return nil
}

// buildMetadata creates the ObjectMeta for associated resources.
func (r *BenchmarkJobReconciler) buildMetadata(benchmarkJob *v1beta1.BenchmarkJob) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      benchmarkJob.Name,
		Namespace: benchmarkJob.Namespace,
		Labels: map[string]string{
			"benchmark": benchmarkJob.Name,
		},
		Annotations: map[string]string{
			"logging-forward": "true",
		},
	}
}

// defaultGPUToleration returns the default GPU toleration for benchmark pods
func defaultGPUToleration() v1.Toleration {
	return v1.Toleration{
		Key:      "nvidia.com/gpu",
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	}
}

// createPodSpec creates a PodSpec for the BenchmarkJob by combining defaults with any user overrides
func (r *BenchmarkJobReconciler) createPodSpec(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob, benchmarkConfig *controllerconfig.BenchmarkJobConfig) (*v1.PodSpec, error) {
	container, err := r.buildDefaultContainer(ctx, benchmarkJob, benchmarkConfig)
	if err != nil {
		return nil, err
	}

	volumes, err := r.buildVolumes(ctx, benchmarkJob, container)
	if err != nil {
		return nil, err
	}

	podSpec := r.buildBasePodSpec(container, volumes)

	// Add node selector for InferenceService base model if specified
	if benchmarkJob.Spec.Endpoint.InferenceService != nil {
		if err := r.addNodeSelectorFromInferenceService(ctx, benchmarkJob, podSpec); err != nil {
			r.Log.Error(err, "Failed to add node selector from InferenceService, continuing without it")
			// Don't fail the whole reconciliation, just log the error
		}
	}

	if benchmarkJob.Spec.PodOverride != nil {
		return r.applyPodOverrides(podSpec, benchmarkJob.Spec.PodOverride)
	}
	return podSpec, nil
}

// addNodeSelectorFromInferenceService adds node affinity based on the InferenceService's base model
func (r *BenchmarkJobReconciler) addNodeSelectorFromInferenceService(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob, podSpec *v1.PodSpec) error {
	ref := benchmarkJob.Spec.Endpoint.InferenceService
	inferenceService, err := benchmarkutils.GetInferenceService(ctx, r.Client, ref)
	if err != nil {
		return err
	}

	baseModelName := benchmarkutils.GetBaseModelName(inferenceService)
	if baseModelName == "" {
		return fmt.Errorf("InferenceService %s/%s has no Model defined", inferenceService.Namespace, inferenceService.Name)
	}

	_, baseModelMeta, err := isvcutils.GetBaseModel(r.Client, baseModelName, inferenceService.Namespace)
	if err != nil {
		return err
	}

	isvcutils.AddNodeSelectorForModelReadyNode(podSpec, baseModelMeta)
	r.Log.Info("Added node selector for benchmark job",
		"baseModel", baseModelMeta.Name,
		"namespace", baseModelMeta.Namespace,
		"benchmarkJob", benchmarkJob.Name)

	return nil
}

// buildDefaultContainer creates the default benchmark container with resources and env vars
func (r *BenchmarkJobReconciler) buildDefaultContainer(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob, config *controllerconfig.BenchmarkJobConfig) (*v1.Container, error) {
	resources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse(config.PodConfig.CPURequest),
			v1.ResourceMemory: resource.MustParse(config.PodConfig.MemoryRequest),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse(config.PodConfig.CPULimit),
			v1.ResourceMemory: resource.MustParse(config.PodConfig.MemoryLimit),
		},
	}

	env := []v1.EnvVar{{Name: envEnableUI, Value: "false"}}
	if ref := benchmarkJob.Spec.HuggingFaceSecretReference; ref != nil && ref.Name != "" {
		env = append(env, v1.EnvVar{
			Name: envHuggingFaceAPIKey,
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: ref.Name},
					Key:                  envHuggingFaceAPIKey,
				},
			},
		})
	}

	cmd, args, err := r.buildBenchmarkCommand(ctx, benchmarkJob)
	if err != nil {
		return nil, err
	}

	return &v1.Container{
		Name:      benchmarkJob.Name,
		Image:     config.PodConfig.Image,
		Resources: resources,
		Env:       env,
		Command:   cmd,
		Args:      args,
	}, nil
}

// buildVolumes creates volumes for the benchmark pod (InferenceService model + PVC storage)
func (r *BenchmarkJobReconciler) buildVolumes(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob, container *v1.Container) ([]v1.Volume, error) {
	var volumes []v1.Volume

	// Add InferenceService model volume if specified
	if benchmarkJob.Spec.Endpoint.InferenceService != nil {
		vol, err := r.buildInferenceServiceVolume(ctx, benchmarkJob, container)
		if err != nil {
			return nil, err
		}
		if vol != nil {
			volumes = append(volumes, *vol)
		}
	}

	// Add PVC volume if storage type is PVC
	pvcVol, pvcMount, err := r.buildPVCVolume(ctx, benchmarkJob)
	if err != nil {
		return nil, err
	}
	if pvcVol != nil {
		volumes = append(volumes, *pvcVol)
		container.VolumeMounts = append(container.VolumeMounts, *pvcMount)
	}

	return volumes, nil
}

// buildInferenceServiceVolume creates volume for the base model from InferenceService
func (r *BenchmarkJobReconciler) buildInferenceServiceVolume(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob, container *v1.Container) (*v1.Volume, error) {
	ref := benchmarkJob.Spec.Endpoint.InferenceService
	inferenceService, err := benchmarkutils.GetInferenceService(ctx, r.Client, ref)
	if err != nil {
		return nil, err
	}

	baseModelName := benchmarkutils.GetBaseModelName(inferenceService)
	if baseModelName == "" {
		return nil, fmt.Errorf("InferenceService %s/%s has no Model defined", inferenceService.Name, inferenceService.Namespace)
	}

	baseModel, _, err := isvcutils.GetBaseModel(r.Client, baseModelName, inferenceService.Namespace)
	if err != nil {
		return nil, err
	}
	if baseModel.Storage == nil || baseModel.Storage.Path == nil {
		return nil, fmt.Errorf("BaseModel %s has no storage path configured", baseModelName)
	}

	benchmarkutils.UpdateVolumeMounts(container, baseModelName, baseModel)

	return &v1.Volume{
		Name: baseModelName,
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{Path: *baseModel.Storage.Path},
		},
	}, nil
}

// buildPVCVolume creates volume and mount for PVC-based output storage
func (r *BenchmarkJobReconciler) buildPVCVolume(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob) (*v1.Volume, *v1.VolumeMount, error) {
	storageType, err := storage.GetStorageType(*benchmarkJob.Spec.OutputLocation.StorageUri)
	if err != nil {
		return nil, nil, fmt.Errorf("error determining storage type: %w", err)
	}

	if storageType != storage.StorageTypePVC {
		return nil, nil, nil
	}

	components, err := storage.ParsePVCStorageURI(*benchmarkJob.Spec.OutputLocation.StorageUri)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing PVC storage URI: %w", err)
	}

	// Verify PVC exists
	pvc := &v1.PersistentVolumeClaim{}
	if err := r.Client.Get(ctx, types.NamespacedName{
		Name:      components.PVCName,
		Namespace: benchmarkJob.Namespace,
	}, pvc); err != nil {
		return nil, nil, fmt.Errorf("PVC %s not found: %w", components.PVCName, err)
	}

	volume := &v1.Volume{
		Name: outputStorageVolumeName,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: components.PVCName,
			},
		},
	}

	mount := &v1.VolumeMount{
		Name:      outputStorageVolumeName,
		MountPath: "/" + components.SubPath,
		SubPath:   components.SubPath,
	}

	return volume, mount, nil
}

// buildBasePodSpec creates the base pod spec with container, volumes and defaults
func (r *BenchmarkJobReconciler) buildBasePodSpec(container *v1.Container, volumes []v1.Volume) *v1.PodSpec {
	return &v1.PodSpec{
		Containers:    []v1.Container{*container},
		Volumes:       volumes,
		Tolerations:   []v1.Toleration{defaultGPUToleration()},
		RestartPolicy: v1.RestartPolicyNever,
	}
}

// applyPodOverrides merges user-provided overrides into the pod spec
func (r *BenchmarkJobReconciler) applyPodOverrides(podSpec *v1.PodSpec, override *v1beta1.PodOverride) (*v1.PodSpec, error) {
	// Merge container
	mergedContainer, err := r.mergeContainer(&podSpec.Containers[0], override)
	if err != nil {
		return nil, err
	}

	// Create pod spec with merged container
	basePodSpec := &v1.PodSpec{
		Containers:    []v1.Container{*mergedContainer},
		Volumes:       podSpec.Volumes,
		Tolerations:   podSpec.Tolerations,
		Affinity:      podSpec.Affinity,
		NodeSelector:  podSpec.NodeSelector,
		RestartPolicy: v1.RestartPolicyNever,
	}

	// Merge pod-level overrides
	return r.mergePodSpec(basePodSpec, override)
}

// strategicMergePatch applies a strategic merge patch and returns the merged result.
func strategicMergePatch[T any](base, override T, dataStruct T) (T, error) {
	var zero T
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return zero, err
	}

	overrideJSON, err := json.Marshal(override)
	if err != nil {
		return zero, err
	}

	mergedJSON, err := strategicpatch.StrategicMergePatch(baseJSON, overrideJSON, dataStruct)
	if err != nil {
		return zero, err
	}

	var merged T
	if err := json.Unmarshal(mergedJSON, &merged); err != nil {
		return zero, err
	}
	return merged, nil
}

// mergeContainer applies container-level overrides using strategic merge patch
func (r *BenchmarkJobReconciler) mergeContainer(base *v1.Container, override *v1beta1.PodOverride) (*v1.Container, error) {
	overrideContainer := v1.Container{
		Name:         benchmarkCommand,
		Image:        override.Image,
		Env:          override.Env,
		EnvFrom:      override.EnvFrom,
		VolumeMounts: override.VolumeMounts,
	}
	if override.Resources != nil {
		overrideContainer.Resources = *override.Resources
	}

	merged, err := strategicMergePatch(*base, overrideContainer, v1.Container{})
	if err != nil {
		return nil, err
	}
	return &merged, nil
}

// mergePodSpec applies pod-level overrides using strategic merge patch
func (r *BenchmarkJobReconciler) mergePodSpec(base *v1.PodSpec, override *v1beta1.PodOverride) (*v1.PodSpec, error) {
	overridePodSpec := v1.PodSpec{
		Volumes:      override.Volumes,
		Affinity:     override.Affinity,
		NodeSelector: override.NodeSelector,
		Tolerations:  override.Tolerations,
	}

	merged, err := strategicMergePatch(*base, overridePodSpec, v1.PodSpec{})
	if err != nil {
		return nil, err
	}

	// Preserve the merged container (strategic merge doesn't handle this well)
	merged.Containers = base.Containers
	return &merged, nil
}

// buildBenchmarkCommand constructs the command line arguments for the benchmark container.
func (r *BenchmarkJobReconciler) buildBenchmarkCommand(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob) ([]string, []string, error) {
	command := []string{benchmarkCommand}

	inferenceArgs, err := benchmarkutils.BuildInferenceServiceArgs(ctx, r.Client, benchmarkJob.Spec.Endpoint, benchmarkJob.Namespace)
	if err != nil {
		return nil, nil, err
	}

	args := []string{
		benchmarkSubcommand,
		"--api-backend", inferenceArgs["--api-backend"],
		"--api-base", inferenceArgs["--api-base"],
		"--api-model-name", inferenceArgs["--api-model-name"],
		"--task", benchmarkJob.Spec.Task,
		"--max-time-per-run", strconv.Itoa(*benchmarkJob.Spec.MaxTimePerIteration),
		"--max-requests-per-run", strconv.Itoa(*benchmarkJob.Spec.MaxRequestsPerIteration),
	}

	// Add optional args only if present
	if v := inferenceArgs["--api-key"]; v != "" {
		args = append(args, "--api-key", v)
	}
	if v := inferenceArgs["--model-tokenizer"]; v != "" {
		args = append(args, "--model-tokenizer", v)
	}

	// Add traffic scenarios
	for _, scenario := range benchmarkJob.Spec.TrafficScenarios {
		args = append(args, "--traffic-scenario", scenario)
	}

	// Add concurrency levels
	for _, concurrency := range benchmarkJob.Spec.NumConcurrency {
		args = append(args, "--num-concurrency", fmt.Sprintf("%d", concurrency))
	}

	// Add experiment folder name
	if benchmarkJob.Spec.ResultFolderName != nil {
		args = append(args, "--experiment-folder-name", *benchmarkJob.Spec.ResultFolderName)
	}

	// Add server metadata
	if benchmarkJob.Spec.ServiceMetadata != nil {
		args = append(args,
			"--server-engine", benchmarkJob.Spec.ServiceMetadata.Engine,
			"--server-gpu-type", benchmarkJob.Spec.ServiceMetadata.GpuType,
			"--server-version", benchmarkJob.Spec.ServiceMetadata.Version,
			"--server-gpu-count", fmt.Sprintf("%d", benchmarkJob.Spec.ServiceMetadata.GpuCount),
		)
	}

	storageArgs, err := benchmarkutils.BuildStorageArgs(benchmarkJob.Spec.OutputLocation)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build storage args: %w", err)
	}
	args = append(args, storageArgs...)

	return command, args, nil
}

// updateStatus updates the BenchmarkJob status based on the underlying Job's state.
func (r *BenchmarkJobReconciler) updateStatus(ctx context.Context, benchmarkJob *v1beta1.BenchmarkJob) error {
	k8sJob := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: benchmarkJob.Namespace,
		Name:      benchmarkJob.Name,
	}, k8sJob)

	if apierr.IsNotFound(err) {
		r.setStatusPending(benchmarkJob)
	} else if err != nil {
		return err
	} else {
		r.syncStatusFromJob(benchmarkJob, k8sJob)
	}

	return r.Status().Update(ctx, benchmarkJob)
}

// setStatusPending sets the benchmark job status to pending (no underlying job exists yet).
func (r *BenchmarkJobReconciler) setStatusPending(benchmarkJob *v1beta1.BenchmarkJob) {
	if benchmarkJob.Status.State == statePending {
		return
	}
	now := metav1.Now()
	benchmarkJob.Status.State = statePending
	benchmarkJob.Status.StartTime = nil
	benchmarkJob.Status.CompletionTime = nil
	benchmarkJob.Status.FailureMessage = ""
	benchmarkJob.Status.LastReconcileTime = &now
}

// syncStatusFromJob updates the benchmark job status based on the k8s Job's conditions.
func (r *BenchmarkJobReconciler) syncStatusFromJob(benchmarkJob *v1beta1.BenchmarkJob, k8sJob *batchv1.Job) {
	state, completionTime, failureMsg := r.parseJobStatus(k8sJob)

	if benchmarkJob.Status.StartTime == nil && k8sJob.Status.StartTime != nil {
		benchmarkJob.Status.StartTime = k8sJob.Status.StartTime
	}

	if benchmarkJob.Status.State == state {
		return
	}

	now := metav1.Now()
	benchmarkJob.Status.State = state
	benchmarkJob.Status.LastReconcileTime = &now

	switch state {
	case stateFailed, stateCompleted:
		benchmarkJob.Status.CompletionTime = completionTime
		benchmarkJob.Status.FailureMessage = failureMsg
	case stateRunning:
		benchmarkJob.Status.CompletionTime = nil
		benchmarkJob.Status.FailureMessage = ""
	}
}

// parseJobStatus extracts the state, completion time, and failure message from a Job.
func (r *BenchmarkJobReconciler) parseJobStatus(k8sJob *batchv1.Job) (state string, completionTime *metav1.Time, failureMsg string) {
	// Check for failure first (takes precedence)
	for _, cond := range k8sJob.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == v1.ConditionTrue {
			return stateFailed, &cond.LastTransitionTime, cond.Message
		}
	}

	// Check for completion
	if k8sJob.Status.CompletionTime != nil {
		return stateCompleted, k8sJob.Status.CompletionTime, ""
	}
	for _, cond := range k8sJob.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == v1.ConditionTrue {
			return stateCompleted, &cond.LastTransitionTime, ""
		}
	}

	// Default to running
	return stateRunning, nil, ""
}

// SetupWithManager sets up the controller with the Manager.
func (r *BenchmarkJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.BenchmarkJob{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
