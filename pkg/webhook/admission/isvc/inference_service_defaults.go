package isvc

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

var mutatorLogger = logf.Log.WithName("inferenceservice-v1beta1-mutating-webhook")

// InferenceServiceDefaulter wires the webhook framework into the
// DefaultInferenceService entrypoint. The kubebuilder markers below
// suppress DeepCopy + OpenAPI codegen since this is a stateless
// wired-once helper, not a persisted CR.
// +kubebuilder:object:generate=false
// +k8s:openapi-gen=false
type InferenceServiceDefaulter struct {
	Client    client.Client
	ClientSet kubernetes.Interface
}

// +kubebuilder:webhook:path=/mutate-ome-io-v1beta1-inferenceservice,mutating=true,failurePolicy=fail,groups=ome.io,resources=inferenceservices,verbs=create;update,versions=v1beta1,name=inferenceservice.ome-webhook-server.defaulter
var _ admission.Defaulter[*v1beta1.InferenceService] = &InferenceServiceDefaulter{}

func (d *InferenceServiceDefaulter) Default(ctx context.Context, isvc *v1beta1.InferenceService) error {
	deployConfig, err := controllerconfig.NewDeployConfig(d.ClientSet)
	if err != nil {
		mutatorLogger.Error(err, "Failed to get deploy config")
		return err
	}
	return DefaultInferenceService(ctx, d.Client, isvc, deployConfig)
}

// DefaultInferenceService sets default values on the InferenceService.
func DefaultInferenceService(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, deployConfig *controllerconfig.DeployConfig) error {
	if isvc.ObjectMeta.Annotations == nil {
		isvc.ObjectMeta.Annotations = map[string]string{}
	}

	if _, modeExists := isvc.ObjectMeta.Annotations[constants.DeploymentMode]; !modeExists {
		// Engine + Decoder ⇒ PDDisaggregated.
		// Engine + Leader + Worker (size>0) ⇒ OMENative (native multi-node).
		// Otherwise fall back to deployConfig.DefaultDeploymentMode
		// (only RawDeployment is honored — other modes require an
		// explicit operator annotation).
		if isvc.Spec.Engine != nil && isvc.Spec.Decoder != nil {
			isvc.ObjectMeta.Annotations[constants.DeploymentMode] = string(constants.PDDisaggregated)
		} else if isvc.Spec.Engine != nil {
			if isvc.Spec.Engine.Leader != nil &&
				isvc.Spec.Engine.Worker != nil &&
				isvc.Spec.Engine.Worker.Size != nil &&
				*isvc.Spec.Engine.Worker.Size > 0 {
				isvc.ObjectMeta.Annotations[constants.DeploymentMode] = string(constants.OMENative)
			} else if deployConfig != nil && deployConfig.DefaultDeploymentMode == string(constants.RawDeployment) {
				isvc.ObjectMeta.Annotations[constants.DeploymentMode] = deployConfig.DefaultDeploymentMode
			}
		} else if deployConfig != nil && deployConfig.DefaultDeploymentMode == string(constants.RawDeployment) {
			isvc.ObjectMeta.Annotations[constants.DeploymentMode] = deployConfig.DefaultDeploymentMode
		}
	}

	// resolvedMode is the effective deployment mode after the ObjectMeta
	// resolution above: the canonical top-level ome.io/deploymentMode
	// annotation (set explicitly by the operator or by the leader+worker
	// heuristic), falling back to spec.DeploymentMode. Components must
	// resolve OMENative from it so a multi-node engine declared the normal
	// way (top-level annotation, no spec.deploymentMode) still receives
	// defaultOMENativeComponent — including the rollout budget defaults
	// that keep its rollouts capped.
	resolvedMode := isvc.Spec.DeploymentMode
	if m := isvc.ObjectMeta.Annotations[constants.DeploymentMode]; m != "" {
		mm := constants.DeploymentModeType(m)
		resolvedMode = &mm
	}

	// Replica defaults are config-driven (deploy.replicas in the
	// inferenceservice-config ConfigMap). An unconfigured value disables
	// defaulting of that field: the spec is stored as authored.
	var replicas *controllerconfig.ReplicasDefaultsConfig
	var gracePeriod *int64
	var minReadySeconds *int32
	var updateStrategy *controllerconfig.UpdateStrategyDefaultsConfig
	if deployConfig != nil {
		replicas = deployConfig.Replicas
		gracePeriod = deployConfig.TerminationGracePeriodSeconds
		minReadySeconds = deployConfig.MinReadySeconds
		updateStrategy = deployConfig.UpdateStrategy
	}

	if isvc.Spec.Engine != nil {
		defaultEngine(isvc.Spec.Engine, resolvedMode, replicas, gracePeriod, updateStrategy.ForComponent(workloadtypes.ComponentEngine))
		defaultOMENativeMinReadySeconds(&isvc.Spec.Engine.ComponentExtensionSpec, resolvedMode, minReadySeconds)
	}
	if isvc.Spec.Decoder != nil {
		defaultDecoder(isvc.Spec.Decoder, resolvedMode, replicas, gracePeriod, updateStrategy.ForComponent(workloadtypes.ComponentDecoder))
		defaultOMENativeMinReadySeconds(&isvc.Spec.Decoder.ComponentExtensionSpec, resolvedMode, minReadySeconds)
	}
	if isvc.Spec.Router != nil {
		defaultRouter(isvc.Spec.Router, resolvedMode, replicas, gracePeriod, updateStrategy.ForComponent(workloadtypes.ComponentRouter))
		defaultOMENativeMinReadySeconds(&isvc.Spec.Router.ComponentExtensionSpec, resolvedMode, minReadySeconds)
	}
	// Rollout pacing/structure defaults are applied at runtime by the resolve
	// layer (coordination.ResolveGroups), not at admission.
	return nil
}

func defaultEngine(engine *v1beta1.EngineSpec, specMode *constants.DeploymentModeType, replicas *controllerconfig.ReplicasDefaultsConfig, gracePeriod *int64, updateStrategy *controllerconfig.ComponentUpdateStrategyDefaults) {
	defaultReplicaBounds(&engine.ComponentExtensionSpec, replicas.Min(), replicas.EngineMax())
	defaultTerminationGracePeriod(&engine.PodSpec, gracePeriod)
	defaultWorkerSize(engine.Leader, engine.Worker)
	// A raw spec without a runner shape may inherit Leader/Worker from its
	// runtime.
	shape := podShapeUnresolved
	if engineIsMultiPod(engine) {
		shape = podShapeMulti
	}
	defaultOMENativeComponent(&engine.ComponentExtensionSpec, shape, specMode, updateStrategy)
}

func defaultDecoder(decoder *v1beta1.DecoderSpec, specMode *constants.DeploymentModeType, replicas *controllerconfig.ReplicasDefaultsConfig, gracePeriod *int64, updateStrategy *controllerconfig.ComponentUpdateStrategyDefaults) {
	defaultReplicaBounds(&decoder.ComponentExtensionSpec, replicas.Min(), replicas.DecoderMax())
	defaultTerminationGracePeriod(&decoder.PodSpec, gracePeriod)
	defaultWorkerSize(decoder.Leader, decoder.Worker)
	// A raw spec without a runner shape may inherit Leader/Worker from its
	// runtime.
	shape := podShapeUnresolved
	if decoderIsMultiPod(decoder) {
		shape = podShapeMulti
	}
	defaultOMENativeComponent(&decoder.ComponentExtensionSpec, shape, specMode, updateStrategy)
}

// defaultReplicaBounds fills unset replica bounds from the configured
// admission defaults. A nil default leaves the corresponding field as
// authored (no defaulting) — the values come only from configuration,
// never from literals baked into the binary. MaxReplicas is a
// non-pointer int, so 0 means "not set".
func defaultReplicaBounds(ext *v1beta1.ComponentExtensionSpec, defaultMin, defaultMax *int) {
	if ext.MinReplicas == nil && defaultMin != nil {
		minReplicas := *defaultMin
		ext.MinReplicas = &minReplicas
	}
	if ext.MaxReplicas == 0 && defaultMax != nil {
		ext.MaxReplicas = defaultedMaxReplicas(*defaultMax, ext.MinReplicas)
	}
}

// defaultedMaxReplicas returns the value to stamp for an omitted
// MaxReplicas: the configured default, raised to MinReplicas when the
// operator authored a larger floor. Filling an unset max below an
// explicit min would manufacture a min>max conflict that the
// replica-bounds validator rejects on create and every later update.
// An explicitly authored MaxReplicas is never adjusted.
func defaultedMaxReplicas(configuredDefault int, minReplicas *int) int {
	if minReplicas != nil && *minReplicas > configuredDefault {
		return *minReplicas
	}
	return configuredDefault
}

// defaultTerminationGracePeriod fills an unset component
// terminationGracePeriodSeconds from the configured admission default. A nil
// default leaves the field as authored, and an authored value always wins, so
// a component that needs a longer drain than the cluster default can keep it.
func defaultTerminationGracePeriod(podSpec *v1beta1.PodSpec, defaultSeconds *int64) {
	if podSpec == nil || defaultSeconds == nil {
		return
	}
	if podSpec.TerminationGracePeriodSeconds != nil {
		return
	}
	seconds := *defaultSeconds
	podSpec.TerminationGracePeriodSeconds = &seconds
}

// defaultOMENativeMinReadySeconds fills an unset lifecycle.minReadySeconds on
// an OMENative-resolving component from the configured admission default. A
// nil default stamps nothing (the component keeps "Available as soon as
// Ready"), an authored value always wins, and non-OMENative components are
// left untouched because the field has no meaning for them. The stamp lands
// on the InferenceService, which overrides the ServingRuntime in the later
// spec merge, so a runtime-authored value yields to the cluster default the
// same way terminationGracePeriodSeconds does.
func defaultOMENativeMinReadySeconds(ext *v1beta1.ComponentExtensionSpec, specMode *constants.DeploymentModeType, defaultSeconds *int32) {
	if ext == nil || defaultSeconds == nil {
		return
	}
	if !componentResolvesToOMENative(ext.Annotations, specMode) {
		return
	}
	if ext.Lifecycle == nil {
		ext.Lifecycle = &v1beta1.LifecycleSpec{}
	}
	if ext.Lifecycle.MinReadySeconds != nil {
		return
	}
	seconds := *defaultSeconds
	ext.Lifecycle.MinReadySeconds = &seconds
}

// defaultWorkerSize sets Worker.Size=1 only for a declared Leader+Worker pair.
// Explicit sizes and one-sided shapes are preserved for validation.
func defaultWorkerSize(leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) {
	if leader == nil || worker == nil {
		return
	}
	if worker.Size != nil {
		return
	}
	size := 1
	worker.Size = &size
}

func defaultRouter(router *v1beta1.RouterSpec, specMode *constants.DeploymentModeType, replicas *controllerconfig.ReplicasDefaultsConfig, gracePeriod *int64, updateStrategy *controllerconfig.ComponentUpdateStrategyDefaults) {
	defaultReplicaBounds(&router.ComponentExtensionSpec, replicas.Min(), replicas.RouterMax())
	defaultTerminationGracePeriod(&router.PodSpec, gracePeriod)
	// Router has no Leader/Worker; always single-pod.
	defaultOMENativeComponent(&router.ComponentExtensionSpec, podShapeSingle, specMode, updateStrategy)
}

// engineIsMultiPod reports whether Engine declares a complete Leader+Worker
// pair with a positive Worker.Size.
func engineIsMultiPod(engine *v1beta1.EngineSpec) bool {
	if engine == nil {
		return false
	}
	return engine.Leader != nil && engine.Worker != nil && engine.Worker.Size != nil && *engine.Worker.Size > 0
}

// decoderIsMultiPod reports whether Decoder declares a complete Leader+Worker
// pair with a positive Worker.Size.
func decoderIsMultiPod(decoder *v1beta1.DecoderSpec) bool {
	if decoder == nil {
		return false
	}
	return decoder.Leader != nil && decoder.Worker != nil && decoder.Worker.Size != nil && *decoder.Worker.Size > 0
}

type componentPodShape uint8

const (
	podShapeUnresolved componentPodShape = iota
	podShapeSingle
	podShapeMulti
)

// defaultOMENativeComponent fills lifecycle defaults for an OMENative Component.
// Shape-dependent policies remain unset when runtime resolution may change
// the shape.
func defaultOMENativeComponent(ext *v1beta1.ComponentExtensionSpec, shape componentPodShape, specMode *constants.DeploymentModeType, defaults *controllerconfig.ComponentUpdateStrategyDefaults) {
	if ext == nil {
		return
	}
	if !componentResolvesToOMENative(ext.Annotations, specMode) {
		return
	}
	if ext.Lifecycle == nil {
		ext.Lifecycle = &v1beta1.LifecycleSpec{}
	}
	spec := ext.Lifecycle

	if shape != podShapeUnresolved && spec.RestartPolicy == nil {
		rp := v1beta1.InstanceRestartPolicyNone
		if shape == podShapeMulti {
			rp = v1beta1.InstanceRestartPolicyRecreateInstance
		}
		spec.RestartPolicy = &rp
	}

	if spec.UpdateStrategy == nil {
		spec.UpdateStrategy = &v1beta1.UpdateStrategy{}
	}
	if spec.UpdateStrategy.Type == "" && defaults != nil && defaults.Type != "" {
		spec.UpdateStrategy.Type = v1beta1.UpdateStrategyType(defaults.Type)
	}
	if spec.UpdateStrategy.InPlaceUpdateStrategy == nil {
		spec.UpdateStrategy.InPlaceUpdateStrategy = &v1beta1.InPlaceUpdateStrategy{}
	}
	if spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds == nil {
		grace := int32(30)
		spec.UpdateStrategy.InPlaceUpdateStrategy.GracePeriodSeconds = &grace
	}
	if spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle == nil {
		mark := true
		spec.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle = &mark
	}

	// Stamp the configured rollout budget so an unset RollingUpdate does not
	// resolve to the uncapped BudgetNoLimit: without a cap the dispatcher can
	// start every Instance's surge/drain in a single reconcile pass, draining
	// a whole Component at once on a spec bump. Only the budget the resolved
	// strategy reads is stamped — a surge strategy never consults
	// MaxUnavailable and a non-surge strategy never consults MaxSurge, and a
	// field that is never read reads as a bound that is doing nothing.
	if defaults != nil {
		// An unset Type dispatches as SurgeThenDrain, so it takes the surge budget.
		surges := spec.UpdateStrategy.Type == v1beta1.UpdateStrategySurgeThenDrain || spec.UpdateStrategy.Type == ""
		budget := defaults.MaxUnavailable
		if surges {
			budget = defaults.MaxSurge
		}
		if budget != nil {
			if spec.UpdateStrategy.RollingUpdate == nil {
				spec.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
			}
			rollingUpdate := spec.UpdateStrategy.RollingUpdate
			value := *budget
			switch {
			case surges && rollingUpdate.MaxSurge == nil:
				rollingUpdate.MaxSurge = &value
			case !surges && rollingUpdate.MaxUnavailable == nil:
				rollingUpdate.MaxUnavailable = &value
			}
		}
	}

	if shape != podShapeUnresolved && spec.ReadyPolicy == nil {
		rp := v1beta1.InstanceReadyPolicyNone
		if shape == podShapeMulti {
			rp = v1beta1.InstanceReadyPolicyAllPodReady
		}
		spec.ReadyPolicy = &rp
	}

	if spec.InstanceReadyTimeout == nil {
		spec.InstanceReadyTimeout = &metav1.Duration{Duration: 30 * time.Minute}
	}

	if spec.MigrationPolicy == nil {
		spec.MigrationPolicy = &v1beta1.MigrationPolicy{}
	}
	if spec.MigrationPolicy.Mode == "" {
		spec.MigrationPolicy.Mode = v1beta1.MigrationPolicyModeAuto
	}
}

// componentResolvesToOMENative reports whether a Component dispatches
// to the OMENative backend after applying the precedence chain:
//  1. Per-Component ome.io/deploymentMode annotation (escape hatch).
//  2. Top-level spec.deploymentMode (typed default).
//
// Returns false when neither resolves to OMENative. Used by the
// lifecycle-policy defaulter so spec.deploymentMode=OMENative also
// triggers the OMENative defaults without forcing operators to repeat
// the annotation on every Component.
func componentResolvesToOMENative(annotations map[string]string, specMode *constants.DeploymentModeType) bool {
	if annotations[constants.DeploymentMode] != "" {
		return annotations[constants.DeploymentMode] == string(constants.OMENative)
	}
	if specMode != nil {
		return *specMode == constants.OMENative
	}
	return false
}
