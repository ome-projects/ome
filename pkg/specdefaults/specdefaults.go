// Package specdefaults fills the fields an InferenceService component may
// leave unset, on the merged component specs the reconciler renders from.
// It runs after the ServingRuntime merge, so the resulting precedence is a
// value authored on the InferenceService, then one authored on the runtime,
// then the operator configuration (the deploy block of
// inferenceservice-config), then a fixed fallback. Nothing here is written
// back to the stored object.
//
// The fixed lifecycle fallbacks (update strategy type, in-place
// mark-not-ready, migration mode, shape-derived ready and restart policies)
// are the workload engine's own reading of an unset field and are not
// materialized here.
package specdefaults

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// defaultWorkerSize is the worker count of a declared Leader+Worker pair
// whose size neither the InferenceService nor the runtime set: the smallest
// gang.
const defaultWorkerSize = 1

// values is the deploy block read once per call; every field is nil when
// unconfigured, and nil fills nothing.
type values struct {
	replicas        *controllerconfig.ReplicasDefaultsConfig
	gracePeriod     *int64
	minReadySeconds *int32
	updateStrategy  *controllerconfig.UpdateStrategyDefaultsConfig
}

func fromConfig(cfg *controllerconfig.DeployConfig) values {
	if cfg == nil {
		return values{}
	}
	return values{
		replicas:        cfg.Replicas,
		gracePeriod:     cfg.TerminationGracePeriodSeconds,
		minReadySeconds: cfg.MinReadySeconds,
		updateStrategy:  cfg.UpdateStrategy,
	}
}

// role is the view of one component the filler writes to: the kind the
// per-component defaults are keyed by, the extension spec, the component's
// own pod spec, and the Leader+Worker pair, which a router does not have.
type role struct {
	kind      workloadtypes.ComponentType
	extension *v1beta1.ComponentExtensionSpec
	podSpec   *v1beta1.PodSpec
	leader    *v1beta1.LeaderSpec
	worker    *v1beta1.WorkerSpec
}

// Role fills one role's merged block in place, read through its view. mode
// is the role's resolved deployment mode; lifecycle fields are filled only
// for OMENative, the one backend that reads them. A nil view is a no-op.
func Role(view *v1beta1.RoleView, mode constants.DeploymentModeType, cfg *controllerconfig.DeployConfig) {
	if view == nil {
		return
	}
	fill(role{
		kind:      v1beta1convert.ComponentTypeToWorkload(view.Type),
		extension: view.Extension,
		podSpec:   view.PodSpec,
		leader:    view.Leader,
		worker:    view.Worker,
	}, mode, cfg)
}

// Engine fills the merged engine spec in place. A nil engine is a no-op.
func Engine(engine *v1beta1.EngineSpec, mode constants.DeploymentModeType, cfg *controllerconfig.DeployConfig) {
	Role(v1beta1.RoleSpecs{Engine: engine}.View(v1beta1.EngineComponent), mode, cfg)
}

// Decoder is Engine for the decoder component.
func Decoder(decoder *v1beta1.DecoderSpec, mode constants.DeploymentModeType, cfg *controllerconfig.DeployConfig) {
	Role(v1beta1.RoleSpecs{Decoder: decoder}.View(v1beta1.DecoderComponent), mode, cfg)
}

// Router is Engine for the router component, which has no Leader or Worker.
func Router(router *v1beta1.RouterSpec, mode constants.DeploymentModeType, cfg *controllerconfig.DeployConfig) {
	Role(v1beta1.RoleSpecs{Router: router}.View(v1beta1.RouterComponent), mode, cfg)
}

// fill writes the configured defaults into one component: the replica
// bounds, the termination grace period of every pod spec the component
// declares, one worker for a Leader+Worker pair without a size, and, for
// OMENative only, the lifecycle fields.
func fill(r role, mode constants.DeploymentModeType, cfg *controllerconfig.DeployConfig) {
	v := fromConfig(cfg)
	replicaBounds(r.extension, v.replicas.Min(), v.replicas.MaxFor(r.kind))
	terminationGracePeriod(r.podSpec, v.gracePeriod)
	if r.leader != nil {
		terminationGracePeriod(&r.leader.PodSpec, v.gracePeriod)
	}
	if r.worker != nil {
		terminationGracePeriod(&r.worker.PodSpec, v.gracePeriod)
	}
	workerSize(r.leader, r.worker)
	if mode == constants.OMENative {
		lifecycle(r.extension, v.minReadySeconds, v.updateStrategy.ForComponent(r.kind))
	}
}

// replicaBounds fills unset replica bounds from the configured defaults. A
// nil default leaves the field unset; the values come only from
// configuration. MaxReplicas is a non-pointer int, so 0 means unset.
func replicaBounds(ext *v1beta1.ComponentExtensionSpec, defaultMin, defaultMax *int) {
	if ext.MinReplicas == nil && defaultMin != nil {
		minReplicas := *defaultMin
		ext.MinReplicas = &minReplicas
	}
	if ext.MaxReplicas == 0 && defaultMax != nil {
		ext.MaxReplicas = raisedMax(*defaultMax, ext.MinReplicas)
	}
}

// raisedMax returns the configured max, raised to the min when the min is
// larger. Filling a max below an authored min would manufacture a min>max
// conflict that the replica-bounds validator rejects on every update. An
// authored max is never adjusted.
func raisedMax(configuredMax int, minReplicas *int) int {
	if minReplicas != nil && *minReplicas > configuredMax {
		return *minReplicas
	}
	return configuredMax
}

// terminationGracePeriod fills an unset terminationGracePeriodSeconds from
// the configured default. A nil default leaves it unset, so the pod takes
// the Kubernetes default.
func terminationGracePeriod(podSpec *v1beta1.PodSpec, defaultSeconds *int64) {
	if podSpec == nil || defaultSeconds == nil || podSpec.TerminationGracePeriodSeconds != nil {
		return
	}
	seconds := *defaultSeconds
	podSpec.TerminationGracePeriodSeconds = &seconds
}

// workerSize gives a declared Leader+Worker pair one worker when no size was
// set. One-sided shapes are left for validation to report.
func workerSize(leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) {
	if leader == nil || worker == nil || worker.Size != nil {
		return
	}
	size := defaultWorkerSize
	worker.Size = &size
}

// lifecycle fills the config-driven lifecycle fields of an OMENative
// component: minReadySeconds, the update strategy type, and the rollout
// budget the resolved strategy reads. A surge strategy (SurgeThenDrain, or
// an unset type, which the engine runs as SurgeThenDrain) is paced by
// maxSurge; every other strategy is paced by maxUnavailable, and the budget
// a strategy never reads is left unset. Nested blocks are attached only when
// a value is written, so an unconfigured cluster adds nothing.
func lifecycle(ext *v1beta1.ComponentExtensionSpec, minReadySeconds *int32, strategy *controllerconfig.ComponentUpdateStrategyDefaults) {
	lc := ext.Lifecycle
	if lc == nil {
		lc = &v1beta1.LifecycleSpec{}
	}
	wrote := false

	if minReadySeconds != nil && lc.MinReadySeconds == nil {
		seconds := *minReadySeconds
		lc.MinReadySeconds = &seconds
		wrote = true
	}

	if strategy != nil {
		us := lc.UpdateStrategy
		if us == nil {
			us = &v1beta1.UpdateStrategy{}
		}
		if us.Type == "" && strategy.Type != "" {
			us.Type = v1beta1.UpdateStrategyType(strategy.Type)
			lc.UpdateStrategy = us
			wrote = true
		}
		surges := us.Type == "" || us.Type == v1beta1.UpdateStrategySurgeThenDrain
		budget := strategy.MaxUnavailable
		if surges {
			budget = strategy.MaxSurge
		}
		if budget != nil {
			ru := us.RollingUpdate
			if ru == nil {
				ru = &v1beta1.RollingUpdate{}
			}
			target := &ru.MaxUnavailable
			if surges {
				target = &ru.MaxSurge
			}
			if *target == nil {
				value := *budget
				*target = &value
				us.RollingUpdate = ru
				lc.UpdateStrategy = us
				wrote = true
			}
		}
	}

	if wrote {
		ext.Lifecycle = lc
	}
}
