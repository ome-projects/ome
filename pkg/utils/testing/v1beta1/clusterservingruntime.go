package v1beta1testing

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ClusterServingRuntimeWrapper mirrors ServingRuntimeWrapper for the
// cluster-scoped variant. ISVCs default to ClusterServingRuntime
// (Spec.Runtime.Kind), so this is the more common type in tests.
type ClusterServingRuntimeWrapper struct {
	v1beta1.ClusterServingRuntime
}

// MakeClusterServingRuntime returns the wrapped scaffold.
func MakeClusterServingRuntime(name string) *ClusterServingRuntimeWrapper {
	return &ClusterServingRuntimeWrapper{
		ClusterServingRuntime: v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: v1beta1.ServingRuntimeSpec{
				ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
					}},
				},
			},
		},
	}
}

func (w *ClusterServingRuntimeWrapper) Obj() *v1beta1.ClusterServingRuntime {
	return &w.ClusterServingRuntime
}

func (w *ClusterServingRuntimeWrapper) Clone() *ClusterServingRuntimeWrapper {
	return &ClusterServingRuntimeWrapper{ClusterServingRuntime: *w.ClusterServingRuntime.DeepCopy()}
}

// SupportsModelFormat / SupportsArchitecture / Disabled / Container
// mirror the namespaced ServingRuntime setters.

func (w *ClusterServingRuntimeWrapper) SupportsModelFormat(formatName string, autoSelect bool, priority int32) *ClusterServingRuntimeWrapper {
	auto := autoSelect
	prio := priority
	w.Spec.SupportedModelFormats = append(w.Spec.SupportedModelFormats, v1beta1.SupportedModelFormat{
		Name:           formatName,
		ModelFormat:    &v1beta1.ModelFormat{Name: formatName},
		ModelFramework: defaultModelFramework(),
		AutoSelect:     &auto,
		Priority:       &prio,
	})
	return w
}

func (w *ClusterServingRuntimeWrapper) SupportsArchitecture(arch string, autoSelect bool, priority int32) *ClusterServingRuntimeWrapper {
	auto := autoSelect
	prio := priority
	a := arch
	w.Spec.SupportedModelFormats = append(w.Spec.SupportedModelFormats, v1beta1.SupportedModelFormat{
		Name:              arch,
		ModelFormat:       &v1beta1.ModelFormat{Name: arch},
		ModelFramework:    defaultModelFramework(),
		ModelArchitecture: &a,
		AutoSelect:        &auto,
		Priority:          &prio,
	})
	return w
}

// WithLastFormatArchitecture sets ModelArchitecture on the most-
// recently-appended SupportedModelFormat. Useful when the chain is
// SupportsModelFormat("safetensors", true, 10).WithLastFormatArchitecture("LlamaForCausalLM")
// — the runtime selector is strict: if the model declares an
// architecture, the runtime's matching SupportedModelFormat must too.
func (w *ClusterServingRuntimeWrapper) WithLastFormatArchitecture(arch string) *ClusterServingRuntimeWrapper {
	if len(w.Spec.SupportedModelFormats) == 0 {
		return w
	}
	a := arch
	w.Spec.SupportedModelFormats[len(w.Spec.SupportedModelFormats)-1].ModelArchitecture = &a
	return w
}

// defaultModelFramework returns the minimum-viable ModelFrameworkSpec
// the CRD requires (+required marker on SupportedModelFormat.modelFramework).
// Tests that need a different framework override post-Make.
func defaultModelFramework() *v1beta1.ModelFrameworkSpec {
	return &v1beta1.ModelFrameworkSpec{Name: "Transformers"}
}

func (w *ClusterServingRuntimeWrapper) Disabled(d bool) *ClusterServingRuntimeWrapper {
	val := d
	w.Spec.Disabled = &val
	return w
}

func (w *ClusterServingRuntimeWrapper) Container(c corev1.Container) *ClusterServingRuntimeWrapper {
	w.Spec.Containers = append(w.Spec.Containers, c)
	return w
}

// EngineRunner sets spec.engineConfig.runner — the canonical place
// runtime catalogs put a single-node engine container. The
// ISVC's Engine spec is then a stub (`Engine(&v1beta1.EngineSpec{})`)
// to opt into the engine component, and MergeRuntimeSpecs combines
// the two. render.UpdateVolumeMounts only injects model-volume mounts
// onto the merged Runner.Container, so this is also the only place
// that picks up PVC volume mounts.
func (w *ClusterServingRuntimeWrapper) EngineRunner(c corev1.Container) *ClusterServingRuntimeWrapper {
	if w.Spec.EngineConfig == nil {
		w.Spec.EngineConfig = &v1beta1.EngineSpec{}
	}
	w.Spec.EngineConfig.Runner = &v1beta1.RunnerSpec{Container: c}
	return w
}

// EngineConfig sets the entire spec.engineConfig (overwrites). Useful
// when a test needs leader/worker/annotations alongside the runner.
// EngineRunner is preferred for the simple case.
func (w *ClusterServingRuntimeWrapper) EngineConfig(spec *v1beta1.EngineSpec) *ClusterServingRuntimeWrapper {
	w.Spec.EngineConfig = spec
	return w
}

// EngineLeaderRunner sets spec.engineConfig.leader.runner — required
// for multi-node engines because the leader has its own pod spec /
// runner (independent of engineConfig.runner). Multi-node runtime
// catalogs canonically place the engine container at
// engineConfig.leader.runner.
func (w *ClusterServingRuntimeWrapper) EngineLeaderRunner(c corev1.Container) *ClusterServingRuntimeWrapper {
	if w.Spec.EngineConfig == nil {
		w.Spec.EngineConfig = &v1beta1.EngineSpec{}
	}
	if w.Spec.EngineConfig.Leader == nil {
		w.Spec.EngineConfig.Leader = &v1beta1.LeaderSpec{}
	}
	w.Spec.EngineConfig.Leader.Runner = &v1beta1.RunnerSpec{Container: c}
	return w
}

// EngineWorkerRunner sets spec.engineConfig.worker.runner. Same
// pattern as EngineLeaderRunner — multi-node workers carry their own
// runner declaration on the runtime.
func (w *ClusterServingRuntimeWrapper) EngineWorkerRunner(c corev1.Container) *ClusterServingRuntimeWrapper {
	if w.Spec.EngineConfig == nil {
		w.Spec.EngineConfig = &v1beta1.EngineSpec{}
	}
	if w.Spec.EngineConfig.Worker == nil {
		w.Spec.EngineConfig.Worker = &v1beta1.WorkerSpec{}
	}
	w.Spec.EngineConfig.Worker.Runner = &v1beta1.RunnerSpec{Container: c}
	return w
}

// DecoderRunner sets spec.decoderConfig.runner — same pattern as
// EngineRunner but for the decoder component (PD-disaggregated).
func (w *ClusterServingRuntimeWrapper) DecoderRunner(c corev1.Container) *ClusterServingRuntimeWrapper {
	if w.Spec.DecoderConfig == nil {
		w.Spec.DecoderConfig = &v1beta1.DecoderSpec{}
	}
	w.Spec.DecoderConfig.Runner = &v1beta1.RunnerSpec{Container: c}
	return w
}

// RouterRunner sets spec.routerConfig.runner.
func (w *ClusterServingRuntimeWrapper) RouterRunner(c corev1.Container) *ClusterServingRuntimeWrapper {
	if w.Spec.RouterConfig == nil {
		w.Spec.RouterConfig = &v1beta1.RouterSpec{}
	}
	w.Spec.RouterConfig.Runner = &v1beta1.RunnerSpec{Container: c}
	return w
}

// No init() / cleanup hook — ClusterServingRuntime is cluster-scoped
// so per-namespace cleanup doesn't apply. Tests that create CSRs are
// responsible for deleting them in AfterEach.
