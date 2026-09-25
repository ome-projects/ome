// Package core is the ISVC-side carrier types and adapter glue for the
// OMENative reconciler: re-exported workload type aliases, the
// ReconcileParams carrier, revision helpers, and the pod-render hook
// shared with the InferenceReplica controller. The bulk of the
// planning + expectations logic lives in
// pkg/controller/v1beta1/workload/. Adapter-aware callers should
// depend on workload directly.
package core

import (
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Re-exported workload value types. Type aliases (`type A = B`) so
// values are interchangeable at compile time — call sites that hold
// onto a `core.ComponentPlan` are holding the exact same memory as
// `workload.ComponentPlan`.
type (
	ComponentPlan    = workloadtypes.ComponentPlan
	InstancePlan     = workloadtypes.InstancePlan
	RunnerPlan       = workloadtypes.RunnerPlan
	MigrationOverlay = workloadtypes.MigrationOverlay
	Expectations     = workloadtypes.Expectations
)

// Re-exported constructors. `var` rather than `func` re-exports so
// callers that took a value pointer (e.g., `f := core.NewExpectations`)
// keep the same observable identity.
var (
	NewExpectations     = workloadtypes.NewExpectations
	DefaultExpectations = workloadtypes.DefaultExpectations
)

// ReconcileParams is the input bag consumed by the ISVC-side
// render / revision helpers in this package (see render.go).
//
// The Expectations field's type comes from the workload package via the
// re-export above so both adapters can share the same cache instance.
type ReconcileParams struct {
	// ISVC is the parent InferenceService being reconciled.
	ISVC *v1beta1.InferenceService

	// ObjectMeta is the rendered per-Component metadata (labels,
	// annotations, owner refs).
	ObjectMeta metav1.ObjectMeta

	// PodSpec is the rendered pod template for the Component's leader /
	// single-pod runner.
	PodSpec *v1.PodSpec

	// Component identifies which of router / engine / decoder is being
	// reconciled.
	Component v1beta1.ComponentType

	// ComponentExt is the merged ComponentExtensionSpec, with the values the
	// reconciler fills after the runtime merge already applied.
	ComponentExt *v1beta1.ComponentExtensionSpec

	// WorkerSize is Worker.Size for multi-pod Instances. Zero when the
	// Component is single-pod or has no Worker block (e.g., Router).
	WorkerSize int

	// WorkerPodSpec is the rendered pod template for the Worker runner.
	// Nil when WorkerSize is 0.
	WorkerPodSpec *v1.PodSpec

	// MultiPod indicates the Component declares Leader or Worker (Size > 0),
	// i.e., each Instance materializes more than one pod. Router is always
	// single-pod (MultiPod=false). Set at the dispatch site since
	// EngineSpec/DecoderSpec/RouterSpec aren't carried through this bag.
	MultiPod bool

	// GangSchedulingAvailable is a cluster-discovery boolean threaded
	// through from the InferenceServiceReconciler — true when the
	// scheduler-plugins `scheduling.x-k8s.io/v1alpha1/PodGroup` CRD is
	// installed at controller startup. The OMENative reconciler reads
	// this to decide whether to call podgroup.EnsurePodGroup for each
	// multi-pod Instance. When false AND the plan has a multi-pod
	// Instance, the reconciler still creates pods but stamps the
	// `GangSchedulingUnavailable=True` Component condition (gang
	// scheduling is recommended but not required; pods will still be
	// scheduled individually). Tests / single-pod Components / pre-wiring
	// callers leave it false.
	GangSchedulingAvailable bool

	// Client is the controller-runtime client used to materialize pods,
	// patch status, and observe cluster state. Set at the dispatch site
	// from BaseComponentFields.Client.
	Client client.Client

	// APIReader is the AuthoritativeReader role (see type docs).
	// When set it is used for the correctness-critical reads in revision
	// bookkeeping where stale cache contents could cause spurious
	// collisions or duplicate Revision numbers. When nil the cached Client
	// is used and the caller accepts the cache-lag exposure. Tests leave
	// it nil since the fake client is a live source.
	APIReader client.Reader

	// Expectations is the create/delete bookkeeping cache the OMENative
	// reconciler uses to avoid re-issuing batches before the controller-
	// runtime watch has confirmed prior writes. Wire the same instance
	// into the pod event handler so observed create / delete events
	// release expectations immediately. When nil the package-level
	// DefaultExpectations singleton is used.
	Expectations *Expectations

	// Recorder emits K8s Events against the parent InferenceService at
	// lifecycle transitions (InstanceCreated, InPlaceUpdateStarted,
	// MigrationCompleted, etc. — see workload/types/events.go for the
	// full reason set). nil-safe: when unset, event emission is a
	// no-op so tests don't need to plumb a recorder.
	Recorder record.EventRecorder

	// Clock supplies wall-clock time to omenative lifecycle helpers.
	// nil falls back to the real clock; tests inject a fake.
	Clock clock.Clock
}

// Reader returns the live API reader when APIReader is set; otherwise the
// cached Client. Callers use this for revision bookkeeping and other
// reads that must not observe a stale cache.
func (p *ReconcileParams) Reader() client.Reader {
	if p.APIReader != nil {
		return p.APIReader
	}
	return p.Client
}

// Now returns the injected clock's time, or time.Now() when unset.
// Mirrors workload.Deps.Now — see that doc for the rule.
func (p *ReconcileParams) Now() time.Time {
	if p.Clock != nil {
		return p.Clock.Now()
	}
	return time.Now()
}

// ExpectationsCache returns the Expectations cache to use this reconcile —
// the per-reconciler instance threaded through ReconcileParams when
// set, or the package-level DefaultExpectations singleton as a
// fallback for tests and pre-wiring callers.
func (p *ReconcileParams) ExpectationsCache() *Expectations {
	if p.Expectations != nil {
		return p.Expectations
	}
	return DefaultExpectations
}
