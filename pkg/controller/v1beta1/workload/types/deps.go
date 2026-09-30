package types

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ObservationReader is a watch-backed (cached) reader; it may lag the
// API server. Safe wherever a later live read or optimistic-concurrency
// write re-validates the observation.
type ObservationReader = client.Reader

// AuthoritativeReader reads live from the API server. Required where
// cache lag is a correctness bug: revision bookkeeping, audit ledger,
// EndpointSlice drain checks, gang topology proofs.
type AuthoritativeReader = client.Reader

// Deps is the manager-scoped wiring the workload reconciler needs.
// Shared across every workload the same controller manages (unlike
// the per-workload ReconcileInput).
type Deps struct {
	// Client is the controller-runtime cached client used for the
	// vast majority of read/write operations.
	Client client.Client

	// APIReader is the AuthoritativeReader role (see type docs).
	// Used for revision bookkeeping and immutable topology observations where
	// stale cache contents could cause a collision or split a live gang. When
	// nil the cached Client is used.
	APIReader client.Reader

	// Recorder emits K8s Events at lifecycle transitions. nil-safe:
	// when unset, event helpers are no-ops.
	Recorder record.EventRecorder

	// Expectations is the create/delete bookkeeping cache used to
	// avoid re-issuing batches before the controller-runtime watch
	// has confirmed prior writes. Adapters wire the same instance
	// into the Pod event handler so observed create / delete events
	// release expectations immediately. nil falls back to
	// DefaultExpectations.
	Expectations *Expectations

	// RenderHook is an optional per-pod template mutator invoked
	// after composing the canonical pod (name, hostname, labels,
	// controller env, owner ref) but before c.Create. The IR
	// reconciler (the sole workload.Reconcile caller) wires this to
	// coordination.InjectPeerEnv via core.ISVCRenderHook so
	// OME_<PEER>_ENDPOINT vars land on every container.
	//
	// The hook MUST be idempotent — Render may be invoked twice in
	// the same reconcile (e.g., cache-lag retry after AlreadyExists).
	RenderHook RenderHook

	// EnsureGangPodGroup, when set, creates the scheduler-plugins
	// PodGroup for one multi-pod surge Instance just before its pods are
	// created. The gang-surge op needs PodGroup-before-pods to hold even
	// in the window before the surge index lands in the plan the
	// top-level EnsurePodGroups keys off (else a gang scheduler rejects
	// the surge pods with "PodGroup not found"). The callback lives on
	// Deps rather than as a direct call so the workload/ops package stays
	// free of the workload/podgroup dependency (ops is imported by
	// podgroup's test deps; a direct edge would close a cycle). The gang
	// package wires it; nil callback / single-pod Instance / absent CRD
	// all no-op. Idempotent.
	EnsureGangPodGroup EnsureGangPodGroupFn

	// Provisioner, when set, confines each pod to capacity it readies and
	// withholds the pod's create until that capacity is ready; the hold
	// pass reports the wait as WaitingReasonCapacityProvisioning. nil
	// creates every pod unconfined.
	Provisioner Provisioner

	// Clock supplies wall-clock time for deadlines and status
	// timestamps. nil falls back to the real clock — inject a fake
	// (k8s.io/utils/clock/testing) for deterministic boundary tests.
	Clock clock.Clock
}

// RenderHook is the optional adapter-specific render-time mutator.
// Receives the pod under construction; may mutate any field except
// Name/Namespace/OwnerReferences (workload-controlled).
type RenderHook func(pod *corev1.Pod, runnerName string, ordinal int32, revisionHash string)

// EnsureGangPodGroupFn ensures the PodGroup for one multi-pod surge Instance
// and returns the effective topology key selected for its live pods. See
// Deps.EnsureGangPodGroup.
type EnsureGangPodGroupFn func(ctx context.Context, input ReconcileInput, plan ComponentPlan, inst InstancePlan) (string, error)

// Reader returns the live API reader when APIReader is set, otherwise
// the cached Client. Use for reads that must not observe a stale
// cache.
func (d *Deps) Reader() client.Reader {
	if d.APIReader != nil {
		return d.APIReader
	}
	return d.Client
}

// ExpectationsCache returns the per-controller cache threaded through
// Deps when set, otherwise the DefaultExpectations singleton.
func (d *Deps) ExpectationsCache() *Expectations {
	if d.Expectations != nil {
		return d.Expectations
	}
	return DefaultExpectations
}

// Now returns the injected clock's time, or time.Now() when no clock
// is wired. Lifecycle code holding a Deps or ReconcileInput should read
// time through Now, not time.Now(); subpackages without a seam
// (audit, podreadiness, gang) still read real time.
func (d *Deps) Now() time.Time {
	if d.Clock != nil {
		return d.Clock.Now()
	}
	return time.Now()
}
