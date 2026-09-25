package workload

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ObservedSnapshot owns the decision-epoch observations for one reconcile.
// Cache and API-reader views remain independent, lazy, and memoized. Evidence
// uses the cache view; destructive planning uses the live-read view.
//
// Not safe for concurrent use — one snapshot per reconcile goroutine.
type ObservedSnapshot struct {
	deps      types.Deps
	input     types.ReconcileInput
	component types.ComponentType
	insts     []types.InstanceStatus

	live   memoObservation
	cached memoObservation
}

// memoObservation memoizes one Pod observation and its read result.
type memoObservation struct {
	done        bool
	observation ComponentObservation
	err         error
}

// NewObservedSnapshot builds the snapshot; pods are materialized lazily on
// first access.
func NewObservedSnapshot(deps types.Deps, input types.ReconcileInput, component types.ComponentType, insts []types.InstanceStatus) *ObservedSnapshot {
	return &ObservedSnapshot{deps: deps, input: input, component: component, insts: insts}
}

// LiveObservation returns the decision-epoch destructive-read view. Its source
// records the cached-client fallback when no API reader is configured.
func (s *ObservedSnapshot) LiveObservation(ctx context.Context) (ComponentObservation, error) {
	if !s.live.done {
		var pods PodObservation
		if s.input.AuthoritativePods != nil {
			scope := PodObservationScopeUnknown
			if s.input.AuthoritativePods.OwnerUID != "" && s.input.OwnerObject != nil &&
				s.input.AuthoritativePods.OwnerUID == s.input.OwnerObject.GetUID() {
				scope = PodObservationScopeOwnerUID
			}
			pods = newPodObservation(
				PodObservationSourceAPIReader,
				scope,
				s.input.AuthoritativePods.Pods,
				s.input.AuthoritativePods.ByInstance,
			)
		} else {
			pods, s.live.err = observePodsLive(ctx, s.deps, s.input, s.component)
		}
		if s.live.err == nil {
			s.live.observation, s.live.err = NewDecisionObservation(s.insts, pods)
		}
		s.live.done = true
	}
	return s.live.observation, s.live.err
}

// CachedObservation returns the decision-epoch cache observation.
func (s *ObservedSnapshot) CachedObservation(ctx context.Context) (ComponentObservation, error) {
	if !s.cached.done {
		pods, err := observePodsCached(ctx, s.deps, s.input, s.component)
		s.cached.err = err
		if err == nil {
			s.cached.observation, s.cached.err = NewDecisionObservation(s.insts, pods)
		}
		s.cached.done = true
	}
	return s.cached.observation, s.cached.err
}

// LivePods returns the Component's pods bucketed by instance from the live-read
// role used by destructive planning.
// Memoized: at most one live List per reconcile.
func (s *ObservedSnapshot) LivePods(ctx context.Context) (map[int32][]*corev1.Pod, error) {
	observation, err := s.LiveObservation(ctx)
	if err != nil {
		return nil, err
	}
	return observation.pods.byInstance, nil
}

// CachedPods returns the Component's pods bucketed by instance from the
// CACHED (informer) source — the non-destructive read the update pass uses.
// Memoized: at most one cached List per reconcile.
func (s *ObservedSnapshot) CachedPods(ctx context.Context) (map[int32][]*corev1.Pod, error) {
	observation, err := s.CachedObservation(ctx)
	if err != nil {
		return nil, err
	}
	return observation.pods.byInstance, nil
}

// observePodsLive performs the selector-scoped live-role List.
func observePodsLive(ctx context.Context, deps types.Deps, input types.ReconcileInput, component types.ComponentType) (PodObservation, error) {
	// The API reader has no Pod field index. useIndex=false preserves one List
	// for this role, including its cached-client fallback.
	reader := deps.Reader()
	source := PodObservationSourceAPIReader
	if deps.APIReader == nil {
		source = PodObservationSourceCache
	}
	pods, err := query.ListOMENativePodsByName(ctx, reader, input.Key.Namespace, input.Key.OwnerName, component, false)
	if err != nil {
		return PodObservation{}, err
	}
	return newPodObservation(source, PodObservationScopeSelector, pods, nil), nil
}

// observePodsCached performs the selector-scoped cache List.
func observePodsCached(ctx context.Context, deps types.Deps, input types.ReconcileInput, component types.ComponentType) (PodObservation, error) {
	// Cached client has the OMENative Pod field index — useIndex=true takes
	// the index fast path instead of scanning every cached pod.
	pods, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, component, true)
	if err != nil {
		return PodObservation{}, err
	}
	return NewCachedSelectorPodObservation(pods, nil), nil
}
