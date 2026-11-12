package workload

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ObservedSnapshot owns the decision-epoch observations for one reconcile.
// Cache and API-reader views remain independent, lazy, and memoized. Evidence
// uses the cache view; destructive planning uses the live-read view.
//
// A pod bound to a Node the cluster no longer holds is read as gone by both
// views: it is left out of the buckets and kept aside for Execute to remove
// (GonePods), so every reader of the pass counts without it.
//
// Not safe for concurrent use — one snapshot per reconcile goroutine.
type ObservedSnapshot struct {
	deps      types.Deps
	input     types.ReconcileInput
	component types.ComponentType
	insts     []types.InstanceStatus

	live   memoObservation
	cached memoObservation

	// nodes answers whether a Node still exists, once per name per pass.
	nodes types.NodePresence
	// gone holds, by UID, the pods either view read as bound to a missing
	// Node.
	gone map[k8stypes.UID]*corev1.Pod

	// runningSpecs memoizes the PodSpec each running revision records, by
	// ControllerRevision name: a Component's Instances mostly share one.
	runningSpecs map[string]memoPodSpec
	// revisionNumbers memoizes each revision's number, by ControllerRevision
	// name: read only when the Instances a roll lists run more than one.
	revisionNumbers map[string]memoRevisionNumber
}

// memoObservation memoizes one Pod observation and its read result.
type memoObservation struct {
	done        bool
	observation ComponentObservation
	err         error
}

// memoPodSpec memoizes one revision's recorded PodSpec and its read result.
type memoPodSpec struct {
	spec *corev1.PodSpec
	err  error
}

// memoRevisionNumber memoizes one revision's number and its read result;
// found is false when the revision is gone.
type memoRevisionNumber struct {
	number int64
	found  bool
	err    error
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
			pods, s.live.err = s.observeAuthoritativePods(ctx)
		} else {
			pods, s.live.err = s.observePodsLive(ctx)
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
		pods, err := s.observePodsCached(ctx)
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

// GonePods returns, in name order, the pods the views taken so far read as
// bound to a Node the cluster no longer holds. They are in no bucket; the
// pass removes them ahead of every verb.
func (s *ObservedSnapshot) GonePods() []*corev1.Pod {
	if len(s.gone) == 0 {
		return nil
	}
	gone := make([]*corev1.Pod, 0, len(s.gone))
	for _, pod := range s.gone {
		gone = append(gone, pod)
	}
	sort.Slice(gone, func(i, j int) bool { return gone[i].Name < gone[j].Name })
	return gone
}

// RunningRevisionPodSpec returns the PodSpec recorded by the ControllerRevision
// an Instance's status names as its running revision: the baseline a
// single-pod in-place start's diff is judged against, read through the
// revision-bookkeeping role the update op reads it with. nil with no error
// when the row names no revision or the revision is gone (no baseline).
// Memoized: at most one Get per revision name per reconcile.
func (s *ObservedSnapshot) RunningRevisionPodSpec(ctx context.Context, idx int32) (*corev1.PodSpec, error) {
	row := s.input.ObservedState.Instance(idx)
	if row == nil || row.RunningRevision == "" {
		return nil, nil
	}
	if memo, ok := s.runningSpecs[row.RunningRevision]; ok {
		return memo.spec, memo.err
	}
	spec, err := workloadops.RevisionPodSpec(ctx, s.deps.Reader(), s.input.Key.Namespace, row.RunningRevision)
	if s.runningSpecs == nil {
		s.runningSpecs = make(map[string]memoPodSpec)
	}
	s.runningSpecs[row.RunningRevision] = memoPodSpec{spec: spec, err: err}
	return spec, err
}

// RevisionNumber returns the number of the named ControllerRevision, read
// through the same role as RunningRevisionPodSpec. The revisions of one
// workload are numbered in the order they were minted, so a smaller
// number is an older revision. found is false when the revision is gone,
// and when the snapshot was built without a reader, as the decision
// tests build it. Memoized: at most one Get per revision name per
// reconcile.
func (s *ObservedSnapshot) RevisionNumber(ctx context.Context, name string) (number int64, found bool, err error) {
	if memo, ok := s.revisionNumbers[name]; ok {
		return memo.number, memo.found, memo.err
	}
	memo := memoRevisionNumber{}
	if reader := s.deps.Reader(); reader != nil {
		memo.number, memo.found, memo.err = workloadops.RevisionNumber(ctx, reader, s.input.Key.Namespace, name)
	}
	if s.revisionNumbers == nil {
		s.revisionNumbers = make(map[string]memoRevisionNumber)
	}
	s.revisionNumbers[name] = memo
	return memo.number, memo.found, memo.err
}

// nodePresence is the Node reading of this pass: the one the adapter
// wired, else the engine's own over its two Node readers, the cache first
// and the live reader to confirm a Node the cache lacks.
func (s *ObservedSnapshot) nodePresence() types.NodePresence {
	if s.nodes == nil {
		s.nodes = s.deps.NodePresence
		if s.nodes == nil {
			s.nodes = query.NewNodeLookup(s.deps.NodeReaders())
		}
	}
	return s.nodes
}

// withoutGone splits the listed pods on the Node reading, keeps the gone
// ones aside and returns the present ones.
func (s *ObservedSnapshot) withoutGone(ctx context.Context, pods []*corev1.Pod) ([]*corev1.Pod, error) {
	present, gone, err := query.SplitPodsOnMissingNodes(ctx, s.nodePresence(), pods)
	if err != nil {
		return nil, err
	}
	for _, pod := range gone {
		if s.gone == nil {
			s.gone = make(map[k8stypes.UID]*corev1.Pod)
		}
		s.gone[pod.UID] = pod
	}
	return present, nil
}

// observeAuthoritativePods is the live view over the pods the adapter
// preloaded. The buckets it handed over are kept as they are unless a pod
// of them is gone.
func (s *ObservedSnapshot) observeAuthoritativePods(ctx context.Context) (PodObservation, error) {
	preloaded := s.input.AuthoritativePods
	scope := PodObservationScopeUnknown
	if preloaded.OwnerUID != "" && s.input.OwnerObject != nil && preloaded.OwnerUID == s.input.OwnerObject.GetUID() {
		scope = PodObservationScopeOwnerUID
	}
	pods, err := s.withoutGone(ctx, preloaded.Pods)
	if err != nil {
		return PodObservation{}, err
	}
	byInstance := preloaded.ByInstance
	if len(pods) != len(preloaded.Pods) {
		byInstance = nil
	}
	return newPodObservation(PodObservationSourceAPIReader, scope, pods, byInstance), nil
}

// observePodsLive performs the selector-scoped live-role List.
func (s *ObservedSnapshot) observePodsLive(ctx context.Context) (PodObservation, error) {
	// The API reader has no Pod field index. useIndex=false preserves one List
	// for this role, including its cached-client fallback.
	reader := s.deps.Reader()
	source := PodObservationSourceAPIReader
	if s.deps.APIReader == nil {
		source = PodObservationSourceCache
	}
	pods, err := query.ListOMENativePodsByName(ctx, reader, s.input.Key.Namespace, s.input.Key.OwnerName, s.component, false)
	if err != nil {
		return PodObservation{}, err
	}
	if pods, err = s.withoutGone(ctx, pods); err != nil {
		return PodObservation{}, err
	}
	return newPodObservation(source, PodObservationScopeSelector, pods, nil), nil
}

// observePodsCached is the selector-scoped cache view: the pass's one
// cached pod list, taken here when no reader took it yet, and its groups
// by Instance, built here when no reader built them yet. A list cut short
// on a missing Node is grouped on its own.
func (s *ObservedSnapshot) observePodsCached(ctx context.Context) (PodObservation, error) {
	pods, err := s.input.CachedPods.Pods(ctx, s.listPodsCached)
	if err != nil {
		return PodObservation{}, err
	}
	if pods, err = s.withoutGone(ctx, pods); err != nil {
		return PodObservation{}, err
	}
	return NewCachedSelectorPodObservation(pods, s.input.CachedPods.ByInstance(pods, query.BucketPodsByInstanceIdx)), nil
}

// listPodsCached performs the selector-scoped cache List.
func (s *ObservedSnapshot) listPodsCached(ctx context.Context) ([]*corev1.Pod, error) {
	// Cached client has the OMENative Pod field index — useIndex=true takes
	// the index fast path instead of scanning every cached pod.
	return query.ListOMENativePodsByName(ctx, s.deps.Client, s.input.Key.Namespace, s.input.Key.OwnerName, s.component, true)
}
