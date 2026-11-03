package workload

import (
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ObservationEpoch identifies where an observation is consumed.
type ObservationEpoch uint8

const (
	ObservationEpochUnknown ObservationEpoch = iota
	ObservationEpochDecision
	ObservationEpochPublication
)

// PodObservationSource identifies the Kubernetes read path.
type PodObservationSource uint8

const (
	PodObservationSourceUnknown PodObservationSource = iota
	PodObservationSourceCache
	PodObservationSourceAPIReader
)

// PodObservationScope identifies which matching Pods a read contains.
type PodObservationScope uint8

const (
	PodObservationScopeUnknown PodObservationScope = iota
	PodObservationScopeSelector
	PodObservationScopeOwnerUID
)

// PodObservation is a per-pass, read-only view of Component Pods.
type PodObservation struct {
	source     PodObservationSource
	scope      PodObservationScope
	byInstance map[int32][]*corev1.Pod
}

func newPodObservation(source PodObservationSource, scope PodObservationScope, pods []*corev1.Pod, byInstance map[int32][]*corev1.Pod) PodObservation {
	if byInstance == nil {
		byInstance = query.BucketPodsByInstanceIdx(pods)
	}
	return PodObservation{source: source, scope: scope, byInstance: byInstance}
}

// NewCachedSelectorPodObservation records a selector-scoped cache read.
func NewCachedSelectorPodObservation(pods []*corev1.Pod, byInstance map[int32][]*corev1.Pod) PodObservation {
	return newPodObservation(PodObservationSourceCache, PodObservationScopeSelector, pods, byInstance)
}

// NewAPIReaderSelectorPodObservation records a selector-scoped API read.
func NewAPIReaderSelectorPodObservation(pods []*corev1.Pod, byInstance map[int32][]*corev1.Pod) PodObservation {
	return newPodObservation(PodObservationSourceAPIReader, PodObservationScopeSelector, pods, byInstance)
}

// PodsForInstance returns the read-only bucket for index.
func (o PodObservation) PodsForInstance(index int32) []*corev1.Pod {
	return o.byInstance[index]
}

// ComponentObservation joins one persisted row set with one Pod observation.
type ComponentObservation struct {
	epoch                ObservationEpoch
	persisted            []types.InstanceStatus
	pods                 PodObservation
	availableByPod       map[string]struct{}
	availabilityWindow   status.AvailabilityWindow
	availabilityObserved bool
	consumed             bool
}

var errDecisionObservationProvenance = errors.New("decision observation requires a supported Pod source and scope")

// NewDecisionObservation borrows persisted as read-only reconcile state.
func NewDecisionObservation(persisted []types.InstanceStatus, pods PodObservation) (ComponentObservation, error) {
	if !pods.validForDecision() {
		return ComponentObservation{}, fmt.Errorf("%w: source=%d scope=%d", errDecisionObservationProvenance, pods.source, pods.scope)
	}
	return ComponentObservation{
		epoch:     ObservationEpochDecision,
		persisted: persisted,
		pods:      pods,
	}, nil
}

var errPublicationObservationProvenance = errors.New("publication observation requires a selector-scoped cache read")

// NewOwnedPublicationObservation takes ownership of persisted. The caller must
// not access that slice after construction. availableByPod is the
// EndpointSlice rotation set and window the Component's minReadySeconds rule;
// together they define the Available counters this observation publishes.
func NewOwnedPublicationObservation(persisted []types.InstanceStatus, pods PodObservation, availableByPod map[string]struct{}, window status.AvailabilityWindow) (*ComponentObservation, error) {
	if pods.source != PodObservationSourceCache || pods.scope != PodObservationScopeSelector {
		return nil, fmt.Errorf("%w: source=%d scope=%d", errPublicationObservationProvenance, pods.source, pods.scope)
	}
	return &ComponentObservation{
		epoch:                ObservationEpochPublication,
		persisted:            persisted,
		pods:                 pods,
		availableByPod:       availableByPod,
		availabilityWindow:   window,
		availabilityObserved: true,
	}, nil
}

// PersistedStatuses returns an isolated copy in epoch row order.
func (o *ComponentObservation) PersistedStatuses() []types.InstanceStatus {
	return cloneInstanceStatusSlice(o.persisted)
}

// CurrentCounters returns index-keyed Pod facts and whether availability was
// observed at this epoch.
func (o *ComponentObservation) CurrentCounters(index int32) (status.InstanceCounters, bool) {
	return status.CountersForInstance(o.pods.PodsForInstance(index), o.availableByPod, o.availabilityWindow), o.availabilityObserved
}

var errPublicationObservationRequired = errors.New("publication materialization requires a publication observation")
var errPublicationObservationConsumed = errors.New("publication observation has already been consumed")

// InlineV1Statuses overlays publication facts on copies of persisted rows.
func (o *ComponentObservation) InlineV1Statuses() ([]types.InstanceStatus, error) {
	if err := o.validatePublication(); err != nil {
		return nil, err
	}
	out := cloneInstanceStatusSlice(o.persisted)
	o.overlayInlineV1(out, nil)
	return out, nil
}

// TakeInlineV1Statuses consumes the owned rows and overlays publication facts
// without allocating another full status slice.
func (o *ComponentObservation) TakeInlineV1Statuses() ([]types.InstanceStatus, error) {
	return o.takeInlineV1Statuses(nil)
}

func (o *ComponentObservation) takeInlineV1Statuses(observe func(*types.InstanceStatus, status.InstanceCounters)) ([]types.InstanceStatus, error) {
	if err := o.validatePublication(); err != nil {
		return nil, err
	}
	out := o.persisted
	o.persisted = nil
	o.consumed = true
	o.overlayInlineV1(out, observe)
	return out, nil
}

func (o *ComponentObservation) validatePublication() error {
	if o == nil || o.epoch != ObservationEpochPublication || !o.availabilityObserved ||
		o.pods.source != PodObservationSourceCache || o.pods.scope != PodObservationScopeSelector {
		return errPublicationObservationRequired
	}
	if o.consumed {
		return errPublicationObservationConsumed
	}
	return nil
}

// overlayInlineV1 materializes the current Pod observation onto each owned
// row. observe sees the row before the overlay — durable identity, revision
// and Operation fields are already final, Pod counts come from current — and
// must treat the row as read-only.
func (o *ComponentObservation) overlayInlineV1(out []types.InstanceStatus, observe func(*types.InstanceStatus, status.InstanceCounters)) {
	for i := range out {
		current, _ := o.CurrentCounters(out[i].Index)
		if observe != nil {
			observe(&out[i], current)
		}
		out[i].PodCount = current.PodCount
		out[i].ReadyPodCount = current.ReadyPodCount
		out[i].ServingPodCount = current.ServingPodCount
		out[i].AvailablePodCount = current.AvailablePodCount
		out[i].ScheduledPodCount = current.ScheduledPodCount
		out[i].Admitted = current.Admitted
		out[i].NodesOccupied = current.NodesOccupied
	}
}

func (o PodObservation) validForDecision() bool {
	switch o.source {
	case PodObservationSourceCache:
		return o.scope == PodObservationScopeSelector
	case PodObservationSourceAPIReader:
		return o.scope == PodObservationScopeUnknown ||
			o.scope == PodObservationScopeSelector ||
			o.scope == PodObservationScopeOwnerUID
	default:
		return false
	}
}

func cloneInstanceStatusSlice(in []types.InstanceStatus) []types.InstanceStatus {
	if in == nil {
		return nil
	}
	out := make([]types.InstanceStatus, len(in))
	for i := range in {
		out[i] = cloneInstanceStatus(in[i])
	}
	return out
}

func cloneInstanceStatus(in types.InstanceStatus) types.InstanceStatus {
	out := in
	if in.NodesOccupied != nil {
		out.NodesOccupied = append([]string(nil), in.NodesOccupied...)
	}
	if in.Conditions != nil {
		out.Conditions = append([]metav1.Condition(nil), in.Conditions...)
	}
	if in.Operation != nil {
		operation := *in.Operation
		if in.Operation.SurgeIndex != nil {
			surgeIndex := *in.Operation.SurgeIndex
			operation.SurgeIndex = &surgeIndex
		}
		if in.Operation.HintTargetNodes != nil {
			operation.HintTargetNodes = append([]string(nil), in.Operation.HintTargetNodes...)
		}
		out.Operation = &operation
	}
	if in.LastFailure != nil {
		lastFailure := *in.LastFailure
		if in.LastFailure.ExitCode != nil {
			exitCode := *in.LastFailure.ExitCode
			lastFailure.ExitCode = &exitCode
		}
		out.LastFailure = &lastFailure
	}
	return out
}

// ComponentCounters is the field-level Component publication result.
type ComponentCounters struct {
	Replicas             int32
	ReadyReplicas        int32
	ServingReplicas      int32
	AvailableReplicas    int32
	UpdatedReplicas      int32
	UpdatedReadyReplicas int32
	// NextAvailableIn is the smallest pending InstanceCounters.NextAvailableIn
	// across the Component, 0 when no pod is inside its window.
	NextAvailableIn time.Duration
}

// TakeInlineV1Publication consumes the owned rows, overlays the publication
// observation, and computes Component counters from that same observation.
// Only durable identity and revision fields participate from the stored rows.
//
// The one lifecycle field the publication writes is the phase of a settled
// row, read against the pods of its own pass so the phase never
// contradicts the counters published beside it.
//
// A settled Ready row (status.DemotableReady) the observation finds no
// pod left that could back (status.PodSetBacksRow: none, or only pods in
// a terminal phase) publishes Pending, the phase the truth pass moves such
// a row to; the running revision stays on the row for the pass that
// rebuilds it. One whose pods are present but serve nothing publishes Restarting: no
// live pod of the set is ContainersReady with the serving gate, the bar
// the promote path read when the row entered Ready, so the set is down and
// the kubelet or the restart policy is bringing it back. The row's revision
// and ReadySince stay, the engine keeps reading the row as Ready
// (v1beta1convert.InstanceStatusToWorkload), and the row publishes Ready
// again on its own once the set serves. A pod serving on its containers
// before the kubelet folds the gate into PodReady serves.
//
// A Failed row that still runs a revision and records an attempt at
// another publishes Ready while the pods on the revision it runs serve at
// the desired count: the replacement failed, the Instance did not. That
// holds for a settled row whose attempt is spent
// (types.FailedOffRunningRevision) and for a row that still keeps the
// surge attempt it failed at (types.FailedKeepingSurgeOffRunningRevision),
// the shape a gang surge's source has until its replacement gang is
// retired, so the row reads the same at every moment of the attempt. The
// failure stays on the row, its record, the attempt, the revision it was
// for and the ReadySince anchor, the retry ladder and the budgets read the
// row as the engine does, Failed (v1beta1convert.InstanceStatusToWorkload),
// and the row publishes Failed again once the pods it runs stop serving.
// Pods on any other revision, a replacement crash-looping or serving
// between two crashes, are not read.
func (o *ComponentObservation) TakeInlineV1Publication(desiredByIdx map[int32]int32, targetRevName string) ([]types.InstanceStatus, ComponentCounters, error) {
	var counters ComponentCounters
	target := query.RevisionFromName(targetRevName)
	statuses, err := o.takeInlineV1Statuses(func(row *types.InstanceStatus, current status.InstanceCounters) {
		desired := status.DesiredFor(desiredByIdx, row.Index, current.PodCount)
		if status.InstanceMeetsThreshold(current.PodCount, current.ReadyPodCount, desired) {
			counters.ReadyReplicas++
		}
		if status.InstanceMeetsThreshold(current.PodCount, current.ServingPodCount, desired) {
			counters.ServingReplicas++
		}
		if status.InstanceMeetsThreshold(current.PodCount, current.AvailablePodCount, desired) {
			counters.AvailableReplicas++
		}
		counters.NextAvailableIn = types.EarliestPositive(counters.NextAvailableIn, current.NextAvailableIn)
		if targetRevName != "" && status.RowOnRevision(row, target) {
			counters.UpdatedReplicas++
			if status.InstanceMeetsThreshold(current.PodCount, current.ReadyPodCount, desired) {
				counters.UpdatedReadyReplicas++
			}
		}
	})
	if err != nil {
		return nil, ComponentCounters{}, err
	}
	for i := range statuses {
		row := &statuses[i]
		pods := o.pods.PodsForInstance(row.Index)
		desired := status.DesiredFor(desiredByIdx, row.Index, row.PodCount)
		switch {
		case status.DemotableReady(row):
			switch {
			case !status.PodSetBacksRow(pods):
				row.Phase = types.InstancePhasePending
			case !query.PodSetFullyServing(podsOnReportingNodes(pods), desired):
				row.Phase = types.InstancePhaseRestarting
			}
		case types.FailedOffRunningRevision(row), types.FailedKeepingSurgeOffRunningRevision(row):
			if query.PodSetFullyServing(podsOnReportingNodes(podsOnRevision(pods, row.RunningRevision)), desired) {
				row.Phase = types.InstancePhaseReady
			}
		}
	}
	counters.Replicas = int32(len(statuses))
	return statuses, counters, nil
}

// podsOnReportingNodes leaves out the pods whose Ready condition the node
// lifecycle controller revoked (podreadiness.ReadyRevokedByNode): their
// kubelet stopped reporting, so the container statuses and the serving
// gate they still carry are stale and no Service publishes them. The set
// they leave is short of the desired count, so the row publishes not
// serving the moment the revocation lands, as its serving count already
// reads, instead of Ready until a rebuild replaces the set.
func podsOnReportingNodes(pods []*corev1.Pod) []*corev1.Pod {
	out := pods[:0:0]
	for _, pod := range pods {
		if podreadiness.ReadyRevokedByNode(pod) {
			continue
		}
		out = append(out, pod)
	}
	return out
}

// podsOnRevision keeps the pods whose revision label names rev: the pods
// a row's running revision speaks for, with a replacement on another
// revision left out. A pod with no revision label is on no revision.
func podsOnRevision(pods []*corev1.Pod, rev string) []*corev1.Pod {
	want := query.RevisionFromName(rev)
	var out []*corev1.Pod
	for _, pod := range pods {
		if query.RevisionFromPod(pod).Same(want) {
			out = append(out, pod)
		}
	}
	return out
}
