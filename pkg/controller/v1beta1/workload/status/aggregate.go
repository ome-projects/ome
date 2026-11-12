// aggregate.go — the aggregated view of a Component's rows: the per-pod
// predicates (ready, serving, scheduled, available), the per-Instance
// counter set, the desired-count lookup every pass sizes a row by, and
// the Component-level convergence gates (ReachedDesiredShape,
// RolloutComplete) both adapters publish from.
package status

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// AvailablePodSet returns pod names that are Ready and non-terminating
// in any EndpointSlice for serviceName. drain.EndpointAvailable
// excludes terminating-but-Ready endpoints — they're not getting new
// traffic. Single slice list avoids the N+1 reads
// drain.IsPodInRotation per pod would do. The slices are the cache's own
// objects: the reading keeps the names alone and writes nothing through
// them.
//
// Adapter-agnostic: takes the namespace and headless-service name
// directly, no v1beta1 ISVC handle needed.
func AvailablePodSet(ctx context.Context, reads client.Reader, namespace, serviceName string) (map[string]struct{}, error) {
	slices, err := drain.EndpointSlicesForService(ctx, reads, namespace, serviceName)
	if err != nil {
		return nil, err
	}
	endpoints := 0
	for i := range slices {
		endpoints += len(slices[i].Endpoints)
	}
	out := make(map[string]struct{}, endpoints)
	for i := range slices {
		for _, ep := range slices[i].Endpoints {
			if ep.TargetRef == nil || ep.TargetRef.Kind != "" && ep.TargetRef.Kind != "Pod" {
				continue
			}
			if !drain.EndpointAvailable(ep) {
				continue
			}
			out[ep.TargetRef.Name] = struct{}{}
		}
	}
	return out, nil
}

// CountReadyPods returns the number of pods whose ContainersReady
// condition is True.
func CountReadyPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if podreadiness.IsContainersReady(p) {
			n++
		}
	}
	return n
}

// CountServingPods counts an Instance's pods in their Service's rotation:
// Ready by the control plane, carrying the controller's serving gate
// (podreadiness.ReadyAndServing), and not terminating. An Instance with no
// routable serving pod serves nothing, whatever its workers read, and
// counts 0: a Service publishes the leader of a gang and never a worker
// (query.RoutedRunner), so a gang whose leader is gone is out of rotation
// however many workers keep the gate, while a gang whose leader serves
// counts every serving member. This is the count MaxUnavailable budgets in
// coordination/ratio.go gate against; it drops when the controller takes a
// pod out of rotation (in-place update drain, recreate Phase A), when the
// node under a pod stops reporting, whatever the stale container statuses
// say, and when a pod is deleted: a terminating pod keeps its conditions
// until its containers stop, but the EndpointSlice controller has already
// taken it out of every Service's ready endpoints, so it serves nothing
// new.
func CountServingPods(pods []*corev1.Pod) int32 {
	var n int32
	routable := false
	for _, p := range pods {
		if p == nil || p.DeletionTimestamp != nil || !podreadiness.ReadyAndServing(p) {
			continue
		}
		n++
		routable = routable || query.RoutedRunner(p)
	}
	if !routable {
		return 0
	}
	return n
}

// CountScheduledPods returns the number of pods with Spec.NodeName set.
func CountScheduledPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if p.Spec.NodeName != "" {
			n++
		}
	}
	return n
}

// AvailabilityWindow is the Ready-age requirement layered on EndpointSlice
// membership when counting Available pods. MinReadySeconds is the
// Component's window; Now is the reconcile's clock reading. A zero window
// leaves rotation membership as the whole rule.
type AvailabilityWindow struct {
	MinReadySeconds int32
	Now             time.Time
}

// CountAvailablePods returns the number of pods that are in rotation
// (named in availableByName, the AvailablePodSet result) and, under a
// positive window, have been Ready for at least MinReadySeconds. The
// duration is how long until the earliest in-rotation pod still inside the
// window becomes Available (0 when none is pending): the counters it feeds
// change at that instant without any watch event, so the caller schedules
// its next reconcile from it.
func CountAvailablePods(pods []*corev1.Pod, availableByName map[string]struct{}, window AvailabilityWindow) (int32, time.Duration) {
	var n int32
	var nextAvailableIn time.Duration
	for _, p := range pods {
		if _, ok := availableByName[p.Name]; !ok {
			continue
		}
		if window.MinReadySeconds > 0 {
			available, remaining := podreadiness.IsPodAvailable(p, window.MinReadySeconds, window.Now)
			if !available {
				nextAvailableIn = types.EarliestPositive(nextAvailableIn, remaining)
				continue
			}
		}
		n++
	}
	return n, nextAvailableIn
}

// UniqueNodes returns the deterministically-sorted set of node names
// hosting at least one pod. Returns nil for empty / all-unscheduled
// so NodesOccupied round-trips through nil ↔ nil rather than nil ↔
// empty-slice.
func UniqueNodes(pods []*corev1.Pod) []string {
	nodes, _ := AppendUniqueNodes(nil, pods)
	return nodes
}

// AppendUniqueNodes appends the set UniqueNodes returns for pods to names
// and returns it beside the grown names. The set is cut from names and
// capped at its own length, so one array can hold the sets of every
// Instance of a publication and an append to one never writes into the
// next; nil names gives the set an array of its own.
func AppendUniqueNodes(names []string, pods []*corev1.Pod) (nodes, grown []string) {
	if names == nil {
		names = make([]string, 0, CountScheduledPods(pods))
	}
	start := len(names)
	for _, p := range pods {
		if p.Spec.NodeName != "" {
			names = append(names, p.Spec.NodeName)
		}
	}
	if len(names) == start {
		return nil, names
	}
	set := names[start:]
	sort.Strings(set)
	set = slices.Compact(set)
	names = names[:start+len(set)]
	return names[start:len(names):len(names)], names
}

// DesiredPodCountByInstance maps each planned Instance index to its
// desired canonical pod count (sum of Runner sizes). Returns nil for
// an empty plan so callers fall back to the observed PodCount
// comparison (during scale-down with stale status entries).
func DesiredPodCountByInstance(plan types.ComponentPlan) map[int32]int32 {
	if len(plan.Instances) == 0 {
		return nil
	}
	out := make(map[int32]int32, len(plan.Instances))
	for _, inst := range plan.Instances {
		out[inst.Index] = inst.TotalPods()
	}
	return out
}

// DesiredFor returns the desired pod count for idx — the plan entry's
// value when present, otherwise the observed PodCount (no plan info ⇒
// use the observed count, for stale-status scale-down Instances).
func DesiredFor(desiredByIdx map[int32]int32, idx int32, observedPodCount int32) int32 {
	if desiredByIdx == nil {
		return observedPodCount
	}
	if d, ok := desiredByIdx[idx]; ok {
		return d
	}
	return observedPodCount
}

// InstanceMeetsThreshold classifies an Instance when at least its desired pod
// count satisfies the relevant Ready, Serving, or Available predicate.
//
// Surge-tolerant: a naive `observed == PodCount` would mis-classify
// mid-surge Instances as not-Ready the whole time the +1 surge pod is
// starting up — the visible serving dip we explicitly want to avoid.
//
// desired falls back to observedPodCount when the plan has no entry
// for this index (scale-down with stale status).
func InstanceMeetsThreshold(observedPodCount, observedSatisfying, desired int32) bool {
	if observedPodCount == 0 {
		return false
	}
	if desired <= 0 {
		return observedSatisfying == observedPodCount
	}
	return observedSatisfying >= desired
}

// DemotableReady reports whether a settled Ready row — Ready with no
// operation — is demoted to Pending once it has no pods. The demotion
// keeps the running revision, so no recovery path keys on the phase: the
// Create pass re-materializes a Pending row exactly as it would a Ready
// one, and under RecreateInstanceOnPodRestart the restart trigger reads
// the demoted row (types.DemotedReady) as it reads Ready and rebuilds at
// that revision. The truth pass and the status publication both decide by
// this, so a row never claims Ready beside a zero pod count.
func DemotableReady(row *types.InstanceStatus) bool {
	return types.StateOf(row) == types.StateReady
}

// PodSetBacksRow reports whether any pod of a settled Ready row's set can
// still back the row: a pod that is not in a terminal phase. A pod on its
// way out backs the row until it is gone, since it leaves on its own. A
// terminal pod never leaves on its own and holds only its name, so it is
// absent here as it is for every other presence decision
// (query.IsTerminalPod): a row whose pods are all terminal is written
// Pending as a row with no pods is, and the pass that owns the rebuild
// deletes the terminal pods and rebuilds the Instance.
func PodSetBacksRow(pods []*corev1.Pod) bool {
	return query.LivePodCount(pods) > 0
}

// ReachedDesiredShape reports whether insts rest at the shape a roll
// converges to under a partition: no missing or extra Instance, every
// Instance Ready, and every Instance off targetRev one the partition holds.
// A partition holds the Instances still on a prior revision, so it holds
// at most `partition` of them and, when fewer than that still run a prior
// revision, every one of them: a member rests staged with between one and
// `partition` Instances held, and a partition raised above the count still
// on the old revision, or a count scaled below the partition, is met at
// what it can hold. partition=0 is a full rollout, so RolloutComplete is
// ReachedDesiredShape(insts, target, 0, len(insts)); a positive partition
// with nothing left to hold reads false, because that member is complete,
// not staged. Held Instances must be Ready too: a staged Component is at
// rest only when every Instance, new and held, is healthy.
func ReachedDesiredShape(insts []types.InstanceStatus, targetRevName string, partition, replicas int32) bool {
	if replicas <= 0 || int32(len(insts)) != replicas || partition < 0 {
		return false
	}
	for _, s := range insts {
		if s.Phase != types.InstancePhaseReady {
			return false
		}
	}
	held := HeldInstanceCount(insts, targetRevName)
	if partition == 0 {
		return held == 0
	}
	return held >= 1 && held <= partition
}

// HeldInstanceCount is the number of Instances whose running revision is
// not targetRevName (an unset one included): the Instances a partition
// holds once the member rests at its staged shape.
func HeldInstanceCount(insts []types.InstanceStatus, targetRevName string) int32 {
	tgt := query.RevisionFromName(targetRevName)
	var held int32
	for _, s := range insts {
		if !query.RevisionFromName(s.RunningRevision).Same(tgt) {
			held++
		}
	}
	return held
}

// RolloutComplete is the CurrentRevision promotion gate: every Instance
// on targetRevName AND Phase=Ready.
//
// Operates on workload.InstanceStatus so both adapters share the same
// gate without converting back to a v1beta1 shape.
func RolloutComplete(insts []types.InstanceStatus, targetRevName string) bool {
	return ReachedDesiredShape(insts, targetRevName, 0, int32(len(insts)))
}

// CurrentRevisionFor is the component-level revision rollup: what
// status.currentRevision should name given the Instance rows, the
// revision their pods carry and the Component's retry ladder. It is the
// update revision once every Instance is Ready on it and no RetryBlock
// names it: a block records a failure of the revision no later attempt
// has answered — a crash of a promoted set included, whose row reads
// Ready between crashes — and is removed once the rows outlive it, a
// corrective revision supersedes it or an operator releases a Held one,
// so the push reads landed only when the revision has proven itself
// rather than by attrition. It is withdrawn (empty) when it already names
// the update revision while some Instance is still off it
// (anyInstanceOffRevision): a fleet rolled back onto its last promoted
// revision is in flight until the last superseded Instance returns and
// the last withdrawn replacement is gone, and current equal to update is
// what every reader takes for "no rollout in flight". A withdrawn current
// is restored to the update revision once no attempt a pass still drives
// is open on any Instance (anyAttemptInFlight) and no Instance is off it
// any more, whatever the ladder holds: the pods are back where they were
// promoted, and a block on that revision gates the next attempt, not the
// rollup. An attempt still in flight whose row records another revision
// keeps its Instance off the update revision whatever its rebuilt pods
// carry, since nothing has promoted the Instance onto it yet, and an
// attempt that rebuilds a lost set toward the revision keeps the restore
// waiting while its new pods come up, however they are labelled; a spent
// attempt leaves the pods to decide. withdrawn names the revision the
// current revision was withdrawn from while the status still records it,
// "" when nothing was withdrawn or the update revision has moved on since,
// so the restore reaches only that revision and a pushed revision whose
// attempts failed stays unlanded. Any other row shape leaves current as
// recorded, so a repair on the current revision never moves it.
func CurrentRevisionFor(insts []types.InstanceStatus, current, update string, blocks []types.RetryBlock, withdrawn string, pods PodRevisions) string {
	if update == "" {
		return current
	}
	if RolloutComplete(insts, update) && types.FindRetryBlock(blocks, update) == nil {
		return update
	}
	if current == update && anyInstanceOffRevision(insts, update, pods) {
		return ""
	}
	if current == "" && withdrawn != "" && withdrawn == update && !anyAttemptInFlight(insts) && !anyInstanceOffRevision(insts, update, pods) {
		return update
	}
	return current
}

// WithdrawnRevisionAfter is the revision a write leaves current withdrawn
// from: update when the write empties a current that named it, before
// while current stays empty on the same update, and none otherwise.
func WithdrawnRevisionAfter(before, oldCurrent, current, update string) string {
	switch {
	case current != "":
		return ""
	case oldCurrent != "" && oldCurrent == update:
		return update
	case before != "" && before == update:
		return before
	}
	return ""
}

// PodRevisions is the revision label of every live pod, keyed by the
// Instance index the pod belongs to; a pod without the label is left out.
type PodRevisions map[int32][]query.RevisionID

// PodRevisionsByInstance reads PodRevisions off pods bucketed by Instance
// index, leaving out pods in a terminal phase and pods with no revision
// label; an Instance with no such pod has no entry. Every Instance's
// revisions are cut from one array, each entry capped at its own length,
// so an append to one never writes into the next.
func PodRevisionsByInstance(byInstance map[int32][]*corev1.Pod) PodRevisions {
	revs := make(PodRevisions, len(byInstance))
	total := 0
	for _, pods := range byInstance {
		total += len(pods)
	}
	revisions := make([]query.RevisionID, 0, total)
	for idx, pods := range byInstance {
		start := len(revisions)
		for _, pod := range pods {
			if pod == nil || query.IsTerminalPod(pod) {
				continue
			}
			if rev := query.RevisionFromPod(pod); !rev.IsZero() {
				revisions = append(revisions, rev)
			}
		}
		if end := len(revisions); end > start {
			revs[idx] = revisions[start:end:end]
		}
	}
	return revs
}

// anyInstanceOffRevision reports whether some Instance is off rev: a live
// pod of it carries another revision, its update operation is pinned to
// another, an attempt still in flight has its row recorded on another, or,
// with no labelled live pod, its row records another.
func anyInstanceOffRevision(insts []types.InstanceStatus, rev string, pods PodRevisions) bool {
	want := query.RevisionFromName(rev)
	rows := make(map[int32]struct{}, len(insts))
	for i := range insts {
		inst := &insts[i]
		rows[inst.Index] = struct{}{}
		if OperationPinnedOffRevision(inst, want) || attemptInFlightOffRevision(inst, want) {
			return true
		}
		if running := pods[inst.Index]; len(running) > 0 {
			if anyRevisionOff(running, want) {
				return true
			}
			continue
		}
		if inst.RunningRevision != "" && !query.RevisionFromName(inst.RunningRevision).Same(want) {
			return true
		}
	}
	// A live pod of another revision under an index with no row is a
	// replacement not gone yet.
	for idx, running := range pods {
		if _, ok := rows[idx]; !ok && anyRevisionOff(running, want) {
			return true
		}
	}
	return false
}

func anyRevisionOff(revs []query.RevisionID, want query.RevisionID) bool {
	for _, r := range revs {
		if !r.Same(want) {
			return true
		}
	}
	return false
}

// RowOnRevision reports whether a row rests on rev with no roll left: it
// runs rev and its update operation, if any, is pinned to rev. A pin left
// behind by a retarget still owns a replacement on the pinned revision.
func RowOnRevision(row *types.InstanceStatus, rev query.RevisionID) bool {
	return row != nil && query.RevisionFromName(row.RunningRevision).Same(rev) && !OperationPinnedOffRevision(row, rev)
}

// OperationPinnedOffRevision reports whether the row's update operation is
// pinned to a revision other than rev; a gang surge marker is such a row
// before it runs anything.
func OperationPinnedOffRevision(row *types.InstanceStatus, rev query.RevisionID) bool {
	if row == nil || row.Operation == nil {
		return false
	}
	op := row.Operation
	if op.Type != types.InstanceOperationUpdate || op.TargetRevision == "" {
		return false
	}
	return !query.RevisionFromName(op.TargetRevision).Same(rev)
}

// attemptInFlightOffRevision reports whether an update attempt a pass
// still drives has the row recorded on a revision other than rev: the set
// it rebuilt may already carry rev, but nothing has promoted the Instance
// onto it. A spent attempt, parked or on a Failed row, decides nothing here.
func attemptInFlightOffRevision(row *types.InstanceStatus, rev query.RevisionID) bool {
	if row == nil || row.RunningRevision == "" || types.Owner(row) != types.OwnerUpdate {
		return false
	}
	return !query.RevisionFromName(row.RunningRevision).Same(rev)
}

// anyAttemptInFlight reports whether a verb pass still drives some row: a
// Create, Update, Restart, Migrate or Delete attempt that has not ended, a
// lost set being rebuilt included. A spent attempt, on a Failed row or with
// its set still serving, is no pass's and decides nothing here.
func anyAttemptInFlight(insts []types.InstanceStatus) bool {
	for i := range insts {
		if types.Owner(&insts[i]) != types.OwnerNone {
			return true
		}
	}
	return false
}

// CountersForInstance computes the per-Instance counter set from the
// live pod bucket. availableByPod is the map returned by
// AvailablePodSet; window is the Component's minReadySeconds rule. The
// Instance's node list is appended to names (AppendUniqueNodes), so a
// reader of many Instances carries one array for all their lists; nil
// names gives the Instance a list of its own.
func CountersForInstance(pods []*corev1.Pod, availableByPod map[string]struct{}, window AvailabilityWindow, names []string) (InstanceCounters, []string) {
	available, nextAvailableIn := CountAvailablePods(pods, availableByPod, window)
	nodes, names := AppendUniqueNodes(names, pods)
	return InstanceCounters{
		PodCount:          int32(len(pods)),
		ReadyPodCount:     CountReadyPods(pods),
		ServingPodCount:   CountServingPods(pods),
		ScheduledPodCount: CountScheduledPods(pods),
		AvailablePodCount: available,
		NextAvailableIn:   nextAvailableIn,
		Admitted:          instancePodsAdmitted(pods),
		NodesOccupied:     nodes,
	}, names
}

func instancePodsAdmitted(pods []*corev1.Pod) bool {
	if len(pods) == 0 {
		return false
	}
	for _, pod := range pods {
		if types.PodAdmissionGated(pod) {
			return false
		}
	}
	return true
}

// InstanceCounters is the field-level result of CountersForInstance.
// The caller maps fields onto the per-Instance status by assignment.
type InstanceCounters struct {
	PodCount          int32
	ReadyPodCount     int32
	ServingPodCount   int32
	ScheduledPodCount int32
	AvailablePodCount int32
	// NextAvailableIn is how long until AvailablePodCount next rises on its
	// own: the remaining minReadySeconds window of the earliest in-rotation
	// pod still inside it, 0 when no pod is pending. Not persisted.
	NextAvailableIn time.Duration
	Admitted        bool
	NodesOccupied   []string
}

// String formats InstanceCounters for debug output.
func (c InstanceCounters) String() string {
	return fmt.Sprintf("pods=%d ready=%d serving=%d scheduled=%d available=%d nextAvailableIn=%s nodes=%v",
		c.PodCount, c.ReadyPodCount, c.ServingPodCount, c.ScheduledPodCount, c.AvailablePodCount, c.NextAvailableIn, c.NodesOccupied)
}
