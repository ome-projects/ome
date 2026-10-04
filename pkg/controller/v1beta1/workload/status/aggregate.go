// aggregate.go — the aggregated view of a Component's rows: the per-pod
// predicates (ready, serving, scheduled, available), the per-Instance
// counter set, the desired-count lookup every pass sizes a row by, and
// the Component-level convergence gates (ReachedDesiredShape,
// RolloutComplete) both adapters publish from.
package status

import (
	"context"
	"fmt"
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
// drain.IsPodInRotation per pod would do.
//
// Adapter-agnostic: takes the namespace and headless-service name
// directly, no v1beta1 ISVC handle needed.
func AvailablePodSet(ctx context.Context, reads client.Reader, namespace, serviceName string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	slices, err := drain.EndpointSlicesForService(ctx, reads, namespace, serviceName)
	if err != nil {
		return nil, err
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
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

// CountServingPods counts pods in their Service's rotation: Ready by the
// control plane, carrying the controller's serving gate
// (podreadiness.ReadyAndServing), and not terminating. This is the count
// MaxUnavailable budgets in coordination/ratio.go gate against; it drops
// when the controller takes a pod out of rotation (in-place update drain,
// recreate Phase A), when the node under a pod stops reporting, whatever
// the stale container statuses say, and when a pod is deleted: a
// terminating pod keeps its conditions until its containers stop, but the
// EndpointSlice controller has already taken it out of every Service's
// ready endpoints, so it serves nothing new.
func CountServingPods(pods []*corev1.Pod) int32 {
	var n int32
	for _, p := range pods {
		if p == nil || p.DeletionTimestamp != nil {
			continue
		}
		if podreadiness.ReadyAndServing(p) {
			n++
		}
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
				nextAvailableIn = EarliestPending(nextAvailableIn, remaining)
				continue
			}
		}
		n++
	}
	return n, nextAvailableIn
}

// earliestPending folds candidate into the running "next wake" value: the
// smallest positive duration wins and a non-positive candidate is no wake.
func EarliestPending(current, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

// UniqueNodes returns the deterministically-sorted set of node names
// hosting at least one pod. Returns nil for empty / all-unscheduled
// so NodesOccupied round-trips through nil ↔ nil rather than nil ↔
// empty-slice.
func UniqueNodes(pods []*corev1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, p := range pods {
		if p.Spec.NodeName == "" {
			continue
		}
		seen[p.Spec.NodeName] = struct{}{}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
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

// CountServingInstances counts Instances whose desired-many pods are
// Ready and serving (podreadiness.ReadyAndServing). This is the count the
// coordination unavailability gate (GateContext.CheckUnavailability)
// works from — "instances actually in the load balancer rotation
// right now."
func CountServingInstances(insts []types.InstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		if InstanceMeetsThreshold(s.PodCount, s.ServingPodCount, desired) {
			n++
		}
	}
	return n
}

// CountAvailableInstances counts Instances whose desired-many pods
// are in EndpointSlice rotation. Strict sub-condition of Ready —
// kube-proxy only publishes Ready pods.
func CountAvailableInstances(insts []types.InstanceStatus, desiredByIdx map[int32]int32) int32 {
	var n int32
	for _, s := range insts {
		desired := DesiredFor(desiredByIdx, s.Index, s.PodCount)
		if InstanceMeetsThreshold(s.PodCount, s.AvailablePodCount, desired) {
			n++
		}
	}
	return n
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

// ReachedDesiredShape reports whether insts have converged to the desired
// staged shape: exactly (replicas-partition) instances Ready on targetRev,
// the remaining `partition` instances Ready on a prior revision, and no
// missing/extra instances. partition=0 is full rollout, so RolloutComplete
// is ReachedDesiredShape(insts, target, 0, len(insts)). Held instances must
// be Ready too — a staged component is only "at rest" when every instance
// (new and held) is healthy.
func ReachedDesiredShape(insts []types.InstanceStatus, targetRevName string, partition, replicas int32) bool {
	if replicas <= 0 || int32(len(insts)) != replicas || partition < 0 || partition > replicas {
		return false
	}
	tgt := query.RevisionFromName(targetRevName)
	var onTarget, held int32
	for _, s := range insts {
		if s.Phase != types.InstancePhaseReady {
			return false
		}
		if query.RevisionFromName(s.RunningRevision).Same(tgt) {
			onTarget++
		} else {
			held++
		}
	}
	return onTarget == replicas-partition && held == partition
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
// status.currentRevision should name given the Instance rows and the
// Component's retry ladder. It is the update revision once every Instance
// is Ready on it and no RetryBlock names it: a block records a failure
// of the revision no later attempt has answered — a crash of a promoted
// set included, whose row reads Ready between crashes — and is removed
// once the rows outlive it, a corrective revision supersedes it or an
// operator releases a Held one, so the push reads landed only when the
// revision has proven itself rather than by attrition. It is withdrawn
// (empty) when it already names the update revision while some Instance
// still runs another: a fleet rolled back onto its last promoted revision
// is in flight until the last superseded Instance returns, and current
// equal to update is what every reader takes for "no rollout in flight".
// Any other row shape leaves current as recorded, so a repair on the
// current revision never moves it.
func CurrentRevisionFor(insts []types.InstanceStatus, current, update string, blocks []types.RetryBlock) string {
	if update == "" {
		return current
	}
	if RolloutComplete(insts, update) && types.FindRetryBlock(blocks, update) == nil {
		return update
	}
	if current == update && anyInstanceRunsAnother(insts, update) {
		return ""
	}
	return current
}

// anyInstanceRunsAnother reports whether some Instance records a running
// revision other than rev. A row that has not run anything yet names no
// revision and does not count.
func anyInstanceRunsAnother(insts []types.InstanceStatus, rev string) bool {
	want := query.RevisionFromName(rev)
	for _, inst := range insts {
		if inst.RunningRevision == "" {
			continue
		}
		if !query.RevisionFromName(inst.RunningRevision).Same(want) {
			return true
		}
	}
	return false
}

// CountersForInstance computes the per-Instance counter set from the
// live pod bucket. availableByPod is the map returned by
// AvailablePodSet; window is the Component's minReadySeconds rule.
func CountersForInstance(pods []*corev1.Pod, availableByPod map[string]struct{}, window AvailabilityWindow) InstanceCounters {
	available, nextAvailableIn := CountAvailablePods(pods, availableByPod, window)
	return InstanceCounters{
		PodCount:          int32(len(pods)),
		ReadyPodCount:     CountReadyPods(pods),
		ServingPodCount:   CountServingPods(pods),
		ScheduledPodCount: CountScheduledPods(pods),
		AvailablePodCount: available,
		NextAvailableIn:   nextAvailableIn,
		Admitted:          instancePodsAdmitted(pods),
		NodesOccupied:     UniqueNodes(pods),
	}
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
