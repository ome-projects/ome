package replay

import (
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// TickView is the read-only picture of one tick an observer receives once
// the pass has run. The service and the replicas are copies of what the
// apiserver holds after the pass. The pods are the ones the pass read: a
// pass of the service creates and deletes no pod, so they are the same
// before and after it.
type TickView struct {
	// Tick is the timeline tick the pass ran for.
	Tick int
	// Now is the injected clock after the tick's events.
	Now time.Time
	// Service is the InferenceService after the pass: the status the
	// flush left, which is the previous publication when the flush failed.
	Service *v1beta1.InferenceService
	// Replicas is each Component's InferenceReplica as published before
	// the pass: the revision pair and the rows the run layer and the
	// executors read.
	Replicas map[v1beta1.ComponentType]*v1beta1.InferenceReplica
	// Pods lists each Component's pods in name order.
	Pods map[v1beta1.ComponentType][]*corev1.Pod
	// Revisions lists every ControllerRevision the members minted, in name
	// order, each carrying the pairing protocol its template was minted
	// under.
	Revisions []*appsv1.ControllerRevision
	// Run is what the run layer did in the pass.
	Run RunView
}

// RunView is the run layer's outcome for one pass.
type RunView struct {
	// Ran reports that the run layer ran: the service declares a rollout
	// spec or still carries a rollout status.
	Ran bool
	// Opened reports that the pass pinned a fresh active run. Adopted
	// marks the open that bound a canary already in flight to a run
	// instead of starting one.
	Opened  bool
	Adopted bool
	// Targets are the revisions the opened run pinned, per Component.
	Targets []v1beta1.RolloutRunTarget
}

// TickObserver receives the view of every tick a pass ran in.
type TickObserver func(view TickView)

// Finding is one invariant violation.
type Finding struct {
	// Invariant is the invariant's short id.
	Invariant string
	// Component is the Component the finding is about; empty when it is
	// about the service as a whole.
	Component v1beta1.ComponentType
	// Class is the stable key a tally over recorded scenarios groups by:
	// the invariant, the rule and the shape involved, with no number or
	// name in it.
	Class string
	// Detail is the one-line account with the numbers and names.
	Detail string
}

// The invariant ids, as the findings and the tallies spell them.
const (
	// InvariantTrafficBacked is I2: every published traffic target with a
	// share above zero names a revision with a serving pod behind it.
	InvariantTrafficBacked = "I2"
	// InvariantRunOpens is the run layer's own rule: a run opens only
	// when there is something to roll.
	InvariantRunOpens = "run"
)

// componentOrder is the order the controller walks the Components in, so
// findings list in one order whatever a map iterates.
var componentOrder = v1beta1.RoleTypes()

// CheckInvariants runs every invariant over one tick's view.
func CheckInvariants(view TickView) []Finding {
	var findings []Finding
	findings = append(findings, CheckTrafficBacked(view)...)
	findings = append(findings, CheckRunOpensOnChange(view)...)
	return findings
}

// CheckTrafficBacked is invariant I2, traffic backed: no published traffic
// target sends a share above zero to a revision that has no serving pod. A
// pod serves when it is not being deleted, its PodReady condition and the
// serving gate are both True, and its runner is one a Service publishes.
// The class says whether the target is the Component's latest revision
// and whether the revision has pods at all.
func CheckTrafficBacked(view TickView) []Finding {
	if view.Service == nil {
		return nil
	}
	var findings []Finding
	for _, c := range componentOrder {
		cs, ok := view.Service.Status.Components[c]
		if !ok {
			continue
		}
		for _, target := range cs.Traffic {
			if target.Percent <= 0 {
				continue
			}
			hash := revisionHashOfService(target.RevisionName)
			var pods, serving int
			for _, pod := range view.Pods[c] {
				if pod.Labels[query.LabelRevisionHash] != hash || !query.RoutedRunner(pod) {
					continue
				}
				pods++
				if pod.DeletionTimestamp == nil && podreadiness.ReadyAndServing(pod) {
					serving++
				}
			}
			if serving > 0 {
				continue
			}
			role := "previous"
			if target.LatestRevision {
				role = "latest"
			}
			why := "pods-not-serving"
			if pods == 0 {
				why = "no-pods"
			}
			findings = append(findings, Finding{
				Invariant: InvariantTrafficBacked,
				Component: c,
				Class:     fmt.Sprintf("%s/traffic-to-dark-revision[%s,%s]", InvariantTrafficBacked, role, why),
				Detail: fmt.Sprintf("%s sends %d%% to %s, which has %d pod(s) and none serving",
					c, target.Percent, target.RevisionName, pods),
			})
		}
	}
	return findings
}

// CheckRunOpensOnChange holds the run layer to opening on a change: a run
// that opens pins at least one Component whose target revision differs
// from its stable one, or finds a replica whose published pair is
// diverged or whose rows are not all on its update revision. A run that
// opens with none of these opened on an unchanged spec.
func CheckRunOpensOnChange(view TickView) []Finding {
	if !view.Run.Opened {
		return nil
	}
	for _, target := range view.Run.Targets {
		if target.Revision != target.StableRevision {
			return nil
		}
	}
	for _, c := range componentOrder {
		ir := view.Replicas[c]
		if ir == nil {
			continue
		}
		if ir.Status.CurrentRevision != ir.Status.UpdateRevision {
			return nil
		}
		for _, row := range ir.Status.InstanceStatuses {
			if row.RunningRevision != ir.Status.UpdateRevision || (row.TargetRevision != "" && row.TargetRevision != row.RunningRevision) {
				return nil
			}
		}
	}
	how := "opened"
	if view.Run.Adopted {
		how = "adopted"
	}
	return []Finding{{
		Invariant: InvariantRunOpens,
		Class:     fmt.Sprintf("%s/opens-on-unchanged-spec[%s]", InvariantRunOpens, how),
		Detail: fmt.Sprintf("a run %s with every target equal to its stable revision (%s) and every replica pair converged",
			how, targetsValue(view.Run.Targets)),
	}}
}

// InvariantClosedRun is R: a run that closed Completed with every pinned
// Component reading Stable is followed by no run toward the same revisions,
// and its weighted traffic does not move, until the spec changes.
const InvariantClosedRun = "R"

// ClosedRunWatch is the observer behind R. Unlike the per-tick checks it
// keeps state: the last run seen closed Completed with its pinned
// Components Stable, the service generation at that close and the weighted
// traffic then. A later generation releases it (the operator changed the
// spec), and so does a run toward other revisions (the next operation); a
// released close is spent and never arms the watch again. A run is known
// by the time it opened: a record closed again with a later close time is
// the same run, not a second one.
type ClosedRunWatch struct {
	armed *closedRun
	spent string
}

type closedRun struct {
	openedAt, closedAt string
	generation         int64
	targets            map[v1beta1.ComponentType]string
	traffic            map[v1beta1.ComponentType]map[string]int32
}

// release forgets the armed close; it never arms the watch again.
func (w *ClosedRunWatch) release() {
	if w.armed != nil {
		w.spent = w.armed.openedAt
	}
	w.armed = nil
}

// Observe reads one tick's view after the pass and returns R's findings
// on it. A run seen toward the closed run's revisions is reported once,
// as is each move of a pinned Component's weighted traffic.
func (w *ClosedRunWatch) Observe(view TickView) []Finding {
	if view.Service == nil || view.Service.Status.Rollout == nil {
		return nil
	}
	rollout := view.Service.Status.Rollout
	var findings []Finding
	if w.armed != nil && view.Service.Generation != w.armed.generation {
		w.release()
	}
	if w.armed != nil {
		switch {
		case rollout.ActiveRun != nil:
			if sameTargets(runTargetsOf(rollout.ActiveRun.TargetRevisions), w.armed.targets) {
				findings = append(findings, w.reopened("open", "a run opened", view.Service.Generation))
			}
			w.release()
		case rollout.LastRun != nil && stampOf(rollout.LastRun.OpenedAt) != w.armed.openedAt:
			if sameTargets(runTargetsOf(rollout.LastRun.TargetRevisions), w.armed.targets) {
				findings = append(findings, w.reopened("closed", fmt.Sprintf("a run opened and closed %s", rollout.LastRun.Outcome), view.Service.Generation))
			}
			w.release()
		default:
			traffic := weightedTrafficOf(view.Service, w.armed.targets)
			for _, c := range componentOrder {
				before, armed := w.armed.traffic[c]
				if !armed || len(before) == 0 || trafficEqual(before, traffic[c]) {
					continue
				}
				findings = append(findings, Finding{
					Invariant: InvariantClosedRun,
					Component: c,
					Class:     fmt.Sprintf("%s/traffic-moves-after-completed-close", InvariantClosedRun),
					Detail: fmt.Sprintf("%s: traffic moved from %s to %s after the run closed Completed at %s with no run open and the service still at generation %d",
						c, trafficWords(before), trafficWords(traffic[c]), w.armed.closedAt, view.Service.Generation),
				})
				w.armed.traffic[c] = traffic[c]
			}
		}
	}
	last := rollout.LastRun
	if rollout.ActiveRun != nil || last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		return findings
	}
	openedAt, closedAt := stampOf(last.OpenedAt), stampOf(last.ClosedAt)
	if openedAt == w.spent || (w.armed != nil && w.armed.openedAt == openedAt) {
		return findings
	}
	targets := runTargetsOf(last.TargetRevisions)
	if len(targets) == 0 {
		return findings
	}
	for c := range targets {
		if phase := view.Service.Status.Components[c].RolloutPhase; phase != "" && phase != v1beta1.RolloutPhaseStable {
			return findings
		}
	}
	w.armed = &closedRun{openedAt: openedAt, closedAt: closedAt, generation: view.Service.Generation,
		targets: targets, traffic: weightedTrafficOf(view.Service, targets)}
	return findings
}

// reopened is the finding for a run seen toward the armed close's revisions.
func (w *ClosedRunWatch) reopened(how, what string, generation int64) Finding {
	return Finding{
		Invariant: InvariantClosedRun,
		Class:     fmt.Sprintf("%s/run-reopens-after-completed-close[%s]", InvariantClosedRun, how),
		Detail: fmt.Sprintf("%s toward %s after the run that closed Completed on the same revisions at %s, with the service still at generation %d",
			what, targetsWords(w.armed.targets), w.armed.closedAt, generation),
	}
}

func stampOf(t *metav1.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// runTargetsOf is the revision a run record pins per Component.
func runTargetsOf(targets []v1beta1.RolloutRunTarget) map[v1beta1.ComponentType]string {
	out := map[v1beta1.ComponentType]string{}
	for _, t := range targets {
		out[t.Component] = t.Revision
	}
	return out
}

// targetsWords renders a target map as component=revision in Component order.
func targetsWords(targets map[v1beta1.ComponentType]string) string {
	var parts []string
	for _, c := range componentOrder {
		if rev, ok := targets[c]; ok {
			parts = append(parts, fmt.Sprintf("%s=%s", c, rev))
		}
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// sameTargets reports whether every Component of want is pinned at the
// same revision in got.
func sameTargets(got, want map[v1beta1.ComponentType]string) bool {
	for c, rev := range want {
		if got[c] != rev {
			return false
		}
	}
	return true
}

// weightedTrafficOf is each pinned Component's traffic targets with a
// share above zero; an entry at 0% that is pruned later is no move.
func weightedTrafficOf(isvc *v1beta1.InferenceService, targets map[v1beta1.ComponentType]string) map[v1beta1.ComponentType]map[string]int32 {
	out := map[v1beta1.ComponentType]map[string]int32{}
	for c := range targets {
		picture := map[string]int32{}
		for _, t := range isvc.Status.Components[c].Traffic {
			if t.Percent > 0 {
				picture[t.RevisionName] = t.Percent
			}
		}
		out[c] = picture
	}
	return out
}

func trafficEqual(a, b map[string]int32) bool {
	if len(a) != len(b) {
		return false
	}
	for name, pct := range a {
		if b[name] != pct {
			return false
		}
	}
	return true
}

// trafficWords renders a weighted picture as name:percent in name order.
func trafficWords(picture map[string]int32) string {
	if len(picture) == 0 {
		return "[nothing weighted]"
	}
	names := make([]string, 0, len(picture))
	for name := range picture {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s:%d", name, picture[name]))
	}
	return "[" + strings.Join(parts, " ") + "]"
}
