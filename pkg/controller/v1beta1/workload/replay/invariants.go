package replay

import (
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TickView is the read-only picture of one tick an observer receives once
// the pass and the deadline park/rearm step have run. Rows, retry blocks
// and migration records are copies; pods and endpoints are read live from
// the fake apiserver, as the next pass will list them.
type TickView struct {
	// Tick is the timeline tick the pass ran for.
	Tick int
	// Now is the injected clock after the tick's events.
	Now time.Time
	// Rows are the owner's rows after the pass, each carrying the PodCount,
	// ServingPodCount and AvailablePodCount the status publication wrote
	// for the next pass to read: counted over the pod set the pass started
	// from, before its own creates, deletes and gate writes, against the
	// rotation and the clock after the pass.
	Rows []types.InstanceStatus
	// RetryBlocks are the owner's per-revision retry blocks after the pass.
	RetryBlocks []types.RetryBlock
	// Migrations are the owner's migration records after the pass, in the
	// order they were recorded.
	Migrations []types.MigrationRecord
	// Pods buckets the Component's pods by Instance index as the apiserver
	// lists them after the pass, Terminating pods included.
	Pods map[int32][]*corev1.Pod
	// Routed maps a pod name to the per-revision routed Service whose
	// EndpointSlice publishes it as an available endpoint. A pod absent
	// from the map is in no routed rotation.
	Routed map[string]string
	// Holds are the rollout holds the pass recorded, in order. A cleared
	// hold is not a hold and is not listed.
	Holds []types.RolloutHold
	// Commits are the row writes the store resolved during the tick, in
	// commit order, rejected and no-change mutations included.
	Commits []RowCommit
	// RetryCommits are the retry-block writes the store resolved during
	// the tick, in commit order.
	RetryCommits []RetryBlockCommit
	// MigrationWrites are the migration record writes the pass made during
	// the tick, in order: every append, and every mutation that changed a
	// record. A mutation that changed nothing is not listed.
	MigrationWrites []MigrationCommit
	// Spec is the desired spec the pass ran against.
	Spec TickSpec
	// StuckPodGrace is the configured grace a pod may fail its readiness
	// for before the engine reads it as out of service.
	StuckPodGrace time.Duration
	// PodWrites are the writes the engine made to pods during the tick, in
	// order, each with the pod as the apiserver stored it once the write
	// landed. The driver's own staging writes are not listed.
	PodWrites []PodWrite
	// Revisions are the stored revisions of the Component after the pass,
	// keyed by the hash a pod's revision label carries.
	Revisions map[string]RevisionTemplate
}

// PodWrite is one write the engine made to a pod, with the pod as the
// apiserver stored it once the write landed.
type PodWrite struct {
	// Verb is the trace's name for the write: create, patch, update, or
	// the subresource form such as status-patch.
	Verb string
	Pod  *corev1.Pod
}

// RevisionTemplate is what a stored revision asks a pod to run, as far as an
// in-place patch moves it: the images of the leader and worker templates and
// the pod template's own labels and annotations.
type RevisionTemplate struct {
	Images       map[string]string
	WorkerImages map[string]string
	Labels       map[string]string
	Annotations  map[string]string
}

// MigrationCommit is one migration record write as the driver applied it.
// Before is nil for an append; both are copies.
type MigrationCommit struct {
	RequestUUID string
	Appended    bool
	Before      *types.MigrationRecord
	After       *types.MigrationRecord
}

// TickSpec is the part of the desired spec the invariants and the reading
// comparison read.
type TickSpec struct {
	Replicas int32
	// PodsPerInstance is the pod count one Instance is expected to run:
	// one, or the leader plus the workers of a gang.
	PodsPerInstance int32
	// MinReadySeconds is how long a pod holds PodReady before it counts
	// as available.
	MinReadySeconds int32
	// MaxUnavailable, MaxSurge and Partition are the rolling-update knobs
	// as the engine receives them; nil is unset.
	MaxUnavailable *intstr.IntOrString
	MaxSurge       *intstr.IntOrString
	Partition      *int32
	// HeldTo are the budgets, in Instances, the capacity and surge-ceiling
	// invariants hold the tick to: the spec's, widened to the budget an
	// attempt still open was admitted under, since an attempt keeps the
	// budget it opened under until it lands. Nil reads the spec's budgets
	// alone, as a view built by hand does.
	HeldTo *BudgetsHeldTo
	Paused bool
	// Teardown marks the owner deleted: the plan carries no Instance and
	// every pod is on its way out.
	Teardown bool
}

// TickObserver receives the view of every tick a pass ran in.
type TickObserver func(view TickView)

// Finding is one invariant violation.
type Finding struct {
	// Invariant is the invariant's short id: I1, I3, I4, I5 or the surge
	// ceiling's.
	Invariant string
	// Index is the Instance the finding is about, or ComponentWide when
	// it is about the Component as a whole.
	Index int32
	// Class is the stable key a tally over recorded scenarios groups by:
	// the invariant, the rule and the row states involved, with no number
	// or name in it.
	Class string
	// Detail is the one-line account with the numbers and names.
	Detail string
}

// ComponentWide is the Finding.Index of a finding about the Component as
// a whole rather than one Instance.
const ComponentWide int32 = -1

// The invariant ids, as the findings and the tallies spell them.
const (
	InvariantCapacity     = "I1"
	InvariantBoundedWaits = "I3"
	InvariantStatusTruth  = "I4"
	InvariantSingleOwner  = "I5"
	InvariantLabelTruth   = "I6"
	InvariantSurgeCeiling = "ceiling"
)

// CheckInvariants runs every invariant over one tick's view.
func CheckInvariants(view TickView) []Finding {
	var findings []Finding
	findings = append(findings, CheckCapacity(view)...)
	findings = append(findings, CheckSurgeCeiling(view)...)
	findings = append(findings, CheckBoundedWaits(view)...)
	findings = append(findings, CheckStatusTruth(view)...)
	findings = append(findings, CheckSingleOwner(view)...)
	findings = append(findings, CheckOwnership(view)...)
	findings = append(findings, CheckLabelTruth(view)...)
	return findings
}

// CheckLabelTruth is invariant I6, label truth: no engine write leaves a pod
// running one revision's template under another revision's label, which the
// routed Services select by. A label naming no stored revision is not judged.
func CheckLabelTruth(view TickView) []Finding {
	var findings []Finding
	for _, write := range view.PodWrites {
		pod := write.Pod
		if pod == nil {
			continue
		}
		hash := pod.Labels[query.LabelRevisionHash]
		template, known := view.Revisions[hash]
		if !known {
			continue
		}
		why := templateDisagreement(pod, template)
		if why == "" {
			continue
		}
		index, _ := query.InstanceIdxFromLabels(pod)
		findings = append(findings, Finding{
			Invariant: InvariantLabelTruth,
			Index:     index,
			Class:     fmt.Sprintf("%s/label-template-apart[%s]", InvariantLabelTruth, write.Verb),
			Detail:    fmt.Sprintf("pod %s after its %s carries revision label %s but %s", pod.Name, write.Verb, hash, why),
		})
	}
	return findings
}

// templateDisagreement says how a pod differs from the template its label
// names, empty when it does not. The labels the workload stamps and the
// in-place transition marker are not template.
func templateDisagreement(pod *corev1.Pod, template RevisionTemplate) string {
	images := template.Images
	if pod.Labels[query.LabelRunner] == RunnerWorker && template.WorkerImages != nil {
		images = template.WorkerImages
	}
	running := make(map[string]string, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		running[container.Name] = container.Image
	}
	for _, name := range sortedKeys(images) {
		if got := running[name]; got != images[name] {
			return fmt.Sprintf("runs container %s at %s where that revision's template has %s", name, orNil(got), images[name])
		}
	}
	for _, key := range sortedKeys(template.Labels) {
		if query.WorkloadOwnedLabel(key) {
			continue
		}
		if got := pod.Labels[key]; got != template.Labels[key] {
			return fmt.Sprintf("carries label %s=%s where that revision's template has %s", key, orNil(got), template.Labels[key])
		}
	}
	for _, key := range sortedKeys(template.Annotations) {
		if key == constants.InferenceServiceInPlaceImageTransitionAnnotationKey {
			continue
		}
		if got := pod.Annotations[key]; got != template.Annotations[key] {
			return fmt.Sprintf("carries annotation %s=%s where that revision's template has %s", key, orNil(got), template.Annotations[key])
		}
	}
	return ""
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// CheckCapacity is invariant I1, capacity: the pods that are Ready,
// serving and in a routed Service's rotation, over every Instance, number
// at least replicas minus the resolved maxUnavailable. The budget comes
// from the engine's own PerComponentMaxUnavailableBudget, so the rounding
// is the engine's; an unset budget caps nothing and the check is skipped,
// as it is for a deleted owner. The replay runs one Component with no
// pairing protocol, so only the protocol-empty branch of the capacity rule
// is checked here.
func CheckCapacity(view TickView) []Finding {
	if view.Spec.Teardown {
		return nil
	}
	budget, specBudget := view.Spec.drainBudget()
	if budget == escalation.BudgetNoLimit {
		return nil
	}
	floor := view.Spec.Replicas - budget
	if floor <= 0 {
		return nil
	}
	var serving int32
	servingByIndex := map[int32]int32{}
	for index, pods := range view.Pods {
		for _, pod := range pods {
			if servesInRotation(view, pod) {
				serving++
				servingByIndex[index]++
			}
		}
	}
	if serving >= floor {
		return nil
	}
	return []Finding{{
		Invariant: InvariantCapacity,
		Index:     ComponentWide,
		Class:     fmt.Sprintf("%s/below-floor[%s]", InvariantCapacity, darkInstanceLabels(view, servingByIndex)),
		Detail: fmt.Sprintf("%d pod(s) serve in a routed rotation; the floor is %d (replicas %d minus maxUnavailable %d%s)",
			serving, floor, view.Spec.Replicas, budget, widenedClause(budget, specBudget)),
	}}
}

// drainBudget is the unavailability budget the tick is held to and the
// spec's own: the same number unless an attempt still open was admitted
// under a wider one.
func (s TickSpec) drainBudget() (budget, specBudget int32) {
	specBudget = escalation.PerComponentMaxUnavailableBudget(&types.RollingUpdate{MaxUnavailable: s.MaxUnavailable}, s.Replicas)
	if s.HeldTo == nil {
		return specBudget, specBudget
	}
	return s.HeldTo.MaxUnavailable, specBudget
}

// surgeBudget is the surge allowance the tick is held to and the spec's
// own, read as drainBudget reads the drain budget.
func (s TickSpec) surgeBudget() (allowance, specAllowance int32) {
	specAllowance = escalation.PerComponentMaxSurgeBudget(&types.RollingUpdate{MaxSurge: s.MaxSurge}, s.Replicas)
	if s.HeldTo == nil {
		return specAllowance, specAllowance
	}
	return s.HeldTo.MaxSurge, specAllowance
}

// widenedClause names the spec's own budget when an open attempt widened
// the one the tick is held to, and is empty otherwise.
func widenedClause(budget, specBudget int32) string {
	if budget == specBudget {
		return ""
	}
	return fmt.Sprintf(", widened from the spec's %d by an attempt still open", specBudget)
}

// servesInRotation reports whether a pod carries traffic: Ready and
// serving by the engine's own reading, and published as an available
// endpoint of a routed Service.
func servesInRotation(view TickView, pod *corev1.Pod) bool {
	return podreadiness.ReadyAndServing(pod) && view.Routed[pod.Name] != ""
}

// darkInstanceLabels names the states of the Instances that serve nothing,
// sorted and deduplicated, with a marker when fewer rows exist than
// replicas. It is the part of a capacity class that says why the floor was
// missed.
func darkInstanceLabels(view TickView, servingByIndex map[int32]int32) string {
	labels := map[string]struct{}{}
	for i := range view.Rows {
		row := &view.Rows[i]
		if servingByIndex[row.Index] == 0 {
			labels[stateLabel(row)] = struct{}{}
		}
	}
	if int32(len(view.Rows)) < view.Spec.Replicas {
		labels["no-row"] = struct{}{}
	}
	return joinSorted(labels)
}

// CheckSurgeCeiling is the surge ceiling: the Component never runs more
// live pods than its desired set plus its surge allowance, counted in
// pods of one Instance each. A pod is live when it exists, is not being
// deleted and has not reached a terminal phase. The allowance is the
// plan's maxSurge, resolved through the engine's own
// PerComponentMaxSurgeBudget. A plan that declares no maxSurge caps
// nothing at this layer, and the engine then admits at most one
// replacement per Instance at a time, so the allowance is one Instance
// per replica. A deleted owner is not checked. The class names the
// allowance in force and the states of the Instances holding pods beyond
// the desired set: an index past replicas, or more live pods than one
// Instance runs.
func CheckSurgeCeiling(view TickView) []Finding {
	if view.Spec.Teardown || view.Spec.PodsPerInstance <= 0 {
		return nil
	}
	allowance, specAllowance := view.Spec.surgeBudget()
	bound := "maxSurge"
	if allowance == escalation.BudgetNoLimit {
		allowance, bound = view.Spec.Replicas, "unbudgeted"
	}
	ceiling := (view.Spec.Replicas + allowance) * view.Spec.PodsPerInstance
	var live int32
	liveByIndex := map[int32]int32{}
	for index, pods := range view.Pods {
		for _, pod := range pods {
			if livePod(pod) {
				live++
				liveByIndex[index]++
			}
		}
	}
	if live <= ceiling {
		return nil
	}
	return []Finding{{
		Invariant: InvariantSurgeCeiling,
		Index:     ComponentWide,
		Class: fmt.Sprintf("%s/live-pods-over[%s][%s]", InvariantSurgeCeiling, bound,
			surplusInstanceLabels(view, liveByIndex)),
		Detail: fmt.Sprintf("%d live pod(s); the ceiling is %d (replicas %d plus a %s allowance of %d%s, %d pod(s) per Instance)",
			live, ceiling, view.Spec.Replicas, bound, allowance, widenedClause(allowance, specAllowance), view.Spec.PodsPerInstance),
	}}
}

// livePod reports whether a pod still holds a slot: it exists, no deletion
// has been requested and the kubelet runs it, or will.
func livePod(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && !query.IsTerminalPod(pod)
}

// surplusInstanceLabels names the states of the Instances that hold pods
// beyond the desired set, sorted and deduplicated, with a marker for an
// index that has pods and no row.
func surplusInstanceLabels(view TickView, liveByIndex map[int32]int32) string {
	rows := map[int32]*types.InstanceStatus{}
	for i := range view.Rows {
		rows[view.Rows[i].Index] = &view.Rows[i]
	}
	labels := map[string]struct{}{}
	for index, live := range liveByIndex {
		if index < view.Spec.Replicas && live <= view.Spec.PodsPerInstance {
			continue
		}
		if row, ok := rows[index]; ok {
			labels[stateLabel(row)] = struct{}{}
		} else {
			labels["no-row"] = struct{}{}
		}
	}
	return joinSorted(labels)
}

// CheckBoundedWaits is invariant I3, bounded waits: an operation still in
// flight past its deadline names what it waits on, or the pass recorded a
// rollout hold for the Component. A settled row runs no clock, and a
// parked (zero) deadline is a wait the adapter has already explained.
func CheckBoundedWaits(view TickView) []Finding {
	if len(view.Holds) > 0 {
		return nil
	}
	var findings []Finding
	for i := range view.Rows {
		row := &view.Rows[i]
		if !types.InFlight(row) {
			continue
		}
		op := row.Operation
		if op == nil || op.Deadline.IsZero() || !view.Now.After(op.Deadline.Time) || op.Waiting != "" {
			continue
		}
		findings = append(findings, Finding{
			Invariant: InvariantBoundedWaits,
			Index:     row.Index,
			Class:     fmt.Sprintf("%s/overdue-unexplained[%s]", InvariantBoundedWaits, stateLabel(row)),
			Detail: fmt.Sprintf("index %d: %s/%s passed its deadline %s ago with no waiting reason and no hold recorded",
				row.Index, op.Type, op.Step, view.Now.Sub(op.Deadline.Time)),
		})
	}
	return findings
}

// CheckStatusTruth is invariant I4, status truth: a Ready row has a pod
// that is Ready and serving; a Failed row has none, unless its LastFailure
// names a replacement attempt beside a serving source; and a row's
// PodCount is the number of pods the apiserver lists for its Instance.
// The published ServingPodCount is set beside the reading of the pods by
// CompareReadings, not here.
func CheckStatusTruth(view TickView) []Finding {
	var findings []Finding
	for i := range view.Rows {
		row := &view.Rows[i]
		pods := view.Pods[row.Index]
		var serving []string
		for _, pod := range pods {
			if podreadiness.ReadyAndServing(pod) {
				serving = append(serving, pod.Name)
			}
		}
		switch row.Phase {
		case types.InstancePhaseReady:
			if len(serving) == 0 {
				findings = append(findings, Finding{
					Invariant: InvariantStatusTruth,
					Index:     row.Index,
					Class:     fmt.Sprintf("%s/ready-row-dark[%s]", InvariantStatusTruth, darkReason(pods)),
					Detail:    fmt.Sprintf("index %d is Ready with %d pod(s) and none Ready and serving", row.Index, len(pods)),
				})
			}
		case types.InstancePhaseFailed:
			if len(serving) > 0 && !failureNamesReplacement(row, serving) {
				findings = append(findings, Finding{
					Invariant: InvariantStatusTruth,
					Index:     row.Index,
					Class:     fmt.Sprintf("%s/failed-row-serving[%s]", InvariantStatusTruth, operationLabel(row.Operation)),
					Detail: fmt.Sprintf("index %d is Failed while %s serve(s); lastFailure names %q",
						row.Index, strings.Join(serving, ","), failedPodName(row)),
				})
			}
		}
		if row.PodCount != int32(len(pods)) {
			findings = append(findings, Finding{
				Invariant: InvariantStatusTruth,
				Index:     row.Index,
				Class:     fmt.Sprintf("%s/podcount-differs[%s]", InvariantStatusTruth, stateLabel(row)),
				Detail:    fmt.Sprintf("index %d publishes podCount=%d but the apiserver lists %d pod(s)", row.Index, row.PodCount, len(pods)),
			})
		}
	}
	return findings
}

// darkReason says why a Ready row has no pod that is Ready and serving:
// it has no pods, a pod is serving on its containers with the kubelet yet
// to fold the gate into PodReady, or its pods have lost readiness.
func darkReason(pods []*corev1.Pod) string {
	if len(pods) == 0 {
		return "no-pods"
	}
	for _, pod := range pods {
		if podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod) {
			return "pod-ready-pending"
		}
	}
	return "pods-not-serving"
}

// operationLabel names an operation for a class string.
func operationLabel(op *types.InstanceOperation) string {
	if op == nil {
		return "no-operation"
	}
	return string(op.Type) + "/" + op.Step
}

// failureNamesReplacement reports whether a Failed row's LastFailure names
// a pod other than the ones serving: the record of a replacement attempt
// that died beside a source still carrying traffic.
func failureNamesReplacement(row *types.InstanceStatus, serving []string) bool {
	name := failedPodName(row)
	if name == "" {
		return false
	}
	for _, pod := range serving {
		if pod == name {
			return false
		}
	}
	return true
}

func failedPodName(row *types.InstanceStatus) string {
	if row.LastFailure == nil {
		return ""
	}
	return row.LastFailure.PodName
}

// CheckSingleOwner is invariant I5, single owner: within one tick at most
// one committed write per index changes a row's phase or its operation's
// identity (presence, id, type or step), and no index is written under two
// operation types. A write belongs to the operation type it leaves on the
// row, or to the type it cleared. A write that only refreshes timestamps,
// counters or the waiting token is a stamp and does not count; a mutation
// that did not commit wrote nothing.
func CheckSingleOwner(view TickView) []Finding {
	type track struct {
		changes int
		path    []string
		opTypes map[string]struct{}
	}
	byIndex := map[int32]*track{}
	var order []int32
	for _, commit := range view.Commits {
		if commit.Outcome != PreconditionOK {
			continue
		}
		t := byIndex[commit.Index]
		if t == nil {
			t = &track{opTypes: map[string]struct{}{}, path: []string{stateLabel(commit.Before)}}
			byIndex[commit.Index] = t
			order = append(order, commit.Index)
		}
		if !identityChanged(commit.Before, commit.After) {
			continue
		}
		t.changes++
		t.path = append(t.path, afterLabel(commit))
		if verb := writtenUnder(commit); verb != "" {
			t.opTypes[verb] = struct{}{}
		}
	}
	var findings []Finding
	for _, index := range order {
		t := byIndex[index]
		if t.changes > 1 {
			findings = append(findings, Finding{
				Invariant: InvariantSingleOwner,
				Index:     index,
				Class:     fmt.Sprintf("%s/identity-writes[%s]", InvariantSingleOwner, strings.Join(t.path, ">")),
				Detail: fmt.Sprintf("index %d took %d committed writes changing its phase or operation in one tick: %s",
					index, t.changes, strings.Join(t.path, " > ")),
			})
		}
		if len(t.opTypes) > 1 {
			findings = append(findings, Finding{
				Invariant: InvariantSingleOwner,
				Index:     index,
				Class:     fmt.Sprintf("%s/two-operation-types[%s]", InvariantSingleOwner, joinSorted(t.opTypes)),
				Detail:    fmt.Sprintf("index %d was written under operation types %s in one tick", index, joinSorted(t.opTypes)),
			})
		}
	}
	return findings
}

// CheckOwnership is the ownership form of invariant I5: within one tick,
// an Instance's row, the retry record its attempt charges and the
// migration record it is pinned to are written by one operation owner.
// A row write belongs to the operation it leaves on the row, else to the
// one it cleared. A retry-record write is charged to the Instances whose
// row writes landed in the same status write, under the operation each
// row carried before that write, which is the attempt the ladder counts.
// A migration record write belongs to Migrate, on the record's source and
// surge Instances. Two owners on one Instance in one tick is a finding.
// The second finding is a row that drops its Migrate pin while the record
// it names stays open and unwritten in the tick: a pinned row is the
// record's to write, and the record closes in the same pass that frees
// its rows. A deleted owner's rows all belong to the teardown.
func CheckOwnership(view TickView) []Finding {
	type track struct {
		owners map[string]struct{}
		path   []string
	}
	byIndex := map[int32]*track{}
	var order []int32
	claim := func(index int32, owner string) *track {
		t := byIndex[index]
		if t == nil {
			t = &track{owners: map[string]struct{}{}}
			byIndex[index] = t
			order = append(order, index)
		}
		if owner != "" {
			t.owners[owner] = struct{}{}
		}
		return t
	}
	for _, commit := range view.Commits {
		if commit.Outcome != PreconditionOK {
			continue
		}
		t := claim(commit.Index, writtenUnder(commit))
		if len(t.path) == 0 {
			t.path = append(t.path, stateLabel(commit.Before))
		}
		if identityChanged(commit.Before, commit.After) {
			t.path = append(t.path, afterLabel(commit))
		}
	}
	for _, block := range view.RetryCommits {
		for _, commit := range view.Commits {
			if commit.Outcome != PreconditionOK || commit.Batch != block.Batch {
				continue
			}
			claim(commit.Index, chargedUnder(commit))
		}
	}
	for _, write := range view.MigrationWrites {
		for _, record := range []*types.MigrationRecord{write.Before, write.After} {
			if record == nil {
				continue
			}
			claim(record.SourceInstance, string(types.InstanceOperationMigrate))
			if record.SurgeInstance != nil {
				claim(*record.SurgeInstance, string(types.InstanceOperationMigrate))
			}
		}
	}
	var findings []Finding
	for _, index := range order {
		t := byIndex[index]
		if len(t.owners) < 2 {
			continue
		}
		path := strings.Join(t.path, ">")
		if path == "" {
			path = "no-row-write"
		}
		findings = append(findings, Finding{
			Invariant: InvariantSingleOwner,
			Index:     index,
			Class:     fmt.Sprintf("%s/two-owners[%s][%s]", InvariantSingleOwner, joinSorted(t.owners), path),
			Detail: fmt.Sprintf("index %d had its row or records written by operations %s in one tick (row: %s)",
				index, joinSorted(t.owners), path),
		})
	}
	if view.Spec.Teardown {
		return findings
	}
	for _, commit := range view.Commits {
		if commit.Outcome != PreconditionOK || !dropsMigratePin(commit) {
			continue
		}
		record := openMigrationRecord(view, commit.Before.Operation.RequestUUID, commit.Index)
		if record == nil || migrationWritten(view, record.RequestUUID) {
			continue
		}
		findings = append(findings, Finding{
			Invariant: InvariantSingleOwner,
			Index:     commit.Index,
			Class: fmt.Sprintf("%s/pin-dropped-record-open[%s>%s]", InvariantSingleOwner,
				stateLabel(commit.Before), afterLabel(commit)),
			Detail: fmt.Sprintf("index %d dropped its Migrate pin (%s > %s) while its migration record stays %s and no pass wrote it this tick",
				commit.Index, stateLabel(commit.Before), afterLabel(commit), record.Phase),
		})
	}
	return findings
}

// chargedUnder names the operation a retry-record charge in the same
// status write counts against: the attempt the row carried before the
// write, else the operation the write left on it.
func chargedUnder(commit RowCommit) string {
	if commit.Before != nil && commit.Before.Operation != nil {
		return string(commit.Before.Operation.Type)
	}
	if commit.After != nil && commit.After.Operation != nil {
		return string(commit.After.Operation.Type)
	}
	return ""
}

// dropsMigratePin reports whether a committed write takes a Migrate
// operation off the row: the row is removed, left with no operation, or
// handed to another operation type.
func dropsMigratePin(commit RowCommit) bool {
	before := commit.Before
	if before == nil || before.Operation == nil || before.Operation.Type != types.InstanceOperationMigrate {
		return false
	}
	after := commit.After
	return commit.Remove || after == nil || after.Operation == nil || after.Operation.Type != types.InstanceOperationMigrate
}

// openMigrationRecord finds the migration record a pin names, by its
// request id when the pin carries one and otherwise by the Instance,
// provided the record is still open after the pass. A closed record, or
// none, owns no row.
func openMigrationRecord(view TickView, requestUUID string, index int32) *types.MigrationRecord {
	for i := range view.Migrations {
		record := &view.Migrations[i]
		if record.Phase.Terminal() {
			continue
		}
		if requestUUID != "" {
			if record.RequestUUID == requestUUID {
				return record
			}
			continue
		}
		if record.SourceInstance == index || (record.SurgeInstance != nil && *record.SurgeInstance == index) {
			return record
		}
	}
	return nil
}

// migrationWritten reports whether the pass wrote the record with the
// given request id.
func migrationWritten(view TickView, requestUUID string) bool {
	for _, write := range view.MigrationWrites {
		if write.RequestUUID == requestUUID {
			return true
		}
	}
	return false
}

// identityChanged reports whether a committed write moved the row's phase
// or its operation's identity. A row appearing or leaving counts; a write
// that touched only timestamps, counters or the waiting token does not.
func identityChanged(before, after *types.InstanceStatus) bool {
	if before == nil || after == nil {
		return before != after
	}
	if before.Phase != after.Phase {
		return true
	}
	if (before.Operation == nil) != (after.Operation == nil) {
		return true
	}
	if before.Operation == nil {
		return false
	}
	return before.Operation.ID != after.Operation.ID ||
		before.Operation.Type != after.Operation.Type ||
		before.Operation.Step != after.Operation.Step
}

// writtenUnder names the operation type a committed write belongs to: the
// one it leaves on the row, else the one it cleared. A write that moves
// only the phase of a row with no operation belongs to no verb.
func writtenUnder(commit RowCommit) string {
	if commit.After != nil && commit.After.Operation != nil {
		return string(commit.After.Operation.Type)
	}
	if commit.Before != nil && commit.Before.Operation != nil {
		return string(commit.Before.Operation.Type)
	}
	return ""
}

// afterLabel names the state a committed write left behind.
func afterLabel(commit RowCommit) string {
	if commit.Remove || commit.After == nil {
		return "removed"
	}
	return stateLabel(commit.After)
}

// stateLabel names a row's state for a class string: the ownership table's
// state id, Empty for no row, and Unknown for a shape the table does not
// name.
func stateLabel(row *types.InstanceStatus) string {
	state := types.StateOf(row)
	if state == types.StateUnknown {
		return "Unknown"
	}
	return string(state)
}

func joinSorted(set map[string]struct{}) string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}
