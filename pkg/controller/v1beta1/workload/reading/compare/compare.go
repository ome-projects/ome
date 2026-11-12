// Package compare sets the readers the passes use beside the reading of
// the same pods and lists where they disagree, so a test can count the
// disagreements over recorded replay scenarios. It only measures: no pass
// calls it, and it keeps the reading package free of the other readers.
package compare

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/reading"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Disagreement records one place where a reader the passes use and the
// reading answer differently for the same pods.
type Disagreement struct {
	// Reader names the reader compared, such as "evidence.PodSetServesNothing".
	Reader string
	// Class is a short, stable key for grouping, such as
	// "servesNothing/evidence=true/reading=false".
	Class string
	// Detail says what each side saw, for a person reading the report.
	Detail string
}

// Reader names, as the Reader field of a Disagreement carries them.
const (
	ReaderCountServingPods       = "status.CountServingPods"
	ReaderCountReadyPods         = "status.CountReadyPods"
	ReaderPodSetFullyServing     = "query.PodSetFullyServing"
	ReaderPodSetServesNothing    = "evidence.PodSetServesNothing"
	ReaderPodSetOutOfRotation    = "evidence.PodSetOutOfRotation"
	ReaderUnavailableOnParked    = "evidence.PodSetUnavailableOnParkedMember"
	ReaderRecreateOfUnservingRow = "types.RecreateOfUnservingRow"
	ReaderPersistedServingPods   = "InstanceStatus.ServingPodCount"
	ReaderPersistedAvailablePods = "InstanceStatus.AvailablePodCount"
	ReaderPodReadyAndServing     = "coordination.PodReadyAndServing"
	ReaderIsPodAvailable         = "podreadiness.IsPodAvailable"
	// ReaderPodCount is the publication's per-Instance pod count.
	ReaderPodCount = "InstanceCounters.PodCount"
	// ReaderReadyReplicas is the Instance's term in the Component's
	// readyReplicas, which the top-level Ready condition reads.
	ReaderReadyReplicas = "ComponentCounters.ReadyReplicas"
	// ReaderLivePods is the presence count the restart trigger and the
	// refill read: the pods not in a terminal phase.
	ReaderLivePods = "query.ExcludeTerminalPods"
	// ReaderRolledInstanceDark is the roll's rule for a rolled Instance
	// that serves nothing, which frees a budget slot.
	ReaderRolledInstanceDark = "ops.RolledInstanceDark"
	// ReaderPodSetIsCapacity is the escalation's exemption for an
	// Instance that is capacity.
	ReaderPodSetIsCapacity = "escalation.podSetIsCapacity"
	// ReaderRunnerRestarted is the restart detector's per-pod reading of
	// a runner that restarted since the row entered Ready.
	ReaderRunnerRestarted = "ops.RunnerRestartedSinceReady"
	// ReaderAdoption is the Ready stamp that adopts a pod set already on
	// the target revision.
	ReaderAdoption = "ops.evaluateUpdateTriggerPods"
	// ReaderAllPodsRuntimeReady is the writers' admission of the serving
	// gate onto a pod set.
	ReaderAllPodsRuntimeReady = "query.AllPodsRuntimeReady"
	// ReaderIsServing is the writers' reading of the gate alone, deciding
	// whether a gate write is still owed.
	ReaderIsServing = "podreadiness.IsServing"
	// ReaderPodSetPromotable is the bar every Ready stamp but adoption
	// waits for.
	ReaderPodSetPromotable = "query.PodSetPromotable"
)

// Compare reads the pods (reading.Read) and evaluates the readers the
// passes use beside that reading, listing where they disagree. row is the
// Instance's persisted status, one pass old by the time the pods are
// read; nil skips the row readers. The set readers come first, then the
// row readers, then the per-pod readers.
func Compare(pods []*corev1.Pod, desired int32, row *types.InstanceStatus, ctx reading.Context) []Disagreement {
	r := reading.Read(pods, desired, ctx)
	minReadySeconds := int32(ctx.AvailabilityWindow / time.Second)
	var out []Disagreement
	out = appendSetReaders(out, pods, desired, minReadySeconds, r, ctx)
	if row != nil {
		out = appendRowReaders(out, pods, desired, row, minReadySeconds, r, ctx)
	}
	out = appendPodReaders(out, pods, row, minReadySeconds, r, ctx)
	return out
}

// appendSetReaders compares the readers that take the pod set and no row.
func appendSetReaders(out []Disagreement, pods []*corev1.Pod, desired, minReadySeconds int32, r reading.InstanceReading, ctx reading.Context) []Disagreement {
	if got := status.CountServingPods(pods); got != r.Counts.Serving {
		out = append(out, Disagreement{
			Reader: ReaderCountServingPods,
			Class:  "servingCount/status" + relation(got, r.Counts.Serving) + "reading",
			Detail: fmt.Sprintf("status counts %d serving pods, the reading %d", got, r.Counts.Serving),
		})
	}
	// The publication's ready count reads ContainersReady on every pod
	// listed; the reading's RuntimeReady level reads it on live pods only.
	if got := status.CountReadyPods(pods); got != r.Counts.RuntimeReady {
		out = append(out, Disagreement{
			Reader: ReaderCountReadyPods,
			Class:  "readyCount/status" + relation(got, r.Counts.RuntimeReady) + "reading",
			Detail: fmt.Sprintf("status counts %d ready pods, the reading %d runtime-ready", got, r.Counts.RuntimeReady),
		})
	}
	out = appendBoolReader(out, ReaderPodSetFullyServing, "fullyServing", "query",
		query.PodSetFullyServing(pods, desired), r.FullyServing())
	out = appendBoolReader(out, ReaderPodSetServesNothing, "servesNothing", "evidence",
		evidence.PodSetServesNothing(pods, ctx.Now, ctx.UnreadyGrace), r.ServesNothing())
	out = appendBoolReader(out, ReaderPodSetOutOfRotation, "outOfRotation", "evidence",
		evidence.PodSetOutOfRotation(pods), r.OutOfRotation())
	out = appendBoolReader(out, ReaderUnavailableOnParked, "unavailableOnParkedMember", "evidence",
		evidence.PodSetUnavailableOnParkedMember(pods, desired, ctx.Now, ctx.UnreadyGrace), r.UnavailableOnHeldMember())

	// The publication's pod count is every pod listed, a pod being
	// deleted or in a terminal phase included; the readers of the count
	// for presence (a Ready row with no pod is demoted, a rebuild waits
	// for a name still held) see such a pod as present. The reading's
	// Live count is the pods that exist and can still run. The class says
	// whether a terminal or a terminating pod made the difference.
	if got := int32(len(pods)); got != r.Counts.Live {
		why := "terminating"
		if anyPod(r, func(p reading.PodReading) bool { return p.Terminal }) {
			why = "terminal"
		}
		out = append(out, Disagreement{
			Reader: ReaderPodCount,
			Class:  "podCount/status" + relation(got, r.Counts.Live) + "reading[" + why + "]",
			Detail: fmt.Sprintf("the publication counts %d pods, the reading %d live", got, r.Counts.Live),
		})
	}
	// The Component's readyReplicas counts an Instance whose ready count
	// (ContainersReady on every pod listed) reaches the desired count,
	// and the top-level Ready condition reads "all Instances Ready" from
	// it. The reading's answer is the same roll-up on the Serving level.
	// The class says whether a pod on its way out still answering its
	// probe made the difference, or a live pod whose containers are ready
	// while it is not in rotation.
	readyPods := status.CountReadyPods(pods)
	readyToCounter := status.InstanceMeetsThreshold(int32(len(pods)), readyPods, desired)
	servingToReading := status.InstanceMeetsThreshold(r.Counts.Pods, r.Counts.Serving, desired)
	if readyToCounter != servingToReading {
		class := fmt.Sprintf("readyReplicas/status=%t/reading=%t", readyToCounter, servingToReading)
		switch {
		case !readyToCounter:
		case readyPods > r.Counts.RuntimeReady:
			class += "[dead-pod]"
		default:
			class += "[not-serving]"
		}
		out = append(out, Disagreement{
			Reader: ReaderReadyReplicas,
			Class:  class,
			Detail: fmt.Sprintf("readyReplicas counts the Instance as Ready=%t on %d ready pods, the reading as serving=%t on %d serving pods",
				readyToCounter, readyPods, servingToReading, r.Counts.Serving),
		})
	}
	// The restart trigger reads the Instance as short when its pods not
	// in a terminal phase number fewer than desired, and the refill reads
	// a pod name as held by the same rule; a pod being deleted counts for
	// both. The reading's Live count leaves it out.
	if live := int32(len(query.ExcludeTerminalPods(pods))); live != r.Counts.Live {
		out = append(out, Disagreement{
			Reader: ReaderLivePods,
			Class:  "liveCount/query" + relation(live, r.Counts.Live) + "reading",
			Detail: fmt.Sprintf("query counts %d pods not in a terminal phase, the reading %d live", live, r.Counts.Live),
		})
	}
	// The writers admit the serving gate onto a set once every pod of it
	// is live with ContainersReady True; the reading's RuntimeReady level
	// is the same reading on live pods.
	runtimeReadySet := r.Counts.Pods > 0 && r.Counts.RuntimeReady == r.Counts.Pods
	out = appendBoolReader(out, ReaderAllPodsRuntimeReady, "gateAdmission", "query",
		query.AllPodsRuntimeReady(pods), runtimeReadySet)
	// Every Ready stamp but adoption waits for the set to be promotable:
	// every pod live, ContainersReady, and PodReady held for the window,
	// with the gate not read. The reading's Available level needs Serving
	// as well, so the gate. The class says what the set lacks on the
	// side that reads it short.
	promotable, _ := query.PodSetPromotable(pods, minReadySeconds, ctx.Now)
	availableSet := r.Counts.Pods > 0 && r.Counts.Available == r.Counts.Pods
	if promotable != availableSet {
		class := fmt.Sprintf("promotable/query=%t/reading=%t", promotable, availableSet)
		if promotable {
			class += "[not-serving]"
		} else {
			class += "[containers-not-ready]"
		}
		out = append(out, Disagreement{
			Reader: ReaderPodSetPromotable,
			Class:  class,
			Detail: fmt.Sprintf("query reads the set promotable=%t, the reading counts %d of %d pods available", promotable, r.Counts.Available, r.Counts.Pods),
		})
	}
	return out
}

// appendRowReaders compares the readers that take the row beside the pods.
func appendRowReaders(out []Disagreement, pods []*corev1.Pod, desired int32, row *types.InstanceStatus, minReadySeconds int32, r reading.InstanceReading, ctx reading.Context) []Disagreement {
	// A row is unserving when none of its pods serves, whatever its
	// phase says: a Failed row whose source pod still serves is not
	// unserving, and publishes Ready. A pod serves when its PodReady
	// condition and the serving gate are both True, so its Service can
	// publish it; a gang worker is never published and does not count.
	// RecreateOfUnservingRow reads this from the persisted serving
	// count, one pass old, on a Failed row that is not an update
	// continuation, so the comparison is made there alone, under a
	// non-surge strategy so the counter decides; the reading reads it
	// from this pass's pods (OutOfRotation), with no grace and no
	// phase.
	if types.StateOf(row) == types.StateFailed && !types.UpdateContinuation(row) {
		unserving := types.RecreateOfUnservingRow(row, types.UpdateStrategyRecreatePod)
		if unserving != r.OutOfRotation() {
			out = append(out, Disagreement{
				Reader: ReaderRecreateOfUnservingRow,
				Class:  fmt.Sprintf("recreateOfDarkRow/row=%t/reading=%t", unserving, r.OutOfRotation()),
				Detail: fmt.Sprintf("the Failed row persisted %d serving pods one pass ago; this pass the pods read outOfRotation=%t",
					row.ServingPodCount, r.OutOfRotation()),
			})
		}
	}
	if row.ServingPodCount != r.Counts.Serving {
		out = append(out, Disagreement{
			Reader: ReaderPersistedServingPods,
			Class:  "persistedServingCount/row" + relation(row.ServingPodCount, r.Counts.Serving) + "reading",
			Detail: fmt.Sprintf("the row persisted %d serving pods one pass ago, the reading counts %d", row.ServingPodCount, r.Counts.Serving),
		})
	}
	// The persisted available count is membership in the headless
	// Service's rotation plus the availability window, one pass old;
	// the reading's Available level is Serving plus the window, now.
	// The class says when the two read different pods rather than
	// the same pods a pass apart: an Available pod no Service
	// publishes, or a published pod that is not Serving.
	if row.AvailablePodCount != r.Counts.Available {
		class := "persistedAvailableCount/row" + relation(row.AvailablePodCount, r.Counts.Available) + "reading"
		switch {
		case row.AvailablePodCount < r.Counts.Available && anyPod(r, func(p reading.PodReading) bool { return p.Available && !p.Routed }):
			class += "[unrouted]"
		case row.AvailablePodCount > r.Counts.Available && anyPod(r, func(p reading.PodReading) bool { return p.Routed && !p.Serving }):
			class += "[routed-not-serving]"
		}
		out = append(out, Disagreement{
			Reader: ReaderPersistedAvailablePods,
			Class:  class,
			Detail: fmt.Sprintf("the row persisted %d available pods one pass ago, the reading counts %d", row.AvailablePodCount, r.Counts.Available),
		})
	}
	// The escalation exempts an Instance that is capacity from every
	// escalation of a pod held past its grace: its pod set fully serving
	// at the rotation bar (every live pod PodReady with the gate, which is
	// the reading's FullyServing by the same membership rule), unless the
	// operation reports that the source it replaces has left the rotation.
	// For a row whose operation names a surge index, the escalation reads
	// the replacement's pods, which this comparison does not hold, so such
	// a row is left out.
	if row.Operation == nil || row.Operation.SurgeIndex == nil {
		capacity := query.PodSetReadyAndServing(pods, desired) && !types.OperationSourceUnrouted(row.Operation)
		if capacity != r.FullyServing() {
			class := fmt.Sprintf("podSetIsCapacity/escalation=%t/reading=%t", capacity, r.FullyServing())
			if types.OperationSourceUnrouted(row.Operation) {
				class += "[source-unrouted]"
			}
			out = append(out, Disagreement{
				Reader: ReaderPodSetIsCapacity,
				Class:  class,
				Detail: fmt.Sprintf("the escalation reads the Instance as capacity=%t, the reading as fully serving=%t", capacity, r.FullyServing()),
			})
		}
	}
	// The roll reads an Instance it moved onto the target as unserving,
	// so it holds no budget slot, when its pods on that revision number
	// at least the desired count and each one serves or has served and
	// then failed its readiness for the grace with its container still
	// running: none is held in a waiting state, exited, terminal or still
	// coming up, no runner restarted since the row entered Ready, and the
	// row is no update continuation and no migration's. The comparison
	// asks it of every row on its own running revision, as the roll asks
	// it of a row on the target, with no revision birth time, as the
	// replay hands the engine none. The reading's answer is the same rule
	// composed from its per-pod levels (rolledInstanceUnserving). The
	// class names the exclusion the roll applied that the composition
	// does not read.
	if row.RunningRevision != "" {
		window := ops.ProvenWindowSeconds(minReadySeconds, ctx.UnreadyGrace)
		unserving, _ := ops.RolledInstanceDark(row, row.RunningRevision, time.Time{}, pods, desired, window, ctx.UnreadyGrace, ctx.Now)
		if composed := rolledInstanceUnserving(r, row.RunningRevision, desired); unserving != composed {
			class := fmt.Sprintf("rolledInstanceUnserving/ops=%t/reading=%t", unserving, composed)
			switch {
			case unserving:
			case types.UpdateContinuation(row) || row.Phase == types.InstancePhaseMigrating || types.ClaimOf(row) == types.OwnerMigrate:
				class += "[continuation]"
			case ops.RolledInstanceCrashed(row, row.RunningRevision, time.Time{}, pods, window):
				class += "[crashed]"
			case anyUnreadyContainerNotRunning(pods, r):
				class += "[exited]"
			}
			out = append(out, Disagreement{
				Reader: ReaderRolledInstanceDark,
				Class:  class,
				Detail: fmt.Sprintf("the roll reads the Instance on %s as unserving=%t, the reading composes unserving=%t from %d serving pods", row.RunningRevision, unserving, composed, r.Counts.Serving),
			})
		}
	}
	// Adoption stamps Ready a row with no running revision whose pods all
	// carry the target revision and are live with ContainersReady True,
	// with no gate and no window read; every other Ready stamp waits for
	// the set to hold Ready for the window. The reading's bar for a Ready
	// stamp is Available on every pod. The comparison is made on the rows
	// adoption is asked of (adoptionAsked), with no running revision and
	// at least one pod. The class says what the set still lacks when
	// adoption would stamp it.
	if row.RunningRevision == "" && len(pods) > 0 && adoptionAsked(row) {
		adopt := query.AllPodsRuntimeReady(pods)
		availableSet := r.Counts.Pods > 0 && r.Counts.Available == r.Counts.Pods
		if adopt != availableSet {
			class := fmt.Sprintf("adopt/query=%t/reading=%t", adopt, availableSet)
			switch {
			case !adopt:
			case r.Counts.Serving < r.Counts.Pods:
				class += "[not-serving]"
			default:
				class += "[window]"
			}
			out = append(out, Disagreement{
				Reader: ReaderAdoption,
				Class:  class,
				Detail: fmt.Sprintf("adoption reads the set as Ready=%t on containers alone, the reading counts %d of %d pods available", adopt, r.Counts.Available, r.Counts.Pods),
			})
		}
	}
	return out
}

// appendPodReaders compares the readers that take one pod at a time.
func appendPodReaders(out []Disagreement, pods []*corev1.Pod, row *types.InstanceStatus, minReadySeconds int32, r reading.InstanceReading, ctx reading.Context) []Disagreement {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		p := reading.ReadPod(pod, ctx)
		// Coordination's serving predicate is set beside
		// podreadiness.ReadyAndServing, the one the Serving level is
		// built on.
		byCoordination := coordination.PodReadyAndServing(pod)
		byPodreadiness := podreadiness.ReadyAndServing(pod)
		if byCoordination != byPodreadiness {
			out = append(out, Disagreement{
				Reader: ReaderPodReadyAndServing,
				Class:  fmt.Sprintf("podReadyAndServing/coordination=%t/podreadiness=%t", byCoordination, byPodreadiness),
				Detail: fmt.Sprintf("pod %s: coordination reads serving=%t, podreadiness reads %t, the reading's Serving level is %t",
					pod.Name, byCoordination, byPodreadiness, p.Serving),
			})
		}
		// The publication's availability predicate reads PodReady held
		// for the window, with no gate and no live filter; the reading's
		// Available level needs Serving on a live pod as well. The class
		// says which of the two the pod lacks.
		available, _ := podreadiness.IsPodAvailable(pod, minReadySeconds, ctx.Now)
		if available != p.Available {
			why := "not-serving"
			if !p.Live {
				why = "not-live"
			}
			out = append(out, Disagreement{
				Reader: ReaderIsPodAvailable,
				Class:  fmt.Sprintf("podAvailable/podreadiness=%t/reading=%t[%s]", available, p.Available, why),
				Detail: fmt.Sprintf("pod %s: podreadiness reads available=%t, the reading's Available level is %t",
					pod.Name, available, p.Available),
			})
		}
		// The writers read the gate alone to decide whether a gate write
		// is still owed; the reading's Serving level needs PodReady as
		// well, on a live pod. The class says what the pod lacks: its
		// life, the kubelet's fold of the gate into PodReady, or its
		// containers' readiness.
		if gate := podreadiness.IsServing(pod); gate != p.Serving {
			class := fmt.Sprintf("gateWritten/podreadiness=%t/reading=%t", gate, p.Serving)
			switch {
			case !gate:
			case !p.Live:
				class += "[not-live]"
			case p.RuntimeReady:
				class += "[not-folded]"
			default:
				class += "[containers-not-ready]"
			}
			out = append(out, Disagreement{
				Reader: ReaderIsServing,
				Class:  class,
				Detail: fmt.Sprintf("pod %s: the gate reads %t, the reading's Serving level is %t", pod.Name, gate, p.Serving),
			})
		}
		// The restart detector reads a Ready row's pod as restarted when
		// its runner container carries a run or a termination dated after
		// the row entered Ready, on a pod created before that; it keeps
		// reading so until the row enters Ready again. The reading has no
		// memory: it reads the pod out of service when it is held in a
		// waiting state or served and then failed its readiness past the
		// grace. The class
		// says what the reading sees of a restarted pod, or why a pod the
		// reading sees out of service is not restarted to the detector.
		if row != nil && row.Phase == types.InstancePhaseReady {
			_, restarted := ops.RunnerRestartedSinceReady(pod, row.ReadySince)
			class := ""
			switch {
			case restarted && p.Serving:
				class = "runnerRestarted/ops=true/reading=serving"
			case restarted && !p.Live:
				class = "runnerRestarted/ops=true/reading=not-live"
			case restarted && !p.OutOfService():
				class = "runnerRestarted/ops=true/reading=coming-up"
			case !restarted && p.OutOfService():
				class = "runnerRestarted/ops=false/reading=out-of-service"
				switch {
				case row.ReadySince == nil || row.ReadySince.IsZero():
					class += "[no-anchor]"
				case !pod.CreationTimestamp.IsZero() && pod.CreationTimestamp.After(row.ReadySince.Time):
					class += "[rebuilt]"
				default:
					class += "[no-restart]"
				}
			}
			if class != "" {
				out = append(out, Disagreement{
					Reader: ReaderRunnerRestarted,
					Class:  class,
					Detail: fmt.Sprintf("pod %s: the detector reads restarted since Ready=%t, the reading serving=%t, out of service=%t",
						pod.Name, restarted, p.Serving, p.OutOfService()),
				})
			}
		}
	}
	return out
}

// rolledInstanceUnserving is the roll's rule for a rolled Instance that
// serves nothing, composed from the reading: the pods on revision, or on
// no revision, that are not being deleted number at least desired, every
// one of them is Serving or served and then failed its readiness past
// the grace, none is terminal or held in a waiting state, and at least
// one is past the grace.
func rolledInstanceUnserving(r reading.InstanceReading, revision string, desired int32) bool {
	rev := query.RevisionFromName(revision)
	var own int32
	unserving := false
	for _, p := range r.Pods {
		if p.Terminating {
			continue
		}
		if podRev := query.RevisionFromHash(p.RevisionHash); !podRev.IsZero() && !podRev.Same(rev) {
			continue
		}
		own++
		switch {
		case p.Serving:
		case p.Terminal, p.HeldIn != "", !p.UnreadyPastGrace:
			return false
		default:
			unserving = true
		}
	}
	return own >= desired && unserving
}

// anyUnreadyContainerNotRunning reports whether a pod the reading reads as
// served-then-unready past the grace has a container that is not running:
// exited and not started again, or with no status reported.
func anyUnreadyContainerNotRunning(pods []*corev1.Pod, r reading.InstanceReading) bool {
	i := 0
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		p := r.Pods[i]
		i++
		if p.UnreadyPastGrace && !evidence.PodRunningNotReady(pod) {
			return true
		}
	}
	return false
}

// adoptionAsked reports whether the update trigger asks adoption of the
// row, by what the row alone says: a Ready row, a Failed row or a stopped
// attempt (step Parked) that is not a migration's, not the target row of
// a gang surge replacement and not an attempt the PodGroup ownership
// conflict ended. The target revision's retry schedule can still hold
// such a row, which this comparison does not read.
func adoptionAsked(row *types.InstanceStatus) bool {
	if row.Phase == types.InstancePhaseMigrating || types.ClaimOf(row) == types.OwnerMigrate || types.EndedByOwnershipVerdict(row) {
		return false
	}
	if op := row.Operation; op != nil && op.Type == types.InstanceOperationUpdate &&
		(op.Step == types.UpdateStepGangSurgeTarget || op.Step == types.UpdateStepGangSurgeTargetCleanup) {
		return false
	}
	return row.Phase == types.InstancePhaseReady || row.Phase == types.InstancePhaseFailed ||
		types.StateOf(row) == types.StateUpdateParked
}

// anyPod reports whether some pod of the reading satisfies the predicate.
func anyPod(r reading.InstanceReading, predicate func(reading.PodReading) bool) bool {
	for _, p := range r.Pods {
		if predicate(p) {
			return true
		}
	}
	return false
}

// appendBoolReader appends a disagreement when a boolean reader and the
// reading's matching answer differ; class is the key's first segment.
func appendBoolReader(out []Disagreement, reader, class, side string, theirs, ours bool) []Disagreement {
	if theirs == ours {
		return out
	}
	return append(out, Disagreement{
		Reader: reader,
		Class:  fmt.Sprintf("%s/%s=%t/reading=%t", class, side, theirs, ours),
		Detail: fmt.Sprintf("%s reads %t, the reading %t", reader, theirs, ours),
	})
}

// relation renders which side of a count comparison is larger.
func relation(theirs, ours int32) string {
	if theirs > ours {
		return ">"
	}
	return "<"
}
