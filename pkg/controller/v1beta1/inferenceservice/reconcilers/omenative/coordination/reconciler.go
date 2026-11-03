package coordination

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
)

// ReconcileInputs is the input bag the top-level OMENative dispatcher
// passes into the coordination layer once per ISVC reconcile (after
// every Component's per-Component reconciler has run).
type ReconcileInputs struct {
	// ISVC is the parent InferenceService. The coordination layer
	// reads ISVC.Spec.Rollout.Coordination and writes
	// ISVC.Status.RolloutCoordination + per-Component
	// Status.Components.<c>.Traffic.
	ISVC *v1beta1.InferenceService

	// Policies is the set of RolloutPolicy objects the run layer observed
	// for this pass; the effective view resolves the spec's references
	// through it outside a pinned run.
	Policies rollout.Policies

	// Client is the cached controller-runtime client. Used for
	// per-revision Service ensure/GC and the coordination layer's own
	// per-revision pod reads. The manager cache scopes the Pod informer
	// to the ome.io/inferenceservice label key, so it carries every pod
	// the coordination layer lists — a cached read hides nothing here
	// while shedding the apiserver LIST cost on the hot reconcile path.
	Client client.Client

	// Reader is the live (uncached) API reader, reserved for
	// correctness-critical reads. The coordination layer reads pods via
	// Client and the EndpointSlices behind the traffic split through this
	// reader, one list per routed Service per pass, so the split never
	// moves a share onto a revision the data plane cannot reach yet; the
	// canary capacity gate (canary.Dispatch -> ObservePerRevisionPods)
	// reads its pods through it for the same reason.
	Reader client.Reader

	// Recorder emits K8s Events for coordination transitions.
	Recorder record.EventRecorder

	// ComponentDeploymentModes holds each Component's mode resolved from its
	// merged spec. Missing entries are not coordination-owned.
	ComponentDeploymentModes map[v1beta1.ComponentType]constants.DeploymentModeType

	// ComponentRunnerPorts holds each Component's effective serving ports.
	// The per-revision routing Service publishes the port resolved from this set
	// so it targets the port the pods actually listen on. A Component missing
	// from the map gets a headless Service only — see
	// EnsurePerRevisionServices.
	ComponentRunnerPorts map[v1beta1.ComponentType][]corev1.ContainerPort

	// Now is the wall-clock used for LastTransitionTime; callers may
	// override for tests. Zero defaults to time.Now().
	// Already an injected-time seam; kept as time.Time (single snapshot per pass).
	Now time.Time

	// TrafficWeightDeadbandPercent is the per-revision hysteresis band
	// (in percent) applied to the pod-proportional traffic writer: a
	// recomputed weight set whose every per-revision percent moves by
	// strictly less than this from what is already in
	// Status.Components.<c>.Traffic is treated as pod-count jitter and the
	// status write is suppressed. Dampens the write+HTTPRoute-re-enqueue
	// churn from pods momentarily Pending between reconciles at scale.
	//
	// There is intentionally NO in-code default: the value is supplied via
	// the inferenceservice-config ConfigMap (set by the Helm chart /
	// GitOps). Zero (absent config) disables the band and preserves the
	// exact prior write-on-any-diff behavior.
	TrafficWeightDeadbandPercent int32

	// DefaultRatioTolerancePercent fills maintainRatio.tolerance for groups
	// that omit it, per the operator's coordination config. There is
	// intentionally NO in-code default: nil (absent config) means a group
	// that omits tolerance rolls with no drift bound.
	DefaultRatioTolerancePercent *int32
}

// Result reports what the coordination layer did this reconcile. The
// caller uses it to:
//   - decide whether to write status (Status* fields)
//   - emit metrics for surge skew / failures
//   - log structured progress lines
//
// RatioBalanced skew is published via the ratio_skew_total Prometheus
// counter, the group status field SurgeHeldByRatio and the RatioSkewRejected
// event (recordObservations), so Result intentionally does not carry a
// per-reconcile skew count.
type Result struct {
	// GroupStatuses are the per-group statuses freshly computed this
	// reconcile.
	GroupStatuses []v1beta1.RolloutCoordinationGroupStatus

	// PerRevisionServicesEnsured counts how many per-revision Services
	// the reconcile created or updated. Surfaces in logs.
	PerRevisionServicesEnsured int
}

// Reconcile is the entry point called by the top-level OMENative
// reconciler once per ISVC reconcile, after every Component's
// per-Component reconciler has run. It:
//
//  1. resolves the spec.rollout.coordination.groups[] declaration
//  2. observes each Component's current per-revision pod state
//  3. drives the per-policy state machine
//  4. ensures per-revision Services
//  5. writes Status.Components.<c>.Traffic + RolloutCoordination
//  6. records metrics + emits events
//
// Returns a non-nil Result on success and a non-nil error on failure.
// On error, the caller surfaces the error to the parent reconcile
// chain — coordination is a status-only layer so a partial reconcile
// is safe to retry next pass.
func Reconcile(ctx context.Context, in ReconcileInputs) (*Result, error) {
	if in.ISVC == nil {
		return nil, fmt.Errorf("coordination.Reconcile: nil ISVC")
	}
	if in.Client == nil {
		return nil, fmt.Errorf("coordination.Reconcile: nil client")
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	groups := ResolveGroups(rollout.Effective(in.ISVC, in.Policies), GroupDefaults{RatioTolerancePercent: in.DefaultRatioTolerancePercent})
	result := &Result{}

	// Bind a logger keyed to the ISVC so every downstream V(1)
	// breadcrumb (per-group transition, per-revision Service GC,
	// skew rejection) inherits the same anchor and operators can
	// `grep isvc=foo` across the coordination layer.
	logger := log.FromContext(ctx).WithValues(
		"isvc", client.ObjectKeyFromObject(in.ISVC),
		"groupCount", len(groups))
	ctx = log.IntoContext(ctx, logger)

	// Per-revision Service / Traffic[] producer: emitted for EVERY
	// OMENative-mode Component, with or without a spec.rollout.coordination
	// group declared. These artifacts are the data-plane for the
	// HTTPRoute weighted-backendRef consumer; an opt-in gate would
	// break the traffic layer for ISVCs that haven't (yet) declared a
	// coord group but still want per-revision routing.
	//
	// The per-group state machine (Status.RolloutCoordination,
	// RolloutCoordinationReady condition, group events) remains
	// opt-in — those only run when groups are declared.
	componentsTouched := collectOMENativeComponents(in.ComponentDeploymentModes)
	// Drop Components owned by a canary group: canary.Dispatch is their
	// authoritative per-revision Service + traffic producer (it sets the explicit
	// step weight, decoupled from pod count). coordination runs unconditionally in
	// v2, so without this its pod-proportional traffic writer would clobber the
	// canary's weight every reconcile.
	componentsTouched = dropCanaryOwned(componentsTouched, in.ISVC)
	if len(componentsTouched) == 0 {
		// No coordination-owned Components (e.g. a canary-only ISVC): reconcile
		// away any stale coordination status a prior rollout left behind, then
		// stop — there is nothing to coordinate.
		mergeAndPersistGroupStatuses(in.ISVC, nil)
		ClearRolloutCoordinationReady(in.ISVC)
		logger.V(1).Info("Reconcile entry; no coordination-owned OMENative Components on ISVC, nothing to coordinate")
		return result, nil
	}
	logger.V(1).Info("Reconcile entry",
		"componentCount", len(componentsTouched),
		"components", componentsTouched)

	// Informational hint: ISVC has multiple OMENative Components but
	// no spec.rollout.coordination block declared. Default behavior rolls
	// each Component independently, which can break PD pairs or
	// shared-state workloads. Operator may want a coord group.
	//
	// Emit the event ONCE, on the transition into this state, tracked by
	// the CoordinationAdvisory status condition. Firing it every reconcile
	// would lean on event aggregation collapsing to one object, but the
	// aggregated count still climbs unboundedly and the Event is re-PATCHed
	// every loop — alarming noise plus apiserver write load on the hot
	// path. Clearing the condition when a group is later
	// declared (or the Component count drops) re-arms the one-shot event.
	if len(componentsTouched) >= 2 && len(groups) == 0 {
		if SetCoordinationAdvisory(in.ISVC, componentsTouched) && in.Recorder != nil {
			in.Recorder.Eventf(in.ISVC, corev1.EventTypeNormal, EventReasonConsiderCoordinationGroup,
				"InferenceService has %d OMENative Components (%v) but no spec.rollout.coordination block declared; consider adding a coord group to define rollout behavior across them",
				len(componentsTouched), componentsTouched)
		}
	} else {
		ClearCoordinationAdvisory(in.ISVC)
	}

	// Cached client carries the OMENative Pod field index — useIndex=true takes
	// the index fast path instead of scanning the whole informer store per Component.
	perRevisionPods, _, perRevisionRouting, observedPods, err := observePerRevisionPods(ctx, in.Client, in.ISVC, componentsTouched, true)
	if err != nil {
		return nil, fmt.Errorf("observe per-revision pods: %w", err)
	}

	// Ensure per-revision Services for every (component, revisionHash)
	// pair that has at least one live pod. Services are additive K8s
	// resources that cost almost nothing; emit them whenever a group
	// is declared so the HTTPRoute weighted-backendRef consumer has
	// the per-revision backends to point at.
	for _, c := range componentsTouched {
		SetPerRevisionServiceCount(in.ISVC.Namespace, in.ISVC.Name, string(c), float64(len(perRevisionPods[c])))

		// Ensure runs EVERY reconcile — it is the self-heal path. A
		// per-revision routing/headless Service deleted out-of-band must
		// be recreated even when the live revision-hash set is unchanged;
		// EnsurePerRevisionServices uses CreateOrUpdate, whose Gets are
		// cache-served and cheap, and recreates a missing Service. Gating
		// ensure on revision-set convergence would let a deleted Service
		// stay gone and strand the HTTPRoute on a dead backend. The revisions
		// are walked in sorted order so a pass is repeatable.
		for _, hash := range SortedRevisionHashes(perRevisionPods[c]) {
			if hash == "" {
				continue
			}
			routing := perRevisionRouting[c][hash]
			if _, err := EnsurePerRevisionServices(ctx, in.Client, in.ISVC, c, hash, routing, in.ComponentRunnerPorts[c]); err != nil {
				// The namespace is being torn down (common during test
				// teardown and any namespace-scoped delete): the create is
				// forbidden and will never succeed, and the ISVC is going
				// away with the namespace. Stop coordinating cleanly rather
				// than logging an error and entering retry backoff.
				if isNamespaceTerminating(err) {
					logger.V(1).Info("namespace terminating; skipping per-revision service ensure",
						"component", c, "revisionHash", hash)
					return result, nil
				}
				return nil, fmt.Errorf("ensure per-revision service for %s/%s: %w", c, hash, err)
			}
			result.PerRevisionServicesEnsured++
		}

		// Delete per-revision Service pairs whose revision-hash has no
		// live pods and is not the Component's roll target. A hash with zero
		// pods that is not the target is genuinely past retention. The
		// target revision is protected even before its first pod exists:
		// peer Components create its Services ahead of the pods whose
		// OME_<PEER>_REVISION_ENDPOINT names them, and sweeping those would
		// leave the pods with a dead peer endpoint until re-created. Pods
		// added between the list and the delete re-Ensure on the next
		// reconcile.
		//
		// The sweep runs unconditionally. It cannot be gated on the live
		// revision-hash set matching Status.Components.<c>.Traffic: the
		// hash set here counts TOTAL pods while Traffic is written from
		// READY pods, so a draining pod leaves Traffic one reconcile ahead.
		// The pass on which the drained pod finally disappears is exactly
		// the pass on which the two sets agree, so a gate would skip the
		// only sweep that could collect that revision's Services. Cost is
		// a cached-client List, not an apiserver round trip.
		retained, err := retainedRevisionHashes(ctx, in.Reader, in.ISVC, c, perRevisionPods[c])
		if err != nil {
			return nil, fmt.Errorf("resolve retained revisions for %s: %w", c, err)
		}
		if err := gcOrphanedPerRevisionServices(ctx, in.Client, in.ISVC, c, retained); err != nil {
			return nil, fmt.Errorf("gc per-revision services for %s: %w", c, err)
		}
	}

	// Write per-Component Traffic[] from the pods that serve and are
	// listed as Ready endpoints of their revision's routed Service
	// (producer-side contract feeding the HTTPRoute weighted-backendRef
	// consumer). Listed, not Ready and not total: a revision whose pods
	// are Pending, or Ready but not yet published by the endpoints
	// controller, cannot take traffic, and weighting it points the
	// HTTPRoute at a Service with no endpoint behind it. The condition
	// names the revisions whose share waits. Service ensure/GC above stays
	// on the total map so a revision's Service exists before its first pod
	// flips Ready.
	endpointReader := in.Reader
	if endpointReader == nil {
		endpointReader = in.Client
	}
	rotation := drain.NewBatcher(endpointReader, in.ISVC.Namespace)
	perRevisionListedPods, endpointWaits, err := servingPodsListedByRevision(ctx, rotation, in.ISVC, componentsTouched, observedPods)
	if err != nil {
		return nil, fmt.Errorf("observe per-revision endpoints: %w", err)
	}
	if err := updateTrafficStatus(ctx, in.Client, in.ISVC, componentsTouched, perRevisionListedPods, in.TrafficWeightDeadbandPercent); err != nil {
		return nil, fmt.Errorf("update traffic status: %w", err)
	}
	SetTrafficEndpointsReady(in.ISVC, endpointWaits)

	// The cached client carries no row decoder; the group observation reads
	// InferenceReplicas through it paired with the decoder the live reader
	// carries, so a ColumnarV2-stored object decodes on this path as well.
	cachedDecoded := irstatus.NewReader(in.Client, irstatus.DecoderOf(in.Reader))

	// Drive the per-group state machine.
	for _, g := range groups {
		if err := ValidateGroupShape(g); err != nil {
			return nil, fmt.Errorf("invalid coordination group shape: %w", err)
		}
		obs, err := buildGroupObservation(ctx, cachedDecoded, in.ISVC, g, perRevisionPods)
		if err != nil {
			return nil, fmt.Errorf("build observation for group %s: %w", g.Name, err)
		}
		// Original follows the members' desired counts (rewritten whenever
		// one changes); Current + NewPods are refreshed from the live
		// observation.
		ratio := buildRatioState(in.ISVC, g, obs, perRevisionPods)
		obs.OriginalReplicas = ratio.Original
		obs.Now = now
		obs.PreviousPhaseEnteredAt = previousPhaseEnteredAt(in.ISVC, g.Name)
		obs.MemberFinished = previousMemberFinish(in.ISVC, g.Name)
		tr := ComputeTransition(obs)
		groupStatus := BuildGroupStatus(g, tr, ratio, now)

		// Record observability for this group.
		prevPhase, prevComposite := previousGroupPhases(in.ISVC, g.Name)
		RecordGroupPhase(in.ISVC.Namespace, in.ISVC.Name, g.Name, string(tr.Phase))
		// Steady-state breadcrumb (no transition): V(1) so operators
		// can `grep group=foo` without drowning the log under normal
		// load. Includes phase + policy + componentCount + rollout
		// count so post-mortem on a misfire can correlate the inputs
		// the per-policy transition function saw.
		rolloutInFlight := 0
		for _, comp := range obs.Components {
			if comp.RolloutInFlight {
				rolloutInFlight++
			}
		}
		logger.V(1).Info("Group transition computed",
			"group", g.Name,
			"policy", g.Policy,
			"phase", tr.Phase,
			"componentCount", len(g.Components),
			"rolloutInFlight", rolloutInFlight,
			"ratioSkewRejected", tr.RatioSkewRejected)
		if prevPhase != "" && string(prevPhase) != string(tr.Phase) {
			RecordGroupTransition(in.ISVC.Namespace, in.ISVC.Name, g.Name, string(prevPhase), string(tr.Phase))
			// Phase-dwell observability: how long the group sat in the
			// phase it is now leaving. obs.PreviousPhaseEnteredAt is the
			// timestamp that phase was entered; now is this transition.
			// Labeled by the phase being left (bounded enum, not per-isvc)
			// to keep series count flat at fleet scale.
			obsmetrics.RecordRolloutPhaseDuration(string(prevPhase), now.Sub(obs.PreviousPhaseEnteredAt).Seconds())
			// Real Phase transition: Info-level so it shows up in
			// operator-facing dashboards alongside the K8s event the
			// emitPhaseEvents call below records.
			logger.Info("Group phase transition",
				"group", g.Name,
				"policy", g.Policy,
				"fromPhase", prevPhase,
				"toPhase", tr.Phase,
				"message", tr.Message)
		}
		if tr.Phase == v1beta1.CoordinationPhaseFailed {
			RecordGroupFailure(in.ISVC.Namespace, in.ISVC.Name, g.Name, tr.Message)
		}

		// The two observations the status carries as state: distinct
		// revisions across the Components of a RollingUpdate group, and a
		// surge RatioBalanced pacing refused to keep the ratio.
		groupStatus.MixedPairing = g.Policy == v1beta1.CoordinationPolicyRollingUpdate && hasMixedPairing(g, perRevisionPods)
		groupStatus.SurgeHeldByRatio = tr.RatioSkewRejected
		previous, _ := previousGroupStatus(in.ISVC, g.Name)
		recordObservations(in.Recorder, in.ISVC, g, previous, groupStatus, tr.RatioHeldStarts)

		emitPhaseEvents(in.Recorder, in.ISVC, g, tr, prevPhase, prevComposite)
		emitSequentialDeferredEvent(in.Recorder, in.ISVC, g, obs, previousActiveComponent(in.ISVC, g.Name))

		result.GroupStatuses = append(result.GroupStatuses, groupStatus)
	}

	mergeAndPersistGroupStatuses(in.ISVC, result.GroupStatuses)

	// Stamp the RolloutCoordinationReady condition for groups that
	// have been observed at least once.
	if len(result.GroupStatuses) > 0 {
		SetRolloutCoordinationReady(in.ISVC, result.GroupStatuses, now)
	} else {
		// No coordination-style groups this pass — resolve any stale
		// RolloutCoordinationReady the ISVC carried from a prior rollout.
		ClearRolloutCoordinationReady(in.ISVC)
	}

	return result, nil
}

// collectOMENativeComponents returns the Components whose merged specs resolve
// to OMENative dispatch. Only those Components participate in coordination's
// per-revision Service emission and traffic write.
func collectOMENativeComponents(modes map[v1beta1.ComponentType]constants.DeploymentModeType) []v1beta1.ComponentType {
	out := make([]v1beta1.ComponentType, 0, len(v1beta1.Roles))
	for _, component := range v1beta1.RoleTypes() {
		if modes[component] == constants.OMENative {
			out = append(out, component)
		}
	}
	return out
}

// observePerRevisionPods returns per-revision pod counts and routing selector
// capabilities: every pod carrying a revision hash, the serving subset as
// servingPodsByRevision counts it, and the selectors. LeaderOnly requires an
// observed leader or worker runner; PodOrdinal additionally requires every
// such pod to carry an ordinal label.
//
// The per-Component pod fetch routes through query.ListOMENativePodsByName so it
// rides the OMENative Pod field index instead of scanning the whole informer
// store per Component. useIndex MUST match the reader: pass true for the cached
// client (coordination's own observe — it has the field index registered) and
// false for the live API reader (canary capacity gate — the apiserver has no
// custom Pod field selector). Both modes return the identical pod set; useIndex
// only picks how the set is fetched.
func observePerRevisionPods(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType, useIndex bool) (map[v1beta1.ComponentType]map[string]int32, map[v1beta1.ComponentType]map[string]int32, map[v1beta1.ComponentType]map[string]RevisionRoutingSelector, map[v1beta1.ComponentType][]*corev1.Pod, error) {
	total := make(map[v1beta1.ComponentType]map[string]int32, len(components))
	ready := make(map[v1beta1.ComponentType]map[string]int32, len(components))
	routing := make(map[v1beta1.ComponentType]map[string]RevisionRoutingSelector, len(components))
	observedPods := make(map[v1beta1.ComponentType][]*corev1.Pod, len(components))
	if reads == nil {
		return total, ready, routing, observedPods, nil
	}
	for _, c := range components {
		pods, err := query.ListOMENativePodsByName(ctx, reads, isvc.Namespace, prefixFor(isvc, c), v1beta1convert.ComponentTypeToWorkload(c), useIndex)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("list pods for %s/%s: %w", isvc.Namespace, c, err)
		}
		observedPods[c] = pods
		byHash := make(map[string]int32, 4)
		routingByHash := make(map[string]RevisionRoutingSelector, 4)
		for _, pod := range pods {
			hash := pod.Labels[query.LabelRevisionHash]
			if hash == "" {
				continue
			}
			byHash[hash]++
			selector := routingByHash[hash]
			switch v1beta1.RunnerName(pod.Labels[query.LabelRunner]) {
			case v1beta1.RunnerNameLeader, v1beta1.RunnerNameWorker:
				hasOrdinal := pod.Labels[query.LabelPodOrdinal] != ""
				if selector.LeaderOnly {
					selector.PodOrdinal = selector.PodOrdinal && hasOrdinal
				} else {
					selector.LeaderOnly = true
					selector.PodOrdinal = hasOrdinal
				}
			}
			routingByHash[hash] = selector
		}
		total[c] = byHash
		ready[c] = servingPodsByRevision(pods)
		routing[c] = routingByHash
	}
	return total, ready, routing, observedPods, nil
}

// podReadyAndServing reports whether a pod is currently serving traffic: Running,
// not being deleted, not held out of rotation by the controller's serving gate,
// and reporting the PodReady condition True. Pods without a readiness probe are
// marked Ready by the kubelet once their containers start, so this holds for
// both real runtimes and probe-less test fixtures.
//
// The gate is read in its own right, not only through PodReady: every drain
// holds it False first, kubelet folds the hold into PodReady asynchronously,
// and the pass that observes the hold must already stop publishing the
// drained revision as live traffic.
func podReadyAndServing(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if podreadiness.HeldNotServing(pod) {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// servingPodsByRevision counts, per revision hash, the pods that serve
// traffic: podReadyAndServing holds and their Instance can be reached. A
// routing Service publishes a gang's leader and never a worker
// (query.RoutedRunner), so an Instance whose routed pod is gone or out of
// rotation serves nothing and counts 0 however many of its workers still
// pass readiness; a gang whose leader serves counts every serving member.
// This is the count the Instance rows publish (workloadstatus.CountServingPods).
// A revision with no serving pod has no entry, so both traffic writers
// withhold its share and the revisions that serve carry it.
func servingPodsByRevision(pods []*corev1.Pod) map[string]int32 {
	type instance struct{ hash, index string }
	serving := make(map[instance]int32)
	routed := make(map[instance]bool)
	for _, pod := range pods {
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" || !podReadyAndServing(pod) {
			continue
		}
		key := instance{hash: hash, index: pod.Labels[query.LabelInstanceIdx]}
		serving[key]++
		if query.RoutedRunner(pod) {
			routed[key] = true
		}
	}
	out := make(map[string]int32, 4)
	for key, n := range serving {
		if routed[key] {
			out[key.hash] += n
		}
	}
	return out
}

// dropCanaryOwned removes Components driven by a canary group from comps. Those
// Components, the group's primary and its secondaries alike, belong to the
// canary engine (canary.Dispatch), which owns their per-revision Services,
// traffic status and rolled-out revision fields; coordination must not touch
// them.
func dropCanaryOwned(comps []v1beta1.ComponentType, isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	owned := rollout.CanaryOwnedComponents(isvc)
	if len(owned) == 0 {
		return comps
	}
	out := comps[:0]
	for _, c := range comps {
		if _, isCanary := owned[c]; !isCanary {
			out = append(out, c)
		}
	}
	return out
}

// updateTrafficStatus writes Status.Components.<c>.Traffic[] from the
// per-revision counts of pods that serve and are listed endpoints (a pod
// no request can reach must not pull a traffic share toward its
// revision's Service). Idempotent; no-op writes are skipped to avoid
// bouncing the apiserver.
func updateTrafficStatus(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType, perRevisionReadyPods map[v1beta1.ComponentType]map[string]int32, deadbandPercent int32) error {
	if isvc.Status.Components == nil {
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	}
	for _, c := range components {
		hashes := perRevisionReadyPods[c]
		if len(hashes) == 0 {
			continue
		}
		var total int32
		for _, n := range hashes {
			total += n
		}
		latestHash, err := latestRevisionHashChecked(ctx, reads, isvc, c)
		if err != nil {
			return err
		}
		weights := ComputeWeightsFromPods(hashes, total, latestHash)
		prefix := prefixFor(isvc, c)
		if err := AttachPairingProtocols(ctx, reads, isvc.Namespace, prefix, c, weights); err != nil {
			return err
		}
		traffic := BuildTrafficTargets(prefix, c, weights)
		cs := isvc.Status.Components[c]
		changed := false
		// The deadband suppresses pod-count jitter without suppressing
		// revision-set or latest-revision changes.
		if TrafficDiffersMeaningfully(traffic, cs.Traffic) &&
			!TrafficWithinDeadband(traffic, cs.Traffic, deadbandPercent) {
			cs.Traffic = traffic
			changed = true
		}
		// Revision metadata is reconciled independently from traffic weights so
		// an already-correct traffic split cannot strand stale companion fields.
		if latestHash != "" && hashes[latestHash] > 0 && SetLatestReadyRevision(&cs, prefix, c, latestHash) {
			changed = true
		}
		// LatestRolledoutRevision advances once the rollout is at 100% on a
		// single revision; the pod view knows no superseded revision, so the
		// prior value is the one demoted to PreviousRolledoutRevision.
		if len(hashes) == 1 {
			for hash := range hashes {
				if RecordRolledOutRevision(&cs, prefix, c, hash, "") {
					changed = true
				}
			}
		}
		if changed {
			isvc.Status.Components[c] = cs
		}
	}
	return nil
}

// latestRevisionHashChecked returns the ControllerRevision hash this
// Component is converging toward. Read from authoritative IR
// status UpdateRevision; falls back to CurrentRevision. A read error
// propagates — writing traffic with a fabricated empty latest hash
// would flip LatestRevision flags on the HTTPRoute consumer.
func latestRevisionHashChecked(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (string, error) {
	summary, err := irprojector.ComponentIRStatusFor(ctx, reads, isvc, c)
	if err != nil {
		return "", err
	}
	if summary == nil {
		return "", nil
	}
	if name := summary.UpdateRevision; name != "" {
		return query.RevisionFromName(name).Hash(), nil
	}
	if name := summary.CurrentRevision; name != "" {
		return query.RevisionFromName(name).Hash(), nil
	}
	return "", nil
}

// buildGroupObservation builds the GroupObservation the state machine
// consumes from observed pod counts + authoritative IR status. A read
// error propagates instead of degrading to a zero-valued observation —
// feeding fabricated zeros into the state machine would fake an Idle
// group (and reset its LastTransitionTime soak anchor) during an
// apiserver blip.
func buildGroupObservation(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, g ResolvedGroup, perRevisionPods map[v1beta1.ComponentType]map[string]int32) (GroupObservation, error) {
	obs := GroupObservation{
		Group:      g,
		Components: make(map[v1beta1.ComponentType]ComponentObservation, len(g.Components)),
	}
	for _, c := range g.Components {
		ir, _, err := irprojector.DecodedComponentIRFor(ctx, reads, isvc, c)
		if err != nil {
			return GroupObservation{}, err
		}
		var summary *v1beta1.InferenceReplicaStatus
		if ir != nil {
			summary = &ir.Status
		}
		// ResolveGroups excludes canary groups, so every Component here is
		// coordination-owned and reads the effective partition from the
		// projected IR spec — the merged ISVC↔runtime lifecycle the workload
		// controller stages Instances at. A runtime-inherited partition is
		// invisible on the raw ISVC, so reading the ISVC here would treat a
		// legitimately staged Component as an incomplete rollout forever.
		// Canary-owned Components never reach this loop (their hold is the
		// projected pacing partition, driven by the canary reconciler's own
		// EffectivePartition path).
		partition, err := irprojector.ComponentIRPartitionFor(ctx, reads, isvc, c)
		if err != nil {
			return GroupObservation{}, err
		}
		co := buildComponentObservation(summary, c, perRevisionPods[c], partition)
		co.SpecReplicas = specReplicasOf(ir)
		co.UpdateStrategy = replicaMechanism(ir)
		obs.Components[c] = co
	}
	if paused, _ := constants.RolloutPauseState(isvc.Annotations); paused {
		obs.PausedGlobal = true
	}
	return obs, nil
}

// specReplicasOf reads the Instance count a replica's spec asks for: nil is
// the API default of one, and a count of zero reads as one because a zero
// count cannot anchor a ratio. Zero when the replica does not exist.
func specReplicasOf(ir *v1beta1.InferenceReplica) int32 {
	if ir == nil {
		return 0
	}
	n := int32(1)
	if ir.Spec.Replicas != nil {
		n = *ir.Spec.Replicas
	}
	if n <= 0 {
		return 1
	}
	return n
}

// buildComponentObservation aggregates one Component's observation from
// the authoritative InferenceReplica status and per-revision pod counts.
func buildComponentObservation(summary *v1beta1.InferenceReplicaStatus, c v1beta1.ComponentType, perRevisionPods map[string]int32, partition int32) ComponentObservation {
	out := ComponentObservation{
		Component: c,
		Partition: partition,
	}
	if summary == nil {
		out.NotReady = true
		return out
	}
	out.DesiredReplicas = summary.Replicas
	out.TotalPods = sumPodCounts(perRevisionPods)
	out.ReadyPods = summary.ReadyReplicas
	out.ServingPods = summary.ServingReplicas
	out.NewRevisionPods = summary.UpdatedReplicas
	out.NewRevisionReadyPods = summary.UpdatedReadyReplicas
	cur := query.RevisionFromName(summary.CurrentRevision)
	tgt := query.RevisionFromName(summary.UpdateRevision)
	out.CurrentRevisionHash = cur.Hash()
	out.TargetRevisionHash = tgt.Hash()
	out.RolloutInFlight = !tgt.IsZero() && !tgt.Same(cur)
	out.ReadyAt = readyTransitionAt(summary.Conditions)
	out.NotReady = readyConditionFalseOrAbsent(summary.Conditions)

	// Per-Instance failure rollup: any Instance escalated to
	// Phase=Failed by the workload escalation pass (or any other
	// terminal-failure path) marks the whole Component Failed. The
	// per-policy state machines (BlueGreen, RollingUpdate, Sequential,
	// Independent) all read Component.Failed and translate it into the
	// group's Failed phase — operators see Phase=Failed instead of the
	// rollout sitting at Surging / Waiting indefinitely after a per-
	// Instance deadline blew past.
	failedInstances := collectFailedInstanceIndices(summary.InstanceStatuses)
	if len(failedInstances) > 0 {
		out.Failed = true
		out.FailureMessage = formatFailedInstancesMessage(c, failedInstances)
	}

	// Convergence: does the Component rest at its staged shape — every
	// Instance Ready and the ones still on the prior revision held by the
	// partition, at most Partition of them? Partition=0 means this is the
	// RolloutComplete predicate.
	out.AtDesiredShape = workloadstatus.ReachedDesiredShape(
		v1beta1convert.InstanceStatusSliceToWorkload(summary.InstanceStatuses),
		summary.UpdateRevision, partition, summary.Replicas)
	return out
}

// readyTransitionAt is the instant a replica's Ready condition last turned
// True, zero while it is absent or not True: the moment the replica recorded
// its roll done, which the Sequential wait after it is measured from.
func readyTransitionAt(conds []metav1.Condition) time.Time {
	for _, cond := range conds {
		if cond.Type != conditionReady {
			continue
		}
		if cond.Status != metav1.ConditionTrue {
			return time.Time{}
		}
		return cond.LastTransitionTime.Time
	}
	return time.Time{}
}

// readyConditionFalseOrAbsent reports whether a replica's Ready condition is
// False or missing: the replica vouches for no serving Instance. Unknown,
// the condition while a roll is recorded, is neither.
func readyConditionFalseOrAbsent(conds []metav1.Condition) bool {
	for _, cond := range conds {
		if cond.Type == conditionReady {
			return cond.Status == metav1.ConditionFalse
		}
	}
	return true
}

// collectFailedInstanceIndices returns the indices of every Instance
// currently in Phase=Failed. Order matches the InstanceStatuses slice;
// callers that need a sorted list can sort the result.
func collectFailedInstanceIndices(instances []v1beta1.OMENativeInstanceStatus) []int32 {
	var out []int32
	for _, s := range instances {
		if s.Phase == v1beta1.OMENativeInstanceFailed {
			out = append(out, s.Index)
		}
	}
	return out
}

// formatFailedInstancesMessage builds the operator-facing summary the
// coordination state machine carries on Component.FailureMessage. Stays
// short — the per-Instance breadcrumb (Operation.Reason +
// OperationStuck condition) carries the full detail.
func formatFailedInstancesMessage(c v1beta1.ComponentType, indices []int32) string {
	if len(indices) == 1 {
		return fmt.Sprintf("%s instance %d in Phase=Failed", c, indices[0])
	}
	return fmt.Sprintf("%s instances %v in Phase=Failed", c, indices)
}

// sumPodCounts sums all values in a per-revision pod count map.
func sumPodCounts(in map[string]int32) int32 {
	var sum int32
	for _, n := range in {
		sum += n
	}
	return sum
}

// retainedRevisionHashes is the revision-hash set the orphan sweep must
// keep for one Component: every hash with a live pod plus the Component's
// roll-target hash from its IR status. The target is retained even with
// zero pods because peer Components create its per-revision Services
// ahead of the pods that name them. Returns liveHashes as-is when the IR
// reports no target yet.
func retainedRevisionHashes(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, liveHashes map[string]int32) (map[string]int32, error) {
	target, err := latestRevisionHashChecked(ctx, reads, isvc, component)
	if err != nil {
		return nil, err
	}
	if target == "" {
		return liveHashes, nil
	}
	if _, live := liveHashes[target]; live {
		return liveHashes, nil
	}
	retained := make(map[string]int32, len(liveHashes)+1)
	for h, n := range liveHashes {
		retained[h] = n
	}
	retained[target] = 0
	return retained, nil
}

// gcOrphanedPerRevisionServices deletes per-revision Service pairs
// whose revision-hash is not in `liveHashes` for `component`.
//
// The sweep lists Services in the ISVC's namespace that carry the
// (inferenceservice + component + revision-hash) selector keys under the
// Component's replica prefix, then drops every pair whose revision-hash
// isn't in `liveHashes`.
//
// Idempotent: deleting an already-deleted Service is a NotFound
// success per GCPerRevisionServices.
func gcOrphanedPerRevisionServices(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, liveHashes map[string]int32) error {
	if c == nil {
		return nil
	}
	live := make(map[string]struct{}, len(liveHashes))
	for h := range liveHashes {
		if h == "" {
			continue
		}
		live[h] = struct{}{}
	}
	prefix := prefixFor(isvc, component)
	svcs := &corev1.ServiceList{}
	if err := c.List(ctx, svcs,
		client.InNamespace(isvc.Namespace),
		client.MatchingLabels{
			constants.InferenceServicePodLabelKey: prefix,
			constants.OMEComponentLabel:           string(component),
		},
	); err != nil {
		return fmt.Errorf("list per-revision services: %w", err)
	}
	for i := range svcs.Items {
		hash := svcs.Items[i].Spec.Selector[query.LabelRevisionHash]
		if hash == "" {
			continue // not a per-revision Service
		}
		if _, alive := live[hash]; alive {
			continue
		}
		// Skip headless variant — GCPerRevisionServices already
		// handles both halves of the pair when called with the
		// routing hash. The routing variant's name is the bare
		// `<prefix>-<c>-rev-<hash>` shape; headless adds `-headless`.
		if svcs.Items[i].Name != PerRevisionServiceName(prefix, component, hash) {
			continue
		}
		if err := GCPerRevisionServices(ctx, c, isvc.Namespace, prefix, component, hash); err != nil {
			return fmt.Errorf("gc service for %s/%s: %w", component, hash, err)
		}
	}
	return nil
}

// buildRatioState assembles the per-group RatioState that BuildGroupStatus
// turns into Status.RolloutCoordination.Groups[i].ObservedRatio.
//
// Original is the reference the ratio band measures against, and it follows
// the spec: it is the Instance count each member's spec asks for now. When an
// operator or an autoscaler changes a member's count, the reference is
// rewritten to the new counts on the next pass, mid-roll or idle, so the band
// measures against the counts the spec asks for now rather than the shape the
// roll began with: the controller honours the operator's intent instead of
// keeping its own. The reference written by the previous pass is carried only
// while no member's count differs from it.
//
// Always populated even for non-RatioBalanced pacing (cheap; BuildGroupStatus
// suppresses it on the status side based on policy).
//
// A member whose replica does not exist yet has no count to anchor to. The
// reference is then carried as recorded, with members that left the group
// dropped, and rewritten once every member's replica exists, so a half-created
// group never anchors a shape no member asked for.
func buildRatioState(isvc *v1beta1.InferenceService, g ResolvedGroup, obs GroupObservation, perRevisionPods map[v1beta1.ComponentType]map[string]int32) *RatioState {
	state := &RatioState{
		Current: make(map[v1beta1.ComponentType]int32, len(g.Components)),
		NewPods: make(map[v1beta1.ComponentType]int32, len(g.Components)),
	}
	// Current = total live pods this Component, any revision.
	// NewPods = pods on target revision (matched by Component obs).
	for _, c := range g.Components {
		hashes := perRevisionPods[c]
		var total int32
		for _, n := range hashes {
			total += n
		}
		state.Current[c] = total
		cObs := obs.Components[c]
		if cObs.TargetRevisionHash != "" {
			state.NewPods[c] = hashes[cObs.TargetRevisionHash]
		}
	}
	state.Original = existingOriginalReplicas(isvc, g.Name)
	// Drop anchors for members no longer in the group so a stale
	// entry cannot keep constraining the band.
	members := make(map[v1beta1.ComponentType]struct{}, len(g.Components))
	for _, c := range g.Components {
		members[c] = struct{}{}
	}
	for c := range state.Original {
		if _, ok := members[c]; !ok {
			delete(state.Original, c)
		}
	}
	desired := make(map[v1beta1.ComponentType]int32, len(g.Components))
	for _, c := range g.Components {
		n := obs.Components[c].SpecReplicas
		if n <= 0 {
			// This member's replica is not there yet: carry the recorded
			// reference and anchor on a later pass.
			return state
		}
		desired[c] = n
	}
	if !maps.Equal(state.Original, desired) {
		state.Original = SnapshotOriginal(desired)
	}
	return state
}

// existingOriginalReplicas reads the previously-recorded Original
// from the ISVC's status, if any. Empty when first observation of the
// group.
func existingOriginalReplicas(isvc *v1beta1.InferenceService, groupName string) map[v1beta1.ComponentType]int32 {
	if isvc.Status.RolloutCoordination == nil {
		return nil
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name != groupName || gs.ObservedRatio == nil {
			continue
		}
		if len(gs.ObservedRatio.Original) == 0 {
			return nil
		}
		out := make(map[v1beta1.ComponentType]int32, len(gs.ObservedRatio.Original))
		for k, v := range gs.ObservedRatio.Original {
			out[k] = v
		}
		return out
	}
	return nil
}

// previousGroupPhases returns the Phase and CompositePhase observed for
// `groupName` in the most recent reconcile (read from the ISVC's
// existing status). Empty when no prior observation exists — callers
// treat that as "no transition" so the very first reconcile doesn't
// synthesize a spurious from="" → to=Idle transition increment.
func previousGroupPhases(isvc *v1beta1.InferenceService, groupName string) (v1beta1.CoordinationPhase, string) {
	previous, _ := previousGroupStatus(isvc, groupName)
	return previous.Phase, previous.CompositePhase
}

// previousGroupStatus returns the group's status as the previous pass wrote
// it, and whether the status names the group at all.
func previousGroupStatus(isvc *v1beta1.InferenceService, groupName string) (v1beta1.RolloutCoordinationGroupStatus, bool) {
	if isvc.Status.RolloutCoordination == nil {
		return v1beta1.RolloutCoordinationGroupStatus{}, false
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName {
			return gs, true
		}
	}
	return v1beta1.RolloutCoordinationGroupStatus{}, false
}

// recordObservations counts the two observations the group status carries
// on every pass they hold and records each one's event on the pass it
// begins. A roll keeps an observation for minutes; an event per pass would
// spend the service's event budget on repeats, and the events that follow
// them, the run's close among them, would be dropped. The ratio event names
// the starts the pacing holds, each as the move its strategy makes it.
func recordObservations(recorder record.EventRecorder, isvc *v1beta1.InferenceService, g ResolvedGroup, previous, next v1beta1.RolloutCoordinationGroupStatus, held []HeldStart) {
	if next.MixedPairing {
		RecordMixedPairing(isvc.Namespace, isvc.Name, g.Name)
		if recorder != nil && !previous.MixedPairing {
			recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonMixedPairingObserved,
				"RollingUpdate group %s observed distinct per-Component revision hashes", g.Name)
		}
	}
	if next.SurgeHeldByRatio {
		RecordRatioSkew(isvc.Namespace, isvc.Name, g.Name)
		if recorder != nil && !previous.SurgeHeldByRatio {
			recorder.Event(isvc, corev1.EventTypeNormal, EventReasonRatioSkewRejected, ratioHoldMessage(g.Name, held))
		}
	}
}

// ratioHoldMessage words a ratio hold for the operator: the move each held
// Component's next start is under its strategy, the Components in the
// group's order, as in "held the next recreate of decoder in group 0" or
// "held the next surge start of engine and decoder in group 0".
func ratioHoldMessage(group string, held []HeldStart) string {
	var moves []string
	members := map[string][]string{}
	for _, h := range held {
		move := startName(h.Strategy)
		if _, seen := members[move]; !seen {
			moves = append(moves, move)
		}
		members[move] = append(members[move], string(h.Component))
	}
	parts := make([]string, 0, len(moves))
	for _, move := range moves {
		parts = append(parts, fmt.Sprintf("the next %s of %s", move, listOf(members[move])))
	}
	if len(parts) == 0 {
		parts = append(parts, "the next start")
	}
	return fmt.Sprintf("RatioBalanced pacing held %s in group %s to keep the cross-Component ratio", strings.Join(parts, " and "), group)
}

// listOf joins names the way a sentence reads them: "engine", "engine and
// decoder", "engine, decoder and router".
func listOf(names []string) string {
	if len(names) <= 1 {
		return strings.Join(names, "")
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

// startName is the move a Component's next start is under its strategy: a
// drain-first strategy rebuilds the Instance, an in-place one restarts its
// pod, and SurgeThenDrain (the default) adds a pod beside the old one.
func startName(strategy workloadtypes.UpdateStrategyType) string {
	switch strategy {
	case workloadtypes.UpdateStrategyRecreatePod:
		return "recreate"
	case workloadtypes.UpdateStrategyInPlaceIfPossible, workloadtypes.UpdateStrategyInPlaceOnly:
		return "in-place restart"
	}
	return "surge start"
}

// previousActiveComponent returns the CurrentComponent observed for
// `groupName` in the most recent reconcile. Used by the Sequential
// deferred-event emitter to dedupe re-fires while the same active
// Component continues rolling.
func previousActiveComponent(isvc *v1beta1.InferenceService, groupName string) v1beta1.ComponentType {
	if isvc.Status.RolloutCoordination == nil {
		return ""
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName {
			return gs.CurrentComponent
		}
	}
	return ""
}

// previousPhaseEnteredAt returns the timestamp the group last entered
// its current Phase, as captured in LastTransitionTime on the
// previously-written group status. Returns zero time when no prior
// status exists — callers (Sequential soak gate) treat zero as
// "no clock running yet."
func previousPhaseEnteredAt(isvc *v1beta1.InferenceService, groupName string) time.Time {
	if isvc.Status.RolloutCoordination == nil {
		return time.Time{}
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName && gs.LastTransitionTime != nil {
			return gs.LastTransitionTime.Time
		}
	}
	return time.Time{}
}

// previousMemberFinish returns the finish record the previously-written
// status of `groupName` carries: the Sequential member whose roll most
// recently read done and the instant it first read so. Zero when the status
// holds none.
func previousMemberFinish(isvc *v1beta1.InferenceService, groupName string) MemberFinish {
	if isvc.Status.RolloutCoordination == nil {
		return MemberFinish{}
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName && gs.MemberFinished != nil {
			return MemberFinish{Component: gs.MemberFinished.Component, At: gs.MemberFinished.At.Time}
		}
	}
	return MemberFinish{}
}

// hasMixedPairing reports whether the RollingUpdate group's Components
// observe distinct revision hashes — the cross-revision pairing case
// RollingUpdate permits but operators want surfaced.
//
// A pairing is "mixed" when at least one Component in the group has
// pods on 2+ revisions OR when different Components in the group are
// on different revisions. Both indicate ad-hoc cross-revision pod
// interaction that's worth flagging.
func hasMixedPairing(g ResolvedGroup, perRevisionPods map[v1beta1.ComponentType]map[string]int32) bool {
	if len(g.Components) < 2 {
		return false
	}
	// Single-Component multi-hash: any Component with >1 hash present.
	for _, c := range g.Components {
		hashes := perRevisionPods[c]
		nonZero := 0
		for _, n := range hashes {
			if n > 0 {
				nonZero++
			}
		}
		if nonZero > 1 {
			return true
		}
	}
	// Cross-Component hash mismatch: pick the first non-empty Component's
	// only hash and compare to others.
	var ref string
	for _, c := range g.Components {
		hashes := perRevisionPods[c]
		for h, n := range hashes {
			if n == 0 {
				continue
			}
			if ref == "" {
				ref = h
				break
			}
			if h != ref {
				return true
			}
		}
	}
	return false
}

// emitPhaseEvents fires K8s events on phase transitions for the
// observer to understand what the coordination layer is doing. Gated on
// the prev != next transition (mirroring the RecordGroupTransition
// guard): an unguarded emit repeats GroupCompleted every reconcile of
// every settled ISVC forever, burning the per-object EventCorrelator
// spam budget and risking suppression of real Warnings.
//
// The first-ever observation (prevPhase == "") emits for active phases
// but not for Idle — a freshly-observed group at rest never completed
// anything. The Sequential.Awaiting composite event is guarded on the
// COMPOSITE transition separately: the soak window holds the base Phase
// at Idle while only CompositePhase advances.
func emitPhaseEvents(recorder record.EventRecorder, isvc *v1beta1.InferenceService, g ResolvedGroup, tr GroupTransition, prevPhase v1beta1.CoordinationPhase, prevComposite string) {
	if recorder == nil {
		return
	}
	if string(prevPhase) != string(tr.Phase) {
		switch tr.Phase {
		case v1beta1.CoordinationPhaseSurging:
			recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonGroupSurging,
				"coordination group %s surging: %s", g.Name, tr.Message)
		case v1beta1.CoordinationPhaseShifting:
			recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonGroupShifting,
				"coordination group %s shifting: %s", g.Name, tr.Message)
		case v1beta1.CoordinationPhaseFailed:
			recorder.Eventf(isvc, corev1.EventTypeWarning, EventReasonGroupFailed,
				"coordination group %s failed: %s", g.Name, tr.Message)
		case v1beta1.CoordinationPhaseIdle:
			if prevPhase != "" {
				recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonGroupCompleted,
					"coordination group %s idle: %s", g.Name, tr.Message)
			}
		}
	}
	if tr.CompositePhase == v1beta1.CompositePhaseSequentialAwaiting && prevComposite != tr.CompositePhase {
		recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonSequentialAwaitingNext,
			"sequential group %s awaiting next Component: %s", g.Name, tr.Message)
	}
}

// emitSequentialDeferredEvent fires SequentialNextSpecBumpDeferred
// when an operator bumped a later-in-Order Component's spec while an
// earlier Component is still rolling — Sequential semantics silently
// defer the later rollout, and the operator gets a warning event.
//
// Detection (pure, stateless): for each Component at index j > i
// where i is the active Component's index in Order, if the later
// Component has RolloutInFlight=true, emit the event. The active
// Component is the one the phase machine and the gate name
// (activeSequentialComponent): a Component resting at its staged shape
// under a partition is done for the next Component and defers nothing.
// No-op for non-Sequential policies and for groups with no Component
// rolling.
//
// Dedup: skips the emit when activeComponent matches the previous
// reconcile's CurrentComponent. The first reconcile of a given
// activeComponent emits; subsequent reconciles of the same active
// Component don't re-emit. When the active Component advances (e.g.,
// decoder finishes → engine becomes active), the dedup clears and
// the event fires once for the new active Component.
func emitSequentialDeferredEvent(recorder record.EventRecorder, isvc *v1beta1.InferenceService, g ResolvedGroup, obs GroupObservation, prevActive v1beta1.ComponentType) {
	if recorder == nil || g.Policy != v1beta1.CoordinationPolicySequential {
		return
	}
	if len(g.Order) == 0 {
		return
	}
	activeC, ok := activeSequentialComponent(g.Order, obs.Components)
	if !ok {
		return
	}
	activeIdx := -1
	for i, c := range g.Order {
		if c == activeC {
			activeIdx = i
			break
		}
	}
	if prevActive == activeC {
		// Already emitted (or attempted) for this activeComponent in a
		// prior reconcile. K8s event recorder also aggregates by
		// (involvedObject, reason) but skipping the call entirely is
		// cleaner.
		return
	}
	for j := activeIdx + 1; j < len(g.Order); j++ {
		laterC := g.Order[j]
		laterObs, ok := obs.Components[laterC]
		if !ok || !laterObs.RolloutInFlight {
			continue
		}
		recorder.Eventf(isvc, corev1.EventTypeWarning, EventReasonSequentialNextSpecBumpDeferred,
			"sequential group %s: spec bump on later Component %s deferred while active Component %s is still rolling",
			g.Name, laterC, activeC)
	}
}

// mergeAndPersistGroupStatuses installs the freshly-computed group
// statuses into ISVC.Status.RolloutCoordination. Preserves transition
// timestamps when the phase didn't change, and prunes group entries no
// longer present in the freshly-computed set — a group renamed or
// removed from spec.rollout would otherwise leave its stale status
// entry (and stale RolloutCoordinationReady input) behind forever.
func mergeAndPersistGroupStatuses(isvc *v1beta1.InferenceService, fresh []v1beta1.RolloutCoordinationGroupStatus) {
	if len(fresh) == 0 {
		// No coordination-style groups: reconcile the stale status away rather
		// than leaving a prior rollout's group entry behind. Idempotent — this is
		// already nil for an ISVC that never coordinated.
		isvc.Status.RolloutCoordination = nil
		return
	}
	merged := isvc.Status.RolloutCoordination
	declared := make(map[string]struct{}, len(fresh))
	for _, g := range fresh {
		declared[g.Name] = struct{}{}
		merged = MergeGroupStatus(merged, g)
	}
	kept := merged.Groups[:0]
	for _, g := range merged.Groups {
		if _, ok := declared[g.Name]; ok {
			kept = append(kept, g)
		}
	}
	merged.Groups = kept
	// Stable order by group name for deterministic status writes.
	sort.SliceStable(merged.Groups, func(i, j int) bool {
		return merged.Groups[i].Name < merged.Groups[j].Name
	})
	isvc.Status.RolloutCoordination = merged
}
