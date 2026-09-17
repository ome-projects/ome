package coordination

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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

	// Client is the cached controller-runtime client. Used for
	// per-revision Service ensure/GC and the coordination layer's own
	// per-revision pod reads. The manager cache scopes the Pod informer
	// to the ome.io/inferenceservice label key, so it carries every pod
	// the coordination layer lists — a cached read hides nothing here
	// while shedding the apiserver LIST cost on the hot reconcile path.
	Client client.Client

	// Reader is the live (uncached) API reader, reserved for
	// correctness-critical pod reads. The coordination layer itself
	// reads pods via Client; the live reader is consumed only by the
	// canary capacity gate (canary.Dispatch -> ObservePerRevisionPods),
	// which must not lag reality before shifting traffic.
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
// RatioBalanced skew is published exclusively via the ratio_skew_total
// Prometheus counter and the RatioSkewRejected event — see the
// RatioSkewRejected branch in Reconcile — so Result intentionally does
// not carry a per-reconcile skew count.
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
	groups := ResolveGroups(v1beta1.EffectiveRollout(in.ISVC), GroupDefaults{RatioTolerancePercent: in.DefaultRatioTolerancePercent})
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
	// (the previous behavior) relied on event aggregation collapsing to one
	// object, but the aggregated count still climbed unboundedly and the
	// Event was re-PATCHed every loop — alarming noise plus apiserver write
	// load on the hot path. Clearing the condition when a group is later
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
	perRevisionPods, perRevisionReadyPods, perRevisionRouting, _, err := observePerRevisionPods(ctx, in.Client, in.ISVC, componentsTouched, true)
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
		// stay gone and strand the HTTPRoute on a dead backend.
		for hash := range perRevisionPods[c] {
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
		// live pods. Safe because we Ensure only for hashes with pods —
		// a hash with zero pods is genuinely past retention. Pods added
		// between the list and the delete re-Ensure on the next reconcile.
		//
		// The sweep runs unconditionally. It cannot be gated on the live
		// revision-hash set matching Status.Components.<c>.Traffic: the
		// hash set here counts TOTAL pods while Traffic is written from
		// READY pods, so a draining pod leaves Traffic one reconcile ahead.
		// The pass on which the drained pod finally disappears is exactly
		// the pass on which the two sets agree, so a gate would skip the
		// only sweep that could collect that revision's Services. Cost is
		// a cached-client List, not an apiserver round trip.
		if err := gcOrphanedPerRevisionServices(ctx, in.Client, in.ISVC, c, perRevisionPods[c]); err != nil {
			return nil, fmt.Errorf("gc per-revision services for %s: %w", c, err)
		}
	}

	// Write per-Component Traffic[] from the READY pod distribution
	// (producer-side contract feeding the HTTPRoute weighted-
	// backendRef consumer). Ready, not total: a revision whose pods are
	// all Pending/ImagePullBackOff cannot take traffic, and weighting it
	// points the HTTPRoute at a zero-endpoint Service. Service ensure/GC
	// above stays on the total map so a not-yet-ready revision's Service
	// exists before its first pod flips Ready.
	if err := updateTrafficStatus(ctx, in.Client, in.ISVC, componentsTouched, perRevisionReadyPods, in.TrafficWeightDeadbandPercent); err != nil {
		return nil, fmt.Errorf("update traffic status: %w", err)
	}

	// Drive the per-group state machine.
	for _, g := range groups {
		if err := ValidateGroupShape(g); err != nil {
			return nil, fmt.Errorf("invalid coordination group shape: %w", err)
		}
		obs, err := buildGroupObservation(ctx, in.Client, in.ISVC, g, perRevisionPods)
		if err != nil {
			return nil, fmt.Errorf("build observation for group %s: %w", g.Name, err)
		}
		// Snapshot Original on first observation, then preserve it
		// across reconciles via the previously-written ObservedRatio.
		// Always refresh Current + NewPods from the live observation.
		ratio := buildRatioState(in.ISVC, g, obs, perRevisionPods)
		obs.OriginalReplicas = ratio.Original
		obs.Now = now
		obs.PreviousPhaseEnteredAt = previousPhaseEnteredAt(in.ISVC, g.Name)
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

		// RollingUpdate mixed-pairing observability: emit when distinct
		// revision hashes coexist across Components (RollingUpdate
		// permits this but operators want to know).
		if g.Policy == v1beta1.CoordinationPolicyRollingUpdate && hasMixedPairing(g, perRevisionPods) {
			RecordMixedPairing(in.ISVC.Namespace, in.ISVC.Name, g.Name)
			if in.Recorder != nil {
				in.Recorder.Eventf(in.ISVC, corev1.EventTypeNormal, EventReasonMixedPairingObserved,
					"RollingUpdate group %s observed distinct per-Component revision hashes", g.Name)
			}
		}

		// RatioBalanced skew rejection: fire metric + event when
		// EvaluateSurge zeroed at least one Component's surge budget
		// to preserve the cross-Component ratio.
		if tr.RatioSkewRejected {
			RecordRatioSkew(in.ISVC.Namespace, in.ISVC.Name, g.Name)
			if in.Recorder != nil {
				in.Recorder.Eventf(in.ISVC, corev1.EventTypeNormal, EventReasonRatioSkewRejected,
					"RatioBalanced pacing rejected surge in group %s to preserve cross-Component ratio", g.Name)
			}
		}

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
	out := make([]v1beta1.ComponentType, 0, 3)
	for _, component := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		if modes[component] == constants.OMENative {
			out = append(out, component)
		}
	}
	return out
}

// observePerRevisionPods returns per-revision pod counts and routing selector
// capabilities. LeaderOnly requires an observed leader or worker runner;
// PodOrdinal additionally requires every such pod to carry an ordinal label.
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
		pods, err := query.ListOMENativePodsByName(ctx, reads, isvc.Namespace, isvc.Name, v1beta1convert.ComponentTypeToWorkload(c), useIndex)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("list pods for %s/%s: %w", isvc.Namespace, c, err)
		}
		observedPods[c] = pods
		byHash := make(map[string]int32, 4)
		readyByHash := make(map[string]int32, 4)
		routingByHash := make(map[string]RevisionRoutingSelector, 4)
		for _, pod := range pods {
			hash := pod.Labels[query.LabelRevisionHash]
			if hash == "" {
				continue
			}
			byHash[hash]++
			if podReadyAndServing(pod) {
				readyByHash[hash]++
			}
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
		ready[c] = readyByHash
		routing[c] = routingByHash
	}
	return total, ready, routing, observedPods, nil
}

// podReadyAndServing reports whether a pod is currently serving traffic: Running,
// not being deleted, and reporting the PodReady condition True. Pods without a
// readiness probe are marked Ready by the kubelet once their containers start, so
// this holds for both real runtimes and probe-less test fixtures.
func podReadyAndServing(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// dropCanaryOwned removes Components driven by a canary group from comps. Those
// Components belong to the canary engine (canary.Dispatch), which owns their
// per-revision Services and traffic status; coordination must not touch them.
func dropCanaryOwned(comps []v1beta1.ComponentType, isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	owned := map[v1beta1.ComponentType]struct{}{}
	for _, g := range isvc.Spec.GetRolloutGroups() {
		if g.Canary == nil {
			continue
		}
		for _, c := range g.Components {
			owned[c] = struct{}{}
		}
	}
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

// updateTrafficStatus writes Status.Components.<c>.Traffic[] from
// observed per-revision READY pod counts (a pod that isn't serving must
// not pull a traffic share toward its zero-endpoint Service). Idempotent;
// no-op writes are skipped to avoid bouncing the apiserver.
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
		latestHash, err := latestRevisionHashChecked(ctx, reads, isvc.Namespace, isvc.Name, c)
		if err != nil {
			return err
		}
		weights := ComputeWeightsFromPods(hashes, total, latestHash)
		if err := AttachPairingProtocols(ctx, reads, isvc.Namespace, isvc.Name, c, weights); err != nil {
			return err
		}
		traffic := BuildTrafficTargets(isvc.Name, c, weights)
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
		if latestHash != "" && hashes[latestHash] > 0 {
			latestReady := PerRevisionServiceName(isvc.Name, c, latestHash)
			if cs.LatestReadyRevision != latestReady {
				cs.LatestReadyRevision = latestReady
				changed = true
			}
		}
		// LatestRolledoutRevision advances to the per-revision Service
		// name once the rollout is at 100% on a single revision. The
		// prior value is demoted to PreviousRolledoutRevision on a real
		// advance so consumers can identify the immediately-prior
		// rolled-out revision during diagnosis and partial rollbacks.
		if len(hashes) == 1 {
			for hash := range hashes {
				rolledOut := PerRevisionServiceName(isvc.Name, c, hash)
				if cs.LatestRolledoutRevision != rolledOut {
					if cs.LatestRolledoutRevision != "" {
						cs.PreviousRolledoutRevision = cs.LatestRolledoutRevision
					}
					cs.LatestRolledoutRevision = rolledOut
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
func latestRevisionHashChecked(ctx context.Context, reads client.Reader, namespace, isvcName string, c v1beta1.ComponentType) (string, error) {
	summary, err := irprojector.ComponentIRStatus(ctx, reads, namespace, isvcName, c)
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
		summary, err := irprojector.DecodedComponentIRStatus(ctx, reads, isvc.Namespace, isvc.Name, c)
		if err != nil {
			return GroupObservation{}, err
		}
		// ResolveGroups excludes canary groups, so every Component here is
		// coordination-owned and reads the effective partition from the
		// projected IR spec — the merged ISVC↔runtime lifecycle the workload
		// controller stages Instances at. A runtime-inherited partition is
		// invisible on the raw ISVC, so reading the ISVC here would treat a
		// legitimately staged Component as an incomplete rollout forever.
		// Canary-owned Components never reach this loop (they leave
		// Partition 0, driven by the canary reconciler's own
		// EffectivePartition path).
		partition, err := irprojector.ComponentIRPartition(ctx, reads, isvc.Namespace, isvc.Name, c)
		if err != nil {
			return GroupObservation{}, err
		}
		obs.Components[c] = buildComponentObservation(summary, c, perRevisionPods[c], partition)
	}
	if paused, _ := constants.RolloutPauseState(isvc.Annotations); paused {
		obs.PausedGlobal = true
	}
	return obs, nil
}

// buildComponentObservation aggregates one Component's observation from
// the authoritative InferenceReplica status and per-revision pod counts.
func buildComponentObservation(summary *v1beta1.InferenceReplicaStatus, c v1beta1.ComponentType, perRevisionPods map[string]int32, partition int32) ComponentObservation {
	out := ComponentObservation{
		Component: c,
		Partition: partition,
	}
	if summary == nil {
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

	// Convergence: has the Component reached its desired staged shape —
	// (Replicas-Partition) instances Ready on the target revision and
	// Partition instances Ready on the prior revision? Partition=0 means
	// this is the RolloutComplete predicate.
	out.AtDesiredShape = workload.ReachedDesiredShape(
		v1beta1convert.InstanceStatusSliceToWorkload(summary.InstanceStatuses),
		summary.UpdateRevision, partition, summary.Replicas)
	return out
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

// gcOrphanedPerRevisionServices deletes per-revision Service pairs
// whose revision-hash has no live pods for `component`.
//
// The sweep lists Services in the ISVC's namespace that carry the
// (inferenceservice + component + revision-hash) selector keys, then
// drops every pair whose revision-hash isn't in `liveHashes`.
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
	svcs := &corev1.ServiceList{}
	if err := c.List(ctx, svcs,
		client.InNamespace(isvc.Namespace),
		client.MatchingLabels{
			constants.InferenceServicePodLabelKey: isvc.Name,
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
		// `<isvc>-<c>-rev-<hash>` shape; headless adds `-headless`.
		if svcs.Items[i].Name != PerRevisionServiceName(isvc.Name, component, hash) {
			continue
		}
		if err := GCPerRevisionServices(ctx, c, isvc.Namespace, isvc.Name, component, hash); err != nil {
			return fmt.Errorf("gc service for %s/%s: %w", component, hash, err)
		}
	}
	return nil
}

// buildRatioState assembles the per-group RatioState that BuildGroupStatus
// turns into Status.RolloutCoordination.Groups[i].ObservedRatio.
// SnapshotOriginal runs on first observation; Original is preserved
// across reconciles via the previously-written ObservedRatio so the
// anchor doesn't shift when MinReplicas changes mid-rollout.
//
// Always populated even for non-RatioBalanced pacing (cheap; BuildGroupStatus
// suppresses it on the status side based on policy).
//
// Snapshot deferral: the first reconcile of a freshly-created ISVC observes
// every Component with status.OMENative.Replicas == 0 because the OMENative
// status writer hasn't run yet (Create happens first; the status write lands
// on a subsequent pass). Snapshotting from that empty status would anchor
// Original to {Engine: 1, Decoder: 1} via SnapshotOriginal's zero-clamp,
// which then deadlocks RatioBalanced rollouts the moment real replica counts
// disagree (e.g., MinReplicas=4:2 produces a live 2:1 ratio that immediately
// skews against the bogus 1:1 anchor). Defer the snapshot until every
// Component reports non-zero status.OMENative.Replicas — by then the live
// counts ARE the desired counts and Original anchors correctly.
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
	// Original: read from existing Status, snapshot from current
	// observed replicas if not yet recorded (first observation that
	// also has all Components populated in status).
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
	if len(state.Original) == 0 {
		desired := make(map[v1beta1.ComponentType]int32, len(g.Components))
		allPopulated := true
		for _, c := range g.Components {
			d := obs.Components[c].DesiredReplicas
			if d <= 0 {
				// Status hasn't been written for this Component yet. Don't
				// snapshot — defer to the next reconcile after the status
				// aggregator writes the real Replicas.
				allPopulated = false
				break
			}
			desired[c] = d
		}
		if allPopulated {
			state.Original = SnapshotOriginal(desired)
		}
		return state
	}
	// Anchor members missing from the snapshot from observed replicas,
	// deferring any member whose status is still unwritten (zero
	// Replicas) — the same rule the initial snapshot applies.
	missing := make(map[v1beta1.ComponentType]int32)
	for _, c := range g.Components {
		if _, ok := state.Original[c]; ok {
			continue
		}
		if d := obs.Components[c].DesiredReplicas; d > 0 {
			missing[c] = d
		}
	}
	for c, r := range SnapshotOriginal(missing) {
		state.Original[c] = r
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
	if isvc.Status.RolloutCoordination == nil {
		return "", ""
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name == groupName {
			return gs.Phase, gs.CompositePhase
		}
	}
	return "", ""
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
// where i is the active rolling Component's index in Order, if the
// later Component has RolloutInFlight=true, emit the event. No-op
// for non-Sequential policies and for groups with no Component rolling.
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
	activeIdx := -1
	for i, c := range g.Order {
		comp, ok := obs.Components[c]
		if !ok {
			continue
		}
		if comp.RolloutInFlight {
			activeIdx = i
			break
		}
	}
	if activeIdx < 0 {
		return
	}
	activeC := g.Order[activeIdx]
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
