package canary

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils"
)

// DispatchDeps is the controller-side input to the canary executor, mirroring
// coordination.ReconcileInputs. The controller calls Dispatch once per reconcile
// when a canary group is set (spec.rollout.groups[].canary); coordination runs in
// the same pass on the non-canary Components.
type DispatchDeps struct {
	Client   client.Client
	Reader   client.Reader
	Recorder record.EventRecorder
	ISVC     *v1beta1.InferenceService
	// Now is the wall-clock used for LastTransitionTime; callers may
	// override for tests. Zero defaults to time.Now().
	// Already an injected-time seam; kept as time.Time (single snapshot per pass).
	Now time.Time
	// Sampler is the controller's shared async analysis sampler (built once at
	// startup). nil disables metric-gated steps (they hold), which keeps a
	// controller without a configured sampler from advancing analysis steps ungated.
	Sampler *Sampler
	// BundledPrometheusAddress and QueryTimeout come from the canaryAnalysis operator
	// config (fetched per-reconcile by the controller), threaded to the sampler.
	BundledPrometheusAddress string
	QueryTimeout             time.Duration
	// DefaultReadyTimeout is the operator-configured capacity-gate bound
	// (rollout.defaultReadyTimeout); zero means no configured default.
	DefaultReadyTimeout time.Duration
	// MetricProviders are this cluster's logical provider bindings; a canary
	// source naming a providerRef resolves through them. Run open already
	// parks an unbound name, so an unbound name HERE is a mid-run config
	// regression — it degrades to a source-less (inconclusive) sample rather
	// than un-gating anything.
	MetricProviders map[string]controllerconfig.MetricProviderBinding
	// DefaultProvider optionally names the binding used when the CR declares
	// neither providerRef nor serverAddress (canaryAnalysis.defaultProvider);
	// empty keeps the BundledPrometheusAddress fallback.
	DefaultProvider string
	// ComponentRunnerPorts holds each Component's effective serving ports.
	// The per-revision routing Service publishes the port resolved from this set
	// so it targets the port the pods actually listen on.
	ComponentRunnerPorts map[v1beta1.ComponentType][]corev1.ContainerPort
}

// Dispatch runs the canary step machine for an InferenceService, mutating
// isvc.Status in-memory (phase, traffic, step) and returning a requeue hint. The
// step machine is ISVC-level; it is driven through the externally-routed
// (primary) Component — the router for PD, otherwise the single Component — whose
// per-revision Services carry the external traffic weight. Per-component pod
// capacity is applied separately by the controller via EffectivePartition.
//
// No-op (requeue 0) when no canary plan is set.
func Dispatch(ctx context.Context, d DispatchDeps) (time.Duration, error) {
	if v1beta1.EffectiveCanaryGroup(d.ISVC) == nil {
		return 0, nil
	}
	primary := primaryComponent(d.ISVC)
	if primary == "" {
		return 0, nil
	}
	// Observe per-revision pods for EVERY configured component (not just the
	// primary): the canary stages capacity on all of them, so the gate must wait
	// on all of them before shifting traffic. perRev supplies revision presence
	// and routing Service inputs. readyRev keeps Pod readiness in every capacity
	// gate; IR runner topology groups those Pods into complete Instances.
	// d.Reader is the live API reader (no Pod field index) — useIndex=false skips
	// the doomed MatchingFields probe and goes straight to the label-selector List.
	perRev, readyRev, routingRev, observedPods, err := coordination.ObservePerRevisionPods(ctx, d.Reader, d.ISVC, configuredComponents(d.ISVC), false)
	if err != nil {
		return 0, err
	}
	pods := perRev[primary]

	revisions, err := observeCanaryRevisions(ctx, d.Reader, d.ISVC, primary)
	if err != nil {
		return 0, err
	}
	canaryHash := revisions.targetHash

	// Ensure the per-revision Services the canary references, for EVERY Component
	// in the canary group — not just the primary. The downstream weighted-route
	// consumer references these by name, and coordination skips canary-owned
	// Components, so the canary engine is their sole producer; ensuring only the
	// primary's would leave a secondary's per-revision Service dangling. Mirrors
	// coordination's per-Component ensure+GC loop.
	for _, comp := range configuredComponents(d.ISVC) {
		// Ensure runs EVERY reconcile — it is the self-heal path. A
		// per-revision Service deleted out-of-band must be recreated even
		// when the live revision-hash set is unchanged; EnsurePerRevisionServices
		// uses CreateOrUpdate, whose Gets are cache-served and cheap, and
		// recreates a missing Service. Gating ensure on revision-set
		// convergence would strand the weighted-route consumer on a
		// dead backend.
		for hash := range perRev[comp] {
			if hash == "" {
				continue
			}
			routing := routingRev[comp][hash]
			if _, err := coordination.EnsurePerRevisionServices(ctx, d.Client, d.ISVC, comp, hash, routing, d.ComponentRunnerPorts[comp]); err != nil {
				return 0, err
			}
		}
		// The orphan sweep runs unconditionally, mirroring coordination.
		// Gating it on the live revision-hash set matching
		// Status.Components.<c>.Traffic is unsound: perRev counts TOTAL
		// pods while Traffic is written from READY pods, so the pass on
		// which a drained pod disappears is exactly the pass on which the
		// two sets agree — skipping it strands that revision's Services.
		if err := coordination.GCOrphanedPerRevisionServices(ctx, d.Client, d.ISVC, comp, perRev[comp]); err != nil {
			return 0, err
		}
	}

	// Revision names and counters from an older IR generation describe a
	// different desired workload. Wait for one internally consistent snapshot.
	if !revisions.statusFresh {
		return reconcileRequeue, nil
	}

	// A live non-current revision can appear before the IR status publishes its
	// new target. Wait for that revision pair so their roles cannot be inverted.
	if revisions.fromIR && revisions.currentHash != "" && revisions.currentHash == canaryHash &&
		otherRevision(pods, canaryHash) != "" &&
		(d.ISVC.Status.Canary == nil || d.ISVC.Status.Canary.StableRevisionHash == "") {
		return reconcileRequeue, nil
	}

	now := d.Now
	if now.IsZero() {
		now = time.Now()
	}

	secondaryReady, secondaryFresh, err := secondaryCapacityReady(ctx, d.Reader, d.ISVC, perRev, readyRev, observedPods, primary)
	if err != nil {
		return 0, err
	}
	if !secondaryFresh {
		return reconcileRequeue, nil
	}
	// The IR revision pair authoritatively assigns current=stable and
	// update=canary while the two revisions are distinct.
	stableHash := activeRunStableRevisionHash(d.ISVC, primary)
	if stableHash == "" {
		if revisions.currentHash != canaryHash {
			stableHash = revisions.currentHash
		}
		if sc := d.ISVC.Status.Canary; sc != nil && sc.StableRevisionHash != "" {
			// A distinct authoritative current revision can repair an active canary
			// whose persisted stable and target identities are the same.
			if revisions.fromIR && revisions.currentHash != "" && revisions.currentHash != canaryHash && sc.StableRevisionHash == canaryHash {
				stableHash = revisions.currentHash
			} else {
				stableHash = sc.StableRevisionHash
			}
		}
	}
	// Resolve the two revisions' pairing protocols through the cached client:
	// a ControllerRevision's protocol is immutable from create, so a cache
	// read can only lag into "" (pairs with anything) for a just-minted CR,
	// which the meaningful-diff traffic write corrects on the next pass.
	canaryProtocol, _, err := coordination.PairingProtocolForRevision(ctx, d.Client, d.ISVC.Namespace, d.ISVC.Name, primary, canaryHash)
	if err != nil {
		return 0, err
	}
	stableProtocol, _, err := coordination.PairingProtocolForRevision(ctx, d.Client, d.ISVC.Namespace, d.ISVC.Name, primary, stableHash)
	if err != nil {
		return 0, err
	}
	res, err := Reconcile(ctx, ReconcileInputs{
		Client:                   d.Client,
		Reader:                   d.Reader,
		ISVC:                     d.ISVC,
		Component:                primary,
		CanaryRevisionHash:       canaryHash,
		StableRevisionHash:       stableHash,
		CanaryPairingProtocol:    canaryProtocol,
		StablePairingProtocol:    stableProtocol,
		ReadyCanaryInstances:     revisions.readyTargetInstanceCount(observedPods[primary]),
		DesiredReplicas:          componentReplicas(d.ISVC, primary),
		PerRevisionPods:          readyRev[primary],
		SecondaryCapacityReady:   secondaryReady,
		Now:                      now,
		Sampler:                  samplerOrNil(d.Sampler),
		Prometheus:               resolveCanarySource(d.ISVC, d.MetricProviders, d.DefaultProvider),
		BundledPrometheusAddress: d.BundledPrometheusAddress,
		QueryTimeout:             d.QueryTimeout,
		RunActive:                v1beta1.RolloutRunActive(d.ISVC),
		TargetID:                 activeCanaryTargetID(d.ISVC),
		DefaultReadyTimeout:      d.DefaultReadyTimeout,
	})
	if err != nil {
		return 0, err
	}

	// Rollback signal: when the executor is rolling back, point EVERY group
	// Component's IR at its own stable ControllerRevision so it rolls every
	// Instance back to stable; otherwise clear the signal. (Reconcile shifted
	// traffic + set the phase; this drives the actual pod revert.) The canary
	// stages capacity on all group Components, so a PD rollback must revert all of
	// them — signaling only the primary leaves the engine/decoder serving the
	// rejected revision behind the rolled-back router.
	for _, comp := range configuredComponents(d.ISVC) {
		persistedStable := componentStableRevisionHash(d.ISVC, comp, primary)
		if err := reconcileRollbackSignal(ctx, d.Client, d.Reader, d.ISVC, comp, persistedStable, res.RolledBack); err != nil {
			return 0, err
		}
	}

	recordDispatch(d.Recorder, d.ISVC, primary, res)
	return res.RequeueAfter, nil
}

// reconcileRollbackSignal sets (rolledBack) or clears one Component IR's
// Spec.Pacing.RollbackToRevision. When rolling back it points at the STABLE
// pre-canary ControllerRevision so the IR reconciler overrides its desired
// template with that revision and rolls every Instance back to it. stableHash
// is the persisted stable identity for this Component ("" when none is
// recorded). Idempotent — only writes on a change.
//
// The target is resolved from the IR's ControllerRevision history, NOT from
// ir.Status.CurrentRevision: CurrentRevision is promoted to the canary revision
// as soon as the Component fully rolls forward onto it (every Instance Ready on
// target). The primary always reaches that state — the router (the PD primary)
// is typically a 1-replica proxy whose every step rounds the new-pod count up to
// its full replica count (partition 0), so it rolls fully onto the canary at the
// FIRST step. Echoing CurrentRevision back would then tell the IR to "roll back"
// to the canary revision it is already on — a no-op — so the canary pods never
// drain and the rollback wedges in RollingBack forever.
func reconcileRollbackSignal(ctx context.Context, c client.Client, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, stableHash string, rolledBack bool) error {
	if c == nil || reads == nil {
		return nil
	}
	ir := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: irprojector.InferenceReplicaName(isvc.Name, comp)}
	if err := reads.Get(ctx, key, ir); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	var want *string
	if rolledBack {
		// An unknown stable identity must not clear a rollback signal that may
		// already carry the correct target, or guess from revision ordering.
		if stableHash == "" {
			return nil
		}
		stable, err := stableRevisionName(ctx, reads, isvc, comp, stableHash)
		if err != nil {
			return err
		}
		// Only signal a real stable target. An empty result (the stable CR was
		// GC'd, or only the canary revision exists) must NOT fall back to the
		// canary revision — that would be a no-op roll. Leaving the signal unset
		// lets the IR reconciler's NotFound path degrade gracefully.
		if stable == "" {
			return nil
		}
		want = &stable
	}
	var cur *string
	if ir.Spec.Pacing != nil {
		cur = ir.Spec.Pacing.RollbackToRevision
	}
	if utils.PtrStrEqual(cur, want) {
		return nil
	}
	if ir.Spec.Pacing == nil {
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	ir.Spec.Pacing.RollbackToRevision = want
	return c.Update(ctx, ir)
}

// stableRevisionName resolves the ControllerRevision name of an exact stable
// revision hash. Rollback safety requires an explicit identity: revision
// ordering cannot distinguish a promoted revision from a newer transient one.
//
// Reading the history is what makes the rollback target survive the canary's
// forward roll — once every Instance is on the canary revision, neither the IR
// status nor the live pod set still names the stable revision, but its
// ControllerRevision is retained.
func stableRevisionName(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, stableHash string) (string, error) {
	if stableHash == "" {
		return "", nil
	}
	list := &appsv1.ControllerRevisionList{}
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(comp),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	})
	if err := reads.List(ctx, list, client.InNamespace(isvc.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return "", err
	}
	want := query.RevisionFromHash(stableHash)
	for i := range list.Items {
		cr := &list.Items[i]
		if query.RevisionOf(cr).Same(want) {
			return cr.Name, nil
		}
	}
	return "", nil
}

// componentStableRevisionHash reads the run-scoped per-Component identity.
// The scalar canary field and component rollout status support objects written
// before per-Component run targets carried stable revisions.
func componentStableRevisionHash(isvc *v1beta1.InferenceService, comp, primary v1beta1.ComponentType) string {
	if stable := activeRunStableRevisionHash(isvc, comp); stable != "" {
		return stable
	}
	if isvc.Status.Rollout != nil {
		if last := isvc.Status.Rollout.LastRun; last != nil && last.Outcome == v1beta1.RolloutRunRolledBack {
			for _, target := range last.TargetRevisions {
				if target.Component == comp && target.StableRevision != "" {
					return target.StableRevision
				}
			}
		}
	}
	if comp == primary && isvc.Status.Canary != nil && isvc.Status.Canary.StableRevisionHash != "" {
		return isvc.Status.Canary.StableRevisionHash
	}
	if status, ok := isvc.Status.Components[comp]; ok {
		return query.RevisionFromName(status.LatestRolledoutRevision).Hash()
	}
	return ""
}

func activeRunStableRevisionHash(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) string {
	if target, ok := activeRunTarget(isvc, comp); ok {
		return target.StableRevision
	}
	return ""
}

// samplerOrNil converts a possibly-nil *Sampler into the stepSampler interface,
// returning a true nil interface (not a non-nil interface wrapping a nil pointer)
// so the executor's `Sampler == nil` guard works and never dereferences nil.
func samplerOrNil(s *Sampler) stepSampler {
	if s == nil {
		return nil
	}
	return s
}

// resolveCanarySource resolves the canary group's shared metrics source into
// the concrete connection the sampler uses. A providerRef resolves through
// the cluster's bindings (binding headers under the CR's, CR key winning); no
// declared source falls back to the operator's default provider when one is
// named, else to nil (the sampler's BundledPrometheusAddress fallback). An
// unbound providerRef mid-run yields an empty source — inconclusive samples,
// never an un-gated step.
func resolveCanarySource(isvc *v1beta1.InferenceService, providers map[string]controllerconfig.MetricProviderBinding, defaultProvider string) *v1beta1.AnalysisPrometheus {
	g := v1beta1.EffectiveCanaryGroup(isvc)
	if g == nil || g.Canary == nil {
		return nil
	}
	p := g.Canary.Prometheus
	if p == nil || (p.ProviderRef == nil && p.ServerAddress == "") {
		if defaultProvider != "" {
			if b, ok := providers[defaultProvider]; ok {
				return bindingSource(b, p)
			}
		}
		return p
	}
	if p.ProviderRef != nil {
		if b, ok := providers[p.ProviderRef.Name]; ok {
			return bindingSource(b, p)
		}
		return &v1beta1.AnalysisPrometheus{}
	}
	return p
}

// bindingSource materializes a provider binding as the sampler's connection,
// overlaying the CR's headers (CR key wins) and honoring an inline authRef
// over the binding's secret.
func bindingSource(b controllerconfig.MetricProviderBinding, overlay *v1beta1.AnalysisPrometheus) *v1beta1.AnalysisPrometheus {
	out := &v1beta1.AnalysisPrometheus{ServerAddress: b.ServerAddress, AuthRef: b.AuthSecretRef}
	if len(b.Headers) > 0 || (overlay != nil && len(overlay.Headers) > 0) {
		out.Headers = map[string]string{}
		for k, v := range b.Headers {
			out.Headers[k] = v
		}
		if overlay != nil {
			for k, v := range overlay.Headers {
				out.Headers[k] = v
			}
		}
	}
	if overlay != nil && overlay.AuthRef != nil {
		out.AuthRef = overlay.AuthRef
	}
	return out
}

// configuredComponents lists the Components the canary operates on — the canary
// group's own Components. The step machine + traffic run through the primary
// (router>engine>decoder); the capacity gate observes EVERY Component in the
// group, so a PD pair stages capacity on engine+decoder together before traffic
// shifts (the secondaryCapacityReady gate).
func configuredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	g := v1beta1.EffectiveCanaryGroup(isvc)
	if g == nil {
		return nil
	}
	return g.Components
}

// secondaryCapacityReady reports whether every NON-primary Component's canary
// Instances have reached that Component's current-step newCount. The step
// machine and external traffic run through the primary, but traffic must not
// shift until EVERY Component's canary capacity is up — otherwise a Ready router
// can advance while the engine/decoder canary pods are still coming up. True
// when there are no secondaries. The step index mirrors EffectivePartition.
//
// Each secondary gates on ITS OWN canary target hash (revision hashes are
// per-Component, so the primary's hash never names a secondary's pods). A
// secondary that was not bumped has no distinct canary revision — its target hash
// resolves to the only revision present — so there is nothing to stage and it is
// treated as ready, rather than blocking the rollout forever.
//
// IR runner topology provides the gang-safe ready Instance count. readyPerRev
// keeps ready Pods as a required capacity signal. perRev detects a live peer
// revision while an equal current/target pair settles.
func secondaryCapacityReady(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, perRev, readyPerRev map[v1beta1.ComponentType]map[string]int32, observedPods map[v1beta1.ComponentType][]*corev1.Pod, primary v1beta1.ComponentType) (bool, bool, error) {
	g := v1beta1.EffectiveCanaryGroup(isvc)
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return true, true, nil
	}
	plan := g.Canary
	idx := int32(0)
	if isvc.Status.Canary != nil {
		idx = isvc.Status.Canary.CurrentStep
	}
	if idx < 0 {
		idx = 0
	}
	if int(idx) >= len(plan.Steps) {
		idx = int32(len(plan.Steps) - 1)
	}
	step := plan.Steps[idx]
	for _, c := range configuredComponents(isvc) {
		if c == primary {
			continue
		}
		revisions, err := observeCanaryRevisions(ctx, reads, isvc, c)
		if err != nil {
			return false, false, err
		}
		if !revisions.statusFresh {
			return false, false, nil
		}
		secHash := revisions.targetHash
		if secHash == "" {
			continue
		}
		// Equal current and target revisions mean this Component was not bumped.
		// A live peer revision is allowed to disappear before the gate releases.
		if revisions.currentHash == secHash {
			if otherRevision(perRev[c], secHash) == "" {
				continue
			}
			return false, true, nil
		}
		readyInstances := revisions.readyTargetInstanceCount(observedPods[c])
		readyCapacity := readyCapacityCount(readyPerRev[c][secHash], readyInstances)
		if readyCapacity < resolveStepNewCount(step, componentReplicas(isvc, c)) {
			return false, true, nil
		}
	}
	return true, true, nil
}

type observedCanaryRevisions struct {
	currentHash string
	targetHash  string
	runners     []v1beta1.Runner
	fromIR      bool
	statusFresh bool
}

// readyTargetInstanceCount counts target Instances whose complete declared
// runner set is PodReady. Runner ordinals prevent duplicate Pods from filling a
// multi-pod gang's missing role. A non-IR observation returns nil.
func (r observedCanaryRevisions) readyTargetInstanceCount(pods []*corev1.Pod) *int32 {
	if !r.fromIR {
		return nil
	}
	ready := int32(0)
	if r.targetHash == "" || len(r.runners) == 0 {
		return &ready
	}
	required := make(map[v1beta1.RunnerName]int32, len(r.runners))
	for _, runner := range r.runners {
		if runner.Size < 1 {
			return &ready
		}
		if _, duplicate := required[runner.Name]; duplicate {
			return &ready
		}
		required[runner.Name] = runner.Size
	}

	readyPods := make(map[int32]map[v1beta1.RunnerName]map[int32]struct{})
	for _, pod := range pods {
		if pod == nil || pod.Labels[query.LabelRevisionHash] != r.targetHash || !coordination.PodReadyAndServing(pod) {
			continue
		}
		index, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		runner := v1beta1.RunnerName(pod.Labels[query.LabelRunner])
		size, known := required[runner]
		if !known {
			continue
		}
		ordinal, ok := query.PodOrdinalFromLabels(pod)
		if !ok {
			continue
		}
		if ordinal < 0 || (size == 1 && ordinal > 1) || (size > 1 && ordinal >= size) {
			continue
		}
		if readyPods[index] == nil {
			readyPods[index] = make(map[v1beta1.RunnerName]map[int32]struct{}, len(required))
		}
		if readyPods[index][runner] == nil {
			readyPods[index][runner] = make(map[int32]struct{})
		}
		readyPods[index][runner][ordinal] = struct{}{}
	}
	for _, byRunner := range readyPods {
		complete := true
		for runner, size := range required {
			if int32(len(byRunner[runner])) < size {
				complete = false
				break
			}
		}
		if complete {
			ready++
		}
	}
	return &ready
}

// observeCanaryRevisions reads the current and target hashes from one IR
// snapshot. The pair assigns stable and canary roles without relying on pod
// population, whose new-revision pods can precede the IR status update. A
// missing IR or empty revision pair is not authoritative and leaves statusFresh
// false so the caller waits without mutating rollout state.
func observeCanaryRevisions(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType) (observedCanaryRevisions, error) {
	if reads != nil {
		ir := &v1beta1.InferenceReplica{}
		key := types.NamespacedName{Namespace: isvc.Namespace, Name: irprojector.InferenceReplicaName(isvc.Name, c)}
		switch err := reads.Get(ctx, key, ir); {
		case err == nil:
			currentHash := query.RevisionFromName(ir.Status.CurrentRevision).Hash()
			targetHash := query.RevisionFromName(ir.Status.UpdateRevision).Hash()
			if targetHash == "" {
				targetHash = currentHash
			}
			statusFresh := ir.Status.ObservedGeneration == ir.Generation
			if !statusFresh {
				return observedCanaryRevisions{
					currentHash: currentHash,
					targetHash:  targetHash,
					runners:     ir.Spec.Runners,
					fromIR:      true,
					statusFresh: false,
				}, nil
			}
			if targetHash != "" {
				return observedCanaryRevisions{
					currentHash: currentHash,
					targetHash:  targetHash,
					runners:     ir.Spec.Runners,
					fromIR:      true,
					statusFresh: true,
				}, nil
			}
		case !apierrors.IsNotFound(err):
			return observedCanaryRevisions{}, err
		}
	}
	return observedCanaryRevisions{}, nil
}

// primaryComponent is the externally-routed Component the canary group drives its
// step machine + traffic weight through: router > engine > decoder, among the
// group's Components. Secondary Components (a PD pair's engine/decoder) stage
// capacity and gate the step but don't carry the external traffic weight.
func primaryComponent(isvc *v1beta1.InferenceService) v1beta1.ComponentType {
	g := v1beta1.EffectiveCanaryGroup(isvc)
	if g == nil || len(g.Components) == 0 {
		return ""
	}
	for _, c := range []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		if groupHasComponent(g, c) {
			return c
		}
	}
	return g.Components[0]
}

// componentReplicas is the steady replica count for a Component (MinReplicas,
// falling back to MaxReplicas, else 0).
func componentReplicas(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) int32 {
	var ext *v1beta1.ComponentExtensionSpec
	switch c {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			ext = &isvc.Spec.Engine.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			ext = &isvc.Spec.Decoder.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if isvc.Spec.Router != nil {
			ext = &isvc.Spec.Router.ComponentExtensionSpec
		}
	}
	if ext == nil {
		return 0
	}
	if ext.MinReplicas != nil {
		return int32(*ext.MinReplicas)
	}
	return int32(ext.MaxReplicas)
}

// otherRevision returns the non-canary revision hash with live pods (the stable
// revision). Empty when only the canary revision is present.
func otherRevision(pods map[string]int32, canaryHash string) string {
	best, bestN := "", int32(0)
	for h, n := range pods {
		if h == canaryHash || h == "" {
			continue
		}
		if n >= bestN {
			best, bestN = h, n
		}
	}
	return best
}
