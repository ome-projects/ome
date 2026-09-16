package ops

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Render builds the corev1.Pod for one ordinal of one Runner of one
// Instance, derived from a shared rendered PodSpec template. Equivalent
// to RenderWithRevision with revisionHash="" — kept as a back-compat
// entry point for tests and callers that don't have a target revision
// in scope.
//
// Owner reference comes from owner + ownerGVK; the workload key seeds
// label/selector composition (key.SelectorLabels carries the
// {ome.io/inferenceservice, component, managed-by} trio adapters
// populate at construction time).
func Render(
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key workload.Key,
	podSpec *corev1.PodSpec,
	plan workload.ComponentPlan,
	inst workload.InstancePlan,
	runner workload.RunnerPlan,
	ordinal int32,
	hook workload.RenderHook,
) (*corev1.Pod, error) {
	return RenderWithRevision(owner, ownerGVK, key, podSpec, nil, plan, inst, runner, ordinal, "", hook)
}

// RenderWithRevision builds the corev1.Pod for one ordinal of one
// Runner of one Instance, stamping ome.io/revision-hash with
// revisionHash so per-revision Services can select the pod.
//
// The returned Pod carries:
//   - stable name <key.Name>-<inst-idx>-<runner>-<ordinal>
//   - hostname + subdomain so peer DNS resolves under the Component's
//     headless service
//   - the workload labels (ome.io/inferenceservice / component /
//     instance-index / instance-incarnation / runner / managed-by /
//     pod-ordinal), plus ome.io/revision-hash when revisionHash is
//     non-empty
//   - pod-template annotations from podTemplateObjectMeta
//   - controller-owned `ome.io/serving` readiness gate appended after
//     any user-declared gates
//   - OME_* environment variables injected into every container
//     (workload-controlled values override user-supplied ones)
//   - one OwnerReference back to the owner (controller=true)
//
// After composing the canonical pod, the adapter-supplied RenderHook
// (workload.Deps.RenderHook) is invoked once with (pod, runnerName,
// ordinal, revisionHash) so adapters can layer their own overlays
// (e.g., coordination.InjectPeerEnv for the ISVC adapter).
//
// podTemplateObjectMeta may be nil for back-compat tests that don't
// have a fully-built ObjectMeta in scope; user labels / annotations
// are then skipped (workload-owned labels still land).
func RenderWithRevision(
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key workload.Key,
	podSpec *corev1.PodSpec,
	podTemplateObjectMeta *metav1.ObjectMeta,
	plan workload.ComponentPlan,
	inst workload.InstancePlan,
	runner workload.RunnerPlan,
	ordinal int32,
	revisionHash string,
	hook workload.RenderHook,
) (*corev1.Pod, error) {
	if owner == nil {
		return nil, fmt.Errorf("nil owner")
	}
	if podSpec == nil {
		return nil, fmt.Errorf("nil PodSpec")
	}

	// Pod-naming inputs derived from the Key + plan. The query.PodName
	// helper takes (isvc-from-OwnerName, component, idx, runner, ordinal);
	// adapters populate Key.OwnerName with the owner's bare name.
	isvcName := key.OwnerName
	name := query.PodName(isvcName, plan.Component, inst.Index, runner.Name, ordinal)
	subdomain := query.HeadlessServiceName(isvcName, plan.Component)

	// Start from the caller's user-intent labels (so user-declared
	// labels land on the pod), then overlay the workload-mandatory
	// labels last so selectors keep working even if the user tried to
	// set the same key. Same merge shape for annotations below.
	lbls := make(map[string]string)
	if podTemplateObjectMeta != nil {
		for k, v := range podTemplateObjectMeta.Labels {
			lbls[k] = v
		}
	}
	for k, v := range podLabels(isvcName, plan.Component, inst.Index, runner.Name, inst.Incarnation, ordinal) {
		lbls[k] = v
	}
	if revisionHash != "" {
		lbls[query.LabelRevisionHash] = revisionHash
	}
	// Informational cohort marker; routing reads the per-revision Service
	// label and the traffic targets, not this. Absent = pairs with anything.
	if plan.PairingProtocol != "" {
		lbls[query.LabelPairingProtocol] = plan.PairingProtocol
	}
	// Multi-pod Instances stamp the scheduler-plugins pod-group label so
	// the gang-aware scheduler co-schedules every pod in the Instance.
	// Single-pod Instances skip the label — no PodGroup, no gang. The
	// PodGroup itself is created by the workload gang reconciler before
	// the first pod is created; this label is the pod-side correlator.
	if inst.TotalPods() > 1 {
		lbls[query.LabelPodGroup] = query.PodGroupName(isvcName, plan.Component, inst.Index)
	}
	var anns map[string]string
	if podTemplateObjectMeta != nil && len(podTemplateObjectMeta.Annotations) > 0 {
		anns = make(map[string]string, len(podTemplateObjectMeta.Annotations))
		for k, v := range podTemplateObjectMeta.Annotations {
			if k == constants.InferenceServiceInPlaceImageTransitionAnnotationKey {
				continue
			}
			anns[k] = v
		}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   key.Namespace,
			Labels:      lbls,
			Annotations: anns,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(owner, ownerGVK),
			},
		},
		Spec: *podSpec.DeepCopy(),
	}

	pod.Spec.Hostname = name
	pod.Spec.Subdomain = subdomain
	// Dedup the ome.io/serving gate when the user template / ServingRuntime
	// already declared it. Kubelet ANDs all gates into PodReady, so two
	// copies would keep PodReady=False forever (controller only writes one
	// status).
	if !hasServingReadinessGate(pod.Spec.ReadinessGates) {
		pod.Spec.ReadinessGates = append(pod.Spec.ReadinessGates, corev1.PodReadinessGate{
			ConditionType: query.ServingConditionType,
		})
	}

	// Last-line container-name backfill: apiserver rejects Pod creates
	// when any container has Name="" with
	// `spec.containers[N].name: Required value`. The upstream merge
	// (utils.MergeRuntimeContainers / utils.restoreRunnerName) already
	// stamps constants.MainContainerName when neither the ServingRuntime
	// nor the ISVC sets a runner name, but a caller that builds a PodSpec
	// directly (back-compat tests, future direct callers) can still hand
	// us an unnamed container. Stamp the canonical default here so the
	// render path is the single source of truth at apply time.
	backfillEmptyContainerNames(&pod.Spec)

	injectOMENativeEnv(pod, plan, inst, runner, ordinal, isvcName)
	injectGangDomainAffinity(pod, plan, inst, runner, isvcName)
	injectTopologySpread(pod, plan, inst, runner, isvcName)
	applyMigrationOverlay(pod, inst.MigrationOverlay)
	// Relocation-directive exclusions: same required NotIn machinery
	// as the migration overlay's FromNode, one term per recorded node.
	// Empty list is a no-op for normal Instances.
	for _, node := range inst.ExcludedNodes {
		applyRequiredNodeExclusion(pod, node)
	}

	// Adapter-supplied hook: peer-env injection. The IR reconciler
	// (the sole workload.Reconcile caller) wires it via
	// core.ISVCRenderHook; nil skips injection.
	if hook != nil {
		hook(pod, runner.Name, ordinal, revisionHash)
	}

	return pod, nil
}

// backfillEmptyContainerNames stamps constants.MainContainerName on any
// regular or init container whose Name is the empty string. Mirrors the
// final fallback in utils.MergeRuntimeContainers so the apply-time pod
// is always nameable regardless of which upstream path produced its
// PodSpec. Idempotent — already-named containers round-trip unchanged.
func backfillEmptyContainerNames(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}
	for i := range spec.Containers {
		if spec.Containers[i].Name == "" {
			spec.Containers[i].Name = constants.MainContainerName
		}
	}
	for i := range spec.InitContainers {
		if spec.InitContainers[i].Name == "" {
			spec.InitContainers[i].Name = constants.MainContainerName
		}
	}
}

// spreadPolicyRequired is the one TopologySpread value with hard semantics.
// Matches the API enum byte-for-byte; the workload package stays free of
// v1beta1 imports.
const spreadPolicyRequired = "Required"

// injectTopologySpread renders the component's TopologySpread policy as a
// standard topologySpreadConstraint on the Instance's ANCHOR pod — the
// leader, or the sole pod of a single-pod Instance. One anchor per Instance
// makes matching-pod counts equal Instance counts, so spreading anchors
// spreads gangs. Workers deliberately carry no constraint: their generated
// worker→leader REQUIRED podAffinity already makes them unschedulable until
// their leader is placed, so the TSC-gated leader always decides the fault
// domain and a worker can never anchor the gang into one the leader could
// not enter. Required renders as DoNotSchedule (maxSkew 1: balanced
// spreading, enforced by any scheduler that runs PodTopologySpread's Filter
// — including the OME gang scheduler's packing profile, which defers the initial
// gang pin until that filter has narrowed the candidates); anything else renders as ScheduleAnyway
// (advisory; inert under a profile that disables the spreading Score). The
// constraint is deliberately not part of the revision hash: toggling the
// policy shapes future placements (creates, repairs, migrations, rollouts)
// without rolling a healthy fleet.
func injectTopologySpread(pod *corev1.Pod, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, isvcName string) {
	if plan.TopologySpread == "" || runner.Name == "worker" {
		return
	}
	key := plan.TopologySpreadKey
	if key == "" {
		key = plan.TopologyKeyForInstance(inst.Index)
	}
	if key == "" {
		return
	}
	// An operator-authored constraint on the same label wins.
	for _, c := range pod.Spec.TopologySpreadConstraints {
		if c.TopologyKey == key {
			return
		}
	}
	when := corev1.ScheduleAnyway
	if plan.TopologySpread == spreadPolicyRequired {
		when = corev1.DoNotSchedule
	}
	pod.Spec.TopologySpreadConstraints = append(pod.Spec.TopologySpreadConstraints, corev1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       key,
		WhenUnsatisfiable: when,
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			constants.InferenceServicePodLabelKey: isvcName,
			constants.OMEComponentLabel:           string(plan.Component),
			query.LabelRunner:                     runner.Name,
		}},
	})
}

// applyMigrationOverlay layers migration placement onto the rendered pod.
// Hard anti-affinity moves the surge off FromNode; preferred affinity
// nudges toward HintTargetNodes without making placement hard-fail.
// Idempotent: a second Render after a cache-lag retry doesn't
// double-stamp the same NotIn / hint term.
func applyMigrationOverlay(pod *corev1.Pod, overlay *workload.MigrationOverlay) {
	if overlay == nil {
		return
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity

	if overlay.FromNode != "" {
		applyRequiredNodeExclusion(pod, overlay.FromNode)
	}

	if len(overlay.HintTargetNodes) > 0 && !hasPreferredHintTerm(na.PreferredDuringSchedulingIgnoredDuringExecution, overlay.HintTargetNodes) {
		// PreferredDuringSchedulingIgnoredDuringExecution: nudge the
		// scheduler toward HintTargetNodes but don't block scheduling
		// when none are available.
		na.PreferredDuringSchedulingIgnoredDuringExecution = append(
			na.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1.PreferredSchedulingTerm{
				Weight: overlayPreferredHintWeight,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   overlay.HintTargetNodes,
					}},
				},
			},
		)
	}
}

// overlayPreferredHintWeight is the scheduler weight on the migration-hint
// preferred affinity. Set mid-range so the nudge stacks with — rather
// than competes against — operator-supplied preferences.
const overlayPreferredHintWeight int32 = 50

// applyRequiredNodeExclusion appends a required NotIn[node] hostname
// term to the pod's NodeAffinity — the pod MUST schedule somewhere
// other than node. NodeSelector terms are OR'd; each term's
// MatchExpressions are AND'd, so the requirement is appended to every
// existing term (and a default term is created when none exist) to
// combine with any caller-supplied required selectors. Idempotent per
// node. Shared by the migration overlay (FromNode) and the
// relocation-directive exclusion list (InstancePlan.ExcludedNodes).
func applyRequiredNodeExclusion(pod *corev1.Pod, node string) {
	if node == "" {
		return
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity
	req := corev1.NodeSelectorRequirement{
		Key:      "kubernetes.io/hostname",
		Operator: corev1.NodeSelectorOpNotIn,
		Values:   []string{node},
	}
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{req}}},
		}
		return
	}
	for i := range na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		term := &na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
		if !hasHostnameNotIn(term.MatchExpressions, node) {
			term.MatchExpressions = append(term.MatchExpressions, req)
		}
	}
}

// WouldOverlayConflictWithNodeAffinity reports whether the migration
// overlay's NotIn[FromNode] would make the source PodSpec unschedulable.
// Thin single-node wrapper over WouldExclusionsConflictWithNodeAffinity.
//
// Called from migrate.go's up-front validation: a true result means
// reject the migration with a Failed audit row rather than create a
// permanently-pending surge pod.
func WouldOverlayConflictWithNodeAffinity(spec *corev1.PodSpec, overlay *workload.MigrationOverlay) bool {
	if overlay == nil || overlay.FromNode == "" {
		return false
	}
	return WouldExclusionsConflictWithNodeAffinity(spec, []string{overlay.FromNode})
}

// WouldExclusionsConflictWithNodeAffinity reports whether excluding
// every node in excluded (required NotIn hostname terms) would make the
// PodSpec unschedulable. Conflict means every existing required
// NodeSelectorTerm pins kubernetes.io/hostname entirely inside the
// excluded set — there's no other host the pod could land on. A term
// that permits an un-excluded host (or doesn't pin hostname at all)
// defuses the conflict.
//
// Shared by the migration up-front validation (via the overlay wrapper)
// and the deadline disposition's relocation-directive guard, which
// checks the suspect node plus the instance's recorded exclusions.
func WouldExclusionsConflictWithNodeAffinity(spec *corev1.PodSpec, excluded []string) bool {
	if spec == nil || spec.Affinity == nil || spec.Affinity.NodeAffinity == nil {
		return false
	}
	req := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(excluded))
	for _, n := range excluded {
		if n != "" {
			set[n] = struct{}{}
		}
	}
	if len(set) == 0 {
		return false
	}
	for _, term := range req.NodeSelectorTerms {
		if !termPinsHostnameWithin(term, set) {
			return false
		}
	}
	return true
}

// termPinsHostnameWithin returns true when term's hostname
// In-requirement permits ONLY hosts inside excluded. A term without any
// hostname In-requirement returns false — it doesn't pin hostname at
// all.
func termPinsHostnameWithin(term corev1.NodeSelectorTerm, excluded map[string]struct{}) bool {
	for _, expr := range term.MatchExpressions {
		if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpIn {
			continue
		}
		if len(expr.Values) == 0 {
			return false
		}
		for _, v := range expr.Values {
			if _, ok := excluded[v]; !ok {
				return false
			}
		}
		return true
	}
	return false
}

// hasHostnameNotIn is the idempotency check for the NotIn[hostname]
// stamp; second-Render after a cache-lag retry won't duplicate.
func hasHostnameNotIn(exprs []corev1.NodeSelectorRequirement, hostname string) bool {
	for _, expr := range exprs {
		if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpNotIn {
			continue
		}
		for _, v := range expr.Values {
			if v == hostname {
				return true
			}
		}
	}
	return false
}

// hasPreferredHintTerm reports whether existing terms already include
// a preferred affinity for the same hostname In-set the overlay would
// add. Order-insensitive: {A,B,C} matches {C,B,A}.
func hasPreferredHintTerm(existing []corev1.PreferredSchedulingTerm, hints []string) bool {
	want := make(map[string]struct{}, len(hints))
	for _, h := range hints {
		want[h] = struct{}{}
	}
	for _, term := range existing {
		for _, expr := range term.Preference.MatchExpressions {
			if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpIn {
				continue
			}
			if len(expr.Values) != len(want) {
				continue
			}
			match := true
			for _, v := range expr.Values {
				if _, ok := want[v]; !ok {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

// hasServingReadinessGate is the dedup check before Render appends the
// ome.io/serving gate.
func hasServingReadinessGate(gates []corev1.PodReadinessGate) bool {
	for _, g := range gates {
		if g.ConditionType == query.ServingConditionType {
			return true
		}
	}
	return false
}

func podLabels(isvc string, component workload.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelInstanceIdx:                fmt.Sprintf("%d", instanceIdx),
		query.LabelInstanceIncarnation:        fmt.Sprintf("%d", incarnation),
		query.LabelRunner:                     runner,
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 fmt.Sprintf("%d", ordinal),
	}
}

// injectOMENativeEnv overlays workload OME_* env vars onto every
// container. If a container declared a variable with the same Name,
// the workload value replaces it (the controller-owned values are the
// authoritative source for these names).
func injectOMENativeEnv(pod *corev1.Pod, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32, isvcName string) {
	env := buildOMENativeEnv(plan, inst, runner, ordinal, isvcName)
	// Multi-host TPU runtimes (JAX/libtpu) read the peer host list directly to
	// build the cross-host device mesh. GKE injects it only for LWS pods, so
	// OMENative supplies it for any TPU-requesting pod from the same peer info.
	if podRequestsResource(pod, constants.GoogleTPUResourceType) {
		env = append(env, buildTPUTopologyEnv(plan, inst, runner, ordinal, isvcName)...)
	}
	owned := make(map[string]bool, len(env))
	for _, e := range env {
		owned[e.Name] = true
	}
	for i := range pod.Spec.Containers {
		kept := pod.Spec.Containers[i].Env[:0]
		for _, e := range pod.Spec.Containers[i].Env {
			if !owned[e.Name] {
				kept = append(kept, e)
			}
		}
		pod.Spec.Containers[i].Env = append(kept, env...)
	}
}

func buildOMENativeEnv(plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32, isvcName string) []corev1.EnvVar {
	// OME_RUNNER_INDEX reports the runner's position within the gang
	// (0..runner.Size-1), NOT the pod-naming ordinal. For single-pod
	// runners (Size=1) it's always 0; SurgeThenDrain alternates the pod
	// name ordinal between 0 and 1 across surges, but the runner is
	// still gang-member 0 of 1 — runtime code that reads this env to
	// pick a shard / tp-rank / etc. must see a stable value across the
	// surge. For multi-pod runners (Size>1) the ordinal IS the gang
	// position; multi-pod surge will need its own naming dimension when
	// it lands.
	gangIndex := ordinal
	if runner.Size <= 1 {
		gangIndex = 0
	}
	env := []corev1.EnvVar{
		{Name: "OME_INFERENCESERVICE_NAME", Value: isvcName},
		{Name: "OME_COMPONENT", Value: string(plan.Component)},
		{Name: "OME_COMPONENT_REPLICAS", Value: fmt.Sprintf("%d", plan.Replicas)},
		{Name: "OME_INSTANCE_INDEX", Value: fmt.Sprintf("%d", inst.Index)},
		{Name: "OME_RUNNER", Value: runner.Name},
		{Name: "OME_RUNNER_SIZE", Value: fmt.Sprintf("%d", runner.Size)},
		{Name: "OME_RUNNER_INDEX", Value: fmt.Sprintf("%d", gangIndex)},
		{Name: "OME_INSTANCE_SUBDOMAIN", Value: query.InstanceSubdomain(isvcName, plan.Component, inst.Index)},
	}
	// OME_LEADER_ADDRESS is emitted only for multi-pod Instances (the
	// Instance has a "leader" Runner). Single-pod Instances have one
	// "default" Runner with no leader, so the variable is omitted —
	// runtimes branch on its presence to detect a multi-pod environment.
	//
	// Alongside it, OME_INSTANCE_POD_RANK / OME_INSTANCE_POD_COUNT give the
	// gang-global node identity multi-node frameworks need (sglang
	// --node-rank/--nnodes, torchrun, NCCL): a flat rank 0..N-1 across the
	// whole Instance plus its size. OME_RUNNER_INDEX alone is per-runner
	// (leader and worker each index from 0) and can't be a gang-wide rank,
	// so without these every multi-node runtime has to reconstruct the rank
	// itself (leader=0, worker=index+1). Same multi-pod gate as the address.
	if addr := leaderAddressForInstance(isvcName, plan.Component, inst); addr != "" {
		env = append(env,
			corev1.EnvVar{Name: "OME_LEADER_ADDRESS", Value: addr},
			corev1.EnvVar{Name: "OME_INSTANCE_POD_RANK", Value: fmt.Sprintf("%d", instancePodRank(inst, runner, ordinal))},
			corev1.EnvVar{Name: "OME_INSTANCE_POD_COUNT", Value: fmt.Sprintf("%d", inst.TotalPods())},
		)
	}
	return env
}

// instancePodRank returns the pod's rank within the whole gang Instance —
// a flat 0..TotalPods-1 across runners in declared order (the leader runner's
// pod first, then the worker runner's). This is the global node-rank a
// multi-node framework wants; OME_RUNNER_INDEX is per-runner (leader and
// worker each restart at 0) and can't serve as a gang-wide rank.
//
// The rank is (sum of the sizes of the runners declared before this one) +
// the pod's ordinal within its runner. With the canonical leader(1)+worker(N)
// layout that yields leader→0 and worker ordinal k→k+1.
func instancePodRank(inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32) int32 {
	var offset int32
	for _, r := range inst.Runners {
		if r.Name == runner.Name {
			break
		}
		offset += r.Size
	}
	return offset + ordinal
}

// leaderAddressForInstance returns the peer-DNS address of the Instance's
// leader pod, or "" when the Instance has no "leader" Runner (single-pod
// "default" layout). The address is the SHORT form
// `<leader-pod-name>.<headless-service-name>` — the pod's DNS search
// path appends `.<ns>.svc.<cluster-domain>` at resolution time, so this
// resolves the same as the fully-qualified
// `<pod>.<headless>.<ns>.svc.cluster.local` form without the controller
// needing to discover the cluster's actual DNS domain.
func leaderAddressForInstance(isvcName string, component workload.ComponentType, inst workload.InstancePlan) string {
	for _, r := range inst.Runners {
		if r.Name == "leader" {
			return fmt.Sprintf("%s.%s",
				query.PodName(isvcName, component, inst.Index, "leader", 0),
				query.HeadlessServiceName(isvcName, component),
			)
		}
	}
	return ""
}

// tpuProcessPort is the inter-host port JAX/libtpu uses for the multi-host
// device-mesh handshake (TPU_PROCESS_ADDRESSES / TPU_PROCESS_PORT), matching the
// value GKE's LWS TPU webhook stamps for multislice pods.
const tpuProcessPort = 8476

// buildTPUTopologyEnv returns the multi-host TPU coordinator env for a multi-pod
// Instance: the full ordered peer host list, this pod's worker id, and the
// process-address list JAX/libtpu reads to form the cross-host device mesh.
// Returns nil for single-pod Instances, where GKE's default single-host slice is
// correct. Gated by the caller on the pod requesting google.com/tpu.
func buildTPUTopologyEnv(plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32, isvcName string) []corev1.EnvVar {
	hosts := instancePeerHostnames(isvcName, plan.Component, inst)
	if len(hosts) < 2 {
		return nil
	}
	addrs := make([]string, len(hosts))
	for i, h := range hosts {
		addrs[i] = fmt.Sprintf("%s:%d", h, tpuProcessPort)
	}
	return []corev1.EnvVar{
		{Name: "TPU_WORKER_HOSTNAMES", Value: strings.Join(hosts, ",")},
		{Name: "TPU_WORKER_ID", Value: fmt.Sprintf("%d", instancePodRank(inst, runner, ordinal))},
		{Name: "TPU_PROCESS_ADDRESSES", Value: strings.Join(addrs, ",")},
		{Name: "TPU_PROCESS_PORT", Value: fmt.Sprintf("%d", tpuProcessPort)},
		{Name: "TPU_NAME", Value: query.InstanceSubdomain(isvcName, plan.Component, inst.Index)},
	}
}

// instancePeerHostnames lists the short peer-DNS name of every pod in the
// Instance in flat gang-rank order — runners in declared order, each runner's
// ordinals 0..Size-1, the same order instancePodRank counts, so hosts[rank] is
// the pod whose TPU_WORKER_ID is rank.
//
// The list depends only on (isvcName, component, inst) — identical for every
// pod in the gang — so the gang-render loop precomputes it once into
// inst.PeerHostnames and Render reuses it here, avoiding the O(gangsize^2)
// per-pod rebuild. When the cache is unset (single-pod paths, direct Render
// callers, tests) it falls back to deriving the list from Runners.
func instancePeerHostnames(isvcName string, component workload.ComponentType, inst workload.InstancePlan) []string {
	if inst.PeerHostnames != nil {
		return inst.PeerHostnames
	}
	return buildInstancePeerHostnames(isvcName, component, inst)
}

// buildInstancePeerHostnames computes the Instance's ordered peer-DNS host
// list from its Runners (the uncached path). Exported within the package so
// the gang-render loop can prime inst.PeerHostnames once per gang.
func buildInstancePeerHostnames(isvcName string, component workload.ComponentType, inst workload.InstancePlan) []string {
	headless := query.HeadlessServiceName(isvcName, component)
	var hosts []string
	for _, r := range inst.Runners {
		for o := int32(0); o < r.Size; o++ {
			hosts = append(hosts, fmt.Sprintf("%s.%s",
				query.PodName(isvcName, component, inst.Index, r.Name, o), headless))
		}
	}
	return hosts
}

// injectGangDomainAffinity co-locates a multi-node gang's worker pods onto the
// SAME topology domain as their Instance's leader. The domain is the explicit
// effective per-Instance topology key. OME does not infer provider-specific
// topology labels; an empty key is a non-blocking no-op.
//
// Worker-follows-leader (not mutual affinity) by design: a required mutual
// affinity would deadlock the gang since the first-scheduled pod has no peer to
// satisfy it. Gang co-scheduling via the scheduling.x-k8s.io/pod-group label
// admits all pods together; this required podAffinity then constrains WHICH
// nodes the workers may take — the leader picks a domain, workers must follow.
// Required (not preferred) because a split gang literally cannot run.
//
// Respects a hand-written affinity: if the user already declared a required
// podAffinity term on the resolved topologyKey, OME defers to it and injects
// nothing (no duplicate / conflicting term). This same check makes the inject
// idempotent — a second render after a cache-lag retry won't duplicate the term.
func injectGangDomainAffinity(pod *corev1.Pod, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, isvcName string) {
	// Only multi-node gang workers need this. Leader / single-pod get
	// nothing (the leader is the domain anchor; single-pod runs on one host).
	if runner.Name != "worker" || !instanceHasLeader(inst) {
		return
	}

	topologyKey := plan.TopologyKeyForInstance(inst.Index)
	if topologyKey == "" {
		return
	}

	// Select THIS Instance's leader pod. OME knows the concrete instance index
	// at render time, so the selector is instance-scoped — something a static
	// ISVC YAML can't express. The literal index keeps the term portable to any
	// k8s version (no matchLabelKeys dependency).
	term := corev1.PodAffinityTerm{
		TopologyKey: topologyKey,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				constants.InferenceServicePodLabelKey: isvcName,
				constants.OMEComponentLabel:           string(plan.Component),
				query.LabelInstanceIdx:                fmt.Sprintf("%d", inst.Index),
				query.LabelRunner:                     "leader",
			},
		},
	}

	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.PodAffinity == nil {
		pod.Spec.Affinity.PodAffinity = &corev1.PodAffinity{}
	}
	pa := pod.Spec.Affinity.PodAffinity
	// Skip when a required term on this topologyKey already exists — either a
	// user-supplied override (respect it) or OME's own term from a prior render
	// (idempotency). Keyed on topologyKey alone: a user override carries a
	// different selector, so a full-term match would miss it.
	if hasRequiredAffinityForTopologyKey(pa.RequiredDuringSchedulingIgnoredDuringExecution, topologyKey) {
		return
	}
	// Append so user-provided podAffinity is preserved.
	pa.RequiredDuringSchedulingIgnoredDuringExecution = append(
		pa.RequiredDuringSchedulingIgnoredDuringExecution, term)
}

// instanceHasLeader reports whether the Instance is multi-pod — i.e. carries a
// "leader" Runner. Single-pod Instances have one "default" Runner and no leader.
func instanceHasLeader(inst workload.InstancePlan) bool {
	for _, r := range inst.Runners {
		if r.Name == "leader" {
			return true
		}
	}
	return false
}

// hasRequiredAffinityForTopologyKey is the override + idempotency check for
// injectGangDomainAffinity: reports whether ANY required podAffinity term
// already targets topologyKey. Keyed on topologyKey alone (not the full term)
// so it catches BOTH a user-supplied override (which carries its own selector)
// and OME's own term from a prior render — in both cases OME must not add
// another term on the same key.
func hasRequiredAffinityForTopologyKey(terms []corev1.PodAffinityTerm, topologyKey string) bool {
	for i := range terms {
		if terms[i].TopologyKey == topologyKey {
			return true
		}
	}
	return false
}

// podRequestsResource reports whether any container requests the named extended
// resource (limits or requests) — e.g. google.com/tpu to detect TPU pods.
func podRequestsResource(pod *corev1.Pod, name string) bool {
	rn := corev1.ResourceName(name)
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if _, ok := c.Resources.Limits[rn]; ok {
			return true
		}
		if _, ok := c.Resources.Requests[rn]; ok {
			return true
		}
	}
	return false
}
