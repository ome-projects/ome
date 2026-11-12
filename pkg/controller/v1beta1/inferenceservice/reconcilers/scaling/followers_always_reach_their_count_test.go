package scaling

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// This file drives the production projector (EnsureInferenceReplica with the
// canary's partition resolver) and the production coordinator (Resolve,
// Follows, ResolveTargets, Reconcile) over a small model of one
// InferenceService: an anchor
// Component, one or two followers, and a roll on the follower that a
// stand-in for the replica controller advances between the service's
// passes. At every state it checks that each follower carries
// ceil(ratio x anchor) inside its own bounds, that a follower's count and
// its canary partition describe the same fleet, that a roll on a follower
// can always finish, and that the pass reaches a fixed point within a few
// passes after every change in the world.
//
// The world's moves are an anchor scaled by its own writer (an external
// scaler, the default HPA, or the projector), a policy added, edited or
// removed, a follower's maximum moved or removed, a follower pod lost, the
// status write of a pass lost (the manager restarted between the replica
// write and the status flush), and the anchor's published count lagging
// its desired count by one pass. Every path is explored to a bound with a
// hash of visited states; the reference shapes are always among the
// configurations and the rest are sampled by a fixed seed.
// OME_SCALING_HUNT=full widens the sample and the bounds.
//
// A follower that declares no maxReplicas follows its anchor with no upper
// bound, and a follower's canary partition is written with its count in
// the same pass; the model names a run that reads otherwise
// ("follower-without-a-maximum", "partition-lags-count") and fails it.

const (
	modelService            = "svc"
	modelNamespace          = "ns"
	modelImage              = "example-serving:v2"
	modelBudget             = "10%"
	modelLifecyclePartition = int32(2)
	modelMaxCount           = int32(40)
	maxSettlePasses         = 8
	// settledPassesAllowed is the most passes a change in the world may take
	// before the replicas stop changing: the pass that sees the change, the
	// pass the follower's own spec event brings, and one more.
	settledPassesAllowed = 3
)

// modelScaler is who writes a Component's count when the coordinator does not.
type modelScaler uint8

const (
	scalerDefault  modelScaler = iota // no autoscaler block: the default HPA writes /scale inside the declared bounds
	scalerExternal                    // an operator's scaler writes /scale; the projector keeps the live count
	scalerNone                        // autoscaler.class None: the projector writes minReplicas every pass
)

var modelScalerNames = [...]string{"default", "external", "none"}

func (s modelScaler) String() string { return modelScalerNames[s] }

// modelRoll is what a Component is doing beside the coordinator.
type modelRoll uint8

const (
	rollIdle                modelRoll = iota // every Instance on the current revision
	rollRecreate                             // RecreatePod at maxUnavailable 10 percent
	rollSurge                                // SurgeThenDrain at maxSurge 10 percent
	rollRecreatePartitioned                  // RecreatePod with a lifecycle partition of two
	rollRecreateCanary                       // RecreatePod under a canary group stepping 25 then 100 percent
	rollSurgeCanary                          // SurgeThenDrain under the same canary group
)

var modelRollNames = [...]string{"idle", "recreate", "surge", "partitioned", "canary", "surge-canary"}

func (r modelRoll) String() string { return modelRollNames[r] }
func (r modelRoll) surges() bool   { return r == rollSurge || r == rollSurgeCanary }
func (r modelRoll) canary() bool   { return r == rollRecreateCanary || r == rollSurgeCanary }
func (r modelRoll) rolls() bool    { return r != rollIdle }

// modelMember is one Component of the service under exploration.
type modelMember struct {
	comp          v1beta1.ComponentType
	min, max      int // spec.<c>.minReplicas and maxReplicas; 0 is an unset maximum
	scaler        modelScaler
	runtimeScaler bool // the ServingRuntime declares an HPA on the Component
	roll          modelRoll
}

func (m modelMember) String() string {
	max := "-"
	if m.max > 0 {
		max = fmt.Sprint(m.max)
	}
	s := fmt.Sprintf("%s=%d:[%d,%s]/%s/%s", m.comp, m.min, m.min, max, m.scaler, m.roll)
	if m.runtimeScaler {
		s += "/runtime-hpa"
	}
	return s
}

// modelConfig is one configuration of the service under exploration. The
// anchor is the first member.
type modelConfig struct {
	name          string
	members       []modelMember
	ratios        map[v1beta1.ComponentType]string
	ratioChoices  []string
	startsWithout bool // the policy is added by a move
	scales        int8
	edits         int8
	losses        int8
	restarts      int8
	lags          int8
	scaleDeltas   []int32 // 2 and -2 mean half the anchor
}

func (c modelConfig) String() string {
	parts := make([]string, 0, len(c.members)+2)
	for _, m := range c.members {
		parts = append(parts, m.String())
	}
	keys := make([]string, 0, len(c.ratios))
	for comp := range c.ratios {
		keys = append(keys, string(comp))
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("ratio %s=%s", k, c.ratios[v1beta1.ComponentType(k)]))
	}
	if c.startsWithout {
		parts = append(parts, "policy added later")
	}
	return strings.Join(parts, " ")
}

// modelPolicy is the scaling policy as the service spec carries it.
type modelPolicy struct {
	present bool
	mode    v1beta1.ScalingMode
	anchor  v1beta1.ComponentType
	ratios  map[v1beta1.ComponentType]string
}

func (p modelPolicy) clone() modelPolicy {
	out := p
	out.ratios = map[v1beta1.ComponentType]string{}
	for c, r := range p.ratios {
		out.ratios[c] = r
	}
	return out
}

func (p modelPolicy) String() string {
	if !p.present {
		return "none"
	}
	if p.mode != v1beta1.ScalingProportional {
		return string(p.mode)
	}
	keys := make([]string, 0, len(p.ratios))
	for c := range p.ratios {
		keys = append(keys, string(c))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, p.ratios[v1beta1.ComponentType(k)]))
	}
	return fmt.Sprintf("%s<-{%s}", p.anchor, strings.Join(parts, ","))
}

// rowKind is what the roll has done to one Instance, as its row reads.
type rowKind uint8

const (
	rowOld      rowKind = iota // Ready on the old revision, serving
	rowNew                     // Ready on the target revision, serving
	rowCreating                // created at the target revision, not serving yet
	rowDrain                   // a recreate toward the target; nothing serving
	rowSurge                   // a surge: the old pod serves while its replacement starts
)

var rowKindNames = [...]string{"old", "new", "creating", "drain", "surge"}

func (r rowKind) String() string { return rowKindNames[r] }
func (r rowKind) serving() bool  { return r == rowOld || r == rowNew || r == rowSurge }

// memberState is one Component's replica as the apiserver holds it, with
// the rows the stand-in replica controller keeps for it.
type memberState struct {
	ir        *v1beta1.InferenceReplica // nil before the projector created it
	rows      []rowKind
	published int32 // status.replicas
	minEdit   int   // a positive value replaces the configured minReplicas
	maxEdit   int   // a positive value replaces the configured maxReplicas; -1 removes it
}

func (m memberState) replicas() int32 {
	if m.ir == nil || m.ir.Spec.Replicas == nil {
		return -1
	}
	return *m.ir.Spec.Replicas
}

func (m memberState) partition() *int32 {
	if m.ir == nil || m.ir.Spec.Pacing == nil {
		return nil
	}
	return m.ir.Spec.Pacing.Partition
}

func (m memberState) scalerClass() v1beta1.AutoscalerClass {
	if m.ir == nil || m.ir.Spec.Autoscaler == nil {
		return ""
	}
	return m.ir.Spec.Autoscaler.Class
}

func (m memberState) count(kinds ...rowKind) int32 {
	var n int32
	for _, r := range m.rows {
		for _, k := range kinds {
			if r == k {
				n++
			}
		}
	}
	return n
}

func (m memberState) String() string {
	parts := make([]string, 0, len(m.rows))
	for _, r := range m.rows {
		parts = append(parts, r.String())
	}
	return fmt.Sprintf("replicas=%d partition=%s published=%d autoscaler=%q rows=[%s]", m.replicas(), ptrText(m.partition()), m.published, m.scalerClass(), strings.Join(parts, ","))
}

func ptrText(p *int32) string {
	if p == nil {
		return "none"
	}
	return fmt.Sprint(*p)
}

func samePtr(a, b *int32) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// modelMove is one thing the world does between two settles.
type modelMove struct {
	kind   string
	member int
	idx    int
	delta  int32
	text   string
}

func (m modelMove) String() string {
	switch m.kind {
	case "scale", "scale-lag":
		return fmt.Sprintf("%s member %d by %+d", m.kind, m.member, m.delta)
	case "serve", "lose":
		return fmt.Sprintf("%s member %d row %d", m.kind, m.member, m.idx)
	case "ratio":
		return fmt.Sprintf("ratio of member %d to %s", m.member, m.text)
	case "max-below":
		return fmt.Sprintf("maxReplicas of member %d to %d", m.member, m.delta)
	case "max-off", "drop-follower":
		return fmt.Sprintf("%s member %d", m.kind, m.member)
	}
	return m.kind
}

// modelState is the model at one moment: every replica, the policy, the
// canary step, the status the last flush wrote, and the moves that led here.
type modelState struct {
	members      []memberState
	policy       modelPolicy
	step         int32
	status       *v1beta1.ScalingPolicyStatus
	statusBefore *v1beta1.ScalingPolicyStatus
	lag          bool                  // the anchor's next publication keeps its old count
	restarted    bool                  // the last pass's status write was lost
	released     v1beta1.ComponentType // a follower the last move released, or ""
	releasedFrom int32                 // the count that follower carried when released
	scales       int8
	edits        int8
	losses       int8
	restarts     int8
	lags         int8
	settledIn    int
	path         []modelMove
	notes        []string
}

func (st *modelState) clone() *modelState {
	out := *st
	out.members = make([]memberState, len(st.members))
	for i, m := range st.members {
		if m.ir != nil {
			m.ir = m.ir.DeepCopy()
		}
		m.rows = append([]rowKind(nil), m.rows...)
		out.members[i] = m
	}
	out.policy = st.policy.clone()
	out.status = st.status.DeepCopy()
	out.statusBefore = st.statusBefore.DeepCopy()
	out.path = append([]modelMove(nil), st.path...)
	out.notes = nil
	return &out
}

func statusDigest(s *v1beta1.ScalingPolicyStatus) string {
	if s == nil {
		return "-"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s/%d/", readyReason(s), s.AnchorDesired)
	for _, f := range s.Followers {
		fmt.Fprintf(&b, "%s:%d:%d:%v,", f.Component, f.Target, f.Current, f.Clamped)
	}
	return b.String()
}

// releasedEntry is the status entry that records a Component the coordinator
// keeps the count for after it stopped following, nil when there is none.
func releasedEntry(s *v1beta1.ScalingPolicyStatus, comp v1beta1.ComponentType) *v1beta1.FollowerScalingStatus {
	if s == nil {
		return nil
	}
	for i := range s.Followers {
		if s.Followers[i].Component == comp && s.Followers[i].Released {
			return &s.Followers[i]
		}
	}
	return nil
}

func readyReason(s *v1beta1.ScalingPolicyStatus) string {
	if s == nil {
		return ""
	}
	for _, c := range s.Conditions {
		if c.Type == "Ready" {
			return c.Reason
		}
	}
	return ""
}

func (st *modelState) key() string {
	var b strings.Builder
	for _, m := range st.members {
		fmt.Fprintf(&b, "%d/%s/%s/%d/%d/%d|", m.replicas(), ptrText(m.partition()), m.scalerClass(), m.published, m.minEdit, m.maxEdit)
		for _, r := range m.rows {
			b.WriteByte(byte('a' + r))
		}
		b.WriteByte(';')
	}
	fmt.Fprintf(&b, "%s|%d|%v|%v|%d%d%d%d%d|", st.policy, st.step, st.lag, st.restarted, st.scales, st.edits, st.losses, st.restarts, st.lags)
	b.WriteString(statusDigest(st.status))
	return b.String()
}

func (st *modelState) String() string {
	parts := make([]string, 0, len(st.members)+2)
	for i, m := range st.members {
		parts = append(parts, fmt.Sprintf("member %d [%s]", i, m))
	}
	parts = append(parts, fmt.Sprintf("policy %s step %d status %s", st.policy, st.step, statusDigest(st.status)))
	return strings.Join(parts, " | ")
}

func modelPath(st *modelState) string {
	parts := make([]string, 0, len(st.path))
	for _, mv := range st.path {
		parts = append(parts, mv.String())
	}
	return strings.Join(parts, " -> ")
}

// modelWorld builds the objects the pass reads from a state and runs the
// production projector and coordinator over them.
type modelWorld struct {
	t        *testing.T
	cfg      modelConfig
	ctx      context.Context
	recorder *record.FakeRecorder
	steps    []v1beta1.RolloutGroupStep
	// passes counts the service passes run: one projection per Component
	// and one coordinator pass each.
	passes int
}

func newModelWorld(t *testing.T, cfg modelConfig) *modelWorld {
	t.Helper()
	return &modelWorld{
		t: t, cfg: cfg, ctx: context.Background(), recorder: record.NewFakeRecorder(1024),
		steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 25},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		},
	}
}

func (w *modelWorld) memberIndex(comp v1beta1.ComponentType) int {
	for i, m := range w.cfg.members {
		if m.comp == comp {
			return i
		}
	}
	return -1
}

// anchorIndex is the member the world scales: the policy's anchor, or the
// configured anchor while no proportional policy stands.
func (w *modelWorld) anchorIndex(st *modelState) int {
	if st.policy.present && st.policy.mode == v1beta1.ScalingProportional {
		if i := w.memberIndex(st.policy.anchor); i >= 0 {
			return i
		}
	}
	return 0
}

func (w *modelWorld) configPolicy() modelPolicy {
	p := modelPolicy{present: true, mode: v1beta1.ScalingProportional, anchor: w.cfg.members[0].comp, ratios: map[v1beta1.ComponentType]string{}}
	for c, r := range w.cfg.ratios {
		p.ratios[c] = r
	}
	return p
}

// bounds are a member's declared minReplicas and maxReplicas as the spec
// reads now, edits applied; max is 0 when none is declared.
func (w *modelWorld) bounds(st *modelState, i int) (min, max int) {
	m, ms := w.cfg.members[i], st.members[i]
	min, max = m.min, m.max
	if ms.minEdit > 0 {
		min = ms.minEdit
	}
	switch {
	case ms.maxEdit < 0:
		max = 0
	case ms.maxEdit > 0:
		max = ms.maxEdit
	}
	return min, max
}

// ext is the merged component spec the pass hands to the projector and the
// coordinator for one member.
func (w *modelWorld) ext(st *modelState, i int) *v1beta1.ComponentExtensionSpec {
	m := w.cfg.members[i]
	min, max := w.bounds(st, i)
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(min), MaxReplicas: max}
	switch m.scaler {
	case scalerExternal:
		ext.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
	case scalerNone:
		ext.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
	}
	strategy := v1beta1.UpdateStrategyRecreatePod
	update := &v1beta1.RollingUpdate{MaxUnavailable: ptr.To(intstr.FromString(modelBudget))}
	if m.roll.surges() {
		strategy = v1beta1.UpdateStrategySurgeThenDrain
		update = &v1beta1.RollingUpdate{MaxSurge: ptr.To(intstr.FromString(modelBudget))}
	}
	if m.roll == rollRecreatePartitioned {
		update.Partition = ptr.To(modelLifecyclePartition)
	}
	ext.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: strategy, RollingUpdate: update}}
	return ext
}

// runtime is the ServingRuntime's spec: empty unless a member carries a
// runtime-declared HPA.
func (w *modelWorld) runtime() *v1beta1.ServingRuntimeSpec {
	rt := &v1beta1.ServingRuntimeSpec{}
	for _, m := range w.cfg.members {
		if !m.runtimeScaler {
			continue
		}
		block := v1beta1.ComponentExtensionSpec{Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}}
		switch m.comp {
		case v1beta1.EngineComponent:
			rt.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: block}
		case v1beta1.DecoderComponent:
			rt.DecoderConfig = &v1beta1.DecoderSpec{ComponentExtensionSpec: block}
		case v1beta1.RouterComponent:
			rt.RouterConfig = &v1beta1.RouterSpec{ComponentExtensionSpec: block}
		}
	}
	return rt
}

// canaryGroup is the canary group the spec declares: every member that
// rolls under a canary. Admission requires the engine beside the decoder,
// so a decoder that rolls under a canary has its anchor in the group too.
func (w *modelWorld) canaryGroup() *v1beta1.RolloutGroup {
	var comps []v1beta1.ComponentType
	for _, m := range w.cfg.members {
		if m.roll.canary() {
			comps = append(comps, m.comp)
		}
	}
	if len(comps) == 0 {
		return nil
	}
	return &v1beta1.RolloutGroup{Components: comps, Canary: &v1beta1.GroupCanary{Steps: w.steps}}
}

// service is the InferenceService as the pass reads it: the spec with the
// policy and the canary group, the status with the canary step the run is
// at and the coordinator's last decision.
func (w *modelWorld) service(st *modelState) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: modelService, Namespace: modelNamespace, UID: types.UID(modelService + "-uid"), Generation: 1},
		Spec:       v1beta1.InferenceServiceSpec{DeploymentMode: ptr.To(constants.OMENative)},
	}
	for i, m := range w.cfg.members {
		ext := w.ext(st, i)
		switch m.comp {
		case v1beta1.EngineComponent:
			isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: *ext}
		case v1beta1.DecoderComponent:
			isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: *ext}
		case v1beta1.RouterComponent:
			isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: *ext}
		}
	}
	if st.policy.present {
		isvc.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: st.policy.mode}
		if st.policy.mode == v1beta1.ScalingProportional {
			ratios := map[v1beta1.ComponentType]resource.Quantity{}
			for c, r := range st.policy.ratios {
				ratios[c] = resource.MustParse(r)
			}
			isvc.Spec.ScalingPolicy.Proportional = &v1beta1.ProportionalPolicy{Anchor: st.policy.anchor, Ratios: ratios}
		}
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
	if g := w.canaryGroup(); g != nil {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{*g}}
		isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID: "run",
			Plan:  v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: v1beta1.RolloutPlanSourceInline, Group: *g.DeepCopy()}}},
		}}
		isvc.Status.Components[rollout.PrimaryOf(g)] = v1beta1.ComponentStatusSpec{
			RolloutPhase: v1beta1.RolloutPhaseCanarying,
			Canary:       &v1beta1.CanaryStatus{CurrentStep: st.step, CanaryRevisionHash: "target", StableRevisionHash: "old"},
		}
	}
	if st.status != nil {
		isvc.Status.ScalingPolicy = st.status.DeepCopy()
	}
	return isvc
}

// memberSnapshot is a replica's count and partition before a pass.
type memberSnapshot struct {
	replicas  int32
	partition *int32
}

// passResult is what one pass of the service controller's scaling part did.
type passResult struct {
	err       error
	changed   bool // a replica was created or rewritten
	clamped   int  // ScalingTargetClamped warnings raised
	conflicts int  // FollowerAutoscalerConflict warnings raised
	before    []memberSnapshot
}

// pass runs the scaling part of one service pass in the controller's order:
// the policy is resolved, the coordinator resolves each follower's count,
// every Component is projected with its follower flag, that count, its
// resolved autoscaler and the canary's partition resolver, and the
// coordinator reads the replicas again and publishes the status.
func (w *modelWorld) pass(st *modelState) passResult {
	w.passes++
	res := passResult{before: make([]memberSnapshot, len(st.members))}
	for i, m := range st.members {
		res.before[i] = memberSnapshot{replicas: m.replicas(), partition: m.partition()}
	}
	isvc := w.service(st)
	var objs []client.Object
	for _, m := range st.members {
		if m.ir != nil {
			objs = append(objs, m.ir.DeepCopy())
		}
	}
	store := fakeclient.New(w.t, fakeclient.WithObjects(objs...))
	rt := w.runtime()
	policy, source := Resolve(isvc, rt)
	bounds := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{}
	for i, m := range w.cfg.members {
		bounds[m.comp] = w.ext(st, i)
		modes[m.comp] = constants.OMENative
	}
	st.statusBefore = st.status.DeepCopy()
	targets, err := ResolveTargets(w.ctx, store, isvc, policy, source, bounds, modes)
	if err != nil {
		res.err = fmt.Errorf("resolve targets: %w", err)
		return res
	}
	for _, m := range w.cfg.members {
		follows := Follows(policy, m.comp, bounds, modes)
		resolved, src, hold := autoscaler.ResolveComponentAutoscalerWithPolicy(rt, isvc, m.comp, nil)
		// The component reconciler replaces a follower's default HPA with
		// no autoscaler, so the coordinator is the follower's only writer.
		if follows && src == autoscaler.SpecSourceDefault && !hold {
			resolved = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
		}
		_, err := irprojector.EnsureInferenceReplica(w.ctx, irprojector.Params{
			ISVC:                isvc,
			Component:           m.comp,
			ComponentExt:        bounds[m.comp],
			ObjectMeta:          metav1.ObjectMeta{Labels: map[string]string{constants.OMEComponentLabel: string(m.comp)}},
			PodSpec:             &corev1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: modelImage}}},
			PacingPartition:     canary.PartitionResolver(isvc, rollout.Policies{}, m.comp),
			ResolvedAutoscaler:  resolved,
			CoordinatorFollower: follows,
			CoordinatorTarget:   targets.Replicas(m.comp),
			FollowerReleased:    targets.Released(m.comp),
			Client:              store,
			Reader:              store,
		})
		if err != nil {
			res.err = fmt.Errorf("project %s: %w", m.comp, err)
			return res
		}
	}
	if err := Reconcile(w.ctx, store, w.recorder, isvc, targets); err != nil {
		res.err = fmt.Errorf("coordinate: %w", err)
		return res
	}
	for i, m := range w.cfg.members {
		live := &v1beta1.InferenceReplica{}
		key := client.ObjectKey{Namespace: modelNamespace, Name: irprojector.InferenceReplicaName(modelService, m.comp)}
		if err := store.Get(w.ctx, key, live); err != nil {
			res.err = fmt.Errorf("read %s: %w", m.comp, err)
			return res
		}
		if was := st.members[i].ir; was == nil || was.ResourceVersion != live.ResourceVersion {
			res.changed = true
		}
		st.members[i].ir = live
	}
	st.status = isvc.Status.ScalingPolicy
	for drained := false; !drained; {
		select {
		case event := <-w.recorder.Events:
			if strings.Contains(event, "ScalingTargetClamped") {
				res.clamped++
			}
			if strings.Contains(event, "FollowerAutoscalerConflict") {
				res.conflicts++
			}
		default:
			drained = true
		}
	}
	return res
}

// held is the set of indices the effective partition keeps on their
// revision: the projected pacing partition when set, else the lifecycle
// partition, applied to the lowest-indexed Instances not on the target.
func (w *modelWorld) held(st *modelState, i int) map[int]bool {
	held := map[int]bool{}
	ms := st.members[i]
	if ms.ir == nil {
		return held
	}
	partition := irprojector.EffectivePartition(ms.ir.Spec.Lifecycle, ms.ir.Spec.Pacing)
	if partition == nil || *partition <= 0 {
		return held
	}
	rank := int32(0)
	for idx, r := range ms.rows {
		if r != rowOld {
			continue
		}
		if rank >= *partition {
			break
		}
		held[idx] = true
		rank++
	}
	return held
}

// replicaControllers is the stand-in for every Component's replica
// controller: it removes the rows beyond the count, creates the missing
// ones at the target revision, admits starts on the Instances the partition
// does not hold up to the Component's own budget, and publishes the rows.
// It reports whether anything moved.
func (w *modelWorld) replicaControllers(st *modelState) bool {
	moved := false
	anchor := w.anchorIndex(st)
	for i := range st.members {
		ms := &st.members[i]
		if ms.ir == nil || ms.ir.Spec.Replicas == nil {
			continue
		}
		m := w.cfg.members[i]
		n := int(*ms.ir.Spec.Replicas)
		if len(ms.rows) > n {
			removed := ms.rows[n:]
			staged := int32(0)
			for _, r := range removed {
				if r == rowNew {
					staged++
				}
			}
			if staged > 0 && m.roll.canary() && int(st.step) < len(w.steps) {
				st.notes = append(st.notes, fmt.Sprintf("%s scaled down to %d: %d of the Instances removed were on the canary revision", m.comp, n, staged))
			}
			ms.rows = ms.rows[:n]
			moved = true
		}
		for len(ms.rows) < n {
			ms.rows = append(ms.rows, rowCreating)
			moved = true
		}
		held := w.held(st, i)
		budget := utils.ScaledCountFromIntOrString(ptr.To(intstr.FromString(modelBudget)), int32(n), true)
		start := rowDrain
		if m.roll.surges() {
			start = rowSurge
		}
		open := ms.count(start)
		for idx, r := range ms.rows {
			if r != rowOld || held[idx] || open >= budget {
				continue
			}
			ms.rows[idx] = start
			open++
			moved = true
		}
		published := int32(n)
		if st.lag && i == anchor {
			published = ms.published
			st.lag = false
		}
		ms.published = published
		ms.ir.Status = w.publish(ms, m)
	}
	return moved
}

// publish is the replica status the rows read as.
func (w *modelWorld) publish(ms *memberState, m modelMember) v1beta1.InferenceReplicaStatus {
	old, target := "old", "target"
	if !m.roll.rolls() {
		old = target
	}
	status := v1beta1.InferenceReplicaStatus{CurrentRevision: old, UpdateRevision: target, Replicas: ms.published, ObservedGeneration: ms.ir.Generation}
	for idx, r := range ms.rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(idx)}
		switch r {
		case rowOld:
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, old, 1, 1, 1
		case rowNew:
			row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, target, 1, 1, 1
		case rowCreating:
			row.Phase, row.TargetRevision = v1beta1.OMENativeInstanceCreating, target
		case rowDrain:
			row.Phase, row.RunningRevision, row.TargetRevision = v1beta1.OMENativeInstanceUpdating, old, target
		case rowSurge:
			row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceUpdating, old, target, 2, 1, 1
		}
		if r.serving() {
			status.ServingReplicas++
			status.ReadyReplicas++
		}
		if r == rowNew {
			status.UpdatedReplicas++
		}
		status.InstanceStatuses = append(status.InstanceStatuses, row)
	}
	return status
}

// expected is the count the policy asks of a follower: ceil(ratio x anchor)
// raised to the follower's minimum and, when it declares a maximum, lowered
// to it. It is written out here so the model does not trust the function it
// checks. ok is false for a member the current policy does not make follow.
func (w *modelWorld) expected(st *modelState, i int) (want int32, wanted int64, ok bool) {
	p, m := st.policy, w.cfg.members[i]
	if !p.present || p.mode != v1beta1.ScalingProportional {
		return 0, 0, false
	}
	ratio, follows := p.ratios[m.comp]
	if !follows {
		return 0, 0, false
	}
	ai := w.memberIndex(p.anchor)
	if ai < 0 || st.members[ai].ir == nil || st.members[ai].ir.Spec.Replicas == nil {
		return 0, 0, false
	}
	anchor := int64(*st.members[ai].ir.Spec.Replicas)
	quantity := resource.MustParse(ratio)
	milli := quantity.MilliValue()
	wanted = (anchor*milli + 999) / 1000
	min, max := w.bounds(st, i)
	if min < 1 {
		min = 1
	}
	result := wanted
	if result < int64(min) {
		result = int64(min)
	}
	if max > 0 {
		if max < min {
			max = min
		}
		if result > int64(max) {
			result = int64(max)
		}
	}
	return int32(result), wanted, true
}

// stepReady reports whether every member of the canary group has staged
// the step's share: the capacity the step machine waits for before it
// advances.
func (w *modelWorld) stepReady(st *modelState) bool {
	if w.canaryGroup() == nil || int(st.step) >= len(w.steps) {
		return false
	}
	isvc := w.service(st)
	for i, m := range w.cfg.members {
		if !m.roll.canary() {
			continue
		}
		ms := st.members[i]
		if ms.ir == nil {
			return false
		}
		desired := ms.replicas()
		p := canary.StepPartition(isvc, rollout.Policies{}, m.comp, desired)
		if p == nil || ms.count(rowNew) < desired-*p {
			return false
		}
	}
	return true
}

// rollComplete reports whether a member's roll has nothing left to do:
// every Instance the partition does not hold is on the target revision and,
// under a canary, the run is past its last step.
func (w *modelWorld) rollComplete(st *modelState, i int) bool {
	ms, m := st.members[i], w.cfg.members[i]
	if ms.ir == nil {
		return false
	}
	if m.roll.canary() && int(st.step) < len(w.steps) {
		return false
	}
	held := w.held(st, i)
	for idx, r := range ms.rows {
		if r == rowNew || (r == rowOld && held[idx]) {
			continue
		}
		return false
	}
	return true
}

func (w *modelWorld) complete(st *modelState) bool {
	for i := range w.cfg.members {
		if !w.rollComplete(st, i) {
			return false
		}
	}
	return true
}

// progressMoves are the moves that advance a roll by themselves: a started
// or created Instance serves, the canary step advances once its capacity
// is staged.
func (w *modelWorld) progressMoves(st *modelState) []modelMove {
	var out []modelMove
	for i, ms := range st.members {
		for _, kind := range []rowKind{rowDrain, rowSurge, rowCreating} {
			for idx, r := range ms.rows {
				if r == kind {
					out = append(out, modelMove{kind: "serve", member: i, idx: idx})
					break
				}
			}
		}
	}
	if w.stepReady(st) {
		out = append(out, modelMove{kind: "step"})
	}
	return out
}

// adverseMoves are the moves that change what the coordinator must answer:
// the anchor scaled, the policy edited, a bound moved, a pod lost, a status
// write lost.
func (w *modelWorld) adverseMoves(st *modelState) []modelMove {
	var out []modelMove
	ai := w.anchorIndex(st)
	if anchor := st.members[ai]; st.scales > 0 && anchor.ir != nil {
		current := anchor.replicas()
		for _, d := range w.cfg.scaleDeltas {
			delta := d
			if d == 2 || d == -2 {
				delta = current / 2
				if d < 0 {
					delta = -delta
				}
			}
			target := current + delta
			if delta == 0 || target < 1 || target > modelMaxCount {
				continue
			}
			if w.cfg.members[ai].scaler == scalerDefault {
				min, max := w.bounds(st, ai)
				if max < min {
					max = min
				}
				if target < int32(min) || target > int32(max) {
					continue
				}
			}
			out = append(out, modelMove{kind: "scale", member: ai, delta: delta})
			if st.lags > 0 {
				out = append(out, modelMove{kind: "scale-lag", member: ai, delta: delta})
			}
		}
	}
	if st.edits > 0 {
		if st.policy.present && st.policy.mode == v1beta1.ScalingProportional {
			for i, m := range w.cfg.members {
				ratio, follows := st.policy.ratios[m.comp]
				if !follows {
					continue
				}
				for _, choice := range w.cfg.ratioChoices {
					if choice != ratio {
						out = append(out, modelMove{kind: "ratio", member: i, text: choice})
					}
				}
				if want, _, ok := w.expected(st, i); ok && want > 1 {
					out = append(out, modelMove{kind: "max-below", member: i, delta: want - 1})
				}
				if _, max := w.bounds(st, i); max > 0 {
					out = append(out, modelMove{kind: "max-off", member: i})
				}
				if len(st.policy.ratios) > 1 {
					out = append(out, modelMove{kind: "drop-follower", member: i})
				}
			}
			out = append(out, modelMove{kind: "policy-off"}, modelMove{kind: "mode-independent"})
			if w.cfg.members[ai].scaler != scalerExternal && len(st.policy.ratios) == 1 {
				out = append(out, modelMove{kind: "anchor-swap"})
			}
		} else {
			out = append(out, modelMove{kind: "policy-on"})
		}
	}
	if st.losses > 0 {
		for i, ms := range st.members {
			held := w.held(st, i)
			lostNew, lostOld := -1, -1
			for idx, r := range ms.rows {
				if r == rowNew && lostNew < 0 {
					lostNew = idx
				}
				if r == rowOld && !held[idx] && lostOld < 0 {
					lostOld = idx
				}
			}
			if lostNew >= 0 {
				out = append(out, modelMove{kind: "lose", member: i, idx: lostNew})
			}
			if lostOld >= 0 {
				out = append(out, modelMove{kind: "lose", member: i, idx: lostOld})
			}
		}
	}
	if st.restarts > 0 && st.status != nil {
		out = append(out, modelMove{kind: "restart"})
	}
	return out
}

// scaleAnchor is the anchor's own writer moving its count: an external
// scaler or the default HPA writes the replica's count; the projector's
// anchor is moved through spec.<anchor>.minReplicas.
func (w *modelWorld) scaleAnchor(st *modelState, i int, target int32) {
	if w.cfg.members[i].scaler == scalerNone {
		st.members[i].minEdit = int(target)
		return
	}
	st.members[i].ir.Spec.Replicas = ptr.To(target)
}

// threeDecimals is the smallest ratio with at most three decimals that is
// not below a/b.
func threeDecimals(a, b int32) string {
	q := (int64(a)*1000 + int64(b) - 1) / int64(b)
	return fmt.Sprintf("%d.%03d", q/1000, q%1000)
}

// release records that a move stopped the coordinator from writing a
// follower, so the settled state can report what the follower does next.
func (w *modelWorld) release(st *modelState, comp v1beta1.ComponentType) {
	st.released = comp
	st.releasedFrom = st.members[w.memberIndex(comp)].replicas()
}

// apply makes one move and then runs the service's passes until the
// replicas stop changing.
func (w *modelWorld) apply(st *modelState, mv modelMove) (*modelState, []modelFinding) {
	next := st.clone()
	next.path = append(next.path, mv)
	next.restarted, next.released = false, ""
	switch mv.kind {
	case "serve":
		next.members[mv.member].rows[mv.idx] = rowNew
	case "step":
		next.step++
	case "scale", "scale-lag":
		next.scales--
		if mv.kind == "scale-lag" {
			next.lags--
			next.lag = true
		}
		w.scaleAnchor(next, mv.member, next.members[mv.member].replicas()+mv.delta)
	case "ratio":
		next.edits--
		next.policy.ratios[w.cfg.members[mv.member].comp] = mv.text
	case "max-below":
		next.edits--
		next.members[mv.member].maxEdit = int(mv.delta)
	case "max-off":
		next.edits--
		next.members[mv.member].maxEdit = -1
	case "drop-follower":
		next.edits--
		comp := w.cfg.members[mv.member].comp
		w.release(next, comp)
		delete(next.policy.ratios, comp)
	case "policy-off", "mode-independent":
		next.edits--
		for _, m := range w.cfg.members {
			if _, follows := next.policy.ratios[m.comp]; follows {
				w.release(next, m.comp)
			}
		}
		if mv.kind == "policy-off" {
			next.policy.present = false
		} else {
			next.policy = modelPolicy{present: true, mode: v1beta1.ScalingIndependent}
		}
	case "policy-on":
		next.edits--
		next.policy = w.configPolicy()
	case "anchor-swap":
		next.edits--
		old := next.policy.anchor
		var follower v1beta1.ComponentType
		for c := range next.policy.ratios {
			follower = c
		}
		a, f := next.members[w.memberIndex(old)].replicas(), next.members[w.memberIndex(follower)].replicas()
		if f < 1 {
			f = 1
		}
		w.release(next, follower)
		next.policy = modelPolicy{present: true, mode: v1beta1.ScalingProportional, anchor: follower,
			ratios: map[v1beta1.ComponentType]string{old: threeDecimals(a, f)}}
	case "lose":
		next.losses--
		next.members[mv.member].rows[mv.idx] = rowCreating
	case "restart":
		next.restarts--
		next.status = next.statusBefore.DeepCopy()
		next.restarted = true
	}
	return next, w.settle(next)
}

// modelFinding is one reading broken, or one known reading met, at one state.
type modelFinding struct {
	class   string
	message string
	state   *modelState
}

// settle runs passes until a pass rewrites no replica and the stand-in
// replica controllers move nothing, then reads every invariant.
func (w *modelWorld) settle(st *modelState) []modelFinding {
	var out []modelFinding
	for st.settledIn = 1; ; st.settledIn++ {
		res := w.pass(st)
		if res.err != nil {
			out = append(out, modelFinding{class: "pass-error", message: res.err.Error(), state: st})
			return out
		}
		out = append(out, w.checkPass(st, res)...)
		moved := w.replicaControllers(st)
		if !res.changed && !moved {
			break
		}
		if st.settledIn >= maxSettlePasses {
			out = append(out, modelFinding{class: "no-fixpoint", message: fmt.Sprintf("the replicas still change after %d passes", st.settledIn), state: st})
			break
		}
	}
	return append(out, w.checkSettled(st)...)
}

// checkPass reads the invariants that hold after every pass: a replica's
// partition is the canary step's partition at the count it carries, and a
// clamped target is warned about once.
func (w *modelWorld) checkPass(st *modelState, res passResult) []modelFinding {
	var out []modelFinding
	add := func(class, format string, args ...any) {
		out = append(out, modelFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	isvc := w.service(st)
	for i, m := range w.cfg.members {
		ms := st.members[i]
		if ms.ir == nil {
			continue
		}
		want := canary.StepPartition(isvc, rollout.Policies{}, m.comp, ms.replicas())
		got := ms.partition()
		if samePtr(want, got) {
			continue
		}
		before := res.before[i]
		if before.replicas != ms.replicas() && samePtr(canary.StepPartition(isvc, rollout.Policies{}, m.comp, before.replicas), got) {
			add("partition-lags-count", "%s carries %d Instances with partition %s; the step at %d Instances asks for partition %s (the partition of %d Instances)",
				m.comp, ms.replicas(), ptrText(got), ms.replicas(), ptrText(want), before.replicas)
			continue
		}
		add("partition-wrong", "%s carries %d Instances with partition %s; the step at %d Instances asks for partition %s",
			m.comp, ms.replicas(), ptrText(got), ms.replicas(), ptrText(want))
	}
	if res.clamped > 0 && st.restarted {
		add("duplicate-clamped-warning", "ScalingTargetClamped was raised again after the status write of the previous pass was lost")
	}
	return out
}

// checkSettled reads the invariants that hold once the replicas stop
// changing: every follower carries the count the policy asks for, a
// released follower keeps the count it carried, a roll on any member can
// still finish, and the settle took few passes.
func (w *modelWorld) checkSettled(st *modelState) []modelFinding {
	var out []modelFinding
	add := func(class, format string, args ...any) {
		out = append(out, modelFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	reason := readyReason(st.status)
	for i, m := range w.cfg.members {
		want, wanted, ok := w.expected(st, i)
		if !ok {
			continue
		}
		got := st.members[i].replicas()
		if got == want {
			continue
		}
		min, max := w.bounds(st, i)
		anchor := st.members[w.memberIndex(st.policy.anchor)].replicas()
		// A hold the status names is read as that hold; a follower held at its
		// minimum under Applied with no maximum declared is the no-maximum reading.
		switch {
		case reason == "FollowerAutoscalerConflict":
			add("held-by-follower-conflict", "%s carries %d Instances, the ratio asks for %d; every follower is held because one follower has a competing autoscaler (Ready %s)", m.comp, got, want, reason)
		case reason != "" && reason != "Applied":
			add("held-"+reason, "%s carries %d Instances, the ratio asks for %d (Ready %s)", m.comp, got, want, reason)
		case max == 0 && got == int32(min) && wanted > int64(min):
			add("follower-without-a-maximum", "%s follows %s (%d Instances) at %s: the ratio asks for %d, the replica carries %d, held at minReplicas %d because no maxReplicas is declared (Ready %s)",
				m.comp, st.policy.anchor, anchor, st.policy.ratios[m.comp], wanted, got, min, reason)
		default:
			add("count-wrong", "%s carries %d Instances; ceil(%s x %d) inside [%d, %d] is %d (Ready %s)", m.comp, got, st.policy.ratios[m.comp], anchor, min, max, want, reason)
		}
	}
	if st.released != "" {
		i := w.memberIndex(st.released)
		ms, m := st.members[i], w.cfg.members[i]
		min, max := w.bounds(st, i)
		maxText := "unset"
		if max > 0 {
			maxText = fmt.Sprint(max)
		}
		hpaMax := max
		if hpaMax < min {
			hpaMax = min
		}
		add("released-follower", "%s released at %d Instances with minReplicas %d and maxReplicas %s now carries %d Instances and autoscaler %q (the default HPA's bounds would be [%d, %d])",
			m.comp, st.releasedFrom, min, maxText, ms.replicas(), ms.scalerClass(), min, hpaMax)
		if ms.replicas() != st.releasedFrom {
			add("count-wrong", "%s released with autoscaler %q carries %d Instances; a Component that stops following keeps the %d it carried", m.comp, ms.scalerClass(), ms.replicas(), st.releasedFrom)
		}
		if recorded := releasedEntry(st.status, m.comp); (recorded != nil) != (m.scaler == scalerNone && st.releasedFrom != int32(min)) {
			add("released-record-wrong", "%s released with autoscaler %q at %d Instances with minReplicas %d has released record %v; the coordinator records a Component it keeps the count for (autoscaler None above its minReplicas) and nothing else", m.comp, ms.scalerClass(), st.releasedFrom, min, recorded)
		} else if recorded != nil && (recorded.Current != st.releasedFrom || recorded.Target != int32(min)) {
			add("released-record-wrong", "%s released record reads current %d, target %d; want the %d it keeps and the %d its minReplicas names", m.comp, recorded.Current, recorded.Target, st.releasedFrom, min)
		}
	}
	if len(w.progressMoves(st)) == 0 {
		for i, m := range w.cfg.members {
			if !w.rollComplete(st, i) {
				add("waits-forever", "the %s roll cannot finish and nothing can move: %s, step %d of %d", m.comp, st.members[i], st.step, len(w.steps))
			}
		}
	}
	if st.settledIn > settledPassesAllowed {
		add("slow-settle", "%d passes before the replicas stopped changing after %s", st.settledIn, st.path[len(st.path)-1])
	}
	if int(st.step) < len(w.steps) {
		isvc := w.service(st)
		for i, m := range w.cfg.members {
			if !m.roll.canary() || st.members[i].ir == nil {
				continue
			}
			ms := st.members[i]
			p := canary.StepPartition(isvc, rollout.Policies{}, m.comp, ms.replicas())
			if p == nil {
				continue
			}
			if staged, allowed := ms.count(rowNew, rowCreating, rowDrain, rowSurge), ms.replicas()-*p; staged > allowed {
				add("scale-up-creates-on-canary-revision", "%s has %d of %d Instances on or moving to the canary revision at a step that stages %d", m.comp, staged, ms.replicas(), allowed)
			}
		}
	}
	for _, note := range st.notes {
		add("scale-down-removes-staged", "%s", note)
	}
	return out
}

// initial builds the state a run starts from: every replica projected and
// coordinated once at the configured counts, then every rolling member put
// on the old revision with a push toward the target open.
func (w *modelWorld) initial() (*modelState, []modelFinding) {
	st := &modelState{
		members: make([]memberState, len(w.cfg.members)),
		policy:  w.configPolicy(),
		scales:  w.cfg.scales, edits: w.cfg.edits, losses: w.cfg.losses, restarts: w.cfg.restarts, lags: w.cfg.lags,
	}
	if w.cfg.startsWithout {
		st.policy.present = false
	}
	w.settle(st)
	for i, m := range w.cfg.members {
		ms := &st.members[i]
		kind := rowNew
		if m.roll.rolls() {
			kind = rowOld
		}
		for idx := range ms.rows {
			ms.rows[idx] = kind
		}
		ms.ir.Status = w.publish(ms, m)
	}
	w.replicaControllers(st)
	return st, w.checkSettled(st)
}

// modelReport is what one configuration's exploration found.
type modelReport struct {
	states, depth, runs int
	findings            map[string]modelFinding
	counts              map[string]int
}

func (rep *modelReport) record(findings []modelFinding) {
	for _, f := range findings {
		rep.counts[f.class]++
		if _, ok := rep.findings[f.class]; !ok {
			rep.findings[f.class] = f
		}
	}
}

// explore walks every reachable state breadth-first to the bounds given,
// with a hash of visited states.
func (w *modelWorld) explore(maxStates, maxDepth int) modelReport {
	rep := modelReport{findings: map[string]modelFinding{}, counts: map[string]int{}}
	start, findings := w.initial()
	rep.record(findings)
	visited := map[string]bool{start.key(): true}
	queue := []*modelState{start}
	for len(queue) > 0 && rep.states < maxStates {
		st := queue[0]
		queue = queue[1:]
		rep.states++
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
		if len(st.path) >= maxDepth {
			if !w.complete(st) {
				rep.record([]modelFinding{{class: "depth", message: "the run did not end within the depth bound", state: st}})
			}
			continue
		}
		for _, mv := range append(w.progressMoves(st), w.adverseMoves(st)...) {
			child, found := w.apply(st, mv)
			rep.record(found)
			k := child.key()
			if visited[k] {
				continue
			}
			visited[k] = true
			queue = append(queue, child)
		}
	}
	return rep
}

// randomRuns plays whole runs with a fixed seed, favouring the moves that
// finish a roll, to reach the depths the breadth-first walk does not.
func (w *modelWorld) randomRuns(rep *modelReport, seed int64, runs, maxDepth int) {
	rng := rand.New(rand.NewSource(seed))
	for r := 0; r < runs; r++ {
		st, findings := w.initial()
		rep.record(findings)
		rep.runs++
		for depth := 0; ; depth++ {
			progress, adverse := w.progressMoves(st), w.adverseMoves(st)
			if (w.complete(st) && len(adverse) == 0) || (len(progress) == 0 && len(adverse) == 0) {
				break
			}
			if depth >= maxDepth {
				rep.record([]modelFinding{{class: "depth", message: "the run did not end within the depth bound", state: st}})
				break
			}
			var mv modelMove
			if len(progress) == 0 || (len(adverse) > 0 && rng.Intn(4) == 0) {
				mv = adverse[rng.Intn(len(adverse))]
			} else {
				mv = progress[rng.Intn(len(progress))]
			}
			var found []modelFinding
			st, found = w.apply(st, mv)
			rep.record(found)
			if len(st.path) > rep.depth {
				rep.depth = len(st.path)
			}
		}
	}
}

// modelShape is an anchor count with the ratio a follower tracks it at and
// the count the ratio gives at that anchor.
type modelShape struct {
	anchor   int
	ratio    string
	follower int
}

// modelShapes are the anchor-to-follower shapes explored, the reference
// prefill-to-decode shapes among them.
var modelShapes = []modelShape{
	{12, "0.666", 8}, {12, "0.667", 9}, {16, "0.5", 8}, {30, "0.333", 10},
	{8, "1", 8}, {4, "2", 8}, {8, "0.5", 4}, {2, "0.5", 1},
}

var modelRatioChoices = []string{"0.5", "1", "2", "0.666"}

// modelFollowerBounds are the follower's declared bounds; a max of 0 is
// unset.
var modelFollowerBounds = []struct{ min, max int }{{1, 0}, {2, 0}, {1, 16}, {2, 8}, {1, 4}}

var modelRolls = []modelRoll{rollIdle, rollRecreate, rollSurge, rollRecreatePartitioned, rollRecreateCanary, rollSurgeCanary}

// modelConfigs lists the configurations to explore: the reference shapes
// under every roll with an external scaler on the anchor, the projector and
// default-HPA anchors, two followers, a follower with a competing scaler,
// a policy added mid-roll, then a seeded sample over the whole space.
func modelConfigs(full bool) []modelConfig {
	mk := func(shape modelShape, anchorScaler modelScaler, roll modelRoll, fmin, fmax int, router, conflict, without bool) modelConfig {
		anchorRoll := rollIdle
		if roll.canary() {
			anchorRoll = roll
		}
		cfg := modelConfig{
			members: []modelMember{
				{comp: v1beta1.EngineComponent, min: shape.anchor, max: 2 * shape.anchor, scaler: anchorScaler, roll: anchorRoll},
				{comp: v1beta1.DecoderComponent, min: fmin, max: fmax, roll: roll},
			},
			ratios:        map[v1beta1.ComponentType]string{v1beta1.DecoderComponent: shape.ratio},
			ratioChoices:  modelRatioChoices,
			startsWithout: without,
			scales:        2, edits: 1, losses: 1, restarts: 1, lags: 1,
			scaleDeltas: []int32{1, -1, 2, -2},
		}
		if router {
			cfg.members = append(cfg.members, modelMember{comp: v1beta1.RouterComponent, min: 1, max: 8, runtimeScaler: conflict})
			cfg.ratios[v1beta1.RouterComponent] = "0.25"
		}
		cfg.name = cfg.String()
		return cfg
	}
	var out []modelConfig
	for _, shape := range modelShapes[:4] {
		for _, roll := range modelRolls[:5] {
			for _, b := range []struct{ min, max int }{{2, 0}, {1, 16}} {
				out = append(out, mk(shape, scalerExternal, roll, b.min, b.max, false, false, false))
			}
		}
	}
	even := modelShapes[4]
	out = append(out,
		mk(even, scalerNone, rollIdle, 1, 16, false, false, false),
		mk(even, scalerNone, rollRecreateCanary, 1, 16, false, false, false),
		mk(even, scalerDefault, rollIdle, 1, 16, false, false, false),
		mk(even, scalerDefault, rollRecreate, 1, 16, false, false, false),
		mk(modelShapes[0], scalerExternal, rollIdle, 2, 8, true, false, false),
		mk(modelShapes[0], scalerExternal, rollRecreate, 2, 8, true, false, false),
		mk(modelShapes[0], scalerExternal, rollIdle, 2, 8, true, true, false),
		mk(modelShapes[0], scalerExternal, rollRecreate, 2, 16, false, false, true),
		mk(modelShapes[0], scalerExternal, rollRecreateCanary, 2, 16, false, false, true),
		mk(modelShapes[2], scalerExternal, rollSurgeCanary, 1, 16, false, false, false),
	)
	rng := rand.New(rand.NewSource(20261019))
	sample := 30
	if full {
		sample = 100
	}
	scalers := []modelScaler{scalerExternal, scalerExternal, scalerDefault, scalerNone}
	for n := 0; n < sample; n++ {
		shape := modelShapes[rng.Intn(len(modelShapes))]
		b := modelFollowerBounds[rng.Intn(len(modelFollowerBounds))]
		out = append(out, mk(shape, scalers[rng.Intn(len(scalers))], modelRolls[rng.Intn(len(modelRolls))], b.min, b.max,
			rng.Intn(4) == 0, rng.Intn(8) == 0, rng.Intn(5) == 0))
	}
	return out
}

// modelExplore explores every configuration keep admits and reports each
// class of finding with the shortest path to it. count-wrong,
// partition-wrong, waits-forever, no-fixpoint, pass-error,
// follower-without-a-maximum, partition-lags-count and
// released-record-wrong fail the test; held-by-follower-conflict, released-follower,
// scale-up-creates-on-canary-revision, scale-down-removes-staged,
// duplicate-clamped-warning, slow-settle and depth are reported only.
func modelExplore(t *testing.T, keep func(modelConfig) bool) {
	t.Helper()
	full := os.Getenv("OME_SCALING_HUNT") == "full"
	maxStates, maxDepth, runs := 40, 10, 3
	if full {
		maxStates, maxDepth, runs = 80, 16, 5
	}
	var configs []modelConfig
	for _, cfg := range modelConfigs(full) {
		if keep(cfg) {
			configs = append(configs, cfg)
		}
	}
	totals := map[string]int{}
	configsWith := map[string]int{}
	var states, passes, allRuns int
	type example struct {
		cfg     modelConfig
		finding modelFinding
	}
	examples := map[string][]example{}
	for n, cfg := range configs {
		w := newModelWorld(t, cfg)
		rep := w.explore(maxStates, maxDepth)
		w.randomRuns(&rep, 1, runs, 3*maxDepth)
		states += rep.states
		passes += w.passes
		allRuns += rep.runs
		if full {
			var parts []string
			for class, c := range rep.counts {
				parts = append(parts, fmt.Sprintf("%s=%d", class, c))
			}
			sort.Strings(parts)
			t.Logf("config %d/%d [%s]: %d states, depth %d, %d passes: %s", n+1, len(configs), cfg.name, rep.states, rep.depth, w.passes, strings.Join(parts, " "))
		}
		for class, c := range rep.counts {
			totals[class] += c
			configsWith[class]++
		}
		for class, f := range rep.findings {
			if len(examples[class]) < 8 {
				examples[class] = append(examples[class], example{cfg: cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d random runs, %d passes", len(configs), states, allRuns, passes)
	for _, class := range classes {
		t.Logf("%s: %d states across %d configurations", class, totals[class], configsWith[class])
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, modelPath(ex.finding.state), ex.finding.state)
		}
		switch class {
		case "count-wrong", "partition-wrong", "waits-forever", "no-fixpoint", "pass-error", "follower-without-a-maximum", "partition-lags-count", "released-record-wrong":
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg.name, examples[class][0].finding.message)
		}
	}
}

// TestScalingModel_FollowersReachTheirCountBesideAPlainRoll explores every
// configuration without a canary: idle followers and followers under a
// recreate, a surge or a partitioned roll, with the anchor scaled, the
// policy edited and removed, bounds moved, pods lost and status writes
// lost.
func TestScalingModel_FollowersReachTheirCountBesideAPlainRoll(t *testing.T) {
	modelExplore(t, func(c modelConfig) bool {
		for _, m := range c.members {
			if m.roll.canary() {
				return false
			}
		}
		return true
	})
}

// TestScalingModel_FollowersReachTheirCountUnderACanaryStep explores the
// same space with the follower and its anchor rolling under a canary group,
// where a count change must land beside the step's partition.
func TestScalingModel_FollowersReachTheirCountUnderACanaryStep(t *testing.T) {
	modelExplore(t, func(c modelConfig) bool {
		for _, m := range c.members {
			if m.roll.canary() {
				return true
			}
		}
		return false
	})
}

// picture builds a configuration of one anchor and one decoder follower for
// the focused readings below.
func picture(anchor int, anchorScaler modelScaler, ratio string, fmin, fmax int, roll modelRoll, followerScaler modelScaler) modelConfig {
	anchorRoll := rollIdle
	if roll.canary() {
		anchorRoll = roll
	}
	cfg := modelConfig{
		members: []modelMember{
			{comp: v1beta1.EngineComponent, min: anchor, max: 2 * anchor, scaler: anchorScaler, roll: anchorRoll},
			{comp: v1beta1.DecoderComponent, min: fmin, max: fmax, scaler: followerScaler, roll: roll},
		},
		ratios:       map[v1beta1.ComponentType]string{v1beta1.DecoderComponent: ratio},
		ratioChoices: modelRatioChoices,
		scales:       1, edits: 1,
		scaleDeltas: []int32{1},
	}
	cfg.name = cfg.String()
	return cfg
}

// TestScalingModel_AFollowerWithNoMaximumFollowsItsAnchor reads the
// follower that declares minReplicas 2 and no maxReplicas, a common shape,
// under 8 engines at a ratio of 1. No maxReplicas is no upper bound, so the
// follower is written to 8; any other count fails.
func TestScalingModel_AFollowerWithNoMaximumFollowsItsAnchor(t *testing.T) {
	w := newModelWorld(t, picture(8, scalerExternal, "1", 2, 0, rollIdle, scalerDefault))
	st, _ := w.initial()
	decoder := st.members[1]
	status := st.status
	if decoder.replicas() != 8 {
		t.Fatalf("the decoder carries %d Instances under 8 engines with no maxReplicas declared; want 8 (Ready %s)", decoder.replicas(), readyReason(status))
	}
	t.Logf("the decoder follows 8 engines to 8 with no maxReplicas declared (Ready %s)", readyReason(status))
}

// TestScalingModel_AFollowerCountAndItsCanaryPartitionLandWithinOnePass
// doubles the anchor from 8 to 16 under a 25 percent step. The anchor's
// replica carries 16 and partition 12 in one write. The decoder's replica
// carries 16 with partition 12 in the same pass, since the coordinator
// resolves the count before the projector runs, and the partition still
// reads 12 after one more pass.
func TestScalingModel_AFollowerCountAndItsCanaryPartitionLandWithinOnePass(t *testing.T) {
	w := newModelWorld(t, picture(8, scalerExternal, "1", 1, 16, rollRecreateCanary, scalerDefault))
	st, _ := w.initial()
	if got := ptrText(st.members[1].partition()); got != "6" || st.members[1].replicas() != 8 {
		t.Fatalf("before the scale the decoder carries %d Instances with partition %s; want 8 with partition 6", st.members[1].replicas(), got)
	}
	w.scaleAnchor(st, 0, 16)
	if res := w.pass(st); res.err != nil {
		t.Fatal(res.err)
	}
	engine, decoder := st.members[0], st.members[1]
	if engine.replicas() != 16 || ptrText(engine.partition()) != "12" {
		t.Fatalf("the engine carries %d Instances with partition %s; want 16 with partition 12 in one write", engine.replicas(), ptrText(engine.partition()))
	}
	if replicas, partition := decoder.replicas(), ptrText(decoder.partition()); replicas != 16 || partition != "12" {
		t.Fatalf("the decoder carries %d Instances with partition %s; want 16 with partition 12 in the pass that saw the anchor move", replicas, partition)
	}
	t.Log("the decoder carries 16 Instances with partition 12 in the pass that saw the anchor move")
	w.replicaControllers(st)
	if res := w.pass(st); res.err != nil {
		t.Fatal(res.err)
	}
	decoder = st.members[1]
	if decoder.replicas() != 16 || ptrText(decoder.partition()) != "12" {
		t.Fatalf("one pass later the decoder carries %d Instances with partition %s; want 16 with partition 12", decoder.replicas(), ptrText(decoder.partition()))
	}
}

// TestScalingModel_AReleasedFollowerKeepsItsCount: 18 engines, the decoder
// following at 0.666 to 12 with minReplicas 2 and maxReplicas 16; the policy
// is removed. A decoder with autoscaler None keeps 12: the coordinator
// records it as released, with the 12 it keeps and the 2 its minReplicas
// names, and the projector keeps the count. A decoder with no autoscaler
// block keeps 12 under the default HPA, whose bounds are the declared
// [2, 16]; nothing is recorded.
func TestScalingModel_AReleasedFollowerKeepsItsCount(t *testing.T) {
	for _, tc := range []struct {
		name     string
		scaler   modelScaler
		class    v1beta1.AutoscalerClass
		recorded bool
	}{
		{name: "autoscaler None", scaler: scalerNone, class: v1beta1.AutoscalerNone, recorded: true},
		{name: "no autoscaler block", scaler: scalerDefault, class: v1beta1.AutoscalerHPA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newModelWorld(t, picture(18, scalerExternal, "0.666", 2, 16, rollIdle, tc.scaler))
			st, _ := w.initial()
			if st.members[1].replicas() != 12 {
				t.Fatalf("the decoder carries %d Instances under 18 engines at 0.666; want 12", st.members[1].replicas())
			}
			next, findings := w.apply(st, modelMove{kind: "policy-off"})
			decoder := next.members[1]
			if decoder.replicas() != 12 || decoder.scalerClass() != tc.class {
				t.Fatalf("after the policy is removed the decoder carries %d Instances with autoscaler %q; want 12 kept with %q", decoder.replicas(), decoder.scalerClass(), tc.class)
			}
			if next.settledIn > settledPassesAllowed {
				t.Fatalf("the release took %d passes to settle", next.settledIn)
			}
			record := releasedEntry(next.status, v1beta1.DecoderComponent)
			switch {
			case tc.recorded && (record == nil || record.Current != 12 || record.Target != 2 || readyReason(next.status) != "Released"):
				t.Fatalf("status = %+v; want the decoder recorded as released, keeping 12 with minReplicas 2, under Ready/Released", next.status)
			case !tc.recorded && next.status != nil:
				t.Fatalf("status = %+v; want no block: the decoder's autoscaler owns its count", next.status)
			}
			for _, f := range findings {
				if f.class == "count-wrong" || f.class == "released-record-wrong" {
					t.Fatal(f.message)
				}
				t.Logf("%s: %s", f.class, f.message)
			}
		})
	}
}

// TestScalingModel_AnAnchorTheProjectorScalesIsFollowedWithinTwoPasses
// moves an anchor with no autoscaler, whose count the projector writes from
// spec.engine.minReplicas, from 8 to 16. The coordinator reads the anchor
// before the projectors run, so the decoder stands at 8 for the pass that
// moved the anchor and carries 16 one pass later.
func TestScalingModel_AnAnchorTheProjectorScalesIsFollowedWithinTwoPasses(t *testing.T) {
	w := newModelWorld(t, picture(8, scalerNone, "1", 1, 16, rollIdle, scalerDefault))
	st, _ := w.initial()
	w.scaleAnchor(st, 0, 16)
	if res := w.pass(st); res.err != nil {
		t.Fatal(res.err)
	}
	if st.members[0].replicas() != 16 {
		t.Fatalf("the engine carries %d Instances after its minReplicas moved to 16", st.members[0].replicas())
	}
	if st.members[1].replicas() != 8 {
		t.Fatalf("the decoder carries %d Instances in the pass that moved the anchor; want 8 until the next pass (Ready %s)", st.members[1].replicas(), readyReason(st.status))
	}
	t.Logf("the decoder stands at 8 for the pass that moved the anchor (Ready %s)", readyReason(st.status))
	w.replicaControllers(st)
	if res := w.pass(st); res.err != nil {
		t.Fatal(res.err)
	}
	if st.members[1].replicas() != 16 {
		t.Fatalf("two passes after the anchor moved to 16 the decoder carries %d Instances", st.members[1].replicas())
	}
}

// TestScalingModel_OneFollowerWithACompetingScalerHoldsEveryFollower
// records the reading that one follower whose runtime declares an HPA
// holds the other follower too: 12 engines, the decoder at 0.666 and the
// router at 0.25 with a runtime HPA; the decoder stays at its minimum 2
// instead of 8 and the status says FollowerAutoscalerConflict.
func TestScalingModel_OneFollowerWithACompetingScalerHoldsEveryFollower(t *testing.T) {
	cfg := picture(12, scalerExternal, "0.666", 2, 16, rollIdle, scalerDefault)
	cfg.members = append(cfg.members, modelMember{comp: v1beta1.RouterComponent, min: 1, max: 8, runtimeScaler: true})
	cfg.ratios[v1beta1.RouterComponent] = "0.25"
	cfg.name = cfg.String()
	w := newModelWorld(t, cfg)
	st, findings := w.initial()
	if reason := readyReason(st.status); reason != "FollowerAutoscalerConflict" {
		t.Fatalf("Ready reason %q; want FollowerAutoscalerConflict", reason)
	}
	if st.members[1].replicas() != 2 {
		t.Fatalf("the decoder carries %d Instances; the known reading holds it at its minimum 2 while the router's scaler conflicts", st.members[1].replicas())
	}
	for _, f := range findings {
		if f.class == "count-wrong" {
			t.Fatal(f.message)
		}
		t.Logf("%s: %s", f.class, f.message)
	}
}

// TestScalingModel_SwappingAnchorAndFollowerHandsTheNewAnchorToTheDefaultHPA
// records what an edit that makes the follower the anchor does: 8 engines
// with no autoscaler (the projector writes their count) and 8 decoders
// following at 1. After the swap the engine follows the decoder at 1 and
// both stay at 8, and the decoder's replica carries the default HPA again,
// whose bounds are its declared [1, 16]; from here the HPA, not the
// operator's spec, decides the pair's size. An anchor with an External
// autoscaler cannot be swapped, because admission requires a follower's
// autoscaler to be None.
func TestScalingModel_SwappingAnchorAndFollowerHandsTheNewAnchorToTheDefaultHPA(t *testing.T) {
	w := newModelWorld(t, picture(8, scalerNone, "1", 1, 16, rollIdle, scalerDefault))
	st, _ := w.initial()
	if st.members[1].scalerClass() != v1beta1.AutoscalerNone || st.members[1].replicas() != 8 {
		t.Fatalf("before the swap the decoder carries %d Instances with autoscaler %q; want 8 with None", st.members[1].replicas(), st.members[1].scalerClass())
	}
	next, findings := w.apply(st, modelMove{kind: "anchor-swap"})
	for _, f := range findings {
		if f.class == "count-wrong" || f.class == "no-fixpoint" {
			t.Fatal(f.message)
		}
		t.Logf("%s: %s", f.class, f.message)
	}
	engine, decoder := next.members[0], next.members[1]
	if engine.replicas() != 8 || decoder.replicas() != 8 {
		t.Fatalf("after the swap the engine carries %d and the decoder %d Instances; want 8 and 8", engine.replicas(), decoder.replicas())
	}
	if decoder.scalerClass() != v1beta1.AutoscalerHPA || engine.scalerClass() != v1beta1.AutoscalerNone {
		t.Fatalf("after the swap the decoder carries autoscaler %q and the engine %q; want HPA and None", decoder.scalerClass(), engine.scalerClass())
	}
	if reason := readyReason(next.status); reason != "Applied" {
		t.Fatalf("Ready reason %q after the swap; want Applied", reason)
	}
	t.Logf("the swap settled in %d passes with policy %s", next.settledIn, next.policy)
}
