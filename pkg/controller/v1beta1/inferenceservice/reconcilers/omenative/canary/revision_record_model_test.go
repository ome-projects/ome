package canary

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The revision model drives the revision record of a service, the three
// fields latestReadyRevision, latestRolledoutRevision and
// previousRolledoutRevision of each Component, against the replica's
// revision pair and its ControllerRevision history, through the production
// code that writes and reads them: the run layer's record of each target
// and stable (rolloutrun.Reconcile and its stable chooser), the canary executor's four writers (BindRun and
// Dispatch), coordination's traffic pass for the Components no canary owns
// (coordination.Reconcile), the replica's current-revision rule
// (workloadstatus.CurrentRevisionFor), the retention sweep
// (revision.RetainControllerRevisions), the replica's rollback-target rule
// (revision.RetainedBy under the service's history scope) and the
// projector's target for a replica projected again (StableSideTarget).
//
// It is the canary world with a history: each member's ControllerRevisions
// with their numbers and owners, its replica identity, and four moves of
// its own beside the canary world's: a push back to the previous revision,
// the sweep collecting old revisions under a history limit, a replica
// deleted and projected again, and a pass that decides from a stale copy of
// the record. At every state it names the record's shape with one of
// seven record states (Empty, Settled, Behind, Ahead, Pinned/Stable,
// Pinned/NoStable, Pinned/Disagrees) and asks: previous never equals
// latest; two writers never alternate with no move between passes; while a
// run is open the dispatch reads the run's stable; a rollback that stops
// for want of a stable revision has none in the history; a replica
// projected again under a standing rollback comes back on the stable
// revision; at rest the record names the revision every Instance runs; and
// the canary world's own questions, every hold with a way out among them.
// OME_REVISION_HUNT=full widens the shapes and the bounds;
// OME_REVISION_HUNT_ONLY keeps the configurations whose name contains it;
// OME_REVISION_HUNT_WORKERS sets the lanes.

// gateExtension is what another model adds to the canary world: the state
// it starts from, the replica rollup, the objects a state holds, the
// revision a replica's rollback target resolves to, what runs after the
// pass, its moves, how they apply, and its checks. Nil for the canary model.
type gateExtension struct {
	initial        func(w *gateWorld, st *gateState)
	rollup         func(w *gateWorld, st *gateState)
	objects        func(w *gateWorld, st *gateState) []runtime.Object
	rollbackTarget func(w *gateWorld, st *gateState, i int) string
	afterPass      func(w *gateWorld, st *gateState, c client.Client, isvc *v1beta1.InferenceService, policies rollout.Policies)
	moves          func(w *gateWorld, st *gateState) []gateMove
	apply          func(w *gateWorld, next *gateState, mv gateMove) bool
	check          func(w *gateWorld, st *gateState) []gateFinding
	// fold renames a finding to the known finding it falls under at its
	// state, so the counts carry the known name; empty keeps the class.
	fold func(w *gateWorld, f gateFinding) string
}

// historyRevision is one ControllerRevision of a member's history: its
// hash, its number, the replica that minted it and whether the service owns
// it beside that replica.
type historyRevision struct {
	hash         string
	number       int64
	minter       types.UID
	serviceOwned bool
}

// historyMember is one member's history and replica identity, with the
// record copies the stale-copy move and the alternation check read.
type historyMember struct {
	revisions  []historyRevision
	replicaUID types.UID
	// retiredCurrent is the revision the replica retired its current from
	// (the current revision read as empty), as the rollup carries it between
	// passes.
	retiredCurrent string
	// last is the member's three revision fields as the last pass left
	// them; previous is the same at the end of the previous state, the
	// copy a stale pass writes back.
	last, previous [3]string
	// recent are the rolled-out revisions the passes since the last move
	// wrote, oldest first.
	recent      []string
	reprojected bool
	// createdToward is the revision the replica projected again was created
	// toward; rejectedThen the revision a standing rollback rejected at that
	// moment, empty when none stood.
	createdToward, rejectedThen string
	// builtOffTheStableSide marks a replica projected again that the
	// projection pointed at a stable revision the new replica could not
	// read as its own (a history minted before the service owned it), so
	// its Instances were built toward the spec's revision instead.
	builtOffTheStableSide bool
	// builtAtTheFinalStep marks a replica projected again while its unit
	// stood at its final step, which keeps no stable side, so
	// its Instances were built toward the spec's revision.
	builtAtTheFinalStep bool
}

func (m *historyMember) find(hash string) *historyRevision {
	for i := range m.revisions {
		if m.revisions[i].hash == hash {
			return &m.revisions[i]
		}
	}
	return nil
}

func (m *historyMember) number(hash string) int64 {
	if rev := m.find(hash); rev != nil {
		return rev.number
	}
	return 0
}

// gateHistory is the revision model's part of a state: every member's
// history, the history limit, and the budgets of the model's own moves.
type gateHistory struct {
	members                                    []historyMember
	limit                                      int
	pushbacks, sweeps, reprojects, staleCopies int8
	recentPath                                 int
}

func (h *gateHistory) clone() *gateHistory {
	if h == nil {
		return nil
	}
	out := *h
	out.members = make([]historyMember, len(h.members))
	for i, m := range h.members {
		m.revisions = append([]historyRevision(nil), m.revisions...)
		m.recent = append([]string(nil), m.recent...)
		out.members[i] = m
	}
	return &out
}

// key is the history's part of the state key: the revisions each member's
// history holds with their owners, its replica identity, the three record
// fields of every Component, and the budgets left.
func (h *gateHistory) key(st *gateState) string {
	var b strings.Builder
	for i, m := range h.members {
		fmt.Fprintf(&b, "h%d:%s:", i, m.replicaUID)
		for _, r := range m.revisions {
			fmt.Fprintf(&b, "%s/%d/%t,", r.hash, r.number, r.serviceOwned)
		}
		fmt.Fprintf(&b, ":w%s;", m.retiredCurrent)
	}
	comps := make([]string, 0, len(st.isvc.Status.Components))
	for c := range st.isvc.Status.Components {
		comps = append(comps, string(c))
	}
	sort.Strings(comps)
	for _, c := range comps {
		cs := st.isvc.Status.Components[v1beta1.ComponentType(c)]
		fmt.Fprintf(&b, "%s:%s/%s/%s;", c, cs.LatestReadyRevision, cs.LatestRolledoutRevision, cs.PreviousRolledoutRevision)
	}
	fmt.Fprintf(&b, "pb%d sw%d rp%d sc%d;", h.pushbacks, h.sweeps, h.reprojects, h.staleCopies)
	return b.String()
}

// mint adds the revision a member's spec names to its history, as the
// replica does when its template changes, numbered after the newest; a
// revision minted now is the service's as well as the replica's.
func (h *gateHistory) mint(st *gateState) {
	for i := range st.members {
		m := &h.members[i]
		if m.find(st.members[i].updateRev) != nil {
			continue
		}
		var newest int64
		for _, r := range m.revisions {
			if r.number > newest {
				newest = r.number
			}
		}
		m.revisions = append(m.revisions, historyRevision{hash: st.members[i].updateRev, number: newest + 1, minter: m.replicaUID, serviceOwned: true})
	}
}

// revisionShape is a reference shape: one engine, or an engine beside a
// decoder.
type revisionShape struct {
	name            string
	engine, decoder int32
}

var revisionShapes = []revisionShape{
	{"engine4", 4, 0},
	{"engine8", 8, 0},
	{"pd6:2", 6, 2},
	{"pd12:8", 12, 8},
}

// revisionUnits are the units a configuration rolls under: one rollingUpdate
// group with no canary, so coordination owns the records, or a canary step
// list, so the executor does.
var revisionUnits = []string{"rolling", "one-timed", "two-manual"}

// revisionConfig is one configuration of the revision model.
type revisionConfig struct {
	shape    revisionShape
	gang     int32
	strategy v1beta1.UpdateStrategyType
	unit     string
	// limit is the history limit the sweep keeps; -1 leaves the sweep
	// unconfigured, as a cluster with no lifecycle.revisionHistoryLimit.
	limit int
	// firstRevisionReplicaOnly marks the first revision as minted before the
	// service owned a projected replica's history, so only the minting
	// replica controls it.
	firstRevisionReplicaOnly bool
	full                     bool
}

func (c revisionConfig) name() string {
	strategy := "recreate"
	if c.strategy == v1beta1.UpdateStrategySurgeThenDrain {
		strategy = "surge"
	}
	s := fmt.Sprintf("%s/gang%d/%s/%s/limit%d", c.shape.name, c.gang, strategy, c.unit, c.limit)
	if c.firstRevisionReplicaOnly {
		s += "/first-replica-only"
	}
	return s
}

// gate is the canary world's configuration for this one: the members at a
// 10 percent budget, the unit, and the budgets of the world's own
// moves.
func (c revisionConfig) gate() gateConfig {
	cfg := gateConfig{stepsName: c.unit, rolling: c.unit == "rolling"}
	if !cfg.rolling {
		cfg.steps = gateStepList(c.unit)
	}
	members := []gateMember{{comp: v1beta1.EngineComponent, desired: c.shape.engine, gang: c.gang}}
	if c.shape.decoder > 0 {
		members = append(members, gateMember{comp: v1beta1.DecoderComponent, desired: c.shape.decoder, gang: c.gang})
	}
	for _, m := range members {
		m.strategy, m.budget = c.strategy, "10%"
		cfg.members = append(cfg.members, m)
	}
	cfg.budgets = gateBudgets{lostPods: 1, lostFlushes: 1, pushes: 1, rollbacks: 1, resumes: 1, pauses: 1}
	if c.full {
		cfg.budgets.scales, cfg.budgets.lostPods = 1, 2
	}
	cfg.name = c.name() + " " + cfg.String()
	return cfg
}

// revisionConfigs lists the configurations: in the bounded mode six that
// cover both owners, both strategies, a gang, every history limit and a
// history whose first revision only its replica controls; in the full mode
// every shape, gang, strategy, unit and history ownership, with the history
// limits spread over them.
func revisionConfigs(full bool) []revisionConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	if !full {
		return []revisionConfig{
			{shape: revisionShapes[0], gang: 1, strategy: recreate, unit: "rolling", limit: 1},
			{shape: revisionShapes[0], gang: 1, strategy: surge, unit: "one-timed", limit: -1},
			{shape: revisionShapes[2], gang: 1, strategy: recreate, unit: "two-manual", limit: 1},
			{shape: revisionShapes[2], gang: 2, strategy: surge, unit: "rolling", limit: 0},
			{shape: revisionShapes[0], gang: 1, strategy: recreate, unit: "one-timed", limit: 0, firstRevisionReplicaOnly: true},
			{shape: revisionShapes[0], gang: 4, strategy: recreate, unit: "rolling", limit: -1},
		}
	}
	limits := []int{-1, 0, 1, 2}
	var out []revisionConfig
	n := 0
	for _, shape := range revisionShapes {
		for _, gang := range []int32{1, 2, 4, 8} {
			for _, strategy := range []v1beta1.UpdateStrategyType{recreate, surge} {
				for _, unit := range revisionUnits {
					for _, replicaOnly := range []bool{false, true} {
						out = append(out, revisionConfig{shape: shape, gang: gang, strategy: strategy, unit: unit, limit: limits[n%len(limits)], firstRevisionReplicaOnly: replicaOnly, full: true})
						n++
					}
				}
			}
		}
	}
	return out
}

const revisionServiceUID = types.UID("svc-service-uid")

var revisionServiceKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// revisionModel is the extension of one world; asked counts the questions
// its checks put to the states.
type revisionModel struct {
	cfg   revisionConfig
	asked int
}

func newRevisionWorld(t *testing.T, cfg revisionConfig, scheme *runtime.Scheme) (*gateWorld, *revisionModel) {
	w := newGateWorld(t, cfg.gate(), scheme)
	m := &revisionModel{cfg: cfg}
	w.ext = &gateExtension{
		initial: m.initial, rollup: m.rollup, objects: m.objects, rollbackTarget: m.rollbackTarget,
		afterPass: m.afterPass, moves: m.moves, apply: m.apply, check: m.check, fold: revisionKnownFindingFor,
	}
	return w, m
}

func revisionReplicaUID(w *gateWorld, i int, generation int) types.UID {
	return types.UID(fmt.Sprintf("%s-%s-uid-%d", gateService, w.cfg.members[i].comp, generation))
}

// initial gives the service its identity and every member a history of one
// revision, the one its Instances serve, minted by its replica.
func (m *revisionModel) initial(w *gateWorld, st *gateState) {
	st.isvc.UID = revisionServiceUID
	h := &gateHistory{limit: m.cfg.limit, pushbacks: 1, sweeps: 1, reprojects: 1, staleCopies: 1}
	if m.cfg.full {
		h.pushbacks, h.sweeps = 2, 2
	}
	for i := range w.cfg.members {
		uid := revisionReplicaUID(w, i, 0)
		h.members = append(h.members, historyMember{
			replicaUID: uid,
			revisions:  []historyRevision{{hash: gateRevisions[0], number: 1, minter: uid, serviceOwned: !m.cfg.firstRevisionReplicaOnly}},
		})
	}
	st.history = h
}

// rollup publishes each member's current revision by the replica's own
// rule: promoted to the update revision once every Instance is Ready on
// it, read as empty while an Instance is off a current that equals the
// update revision, restored once the fleet is back on it.
func (m *revisionModel) rollup(w *gateWorld, st *gateState) {
	for i, cfg := range w.cfg.members {
		ms := &st.members[i]
		hm := &st.history.members[i]
		// The workload drops a surge whose target came back to the revision
		// the Instance's old pods run: the replacement is deleted and the
		// Instance is Ready where it stands.
		for idx := range ms.rows {
			if r := &ms.rows[idx]; r.kind == gateRowSurging && r.target == r.rev {
				r.kind, r.target = gateRowServing, ""
			}
		}
		r := w.replica(st, i)
		insts := v1beta1convert.InstanceStatusSliceToWorkload(r.Status.InstanceStatuses)
		byInstance := map[int32][]*corev1.Pod{}
		for idx, row := range ms.rows {
			for _, obj := range w.pods(i, idx, row) {
				if pod, ok := obj.(*corev1.Pod); ok {
					byInstance[int32(idx)] = append(byInstance[int32(idx)], pod)
				}
			}
		}
		current := ""
		if ms.current != "" {
			current = gateRevisionName(cfg.comp, ms.current)
		}
		update := gateRevisionName(cfg.comp, ms.updateRev)
		next := workloadstatus.CurrentRevisionFor(insts, current, update, nil, hm.retiredCurrent, workloadstatus.PodRevisionsByInstance(byInstance))
		hm.retiredCurrent = workloadstatus.WithdrawnRevisionAfter(hm.retiredCurrent, current, next, update)
		ms.current = query.RevisionFromName(next).Hash()
	}
}

// objects are the API objects of a state: the service, each member's
// replica under the service's control, its pods, an EndpointSlice per
// revision listing the Ready pods behind that revision's Service, and the
// ControllerRevisions its history holds.
func (m *revisionModel) objects(w *gateWorld, st *gateState) []runtime.Object {
	h := st.history
	h.mint(st)
	objs := []runtime.Object{st.isvc.DeepCopy()}
	for i, cfg := range w.cfg.members {
		hm := h.members[i]
		r := w.replica(st, i)
		r.UID = hm.replicaUID
		r.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(st.isvc, revisionServiceKind)}
		objs = append(objs, r)
		prefix := irprojector.RoleReplicaPrefix(st.isvc, cfg.comp)
		listed := map[string][]*corev1.Pod{}
		for idx, row := range st.members[i].rows {
			for _, obj := range w.pods(i, idx, row) {
				objs = append(objs, obj)
				if pod, ok := obj.(*corev1.Pod); ok && revisionPodReady(pod) {
					hash := pod.Labels[query.LabelRevisionHash]
					listed[hash] = append(listed[hash], pod)
				}
			}
		}
		hashes := make([]string, 0, len(listed))
		for hash := range listed {
			hashes = append(hashes, hash)
		}
		sort.Strings(hashes)
		for _, hash := range hashes {
			objs = append(objs, revisionEndpointSlice(st.isvc.Namespace, coordination.PerRevisionServiceName(prefix, cfg.comp, hash), listed[hash]))
		}
		for _, rev := range hm.revisions {
			objs = append(objs, revisionControllerRevision(st.isvc, cfg.comp, rev))
		}
	}
	return objs
}

func revisionPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// revisionEndpointSlice lists pods as Ready, serving endpoints of a Service,
// as the endpoints controller publishes them.
func revisionEndpointSlice(ns, service string, pods []*corev1.Pod) *discoveryv1.EndpointSlice {
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: ns, Name: service + "-endpoints", Labels: map[string]string{discoveryv1.LabelServiceName: service}},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	for n, pod := range pods {
		slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{fmt.Sprintf("10.0.%d.%d", n/250, n%250+1)},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true), Serving: ptr.To(true), Terminating: ptr.To(false)},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
		})
	}
	return slice
}

// revisionControllerRevision is one revision of a member's history: the
// minting replica controls it, and the service owns it beside the replica
// when it was minted under the service's scope.
func revisionControllerRevision(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, rev historyRevision) *appsv1.ControllerRevision {
	cr := canaryControllerRevision(isvc.Namespace, isvc.Name, string(comp), rev.hash, rev.number)
	replica := &metav1.ObjectMeta{Name: isvc.Name + "-" + string(comp), UID: rev.minter}
	cr.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(replica, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))}
	if rev.serviceOwned {
		cr.OwnerReferences = append(cr.OwnerReferences, metav1.OwnerReference{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: isvc.Name, UID: isvc.UID})
	}
	return cr
}

// scope is the history scope of a member's replica: the service that
// controls it.
func (m *revisionModel) scope(st *gateState) *metav1.OwnerReference {
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(st.isvc, revisionServiceKind)}}}
	return irprojector.HistoryScope(ir)
}

// retained reports whether a member's replica reads a revision as its own:
// it controls it, or its service owns it.
func (m *revisionModel) retained(st *gateState, comp v1beta1.ComponentType, hm *historyMember, rev historyRevision) bool {
	return revision.RetainedBy(revisionControllerRevision(st.isvc, comp, rev), hm.replicaUID, m.scope(st))
}

// rollbackTarget is the revision a member's workload builds toward under
// the rollback target its replica carries, by the replica's rule: a
// revision of its history; one missing or another owner's leaves it on the
// spec.
func (m *revisionModel) rollbackTarget(w *gateWorld, st *gateState, i int) string {
	hash := query.RevisionFromName(st.members[i].rollbackTo).Hash()
	hm := &st.history.members[i]
	rev := hm.find(hash)
	if rev == nil || !m.retained(st, w.cfg.members[i].comp, hm, *rev) {
		return ""
	}
	return hash
}

// afterPass runs coordination's pass, which writes the records of the
// Components no canary owns, then keeps the history's bookkeeping: a
// rollback target the replica controls gains the service as an owner, and
// the record copies move on.
func (m *revisionModel) afterPass(w *gateWorld, st *gateState, c client.Client, isvc *v1beta1.InferenceService, policies rollout.Policies) {
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{}
	for _, mc := range w.cfg.members {
		modes[mc.comp] = constants.OMENative
	}
	events := &gateEvents{}
	if _, err := coordination.Reconcile(w.ctx, coordination.ReconcileInputs{
		Client: c, Reader: c, Recorder: events, ISVC: isvc, Policies: policies,
		ComponentDeploymentModes: modes, ComponentRunnerPorts: canaryRunnerPorts(), Now: st.now,
	}); err != nil {
		w.fail("coordination: %v (state %s)", err, st)
	}
	st.passEvents = append(st.passEvents, events.messages...)
	h := st.history
	moved := h.recentPath != len(st.path)
	h.recentPath = len(st.path)
	for i, mc := range w.cfg.members {
		hm := &h.members[i]
		if hash := query.RevisionFromName(st.members[i].rollbackTo).Hash(); hash != "" {
			if rev := hm.find(hash); rev != nil && rev.minter == hm.replicaUID {
				rev.serviceOwned = true
			}
		}
		rec := revisionRecordOf(isvc, mc.comp)
		if moved {
			hm.previous, hm.recent = hm.last, nil
		}
		hm.last = rec
		hm.recent = append(hm.recent, rec[1])
		if len(hm.recent) > 6 {
			hm.recent = hm.recent[1:]
		}
	}
}

// revisionRecordOf is one Component's three revision fields: ready,
// rolled out, previous.
func revisionRecordOf(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) [3]string {
	cs := isvc.Status.Components[comp]
	return [3]string{cs.LatestReadyRevision, cs.LatestRolledoutRevision, cs.PreviousRolledoutRevision}
}

// pushbackTarget is the revision a push back goes to: the one the fleet
// still runs while a roll is open, else the newest revision of the history
// below the spec's.
func (m *revisionModel) pushbackTarget(st *gateState) string {
	ms := st.members[0]
	if ms.current != "" && ms.current != ms.updateRev {
		return ms.current
	}
	hm := &st.history.members[0]
	spec := hm.number(ms.updateRev)
	var best historyRevision
	for _, rev := range hm.revisions {
		if rev.number < spec && rev.number > best.number {
			best = rev
		}
	}
	return best.hash
}

// sweepKey is the retention sweep's key for a member: the labels every
// ControllerRevision of the member carries.
func (m *revisionModel) sweepKey(st *gateState, comp v1beta1.ComponentType) revision.Key {
	return revision.Key{
		Namespace: st.isvc.Namespace,
		Name:      st.isvc.Name + "-" + string(comp),
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: st.isvc.Name,
			constants.OMEComponentLabel:           string(comp),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// liveNames are the revision names a member's sweep keeps: its current and
// update revisions and every row's running and target revision.
func (m *revisionModel) liveNames(w *gateWorld, st *gateState, i int) []string {
	r := w.replica(st, i)
	return revision.LiveRevisionNames(r.Status.CurrentRevision, r.Status.UpdateRevision, revision.RowRevisions(v1beta1convert.InstanceStatusSliceToWorkload(r.Status.InstanceStatuses)))
}

// sweepWouldCollect reports whether a sweep now would delete a revision of
// some member: more revisions off the live names than the limit keeps.
func (m *revisionModel) sweepWouldCollect(w *gateWorld, st *gateState) bool {
	for i, mc := range w.cfg.members {
		live := map[string]bool{}
		for _, name := range m.liveNames(w, st, i) {
			live[name] = true
		}
		off := 0
		for _, rev := range st.history.members[i].revisions {
			if !live[gateRevisionName(mc.comp, rev.hash)] {
				off++
			}
		}
		if off > st.history.limit {
			return true
		}
	}
	return false
}

// sweep runs the retention sweep of every member over its history and
// drops the revisions it deleted.
func (m *revisionModel) sweep(w *gateWorld, st *gateState) {
	for i, mc := range w.cfg.members {
		hm := &st.history.members[i]
		var objs []runtime.Object
		for _, rev := range hm.revisions {
			objs = append(objs, revisionControllerRevision(st.isvc, mc.comp, rev))
		}
		c := fake.NewClientBuilder().WithScheme(w.scheme).WithRuntimeObjects(objs...).Build()
		if err := revision.RetainControllerRevisions(w.ctx, c, c, m.sweepKey(st, mc.comp), st.history.limit, m.liveNames(w, st, i)...); err != nil {
			w.fail("sweep: %v", err)
		}
		list := &appsv1.ControllerRevisionList{}
		if err := c.List(w.ctx, list, client.InNamespace(st.isvc.Namespace)); err != nil {
			w.fail("list after the sweep: %v", err)
		}
		left := map[string]bool{}
		for i := range list.Items {
			left[query.RevisionOf(&list.Items[i]).Hash()] = true
		}
		kept := make([]historyRevision, 0, len(hm.revisions))
		for _, rev := range hm.revisions {
			if left[rev.hash] {
				kept = append(kept, rev)
			}
		}
		hm.revisions = kept
	}
}

// moves are the model's own: a push back, a sweep that would collect
// something, each member's replica projected again, and a pass over a
// stale copy of a record that has since moved.
func (m *revisionModel) moves(w *gateWorld, st *gateState) []gateMove {
	h := st.history
	var out []gateMove
	if h.pushbacks > 0 && m.pushbackTarget(st) != "" {
		out = append(out, gateMove{kind: "pushback"})
	}
	if h.limit >= 0 && h.sweeps > 0 && m.sweepWouldCollect(w, st) {
		out = append(out, gateMove{kind: "sweep"})
	}
	if h.reprojects > 0 {
		for i := range st.members {
			out = append(out, gateMove{kind: "reproject", member: i})
		}
	}
	if h.staleCopies > 0 {
		for _, hm := range h.members {
			if hm.previous != hm.last && hm.previous[1] != "" {
				out = append(out, gateMove{kind: "stalecopy"})
				break
			}
		}
	}
	return out
}

func (m *revisionModel) apply(w *gateWorld, next *gateState, mv gateMove) bool {
	h := next.history
	switch mv.kind {
	case "pushback":
		// The workload reads the spec before the controller's next pass: a
		// surge toward the old target whose old pods run the restored
		// revision is dropped at once, and the Instance is Ready where it
		// stands; a rebuild in flight turns toward the restored revision.
		h.pushbacks--
		hash := m.pushbackTarget(next)
		for i := range next.members {
			ms := &next.members[i]
			ms.updateRev = hash
			ms.generation++
			for idx := range ms.rows {
				if r := &ms.rows[idx]; r.kind == gateRowSurging && r.rev == hash {
					r.kind, r.target = gateRowServing, ""
				}
			}
		}
		next.isvc.Generation++
	case "sweep":
		h.sweeps--
		m.sweep(w, next)
	case "reproject":
		// The operator deletes the replica: the garbage collector keeps
		// the revisions the service owns and removes the rest; the
		// projector creates the replica again, pointed at the standing
		// rollback target when one holds the Component, else at the spec.
		h.reprojects--
		i := mv.member
		hm := &h.members[i]
		ms := &next.members[i]
		ms.generation++
		hm.replicaUID = revisionReplicaUID(w, i, int(ms.generation))
		kept := make([]historyRevision, 0, len(hm.revisions))
		for _, rev := range hm.revisions {
			if rev.serviceOwned {
				kept = append(kept, rev)
			}
		}
		hm.revisions = kept
		hm.reprojected, hm.retiredCurrent = true, ""
		hm.rejectedThen = rejectedRevisionFor(next.isvc, w.cfg.members[i].comp)
		ms.current, ms.partition = "", nil
		ms.rollbackTo = StableSideTarget(next.isvc, rollout.Policies{}, w.cfg.members[i].comp)
		target := w.buildTarget(next, i)
		hm.createdToward = target
		hm.builtOffTheStableSide = ms.rollbackTo != "" && target == ms.updateRev
		hm.builtAtTheFinalStep = ms.rollbackTo == "" && unitAtItsFinalStep(next.isvc, w.cfg.members[i].comp)
		ms.rows = make([]gateRow, ms.desired)
		for idx := range ms.rows {
			ms.rows[idx] = gateRow{kind: gateRowCreating, target: target}
		}
	case "stalecopy":
		h.staleCopies--
		for i, mc := range w.cfg.members {
			cs := next.isvc.Status.Components[mc.comp]
			p := h.members[i].previous
			cs.LatestReadyRevision, cs.LatestRolledoutRevision, cs.PreviousRolledoutRevision = p[0], p[1], p[2]
			next.isvc.Status.Components[mc.comp] = cs
		}
	default:
		return false
	}
	return true
}

// rejectedRevisionFor is the revision a standing rollback of the member's
// unit rejected, empty when no rollback stands on it.
func rejectedRevisionFor(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) string {
	g := rollout.CanaryGroupFor(isvc, rollout.Policies{}, comp)
	if g == nil {
		return ""
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, rollout.PrimaryOf(g))
	if cs == nil || stoppedWithoutStable(cs) {
		return ""
	}
	return cs.RolledBackRevisionHash
}

// revisionRunning is the revision every row of a member stands on: serving
// it, rebuilding or repairing toward it from it, or created toward it;
// empty while the rows are mixed.
func revisionRunning(ms gateMemberState) string {
	shared := ""
	for _, r := range ms.rows {
		on := r.rev
		if on == "" {
			on = r.target
		}
		if on == "" || (r.target != "" && r.target != on) || (shared != "" && on != shared) {
			return ""
		}
		shared = on
	}
	return shared
}

// classify names the shape of a member's record with one of the seven
// record states.
func (m *revisionModel) classify(w *gateWorld, st *gateState, i int) string {
	comp := w.cfg.members[i].comp
	hm := &st.history.members[i]
	ms := st.members[i]
	if target, ok := activeRunTarget(st.isvc, comp); ok {
		rev := hm.find(target.StableRevision)
		switch {
		case target.StableRevision == "":
			return "Pinned/NoStable"
		case rev != nil && m.retained(st, comp, hm, *rev):
			return "Pinned/Stable"
		default:
			return "Pinned/Disagrees"
		}
	}
	latest := query.RevisionFromName(revisionRecordOf(st.isvc, comp)[1]).Hash()
	if latest == "" {
		return "Empty"
	}
	if running := revisionRunning(ms); running != "" && running == latest {
		return "Settled"
	}
	recorded := hm.number(latest)
	if recorded == 0 {
		return "Ahead"
	}
	var newest int64
	for _, r := range ms.rows {
		for _, hash := range []string{r.rev, r.target} {
			if n := hm.number(hash); n > newest {
				newest = n
			}
		}
	}
	switch {
	case recorded < newest:
		return "Behind"
	case recorded > newest:
		return "Ahead"
	}
	return "Level/Mixed"
}

func revisionAlternates(recent []string) bool {
	for n := 2; n < len(recent); n++ {
		if recent[n] == recent[n-2] && recent[n] != recent[n-1] {
			return true
		}
	}
	return false
}

// revisionFindings collects the findings of one state; atRest is whether
// the world reads the state as a run at its end.
type revisionFindings struct {
	st     *gateState
	atRest bool
	out    []gateFinding
}

func (f *revisionFindings) add(class, format string, args ...any) {
	f.out = append(f.out, gateFinding{class: class, message: fmt.Sprintf(format, args...), state: f.st})
}

// check asks the model's questions of one state, member by member, and
// names the state's class.
func (m *revisionModel) check(w *gateWorld, st *gateState) []gateFinding {
	found := &revisionFindings{st: st, atRest: w.finished(st)}
	for i := range w.cfg.members {
		m.asked += 9
		m.checkRecord(w, found, i)
		m.checkRunStable(w, found, i)
		m.checkUnit(w, found, i)
		m.checkReplica(w, found, i)
	}
	return found.out
}

// checkRecord names the class of a member's record and asks the record's
// own questions: previous never equals latest, the rolled-out revision
// never alternates with no move between passes, and the record names a
// revision the history still holds.
func (m *revisionModel) checkRecord(w *gateWorld, found *revisionFindings, i int) {
	comp := w.cfg.members[i].comp
	hm := &found.st.history.members[i]
	rec := revisionRecordOf(found.st.isvc, comp)
	latest, previous := query.RevisionFromName(rec[1]).Hash(), query.RevisionFromName(rec[2]).Hash()
	found.add("state/"+m.classify(w, found.st, i), "%s record ready=%s latest=%s previous=%s", comp, query.RevisionFromName(rec[0]).Hash(), latest, previous)
	if previous != "" && previous == latest {
		found.add("previous-collapses", "%s previousRolledoutRevision equals latestRolledoutRevision (%s)", comp, latest)
	}
	if revisionAlternates(hm.recent) {
		found.add("record-alternates", "%s latestRolledoutRevision went %s across passes with no move between them", comp, strings.Join(hm.recent, " > "))
	}
	if latest != "" && hm.find(latest) == nil {
		found.add("record-names-collected-revision", "%s record names %s, which no ControllerRevision of the member's history backs", comp, latest)
	}
}

// checkRunStable asks whether the stable revision an open run records for
// a member is in its history and read by its replica as its own.
func (m *revisionModel) checkRunStable(w *gateWorld, found *revisionFindings, i int) {
	comp := w.cfg.members[i].comp
	hm := &found.st.history.members[i]
	target, ok := activeRunTarget(found.st.isvc, comp)
	if !ok || target.StableRevision == "" {
		return
	}
	switch rev := hm.find(target.StableRevision); {
	case rev == nil:
		found.add("run-stable-collected", "%s run records stable %s, which no ControllerRevision of the member's history backs", comp, target.StableRevision)
	case !m.retained(found.st, comp, hm, *rev):
		found.add("run-stable-other-owner", "%s run records stable %s, whose ControllerRevision neither the replica controls nor the service owns", comp, target.StableRevision)
	}
}

// checkUnit asks the questions of the canary unit a member rolls under:
// the dispatch reads the run's recorded stable, the unit's own record on
// its primary holds, and a replica projected again under a standing
// rollback was not created toward the rejected revision.
func (m *revisionModel) checkUnit(w *gateWorld, found *revisionFindings, i int) {
	isvc := found.st.isvc
	comp := w.cfg.members[i].comp
	g := rollout.CanaryGroupFor(isvc, rollout.Policies{}, comp)
	if g == nil {
		return
	}
	hm := &found.st.history.members[i]
	primary := rollout.PrimaryOf(g)
	if target, ok := activeRunTarget(isvc, comp); ok && target.StableRevision != "" {
		if got := componentStableRevisionHash(isvc, comp, primary); got != target.StableRevision {
			found.add("run-stable-ignored", "%s run records stable %s and the dispatch reads %s", comp, target.StableRevision, got)
		}
	}
	if comp == primary {
		m.checkPrimary(w, found, g, primary)
	}
	if hm.rejectedThen != "" && hm.createdToward == hm.rejectedThen {
		found.add("reprojected-on-rejected-revision", "%s projected again under a standing rollback was created toward the rejected revision %s", comp, hm.rejectedThen)
	}
}

// checkPrimary asks the questions of a unit's record on its primary: the
// unit never walks its steps toward the revision it already runs, and a
// stop for want of a stable revision has a member whose history lacks one.
func (m *revisionModel) checkPrimary(w *gateWorld, found *revisionFindings, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType) {
	isvc := found.st.isvc
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	if cs == nil {
		return
	}
	if cs.CanaryRevisionHash != "" && cs.CanaryRevisionHash == cs.StableRevisionHash && !unitAtRest(isvc, g, primary, cs) {
		found.add("canary-of-the-stable-onto-itself", "%s unit walks its steps toward %s, the revision it already runs: step %d, phase %s", primary, cs.CanaryRevisionHash, cs.CurrentStep, isvc.Status.Components[primary].RolloutPhase)
	}
	if !stoppedWithoutStable(cs) {
		return
	}
	if missing := m.memberWithoutStable(w, found.st, g, primary); missing == "" {
		found.add("rollback-stops-with-stable-retained", "%s unit stopped Failed for want of a stable revision while every member's stable is in its history", primary)
	} else {
		found.add("rollback-stable-missing", "%s unit stopped Failed: %s", primary, missing)
	}
}

// checkReplica asks whether a member's replica reads the rollback target it
// carries as its own, and whether at rest the record names the revision
// every Instance runs.
func (m *revisionModel) checkReplica(w *gateWorld, found *revisionFindings, i int) {
	comp := w.cfg.members[i].comp
	ms := found.st.members[i]
	if ms.rollbackTo != "" && m.rollbackTarget(w, found.st, i) == "" {
		found.add("rollback-target-outside-history", "%s carries rollbackToRevision %s, which its replica does not read as its own; the workload builds toward the spec %s", comp, ms.rollbackTo, ms.updateRev)
	}
	latest := query.RevisionFromName(revisionRecordOf(found.st.isvc, comp)[1]).Hash()
	if running := revisionRunning(ms); found.atRest && running != "" && latest != running {
		found.add("record-off-fleet-at-rest", "%s rests with every Instance on %s and latestRolledoutRevision naming %s", comp, running, latest)
	}
}

// memberWithoutStable names the first member of a unit whose stable
// revision is unknown or has no ControllerRevision its replica reads as its
// own, empty when every member's stable is retained.
func (m *revisionModel) memberWithoutStable(w *gateWorld, st *gateState, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType) string {
	for i, mc := range w.cfg.members {
		if !gateGroupHas(g, mc.comp) {
			continue
		}
		hm := &st.history.members[i]
		stable := componentStableRevisionHash(st.isvc, mc.comp, primary)
		if stable == "" {
			return fmt.Sprintf("%s has no stable revision", mc.comp)
		}
		if rev := hm.find(stable); rev == nil || !m.retained(st, mc.comp, hm, *rev) {
			return fmt.Sprintf("%s stable %s has no ControllerRevision in its history", mc.comp, stable)
		}
	}
	return ""
}

// unitAtRest reports a unit idle, rolled back, or done and reading Stable.
func unitAtRest(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, primary v1beta1.ComponentType, cs *v1beta1.CanaryStatus) bool {
	phase := isvc.Status.Components[primary].RolloutPhase
	switch rollout.StateOf(cs, phase, len(g.Canary.Steps)) {
	case rollout.CanaryStateIdle, rollout.CanaryStateRolledBack:
		return true
	case rollout.CanaryStateDone:
		return phase == v1beta1.RolloutPhaseStable
	}
	return false
}

// revisionKnownFinding is a finding the model keeps visible without failing
// while its decision is open: the classes it folds, the states it matches,
// and why in plain words. A fix removes the entry.
type revisionKnownFinding struct {
	name    string
	classes map[string]bool
	matches func(w *gateWorld, st *gateState) bool
	why     string
}

// rebuiltUnderALegacyHistory reports a member projected again while its
// unit walks a step whose stable revision the new replica could not read as
// its own, so the fleet was built toward the canary revision.
func rebuiltUnderALegacyHistory(w *gateWorld, st *gateState) bool {
	for _, g := range rollout.CanaryGroups(st.isvc, rollout.Policies{}) {
		primary := rollout.PrimaryOf(g)
		cs := rollout.CanaryStatusFor(&st.isvc.Status, primary)
		if cs == nil || cs.RolledBackRevisionHash != "" || unitAtRest(st.isvc, g, primary, cs) {
			continue
		}
		for i := range w.cfg.members {
			if st.history.members[i].builtOffTheStableSide {
				return true
			}
		}
	}
	return false
}

// unitAtItsFinalStep reports whether the unit governing comp stands at the
// last of its steps with no rollback standing: staging it or draining
// through its window, the two states that keep no stable side.
func unitAtItsFinalStep(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) bool {
	g := rollout.CanaryGroupFor(isvc, rollout.Policies{}, comp)
	if g == nil || g.Canary == nil {
		return false
	}
	primary := rollout.PrimaryOf(g)
	cs := rollout.CanaryStatusFor(&isvc.Status, primary)
	steps := len(g.Canary.Steps)
	if cs == nil || cs.RolledBackRevisionHash != "" || steps == 0 || int(cs.CurrentStep) != steps-1 {
		return false
	}
	switch rollout.StateOf(cs, isvc.Status.Components[primary].RolloutPhase, len(g.Canary.Steps)) {
	case rollout.CanaryStateStaging, rollout.CanaryStateDraining:
		return true
	}
	return false
}

// rebuiltAtTheFinalStep reports a member projected again while its unit
// stood at its final step, so the fleet was built toward the canary
// revision and the member's one held stable Instance was not rebuilt.
func rebuiltAtTheFinalStep(w *gateWorld, st *gateState) bool {
	for _, g := range rollout.CanaryGroups(st.isvc, rollout.Policies{}) {
		primary := rollout.PrimaryOf(g)
		cs := rollout.CanaryStatusFor(&st.isvc.Status, primary)
		if cs == nil || cs.RolledBackRevisionHash != "" || unitAtRest(st.isvc, g, primary, cs) {
			continue
		}
		for i := range w.cfg.members {
			if st.history.members[i].builtAtTheFinalStep {
				return true
			}
		}
	}
	return false
}

var revisionKnownFindings = []revisionKnownFinding{
	{
		name:    "reprojected-at-the-final-step-comes-back-on-the-canary-revision",
		classes: map[string]bool{"held-short": true, "share-exceeded": true, "traffic-empty-revision": true, "traffic-empty-stable-revision-under-a-hold": true},
		matches: rebuiltAtTheFinalStep,
		why: "the final step keeps no stable side (its share is the whole fleet, less the held Instances), so a replica deleted and projected again there follows the spec and every Instance comes back on the canary revision; the member's one held stable Instance is not rebuilt, and a rollback inside the drain window rebuilds the member from the ControllerRevision the service owns " +
			"(rebuilding the fleet on the stable side there would break the unit's canary path and rest the other members' traffic on one held Instance for two load rounds)",
	},
	{
		name:    "reprojected-under-a-legacy-history-comes-back-on-the-canary-revision",
		classes: map[string]bool{"held-short": true, "share-exceeded": true, "traffic-empty-revision": true, "traffic-empty-stable-revision-under-a-hold": true},
		matches: rebuiltUnderALegacyHistory,
		why: "a replica deleted and projected again while its unit walks a step is pointed at the stable revision, but a history minted before the service owned the replica's revisions loses that revision with the deleted replica, so the new replica rolls to the spec and every Instance comes back on the canary revision " +
			"(a legacy history survives a re-projection only once a rollback read patched the owner in)",
	},
}

// revisionKnownFindingFor is the name of the known finding f falls under,
// empty when none matches.
func revisionKnownFindingFor(w *gateWorld, f gateFinding) string {
	for _, k := range revisionKnownFindings {
		if k.classes[f.class] && k.matches(w, f.state) {
			return "known-" + k.name
		}
	}
	return ""
}

func revisionKnownFindingWhy(name string) string {
	for _, k := range revisionKnownFindings {
		if "known-"+k.name == name {
			return k.why
		}
	}
	return ""
}

// revisionStateOwners are the seven record states this model names, each
// in words; a state outside this map is one no class names.
var revisionStateOwners = map[string]string{
	"Empty":            "no record yet, a first rollout",
	"Settled":          "the record names the revision every Instance runs",
	"Behind":           "the replica moved past the record; the next write levels it",
	"Ahead":            "the record names a revision no Instance runs: a rollback onto the last promoted revision, a stale copy, a collected revision; a bind may write over a collected record",
	"Pinned/Stable":    "a run records a stable the history carries",
	"Pinned/NoStable":  "a run records no stable, a first rollout",
	"Pinned/Disagrees": "a run records a stable the history no longer carries: the stable was collected, or the run opened on the latest rolled-out revision as its stable",
}

// revisionKnownClasses are the readings reported without failing, each a
// reading a test already holds.
var revisionKnownClasses = map[string]string{
	"record-names-collected-revision": "the record keeps no revision alive: a record left behind by a roll names a collected revision until the next write",
	"run-stable-collected":            "the sweep does not count a run's recorded stable as live, so the run's record disagrees with the history once it goes; a revert toward it stops Failed",
	"rollback-stable-missing":         "a rollback whose stable revision no ControllerRevision backs stops Failed with StableRevisionMissing until a resume or a new target (TestDispatch_ASettledHoldWhoseStableRevisionIsGoneStopsFailedAtEveryLink)",
	"rollback-target-outside-history": "the replica rolls to its spec when the rollback target it carries is gone or another owner's",
}

// revisionFailingClasses are the classes read as bugs.
var revisionFailingClasses = map[string]bool{
	"previous-collapses": true, "record-alternates": true, "run-stable-ignored": true, "run-stable-other-owner": true, "canary-of-the-stable-onto-itself": true,
	"rollback-stops-with-stable-retained": true, "reprojected-on-rejected-revision": true, "record-off-fleet-at-rest": true,
	"no-way-out": true, "hold-unnamed": true, "capacity-never-arrives": true, "rollback-target-left-behind": true,
	"traffic-empty-revision": true, "traffic-empty-stable-revision-under-a-hold": true, "held-short": true, "share-exceeded": true,
	"run-closed-twice": true, "reopen-after-complete": true, "unsettled": true, "depth": true, "model-error": true,
}

func revisionWorkers(full bool) int {
	if n, err := strconv.Atoi(os.Getenv("OME_REVISION_HUNT_WORKERS")); err == nil && n > 0 {
		return n
	}
	if full {
		return 16
	}
	return 4
}

// revisionExample is one finding of a class with the configuration it came
// from.
type revisionExample struct {
	cfg     string
	finding gateFinding
}

// revisionTally sums the explored configurations: the states, controller
// passes, random runs and questions, and per class its count, the number of
// configurations it appeared in and up to six examples.
type revisionTally struct {
	states, passes, runs, asked int
	totals, configsWith         map[string]int
	examples                    map[string][]revisionExample
}

func newRevisionTally() *revisionTally {
	return &revisionTally{totals: map[string]int{}, configsWith: map[string]int{}, examples: map[string][]revisionExample{}}
}

// add folds one configuration's result in and returns its report; a world
// that failed before it reported counts as one model-error finding.
func (ta *revisionTally) add(cfg revisionConfig, gate gateConfig, res gateRunResult, model *revisionModel) gateReport {
	rep := res.rep
	if res.failure != "" {
		if rep.counts == nil {
			rep.counts, rep.findings = map[string]int{}, map[string]gateFinding{}
		}
		rep.counts["model-error"]++
		rep.findings["model-error"] = gateFinding{class: "model-error", message: res.failure, state: &gateState{isvc: gateISVC(gate)}}
	}
	ta.states += rep.states
	ta.passes += res.passes
	ta.runs += rep.runs
	if model != nil {
		ta.asked += model.asked
	}
	for class, c := range rep.counts {
		ta.totals[class] += c
	}
	for class, f := range rep.findings {
		ta.configsWith[class]++
		if len(ta.examples[class]) < 6 {
			ta.examples[class] = append(ta.examples[class], revisionExample{cfg: cfg.name(), finding: f})
		}
	}
	return rep
}

// classes are the classes seen at least once, sorted.
func (ta *revisionTally) classes() []string {
	classes := make([]string, 0, len(ta.totals))
	for class, c := range ta.totals {
		if c > 0 {
			classes = append(classes, class)
		}
	}
	sort.Strings(classes)
	return classes
}

// report prints one class with its owner or its known reading and its
// examples, and fails the test on a class read as a bug or on a state no
// record state names.
func (ta *revisionTally) report(t *testing.T, class string) {
	t.Helper()
	total, n, examples := ta.totals[class], ta.configsWith[class], ta.examples[class]
	switch {
	case strings.HasPrefix(class, "state/"):
		name := strings.TrimPrefix(class, "state/")
		if owner, ok := revisionStateOwners[name]; ok {
			t.Logf("state %s: %d member-states across %d configurations: %s", name, total, n, owner)
			return
		}
		t.Errorf("state %s: %d member-states across %d configurations: no record state names it (first: %s: %s; path: %s)", name, total, n, examples[0].cfg, examples[0].finding.message, gatePath(examples[0].finding.state))
	case revisionKnownClasses[class] != "":
		t.Logf("%s: %d states across %d configurations: %s", class, total, n, revisionKnownClasses[class])
	case revisionKnownFindingWhy(class) != "":
		t.Logf("%s: %d states across %d configurations: %s", class, total, n, revisionKnownFindingWhy(class))
	default:
		t.Logf("%s: %d states across %d configurations", class, total, n)
	}
	for _, ex := range examples {
		t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, gatePath(ex.finding.state), ex.finding.state)
	}
	if revisionFailingClasses[class] {
		t.Errorf("%s: %d states (first: %s: %s)", class, total, examples[0].cfg, examples[0].finding.message)
	}
}

// revisionClassCounts prints a report's counts as "class=count", sorted.
func revisionClassCounts(counts map[string]int) string {
	parts := make([]string, 0, len(counts))
	for class, c := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", class, c))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// revisionConfigsNamed keeps the configurations whose name contains one of
// the comma-separated parts; an empty filter keeps them all.
func revisionConfigsNamed(configs []revisionConfig, only string) []revisionConfig {
	if only == "" {
		return configs
	}
	var kept []revisionConfig
	for _, cfg := range configs {
		for _, part := range strings.Split(only, ",") {
			if part != "" && strings.Contains(cfg.name(), part) {
				kept = append(kept, cfg)
				break
			}
		}
	}
	return kept
}

// revisionBounds are the exploration's bounds: 40 states, depth 40 and two
// random runs per configuration in the bounded mode; 80, 100 and three with
// the probes on in the full mode.
func revisionBounds(full bool) gateRunBounds {
	if full {
		return gateRunBounds{states: 80, depth: 100, runs: 3, probe: true, full: true}
	}
	return gateRunBounds{states: 40, depth: 40, runs: 2}
}

// revisionScheme is the canary scheme with EndpointSlices, which the
// per-revision Services' endpoints are listed through.
func revisionScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := canaryScheme(t)
	if err := discoveryv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// revisionExploreConfigs explores every configuration and reports the
// state classes with their owners, every other class with its count, and
// fails on a class read as a bug or a state no class names.
func revisionExploreConfigs(t *testing.T, configs []revisionConfig) {
	t.Helper()
	full := os.Getenv("OME_REVISION_HUNT") == "full"
	configs = revisionConfigsNamed(configs, os.Getenv("OME_REVISION_HUNT_ONLY"))
	scheme := revisionScheme(t)
	models := make([]*revisionModel, len(configs))
	gates := make([]gateConfig, len(configs))
	for i, cfg := range configs {
		gates[i] = cfg.gate()
	}
	started := time.Now()
	results := gateRunConfigs(gates, func(n int) *gateWorld {
		w, m := newRevisionWorld(t, configs[n], scheme)
		models[n] = m
		return w
	}, revisionBounds(full), revisionWorkers(full))
	tally := newRevisionTally()
	for i, cfg := range configs {
		rep := tally.add(cfg, gates[i], results[i], models[i])
		if full {
			t.Logf("config %d/%d [%s]: %d states, depth %d, %d passes: %s", i+1, len(configs), cfg.name(), rep.states, rep.depth, results[i].passes, revisionClassCounts(rep.counts))
		}
	}
	t.Logf("%d configurations, %d states, %d random runs, %d questions, %d controller passes, %s", len(configs), tally.states, tally.runs, tally.asked, tally.passes, time.Since(started).Round(time.Second))
	for _, class := range tally.classes() {
		tally.report(t, class)
	}
}

// TestRevisionRecord_EveryStateIsNamedAndHasAWayOut drives the revision
// record over the reference shapes under both owners and asks the model's
// questions at every state.
func TestRevisionRecord_EveryStateIsNamedAndHasAWayOut(t *testing.T) {
	revisionExploreConfigs(t, revisionConfigs(os.Getenv("OME_REVISION_HUNT") == "full"))
}

// revisionParseMove reads a move as gateMove.String printed it.
func revisionParseMove(s string) (gateMove, error) {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) == 0 {
		return gateMove{}, fmt.Errorf("empty move")
	}
	mv := gateMove{kind: fields[0]}
	switch {
	case len(fields) == 5 && fields[1] == "member" && fields[3] == "row":
		member, err := strconv.Atoi(fields[2])
		if err != nil {
			return mv, err
		}
		idx, err := strconv.Atoi(fields[4])
		if err != nil {
			return mv, err
		}
		mv.member, mv.idx = member, idx
	case len(fields) == 5 && fields[1] == "member" && fields[3] == "by":
		member, err := strconv.Atoi(fields[2])
		if err != nil {
			return mv, err
		}
		delta, err := strconv.Atoi(fields[4])
		if err != nil {
			return mv, err
		}
		mv.member, mv.delta = member, int32(delta)
	case len(fields) == 3 && fields[1] == "member":
		member, err := strconv.Atoi(fields[2])
		if err != nil {
			return mv, err
		}
		mv.member = member
	case len(fields) != 1:
		return mv, fmt.Errorf("unreadable move %q", s)
	}
	return mv, nil
}

// revisionDescribe is one state's revision surfaces: each member's record,
// the run and each unit's status.
func revisionDescribe(w *gateWorld, st *gateState) string {
	var parts []string
	for _, mc := range w.cfg.members {
		rec := revisionRecordOf(st.isvc, mc.comp)
		parts = append(parts, fmt.Sprintf("%s ready=%s latest=%s previous=%s", mc.comp, query.RevisionFromName(rec[0]).Hash(), query.RevisionFromName(rec[1]).Hash(), query.RevisionFromName(rec[2]).Hash()))
	}
	if r := st.isvc.Status.Rollout; r != nil {
		if r.ActiveRun != nil {
			var targets []string
			for _, t := range r.ActiveRun.TargetRevisions {
				targets = append(targets, fmt.Sprintf("%s=%s/stable %s", t.Component, t.Revision, t.StableRevision))
			}
			parts = append(parts, "run "+r.ActiveRun.RunID[:8]+" "+strings.Join(targets, ","))
		} else if r.LastRun != nil {
			parts = append(parts, "last run "+string(r.LastRun.Outcome))
		}
	}
	parts = append(parts, w.describe(st))
	return strings.Join(parts, " | ")
}

// TestRevisionRecord_ReplayAPath replays a printed path on the configuration
// OME_REVISION_HUNT_ONLY names (OME_REVISION_HUNT_PATH holds the moves
// joined by " -> ") and prints the revision surfaces after each move and
// after each of four more passes.
func TestRevisionRecord_ReplayAPath(t *testing.T) {
	only, path := os.Getenv("OME_REVISION_HUNT_ONLY"), os.Getenv("OME_REVISION_HUNT_PATH")
	if only == "" {
		t.Skip("set OME_REVISION_HUNT_ONLY to a configuration name and OME_REVISION_HUNT_PATH to a path")
	}
	var cfg *revisionConfig
	for _, c := range revisionConfigs(os.Getenv("OME_REVISION_HUNT") == "full") {
		if c.name() == only {
			c := c
			cfg = &c
			break
		}
	}
	if cfg == nil {
		t.Fatalf("no configuration named %q", only)
	}
	w, _ := newRevisionWorld(t, *cfg, revisionScheme(t))
	st := w.initial()
	w.settle(st)
	t.Logf("start: %s\n  %s", st, revisionDescribe(w, st))
	for _, s := range strings.Split(path, "->") {
		if strings.TrimSpace(s) == "" {
			continue
		}
		mv, err := revisionParseMove(s)
		if err != nil {
			t.Fatal(err)
		}
		st = w.apply(st, mv)
		t.Logf("after %s: %s\n  %s\n  events: %s", mv, st, revisionDescribe(w, st), strings.Join(st.passEvents, " / "))
		for _, f := range w.check(st, true) {
			t.Logf("  %s: %s", f.class, f.message)
		}
	}
	for pass := 1; pass <= 4; pass++ {
		w.pass(st)
		t.Logf("pass +%d: %s\n  %s", pass, st, revisionDescribe(w, st))
	}
	t.Logf("finished=%t moves=%v %v", w.finished(st), w.progressMoves(st), w.extraMoves(st))
}
