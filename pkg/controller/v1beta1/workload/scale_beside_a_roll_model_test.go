package workload_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/scaling"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives the production engine of a prefill/decode service while
// the operator scales it beside an open roll: one workload.Reconcile loop
// per Component on the recovery harness's kubelet, scheduler and node
// models, the coordination gates wired between the Components the way the
// replica adapter wires them (EvaluateUpdateGate, EvaluatePlanGate,
// EvaluatePairingDrain), the follower count of a proportional scaling
// policy resolved by scaling.ResolveTargets, the ratio reference the
// service pass records (coordination.SnapshotOriginal) and the partition a
// canary step projects (canary.StepPartition). The plan's choice of which
// Instance a scale-down removes (BuildPlan) and the create pass's choice of
// the revision a scale-up Instance is built on are the engine's own.
//
// Every configuration (a reference shape, pods per Instance, a strategy
// per side at ten percent, with or without a ratio rule, keepRatio, a
// first side, a partition, a canary, a scaling policy, a router beside the
// pair) is rolled once from version 1 to version 2, and the moments of
// that roll are kept: idle, settling, rotating, draining, the last
// Instance, held on an image no pod can pull, a canary step. At each
// moment the operator's moves and the world's are tried one at a time
// (a scale of one side, both sides or the ratio, by one Instance, a step
// or half; the same through the anchor of a scaling policy; a partition
// set, raised, lowered or cleared; a policy added, edited or removed; the
// ratio rule added or removed; pause and resume; a roll back; a second
// push; a pod, a node or a gang member lost; the canary promoted), and the
// roll is then played to its end. Seeded random runs mix several moves
// into one roll.
//
// At every tick the model checks that each side holds no more attempts in
// flight than its own budget and the group's, and that under a ratio rule
// the two sides' new-version shares sit within the tolerance plus one step
// of each other. At every settled state (no change for three ticks) the
// roll must be complete, or paused, or stopped on a spent retry schedule;
// every side must carry the Instance count its spec asks for; no two gates
// may wait on each other; and a side that stands still must name a hold.
// OME_SCALE_ROLL_HUNT=full widens the configurations, the moves and the
// bounds; OME_SCALE_ROLL_HUNT_ONLY=<text> keeps the configurations whose
// name carries the text.

const (
	scaleRollFullEnv = "OME_SCALE_ROLL_HUNT"
	scaleRollOnlyEnv = "OME_SCALE_ROLL_HUNT_ONLY"
	// scaleRollBudget is the budget every side rolls on: ten percent on
	// the budget its strategy uses, surge or unavailability.
	scaleRollBudget = "10%"
	// scaleRollTolerance is the ratio tolerance the model uses.
	scaleRollTolerance = int32(20)
	// scaleRollKeepRatioTolerance is the chart's default tolerance, which
	// keepRatio plans batches to when the group names none.
	scaleRollKeepRatioTolerance = int32(5)
	// scaleRollThirdImage is the image of a second push during a roll.
	scaleRollThirdImage = "registry.test/serving:v3"
	// scaleRollSettleTicks and scaleRollSettleSpan are how long a state
	// must stand unchanged to read as settled while the roll is open: two
	// ticks and the longest wait the engine keeps without asking for a
	// requeue (the force-delete threshold and its slack). A complete roll
	// settles in one tick.
	scaleRollSettleTicks = 2
	scaleRollSettleSpan  = scaleRollNodeUnreachable + scaleRollOverdueSlack + time.Minute
	// scaleRollReadyType is the replica's Ready condition the Sequential
	// gate reads a member's finish time from.
	scaleRollReadyType = "Ready"
	// scaleRollOverdueSlack and scaleRollNodeUnreachable are the force
	// delete values the chart's lifecycle block documents, so a lost node
	// is cleared by the engine and the roll goes on past it.
	scaleRollOverdueSlack    = 2 * time.Minute
	scaleRollNodeUnreachable = 5 * time.Minute
	// scaleRollPodsPerNode sizes a side's node pool: a lost node takes
	// about this many pods with it.
	scaleRollPodsPerNode = 4
)

// scaleShape is a reference shape: prefill and decode Instance counts and
// the pods each Instance runs (one, or a leader with workers). A decode
// count of zero is an engine-only service: one Component, no pair.
type scaleShape struct {
	prefill, decode, pods int32
}

func (s scaleShape) String() string {
	counts := fmt.Sprintf("%d:%d", s.prefill, s.decode)
	if s.decode == 0 {
		counts = fmt.Sprintf("engine%d", s.prefill)
	}
	if s.pods > 1 {
		return fmt.Sprintf("%sx%d", counts, s.pods)
	}
	return counts
}

// scaleSettings are the operator's rollout settings for one configuration.
type scaleSettings struct {
	prefill, decode workloadtypes.UpdateStrategyType
	// maintainRatio turns the group's ratio rule on at the production
	// tolerance.
	maintainRatio bool
	// keepRatio plans both sides' batches together.
	keepRatio bool
	// firstSide names the side that moves first inside every batch.
	firstSide v1beta1.ComponentType
	// partition is the operator's partition on the prefill side at the
	// push; zero for none.
	partition int32
	// decodePartition is the operator's partition on the decode side at
	// the push; zero for none.
	decodePartition int32
	// canary rolls the pair as a canary unit stepping 25 then 100 percent.
	canary bool
	// policy makes the decoder follow the engine at the shape's ratio.
	policy bool
	// followerMax declares a maxReplicas on the follower at twice its count.
	followerMax bool
	// router adds a router of two Instances to the group.
	router bool
	// sequential rolls the pair as a Sequential chain, the decoder first.
	sequential bool
	// blueGreen rolls the members as one blueGreen group.
	blueGreen bool
	// budget is the share of each side its strategy moves at once, on
	// the count its strategy uses; empty means scaleRollBudget.
	budget string
}

func strategyShort(s workloadtypes.UpdateStrategyType) string {
	switch s {
	case workloadtypes.UpdateStrategySurgeThenDrain:
		return "surge"
	case workloadtypes.UpdateStrategyInPlaceIfPossible:
		return "inplace"
	case workloadtypes.UpdateStrategyInPlaceOnly:
		return "inplaceonly"
	}
	return "recreate"
}

func (s scaleSettings) String() string {
	parts := []string{strategyShort(s.prefill) + "/" + strategyShort(s.decode)}
	if s.maintainRatio {
		parts = append(parts, "ratio")
	}
	if s.keepRatio {
		parts = append(parts, "keepratio")
	}
	if s.firstSide != "" {
		parts = append(parts, "first="+string(s.firstSide))
	}
	if s.partition > 0 {
		parts = append(parts, fmt.Sprintf("partition=%d", s.partition))
	}
	if s.decodePartition > 0 {
		parts = append(parts, fmt.Sprintf("decode-partition=%d", s.decodePartition))
	}
	if s.canary {
		parts = append(parts, "canary")
	}
	if s.policy {
		parts = append(parts, "policy")
		if s.followerMax {
			parts = append(parts, "followermax")
		}
	}
	if s.router {
		parts = append(parts, "router")
	}
	if s.sequential {
		parts = append(parts, "sequential")
	}
	if s.blueGreen {
		parts = append(parts, "bluegreen")
	}
	if s.budget != "" {
		parts = append(parts, "budget="+s.budget)
	}
	return strings.Join(parts, "+")
}

// budgetText is the budget a configuration's sides roll on.
func (s scaleSettings) budgetText() string {
	if s.budget != "" {
		return s.budget
	}
	return scaleRollBudget
}

// scaleConfig is one configuration the model explores.
type scaleConfig struct {
	shape    scaleShape
	settings scaleSettings
}

func (c scaleConfig) String() string { return c.shape.String() + "/" + c.settings.String() }

// scaleMoment is where in the roll a move lands.
type scaleMoment uint8

const (
	scaleAtIdle scaleMoment = iota
	scaleAtSettling
	scaleAtRotating
	scaleAtDraining
	scaleAtLast
	scaleAtHeld
	scaleAtCanaryStep
)

var scaleMomentNames = [...]string{"idle", "settling", "rotating", "draining", "last", "held", "canary-step"}

func (m scaleMoment) String() string { return scaleMomentNames[m] }

// scaleMoveKind is what the operator or the world does at a moment.
type scaleMoveKind uint8

const (
	moveScale           scaleMoveKind = iota // one side's count moves by delta
	moveScaleBoth                            // both sides move by delta
	moveScaleRatio                           // the prefill side moves by delta and the decode side by delta2
	moveAnchorScale                          // the anchor of the scaling policy moves by delta; the follower follows
	movePartition                            // the operator's partition on a side becomes delta
	movePartitionTwice                       // the operator's partition on a side becomes delta, then delta2 once the roll settles
	movePolicyAdd                            // a scaling policy is added
	movePolicyEdit                           // the policy's ratio is changed
	movePolicyRemove                         // the policy is removed
	moveRatioRuleAdd                         // the group's ratio rule is added
	moveRatioRuleRemove                      // the group's ratio rule is removed
	movePause                                // the rollout is paused
	moveResume                               // the rollout is resumed
	moveRollBack                             // version 1 is pushed again
	movePush                                 // a third version is pushed
	moveLosePod                              // a pod of Instance index on a side is lost
	moveLoseNode                             // the node under a pod of Instance index on a side stops reporting, with every pod on it
	moveLoseMember                           // a worker of gang index on a side is lost
	moveCanaryPromote                        // the canary's step counter moves past its last step
	moveStrategy                             // a side's update strategy becomes strategy, its budgets written as budgets say
	moveStrategyTwice                        // a side's update strategy becomes strategy, then strategy2 once the roll settles
	moveBudget                               // a side's own maxSurge and maxUnavailable become budgets
	moveGroupBudget                          // the rolling group's maxSurge and maxUnavailable become text
)

var scaleMoveKindNames = [...]string{"scale", "scale-both", "scale-ratio", "anchor-scale", "partition", "partition-twice", "policy-add", "policy-edit", "policy-remove", "ratio-rule-add", "ratio-rule-remove", "pause", "resume", "roll-back", "push", "lose-pod", "lose-node", "lose-member", "canary-promote", "strategy", "strategy-twice", "budget", "group-budget"}

func (k scaleMoveKind) String() string { return scaleMoveKindNames[k] }

// scaleMove is one move at one moment.
type scaleMove struct {
	kind   scaleMoveKind
	side   int
	delta  int32
	delta2 int32
	index  int32
	// strategy and strategy2 are the strategies a strategy move writes on
	// the side, budgets the side's own maxSurge and maxUnavailable a
	// strategy or budget move writes, and text the rolling group's budget
	// a group budget move writes.
	strategy  workloadtypes.UpdateStrategyType
	strategy2 workloadtypes.UpdateStrategyType
	budgets   scaleRolling
	text      string
}

func (m scaleMove) String() string {
	switch m.kind {
	case moveScale:
		return fmt.Sprintf("scale side %d by %+d", m.side, m.delta)
	case moveScaleBoth:
		return fmt.Sprintf("scale both sides by %+d", m.delta)
	case moveScaleRatio:
		return fmt.Sprintf("scale prefill by %+d and decode by %+d", m.delta, m.delta2)
	case moveAnchorScale:
		return fmt.Sprintf("scale the anchor by %+d", m.delta)
	case movePartition:
		return fmt.Sprintf("partition side %d to %d", m.side, m.delta)
	case movePartitionTwice:
		return fmt.Sprintf("partition side %d to %d, then to %d once the roll settles", m.side, m.delta, m.delta2)
	case moveLosePod, moveLoseNode, moveLoseMember:
		return fmt.Sprintf("%s side %d index %d", m.kind, m.side, m.index)
	case moveStrategy:
		return fmt.Sprintf("strategy of side %d to %s with %s", m.side, strategyShort(m.strategy), m.budgets)
	case moveStrategyTwice:
		return fmt.Sprintf("strategy of side %d to %s with %s, then to %s once the roll settles", m.side, strategyShort(m.strategy), m.budgets, strategyShort(m.strategy2))
	case moveBudget:
		return fmt.Sprintf("budgets of side %d to %s", m.side, m.budgets)
	case moveGroupBudget:
		return fmt.Sprintf("group budgets to %s", m.text)
	}
	return m.kind.String()
}

// scaleFinding is one broken invariant at one state.
type scaleFinding struct {
	class   string
	message string
	// path is the moment and the moves that led to the state.
	path  string
	state string
}

// scaleSide is one Component of the service under exploration: its
// engine loop on the recovery harness, and the spec the operator wrote
// for it.
type scaleSide struct {
	comp     workloadtypes.ComponentType
	h        *recoveryHarness
	strategy workloadtypes.UpdateStrategyType
	// image is the image the spec names now; its revision is the target.
	image string
	// paused is the rollout pause as the projector writes it.
	paused bool
	// partition is the operator's partition on this side; zero for none.
	partition int32
	// rolling is the side's own maxSurge and maxUnavailable as the operator
	// last wrote them; unwritten, the configuration's budget stands on the
	// field the strategy uses.
	rolling scaleRolling
	// stable is the revision every Instance of this side ran when the run
	// opened, kept across pushes until a roll completes: a push back to it
	// while an Instance still runs another revision takes the roll back.
	stable string
	// revisions maps an image to the ControllerRevision minted for it.
	revisions map[string]*appsv1.ControllerRevision
}

func (s *scaleSide) component() v1beta1.ComponentType { return v1beta1.ComponentType(s.comp) }

// scaleWorld is the service under exploration: its sides, the service
// object the gates read, one clock for every loop, and what the run has
// found so far.
type scaleWorld struct {
	t        *testing.T
	cfg      scaleConfig
	ctx      context.Context
	clk      *clocktesting.FakeClock
	sides    []*scaleSide
	isvc     *v1beta1.InferenceService
	defaults coordination.GroupDefaults
	policy   *v1beta1.ScalingPolicy
	// maintainRatio is the group's ratio rule as the spec reads now.
	maintainRatio bool
	// canaryStep is the canary's current step while its record stands; -1
	// when none does: no canary group, or the record ended under a push
	// the operator took back.
	canaryStep int32
	// scaled is set once a move changed a side's count in this run;
	// startedAtScale keeps, per side, the Instances that had moved toward
	// the target when the last scale landed, so a wider version mix can be
	// told apart by its cause.
	scaled         bool
	startedAtScale map[*scaleSide]map[int32]bool
	indicesAtScale map[*scaleSide][]int32
	// path names the moment and the moves of this run.
	path []string

	// checking is off while the fleet first comes up on version 1.
	checking bool
	// waiting is the longest wait a pass of the last tick asked for: above
	// the cadences, an engine timer is still to run and the state is not
	// settled however still it looks.
	waiting time.Duration
	// unchanged counts the ticks the state has stood still; the clock then
	// moves by the full step, so a wait the engine keeps without asking
	// for a requeue elapses in a few ticks.
	unchanged int
	// replicas caches each side's decorated replica for one gate consult,
	// which reads it several times; consult counts the consults.
	replicas map[*scaleSide]*v1beta1.InferenceReplica
	consult  int
	cachedAt map[*scaleSide]int
	// builtOn counts the Instances the create pass built in this run by
	// the revision it chose; builtSeen keeps each build counted once.
	builtOn   map[string]int
	builtSeen map[string]struct{}
	// surging keeps, per side, the sources whose replacement was coming up
	// at the last check: a scale-down keeps such a source until its surge
	// finishes.
	surging map[*scaleSide]map[int32]struct{}
	// belowCount counts, per side, the checks in a row a side has stood
	// below its count: a scale-up is built at once, by a Sequential
	// chain's waiting member too.
	belowCount map[*scaleSide]int
	// faulted keeps, per side, the Instances a fault of this run took
	// pods from: a rebuild of one on the target is a start the batch plan
	// never made, which the version-mix reader names as its own class.
	faulted map[*scaleSide]map[int32]bool
	// moves, when set, lists the moves a hunt tries from a moment in place
	// of the scaling hunt's lists; moreChecks runs after the per-tick
	// checks and endChecks at every complete settled end, when set.
	moves      func(m scaleMoment, full bool) []scaleMove
	moreChecks func()
	endChecks  func()
	// openAttempts, when set, is told which side an edit of the strategy
	// or the budgets is about to land on, before it lands; pastBudget,
	// when set, is how many attempts beyond a side's budget a hunt allows
	// in flight on the budget kind named (surge or unavailability): the
	// attempts open when the budget was lowered, which the engine lets
	// finish.
	openAttempts func(s *scaleSide)
	pastBudget   func(s *scaleSide, surge bool, budget int32) int32
	// endClass, when set, may give a settled end that is not complete a
	// more exact class than the shape of its holds: a hunt names the
	// pictures it knows so every other picture of the same shape still
	// fails.
	endClass func(class string) string
	// tickBudget, when set, is the most ticks one configuration may spend
	// in place of scaleTickBudget.
	tickBudget int

	gateCalls, passes, ticks, probes, skipped int
	seen                                      map[string]struct{}
	findings                                  map[string]scaleFinding
	counts                                    map[string]int
}

func newScaleWorld(t *testing.T, cfg scaleConfig) *scaleWorld {
	t.Helper()
	w := &scaleWorld{
		t:             t,
		cfg:           cfg,
		ctx:           logr.NewContext(context.Background(), logr.Discard()),
		clk:           clocktesting.NewFakeClock(time.Now()),
		maintainRatio: cfg.settings.maintainRatio,
		canaryStep:    -1,
		seen:          map[string]struct{}{},
		findings:      map[string]scaleFinding{},
		counts:        map[string]int{},
		builtOn:       map[string]int{},
		builtSeen:     map[string]struct{}{},
		surging:       map[*scaleSide]map[int32]struct{}{},
		belowCount:    map[*scaleSide]int{},
		faulted:       map[*scaleSide]map[int32]bool{},
		replicas:      map[*scaleSide]*v1beta1.InferenceReplica{},
		cachedAt:      map[*scaleSide]int{},
	}
	w.addSide(workloadtypes.ComponentEngine, cfg.shape.prefill, cfg.settings.prefill, cfg.shape.pods)
	if cfg.shape.decode > 0 {
		w.addSide(workloadtypes.ComponentDecoder, cfg.shape.decode, cfg.settings.decode, cfg.shape.pods)
	}
	if cfg.settings.router {
		w.addSide(workloadtypes.ComponentRouter, 2, workloadtypes.UpdateStrategyRecreatePod, 1)
	}
	w.sides[0].partition = cfg.settings.partition
	if d := w.decode(); d != nil {
		d.partition = cfg.settings.decodePartition
	}
	w.defaults = coordination.GroupDefaults{PrefillDecode: coordination.PrefillDecodeRules{KeepRatio: cfg.settings.keepRatio, FirstSide: cfg.settings.firstSide}}
	if cfg.settings.keepRatio {
		w.defaults.RatioTolerancePercent = ptr.To(scaleRollKeepRatioTolerance)
	}
	if cfg.settings.canary {
		w.canaryStep = 0
	}
	w.isvc = w.service()
	if cfg.settings.policy {
		w.setPolicy(ptr.To(w.shapeRatio()))
	}
	w.recordPlan()
	for _, s := range w.sides {
		w.wire(s)
		w.render(s)
	}
	return w
}

// addSide builds one Component's loop: a harness on the shared clock with
// the gang size and count the shape names.
func (w *scaleWorld) addSide(comp workloadtypes.ComponentType, count int32, strategy workloadtypes.UpdateStrategyType, pods int32) {
	w.t.Helper()
	h := newComponentRecoveryHarness(w.t, pods > 1, "main", comp)
	h.ctx = logr.NewContext(context.Background(), logr.Discard())
	h.clk = w.clk
	h.workers = pods - 1
	h.replicas = count
	h.atomicStatus = true
	h.podGrace = scaleRollLeastStep
	h.forceDelete = &workloadtypes.ForceDeletePolicy{OverdueSlack: scaleRollOverdueSlack, NodeUnreachableThreshold: scaleRollNodeUnreachable}
	nodes := make([]string, 0, int(count*pods)/scaleRollPodsPerNode+2)
	for i := 0; i < cap(nodes); i++ {
		nodes = append(nodes, fmt.Sprintf("%s-node-%d", comp, i))
	}
	h.useNodes(nodes...)
	w.sides = append(w.sides, &scaleSide{
		comp:      comp,
		h:         h,
		strategy:  strategy,
		image:     goodImage,
		revisions: map[string]*appsv1.ControllerRevision{goodImage: h.revV1, badImage: h.revBad, fixedImage: h.revFixed},
	})
}

func (w *scaleWorld) prefill() *scaleSide { return w.sides[0] }

// decode is the decoder side, nil on an engine-only service.
func (w *scaleWorld) decode() *scaleSide {
	for _, s := range w.sides {
		if s.comp == workloadtypes.ComponentDecoder {
			return s
		}
	}
	return nil
}

// bounds is a Component's declared replica bounds on the service: a
// minimum of one and, when max is positive, a maximum.
func scaleBounds(max int32) v1beta1.ComponentExtensionSpec {
	out := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}
	if max > 0 {
		out.MaxReplicas = int(max)
	}
	return out
}

// service is the InferenceService the gates and the scaling coordinator
// read: the Components with their bounds, the rollout groups the settings
// name, and the canary's step while one runs.
func (w *scaleWorld) service() *v1beta1.InferenceService {
	settings := w.cfg.settings
	shape := w.cfg.shape
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: recoveryOwner, Namespace: recoveryNS, UID: "uid-1", Generation: 1}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: scaleBounds(2 * shape.prefill)}
	pair := []v1beta1.ComponentType{v1beta1.EngineComponent}
	if shape.decode > 0 {
		followerMax := int32(0)
		if settings.followerMax {
			followerMax = 2 * shape.decode
		}
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: scaleBounds(followerMax)}
		pair = append(pair, v1beta1.DecoderComponent)
	}
	members := append([]v1beta1.ComponentType(nil), pair...)
	if settings.router {
		isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: scaleBounds(4)}
		members = append(members, v1beta1.RouterComponent)
	}
	var groups []v1beta1.RolloutGroup
	switch {
	case settings.blueGreen:
		groups = []v1beta1.RolloutGroup{{Components: members, BlueGreen: &v1beta1.GroupBlueGreen{}}}
	case settings.canary:
		groups = []v1beta1.RolloutGroup{{Components: pair, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 25},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}}}}
		isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhaseCanarying, Canary: &v1beta1.CanaryStatus{CurrentStep: 0}},
		}
	case settings.sequential:
		groups = []v1beta1.RolloutGroup{
			{Components: []v1beta1.ComponentType{v1beta1.DecoderComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
			{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}},
		}
	default:
		groups = []v1beta1.RolloutGroup{{Components: members, RollingUpdate: &v1beta1.GroupRollingUpdate{}}}
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: groups}
	w.applyRatioRule(isvc)
	return isvc
}

// applyRatioRule writes the group's ratio rule as the spec reads now.
func (w *scaleWorld) applyRatioRule(isvc *v1beta1.InferenceService) {
	group := &isvc.Spec.Rollout.Groups[0]
	group.MaintainRatio = nil
	if w.maintainRatio && group.RollingUpdate != nil {
		group.MaintainRatio = &v1beta1.MaintainRatio{Tolerance: ptr.To(scaleRollTolerance)}
	}
}

// recordPlan records the run's plan on the service, as the run layer does when
// a push opens a run: the gates read the recorded groups, not the live spec.
func (w *scaleWorld) recordPlan() {
	groups := make([]v1beta1.RolloutRunGroup, 0, len(w.isvc.Spec.Rollout.Groups))
	for i := range w.isvc.Spec.Rollout.Groups {
		groups = append(groups, v1beta1.RolloutRunGroup{Source: v1beta1.RolloutPlanSourceInline, Group: *w.isvc.Spec.Rollout.Groups[i].DeepCopy()})
	}
	w.isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		RunID:    "run",
		OpenedAt: metav1.NewTime(w.clk.Now()),
		Plan:     v1beta1.RolloutRunPlan{Groups: groups},
	}}
	w.recordRatioReference()
}

// recordedRatioRule reports whether the run's recorded plan carries the ratio rule.
func (w *scaleWorld) recordedRatioRule() bool {
	run := w.isvc.Status.Rollout.ActiveRun
	return len(run.Plan.Groups) > 0 && run.Plan.Groups[0].Group.MaintainRatio != nil
}

// desiredCounts is the count each side's spec asks for now.
func (w *scaleWorld) desiredCounts() map[v1beta1.ComponentType]int32 {
	out := map[v1beta1.ComponentType]int32{}
	for _, s := range w.sides {
		out[s.component()] = s.h.replicas
	}
	return out
}

// recordRatioReference writes the ratio reference the service pass records
// from the counts the spec asks for, while the recorded plan carries the rule.
func (w *scaleWorld) recordRatioReference() {
	if !w.recordedRatioRule() {
		w.isvc.Status.RolloutCoordination = nil
		return
	}
	group := w.isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group
	w.isvc.Status.RolloutCoordination = &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{{
		Name:          "0",
		Components:    group.Components,
		Policy:        v1beta1.CoordinationPolicyRollingUpdate,
		ObservedRatio: &v1beta1.RolloutCoordinationRatio{Original: coordination.SnapshotOriginal(w.desiredCounts())},
	}}}
}

// shapeRatio is the largest three-decimal ratio under which the decoder's
// count follows the engine's at the shape's own counts.
func (w *scaleWorld) shapeRatio() resource.Quantity {
	milli := int64(w.cfg.shape.decode) * 1000 / int64(w.cfg.shape.prefill)
	return *resource.NewMilliQuantity(milli, resource.DecimalSI)
}

// setPolicy writes a proportional policy (the decoder follows the engine at
// ratio) on the service, or removes it when ratio is nil.
func (w *scaleWorld) setPolicy(ratio *resource.Quantity) {
	if ratio == nil {
		w.policy = nil
		w.isvc.Spec.ScalingPolicy = nil
		return
	}
	w.policy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent,
		Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: *ratio},
	}}
	w.isvc.Spec.ScalingPolicy = w.policy.DeepCopy()
}

// policyComponents are the component specs and modes the scaling
// coordinator resolves a policy against.
func (w *scaleWorld) policyComponents() (map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec, map[v1beta1.ComponentType]constants.DeploymentModeType) {
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent: &w.isvc.Spec.Engine.ComponentExtensionSpec,
	}
	if w.isvc.Spec.Decoder != nil {
		components[v1beta1.DecoderComponent] = &w.isvc.Spec.Decoder.ComponentExtensionSpec
	}
	if w.isvc.Spec.Router != nil {
		components[v1beta1.RouterComponent] = &w.isvc.Spec.Router.ComponentExtensionSpec
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{}
	for c := range components {
		modes[c] = constants.OMENative
	}
	return components, modes
}

// wire installs the coordination gates on a side's loop the way the
// replica adapter does: the start-time gate, the plan gate and the
// drain-time pairing gate, every one reading the live picture of the
// whole service.
func (w *scaleWorld) wire(s *scaleSide) {
	comp := s.component()
	s.h.gate = func(from string, strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, workloadtypes.RolloutHoldGate, string) {
		w.gateCalls++
		w.consult++
		allowed, gate, reason := coordination.EvaluateUpdateGate(w.ctx, w.reader(), w.isvc, comp, from, s.h.target, nil, w.defaults, strategy, inFlightSurge, inFlightUnavail)
		return allowed, workloadtypes.RolloutHoldGate(gate), reason
	}
	s.h.planGate = func() (bool, workloadtypes.RolloutHoldGate, string) {
		allowed, gate, reason := coordination.EvaluatePlanGate(w.isvc, comp)
		return allowed, workloadtypes.RolloutHoldGate(gate), reason
	}
	s.h.drainGate = func(sourcePods []string) (bool, workloadtypes.RolloutHoldGate, string) {
		w.gateCalls++
		w.consult++
		allowed, gate, reason := coordination.EvaluatePairingDrain(w.ctx, w.reader(), w.isvc, comp, s.h.target, w.defaults, sourcePods)
		return allowed, workloadtypes.RolloutHoldGate(gate), reason
	}
}

// render writes a side's spec into its loop: the strategy on the
// configuration's budget, the operator's partition, the target revision
// of the image the spec names, the pause, and the partition a canary
// step projects.
func (w *scaleWorld) render(s *scaleSide) {
	w.t.Helper()
	rolling := w.rollingUpdateOf(s)
	if s.partition > 0 {
		rolling.Partition = ptr.To(s.partition)
	}
	s.h.lifecycle.UpdateStrategy = &workloadtypes.UpdateStrategy{Type: s.strategy, RollingUpdate: rolling}
	rev := s.revisions[s.image]
	if rev == nil {
		rev = s.h.pushRevision(s.image)
		s.revisions[s.image] = rev
	}
	s.h.setTarget(rev, s.image)
	s.h.desired.Paused = s.paused
	if p := w.canaryPartition(s); p != nil {
		s.h.desired.Pacing = &workloadtypes.WorkloadPacing{Partition: p}
	}
}

// rollingUpdateOf is a side's rollingUpdate block before the partition:
// the fields a strategy or budget move wrote, else the configuration's
// budget on the field the strategy uses.
func (w *scaleWorld) rollingUpdateOf(s *scaleSide) *workloadtypes.RollingUpdate {
	rolling := &workloadtypes.RollingUpdate{}
	if s.rolling.written {
		rolling.MaxSurge = budgetValue(s.rolling.surge)
		rolling.MaxUnavailable = budgetValue(s.rolling.unavailable)
		return rolling
	}
	budget := intstr.Parse(w.cfg.settings.budgetText())
	if s.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
		rolling.MaxSurge = &budget
	} else {
		rolling.MaxUnavailable = &budget
	}
	return rolling
}

// canaryPartition is the partition the canary projects on a side of the
// canary unit, through the canary's own arithmetic over the status the
// world keeps: the current step's while a record stands; 0 while a record
// a push back ended has an Instance still coming back; step 0's once none
// is, the hold the next push lands on.
func (w *scaleWorld) canaryPartition(s *scaleSide) *int32 {
	if !w.cfg.settings.canary || s.comp == workloadtypes.ComponentRouter {
		return nil
	}
	return canary.StepPartition(w.isvc, rollout.Policies{}, s.component(), s.h.replicas)
}

// scaleReader is the client.Reader the gates and the scaling coordinator
// read through: every side's pods and revisions from its own loop, and
// every side's replica as the projector and the status writer would have
// left it.
type scaleReader struct {
	w *scaleWorld
}

func (w *scaleWorld) reader() client.Reader { return scaleReader{w: w} }

func (r scaleReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch out := obj.(type) {
	case *v1beta1.InferenceReplica:
		for _, s := range r.w.sides {
			if key.Namespace != recoveryNS || key.Name != s.h.irKey().Name {
				continue
			}
			ir, err := r.w.replicaOf(s)
			if err != nil {
				return err
			}
			ir.DeepCopyInto(out)
			return nil
		}
		return apierrors.NewNotFound(v1beta1.Resource("inferencereplicas"), key.Name)
	case *v1beta1.InferenceService:
		if key.Namespace == r.w.isvc.Namespace && key.Name == r.w.isvc.Name {
			r.w.isvc.DeepCopyInto(out)
			return nil
		}
		return apierrors.NewNotFound(v1beta1.Resource("inferenceservices"), key.Name)
	}
	var last error
	for _, s := range r.w.sides {
		err := s.h.c.Get(ctx, key, obj, opts...)
		if err == nil || !apierrors.IsNotFound(err) {
			return err
		}
		last = err
	}
	return last
}

func (r scaleReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	switch out := list.(type) {
	case *corev1.PodList:
		out.Items = nil
		for _, s := range r.w.sides {
			part := &corev1.PodList{}
			if err := s.h.c.List(ctx, part, opts...); err != nil {
				return err
			}
			out.Items = append(out.Items, part.Items...)
		}
		return nil
	case *appsv1.ControllerRevisionList:
		out.Items = nil
		for _, s := range r.w.sides {
			part := &appsv1.ControllerRevisionList{}
			if err := s.h.c.List(ctx, part, opts...); err != nil {
				return err
			}
			out.Items = append(out.Items, part.Items...)
		}
		return nil
	case *v1beta1.InferenceReplicaList:
		out.Items = nil
		for _, s := range r.w.sides {
			ir, err := r.w.replicaOf(s)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			out.Items = append(out.Items, *ir)
		}
		return nil
	}
	return r.w.sides[0].h.c.List(ctx, list, opts...)
}

// replicaOf is a side's InferenceReplica as the service pass projects it
// and the status writer publishes it: the stored rows under the spec the
// side rolls on, with the counters, the revisions and the retry records
// the gates read.
func (w *scaleWorld) replicaOf(s *scaleSide) (*v1beta1.InferenceReplica, error) {
	if at, ok := w.cachedAt[s]; ok && at == w.consult {
		return w.replicas[s], nil
	}
	h := s.h
	ir := &v1beta1.InferenceReplica{}
	if err := h.c.Get(w.ctx, h.irKey(), ir); err != nil {
		return nil, err
	}
	ir.Spec.ParentRef = &v1beta1.ParentReference{Name: recoveryOwner}
	ir.Spec.Component = s.component()
	ir.Spec.Replicas = ptr.To(h.replicas)
	ir.Spec.Lifecycle = w.lifecycleSpec(s)
	ir.Spec.Runners = nil
	for _, r := range h.desired.Runners {
		ir.Spec.Runners = append(ir.Spec.Runners, v1beta1.Runner{Name: v1beta1.RunnerName(r.Name), Size: r.Size})
	}
	if h.desired.Pacing != nil && h.desired.Pacing.Partition != nil {
		ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(*h.desired.Pacing.Partition)}
	}
	if ir.Annotations == nil {
		ir.Annotations = map[string]string{}
	}
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = strconv.FormatInt(w.isvc.Generation, 10)
	ir.Generation, ir.Status.ObservedGeneration = 1, 1
	_, counters := h.publication()
	ir.Status.Replicas = counters.Replicas
	ir.Status.ReadyReplicas = counters.ReadyReplicas
	ir.Status.ServingReplicas = counters.ServingReplicas
	ir.Status.AvailableReplicas = counters.AvailableReplicas
	ir.Status.UpdatedReplicas = counters.UpdatedReplicas
	ir.Status.CurrentRevision = h.currentRevision
	ir.Status.UpdateRevision = h.target.Name
	ir.Status.RetryBlocks = nil
	for _, b := range h.blocks {
		ir.Status.RetryBlocks = append(ir.Status.RetryBlocks, v1beta1.RetryBlock{
			TargetRevision: b.TargetRevision, State: v1beta1.RetryBlockState(b.State), AttemptsStarted: b.AttemptsStarted,
			NextRetryAt: b.NextRetryAt, FirstFailureAt: b.FirstFailureAt, LastFailureAt: b.LastFailureAt, Reason: b.Reason,
		})
	}
	ready := metav1.ConditionFalse
	if w.sideComplete(s) {
		ready = metav1.ConditionTrue
	}
	ir.Status.Conditions = []metav1.Condition{{Type: scaleRollReadyType, Status: ready, Reason: "Observed", LastTransitionTime: metav1.NewTime(w.clk.Now())}}
	w.replicas[s], w.cachedAt[s] = ir, w.consult
	return ir, nil
}

// lifecycleSpec is a side's update strategy as the projector writes it on
// the replica.
func (w *scaleWorld) lifecycleSpec(s *scaleSide) *v1beta1.LifecycleSpec {
	strategy := s.h.lifecycle.UpdateStrategy
	out := &v1beta1.UpdateStrategy{Type: v1beta1convert.UpdateStrategyTypeFromWorkload(strategy.Type)}
	if strategy.RollingUpdate != nil {
		out.RollingUpdate = &v1beta1.RollingUpdate{
			MaxSurge:       strategy.RollingUpdate.MaxSurge,
			MaxUnavailable: strategy.RollingUpdate.MaxUnavailable,
			Partition:      strategy.RollingUpdate.Partition,
		}
	}
	return &v1beta1.LifecycleSpec{UpdateStrategy: out}
}

// scaleRollStepCap bounds how far one tick moves the clock when every pass
// asked for a long wait, so the kubelet model keeps running between the
// engine's waits.
const scaleRollStepCap = 2 * time.Minute

// scaleRollLeastStep is the least the clock moves between passes and the
// grace a pod gets to stop: the step the harness paces by.
const scaleRollLeastStep = 30 * time.Second

// note records a finding in its class: the first one with the path and the
// picture that reached it, every one in the count.
func (w *scaleWorld) note(class, format string, args ...any) {
	w.counts[class]++
	if _, ok := w.findings[class]; ok {
		return
	}
	w.findings[class] = scaleFinding{class: class, message: fmt.Sprintf(format, args...), path: strings.Join(w.path, " -> "), state: w.picture()}
}

// servicePass is the part of the service pass the engine loops depend on:
// the follower's count from the scaling policy, the ratio reference from
// the counts the spec asks for, and the canary step's partition on each
// side of the unit.
func (w *scaleWorld) servicePass() {
	w.t.Helper()
	w.consult++
	if w.policy != nil {
		components, modes := w.policyComponents()
		targets, err := scaling.ResolveTargets(w.ctx, w.reader(), w.isvc, w.policy, "isvc", components, modes)
		if err != nil {
			w.note("error", "the scaling coordinator failed: %v", err)
		} else if n := targets.Replicas(v1beta1.DecoderComponent); n != nil && *n != w.decode().h.replicas {
			w.setReplicas(w.decode(), *n)
		}
	}
	w.recordRatioReference()
	w.settleUnitPhase()
	for _, s := range w.sides {
		if p := w.canaryPartition(s); p != nil {
			s.h.desired.Pacing = &workloadtypes.WorkloadPacing{Partition: p}
		}
	}
}

// settleUnitPhase moves a canary unit with no record from RollingBack to
// Stable once no Instance of the pair runs a revision other than its
// side's, as the canary's own pass does when the last Instance of a roll
// the operator took back is back.
func (w *scaleWorld) settleUnitPhase() {
	if !w.cfg.settings.canary || w.canaryStep >= 0 {
		return
	}
	unit := w.isvc.Status.Components[v1beta1.EngineComponent]
	if phase := w.phaseWithNoRecord(); unit.RolloutPhase != phase {
		unit.RolloutPhase = phase
		w.isvc.Status.Components[v1beta1.EngineComponent] = unit
	}
}

// setReplicas writes a side's count, as a scale of the service or the
// coordinator's follower write would.
func (w *scaleWorld) setReplicas(s *scaleSide, n int32) {
	w.t.Helper()
	if n < 1 {
		n = 1
	}
	if n == s.h.replicas {
		return
	}
	s.h.replicas = n
	w.scaled = true
	w.startedAtScale = map[*scaleSide]map[int32]bool{}
	w.indicesAtScale = map[*scaleSide][]int32{}
	for _, side := range w.sides {
		w.startedAtScale[side] = map[int32]bool{}
		if !w.sideComplete(side) {
			w.startedAtScale[side] = w.startedIndices(side)
		}
		for _, r := range w.rows(side) {
			w.indicesAtScale[side] = append(w.indicesAtScale[side], r.Index)
		}
	}
	w.render(s)
}

// tick runs the service pass, then one engine pass per side, then moves
// the clock by the shortest wait a pass asked for, at least the step. It
// returns the state's key.
func (w *scaleWorld) tick() string {
	w.t.Helper()
	w.servicePass()
	var wait time.Duration
	w.waiting = 0
	for _, s := range w.sides {
		res, err := s.h.pass()
		w.passes++
		if err != nil {
			w.note("error", "%s: Reconcile returned an error: %v", s.comp, err)
		}
		if res.RequeueAfter > w.waiting {
			w.waiting = res.RequeueAfter
		}
		advance := scaleRollLeastStep
		if res.RequeueAfter > advance {
			advance = res.RequeueAfter
		}
		if wait == 0 || advance < wait {
			wait = advance
		}
	}
	if wait > scaleRollStepCap || w.unchanged > 0 {
		wait = scaleRollStepCap
	}
	w.clk.Step(wait)
	w.ticks++
	key := w.key()
	w.seen[key] = struct{}{}
	w.check()
	return key
}

// scaleSettling watches the state across ticks and says when it has
// settled: one unchanged tick for a complete roll; otherwise
// scaleRollSettleTicks unchanged ticks spanning scaleRollSettleSpan of the
// clock, with no engine timer still to run (a pass asking for more than
// its cadence is waiting on one: a retry delay, a grace, a deadline).
type scaleSettling struct {
	prev      string
	changedAt time.Time
}

func (w *scaleWorld) startSettling() *scaleSettling {
	w.unchanged = 0
	return &scaleSettling{prev: w.key(), changedAt: w.clk.Now()}
}

// observe takes the key of the tick that just ran and reports whether the
// world has settled.
func (t *scaleSettling) observe(w *scaleWorld, key string) bool {
	if key != t.prev {
		t.prev, t.changedAt, w.unchanged = key, w.clk.Now(), 0
		return false
	}
	w.unchanged++
	if w.unchanged >= 1 && w.complete() {
		return true
	}
	return w.unchanged >= scaleRollSettleTicks && w.clk.Now().Sub(t.changedAt) >= scaleRollSettleSpan && w.waiting <= testRequeueIntervals.Operation
}

// playOut ticks until the world has settled or the bound is reached; it
// reports whether the world settled.
func (w *scaleWorld) playOut(maxTicks int) bool {
	w.t.Helper()
	settling := w.startSettling()
	for i := 0; i < maxTicks; i++ {
		if settling.observe(w, w.tick()) {
			return true
		}
	}
	return false
}

// short names a revision by the image it renders: v1, v2, v3 or bad.
func (w *scaleWorld) short(s *scaleSide, name string) string {
	for image, cr := range s.revisions {
		if cr.Name == name {
			return scaleImageShort(image)
		}
	}
	if name == "" {
		return "-"
	}
	return name
}

func scaleImageShort(image string) string {
	switch image {
	case goodImage:
		return "v1"
	case fixedImage:
		return "v2"
	case scaleRollThirdImage:
		return "v3"
	case badImage:
		return "bad"
	}
	return image
}

// rows is a side's rows in index order.
func (w *scaleWorld) rows(s *scaleSide) []workloadtypes.InstanceStatus {
	rows := s.h.irStatuses()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Index < rows[j].Index })
	return rows
}

// pods is every pod object of a side, the ones being deleted included,
// in name order.
func (w *scaleWorld) pods(s *scaleSide) []*corev1.Pod {
	w.t.Helper()
	list := &corev1.PodList{}
	if err := s.h.c.List(w.ctx, list, client.InNamespace(recoveryNS)); err != nil {
		w.t.Fatalf("list pods: %v", err)
	}
	out := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// key is the state: every side's spec, rows, retry records and pods, and
// the service's policy, ratio rule and canary step.
func (w *scaleWorld) key() string {
	var b strings.Builder
	for _, s := range w.sides {
		fmt.Fprintf(&b, "%s[n=%d p=%d pause=%t img=%s %s %s", s.comp, s.h.replicas, s.partition, s.paused, scaleImageShort(s.image), strategyShort(s.strategy), s.rolling)
		if s.h.desired.Pacing != nil && s.h.desired.Pacing.Partition != nil {
			fmt.Fprintf(&b, " step=%d", *s.h.desired.Pacing.Partition)
		}
		b.WriteString("]")
		for _, r := range w.rows(s) {
			fmt.Fprintf(&b, "%d:%s:%s>%s:%d", r.Index, r.Phase, w.short(s, r.RunningRevision), w.short(s, r.TargetRevision), r.Incarnation)
			if r.Operation != nil {
				fmt.Fprintf(&b, ":%s/%s/%s", r.Operation.Type, r.Operation.Step, r.Operation.Waiting)
				if r.Operation.SurgeIndex != nil {
					fmt.Fprintf(&b, "/s%d", *r.Operation.SurgeIndex)
				}
			}
			b.WriteString(";")
		}
		b.WriteString("#")
		for _, blk := range s.h.blocks {
			fmt.Fprintf(&b, "%s:%s:%d;", w.short(s, blk.TargetRevision), blk.State, blk.AttemptsStarted)
		}
		b.WriteString("#")
		for _, pod := range w.pods(s) {
			fmt.Fprintf(&b, "%s:%s:%t:%t:%t;", strings.TrimPrefix(pod.Name, recoveryOwner+"-"), pod.Status.Phase,
				podConditionTrue(pod, corev1.PodReady), podreadiness.IsServing(pod), pod.DeletionTimestamp != nil)
		}
		b.WriteString("|")
	}
	fmt.Fprintf(&b, "policy=%s ratio=%t canary=%d/%s", w.policyText(), w.recordedRatioRule(), w.canaryStep, w.isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase)
	return b.String()
}

// policyText names the scaling policy in force.
func (w *scaleWorld) policyText() string {
	if w.policy == nil {
		return "none"
	}
	ratio := w.policy.Proportional.Ratios[v1beta1.DecoderComponent]
	return "decoder=" + ratio.String()
}

// picture is one line per side a person can read: how many Instances the
// spec asks for and what the rows say.
func (w *scaleWorld) picture() string {
	var lines []string
	for _, s := range w.sides {
		kinds := map[string]int{}
		var order []string
		for _, r := range w.rows(s) {
			kind := fmt.Sprintf("%s on %s", strings.ToLower(string(r.Phase)), w.short(s, r.RunningRevision))
			if r.Operation != nil {
				kind += fmt.Sprintf(" %s/%s toward %s", strings.ToLower(string(r.Operation.Type)), r.Operation.Step, w.short(s, r.Operation.TargetRevision))
				if r.Operation.Waiting != "" {
					kind += " waiting " + string(r.Operation.Waiting)
				}
			}
			if _, seen := kinds[kind]; !seen {
				order = append(order, kind)
			}
			kinds[kind]++
		}
		var parts []string
		for _, kind := range order {
			parts = append(parts, fmt.Sprintf("%d %s", kinds[kind], kind))
		}
		line := fmt.Sprintf("%s wants %d: %s", s.comp, s.h.replicas, strings.Join(parts, ", "))
		if hold := s.h.hold; hold != nil {
			line += fmt.Sprintf("; holds on %s: %s", hold.Gate, hold.Reason)
		}
		for _, blk := range s.h.blocks {
			line += fmt.Sprintf("; retry record %s %s after %d attempt(s)", w.short(s, blk.TargetRevision), blk.State, blk.AttemptsStarted)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, " / ")
}

// effectivePartition is the partition a side rolls under: the canary
// step's when one is projected, else the operator's.
func (w *scaleWorld) effectivePartition(s *scaleSide) *int32 {
	var rolling *workloadtypes.RollingUpdate
	if s.h.lifecycle.UpdateStrategy != nil {
		rolling = s.h.lifecycle.UpdateStrategy.RollingUpdate
	}
	return workloadops.EffectivePartition(s.h.desired.Pacing, rolling)
}

// heldIndices is the set of Instances a side's partition keeps off the
// target, by the roll's own rule.
func (w *scaleWorld) heldIndices(s *scaleSide) map[int32]bool {
	rows := w.rows(s)
	observed := make([]*workloadtypes.InstanceStatus, 0, len(rows))
	for i := range rows {
		observed = append(observed, &rows[i])
	}
	return query.PartitionHeldIndices(w.effectivePartition(s), observed, s.h.target.Name)
}

// podsPerInstance is the pod count of one Instance of a side.
func (w *scaleWorld) podsPerInstance(s *scaleSide) int32 {
	var n int32
	for _, r := range s.h.desired.Runners {
		n += r.Size
	}
	return n
}

// livePodsByIndex buckets a side's live pods by Instance index, listed
// once.
func (w *scaleWorld) livePodsByIndex(s *scaleSide) map[int32][]*corev1.Pod {
	return query.BucketPodsByInstanceIdx(s.h.livePods())
}

// servingSet reports whether an Instance's whole pod set is Ready and in
// the serving gate.
func (w *scaleWorld) servingSet(s *scaleSide, pods []*corev1.Pod) bool {
	if int32(len(pods)) != w.podsPerInstance(s) {
		return false
	}
	for _, pod := range pods {
		if !podreadiness.IsServing(pod) || !podConditionTrue(pod, corev1.PodReady) {
			return false
		}
	}
	return true
}

// sideComplete reports a side at rest at the count its spec asks for:
// every Instance Ready with its whole pod set serving, on the target unless
// the partition holds it, with no operation open.
func (w *scaleWorld) sideComplete(s *scaleSide) bool {
	rows := w.rows(s)
	if int32(len(rows)) != s.h.replicas {
		return false
	}
	held := w.heldIndices(s)
	byIndex := w.livePodsByIndex(s)
	for _, r := range rows {
		if r.Phase != workloadtypes.InstancePhaseReady || r.Operation != nil {
			return false
		}
		if r.RunningRevision != s.h.target.Name && !held[r.Index] {
			return false
		}
		if !w.servingSet(s, byIndex[r.Index]) {
			return false
		}
	}
	return true
}

// complete reports every side complete.
func (w *scaleWorld) complete() bool {
	for _, s := range w.sides {
		if !w.sideComplete(s) {
			return false
		}
	}
	return true
}

// scaleSnapshot is the world at one moment, to be restored for every move
// tried from it.
type scaleSnapshot struct {
	now           time.Time
	sides         []*repairSnapshot
	specs         []scaleSide
	isvc          *v1beta1.InferenceService
	policy        *v1beta1.ScalingPolicy
	maintainRatio bool
	canaryStep    int32
	scaled        bool
	path          []string
	startedAt     []map[int32]bool
	indicesAt     [][]int32
}

func (w *scaleWorld) snapshot() *scaleSnapshot {
	w.t.Helper()
	snap := &scaleSnapshot{
		now:           w.clk.Now(),
		isvc:          w.isvc.DeepCopy(),
		policy:        w.policy.DeepCopy(),
		maintainRatio: w.maintainRatio,
		canaryStep:    w.canaryStep,
		scaled:        w.scaled,
		path:          append([]string(nil), w.path...),
	}
	for _, s := range w.sides {
		snap.sides = append(snap.sides, snapshotHarness(s.h))
		spec := *s
		spec.h = nil
		spec.revisions = cloneMap(s.revisions)
		snap.specs = append(snap.specs, spec)
		snap.startedAt = append(snap.startedAt, cloneMap(w.startedAtScale[s]))
		snap.indicesAt = append(snap.indicesAt, append([]int32(nil), w.indicesAtScale[s]...))
	}
	return snap
}

// restore puts the world back at a snapshot: new loops over copies of the
// snapshot's objects, one new clock, the gates wired again.
func (w *scaleWorld) restore(snap *scaleSnapshot) {
	w.t.Helper()
	w.clk = clocktesting.NewFakeClock(snap.now)
	rebuild := &repairWorld{t: w.t, scheme: makeScheme(w.t)}
	for i, spec := range snap.specs {
		s := w.sides[i]
		*s = spec
		s.revisions = cloneMap(spec.revisions)
		s.h = rebuild.restore(snap.sides[i])
		s.h.clk = w.clk
		s.h.recorder = nil
		w.wire(s)
	}
	w.isvc = snap.isvc.DeepCopy()
	w.policy = snap.policy.DeepCopy()
	w.maintainRatio = snap.maintainRatio
	w.canaryStep = snap.canaryStep
	w.scaled = snap.scaled
	w.belowCount = map[*scaleSide]int{}
	w.faulted = map[*scaleSide]map[int32]bool{}
	for _, s := range w.sides {
		w.surging[s] = w.surgingSources(s)
	}
	w.startedAtScale = map[*scaleSide]map[int32]bool{}
	w.indicesAtScale = map[*scaleSide][]int32{}
	for i, s := range w.sides {
		if snap.startedAt[i] != nil {
			w.startedAtScale[s] = cloneMap(snap.startedAt[i])
		}
		w.indicesAtScale[s] = append([]int32(nil), snap.indicesAt[i]...)
	}
	w.path = append([]string(nil), snap.path...)
	for k := range w.cachedAt {
		delete(w.cachedAt, k)
	}
}

// reachIdle brings every side up at its count on version 1, with the
// checks off: the first creation of a fleet is not a roll.
func (w *scaleWorld) reachIdle() {
	w.t.Helper()
	w.checking = false
	w.path = []string{"idle on v1"}
	if !w.playOut(60) || !w.complete() {
		w.t.Fatalf("%s: the service did not come up on version 1: %s", w.cfg, w.picture())
	}
	w.checking = true
	w.scaled = false
}

// push points every side at image and records the plan the spec names
// now, as the run layer does when a push opens a run; under a canary the
// record follows the push as the engine keeps it.
func (w *scaleWorld) push(image string) {
	w.t.Helper()
	w.noteStable()
	for _, s := range w.sides {
		s.image = image
	}
	w.recordPlan()
	w.canaryRecordForPush()
	for _, s := range w.sides {
		w.render(s)
	}
	w.path = append(w.path, "push "+scaleImageShort(image))
}

// noteStable records, per side, the revision the run opens from: the one
// every Instance runs when a push lands on a side at rest; a push while a
// roll is in flight keeps the one recorded, as the run layer keeps the
// stable revision across retargets.
func (w *scaleWorld) noteStable() {
	for _, s := range w.sides {
		if s.stable == "" || w.everyRowRuns(s, s.h.target.Name) {
			s.stable = s.h.target.Name
		}
	}
}

// everyRowRuns reports every Instance of a side running revision name.
func (w *scaleWorld) everyRowRuns(s *scaleSide, name string) bool {
	for _, r := range w.rows(s) {
		if r.RunningRevision != name {
			return false
		}
	}
	return true
}

// pair is the prefill side and, when the service has one, the decode
// side, at the indices a move names them by.
func (w *scaleWorld) pair() []*scaleSide {
	sides := []*scaleSide{w.prefill()}
	if d := w.decode(); d != nil {
		sides = append(sides, d)
	}
	return sides
}

// canaryRecordForPush keeps the canary's record as the engine keeps it
// across a push. A push back to the revision the run opened from ends the
// record (canary/holds.go endTakenBackCanary): the run records every
// member of the unit at its stable revision, the unit keeps no record and
// reads RollingBack while an Instance still runs another revision, so the
// canary projects partition 0 by its own rule and every Instance returns.
// A push of another revision while no record stands opens one at step 0.
// A push while the record's own roll stands changes nothing here.
func (w *scaleWorld) canaryRecordForPush() {
	if !w.cfg.settings.canary {
		return
	}
	takenBack := true
	var targets []v1beta1.RolloutRunTarget
	for _, s := range w.pair() {
		rev := s.revisions[s.image]
		if rev == nil || rev.Name != s.stable {
			takenBack = false
			break
		}
		hash := query.RevisionHashFromControllerRevisionName(rev.Name)
		targets = append(targets, v1beta1.RolloutRunTarget{Component: s.component(), Revision: hash, StableRevision: hash})
	}
	unit := w.isvc.Status.Components[v1beta1.EngineComponent]
	switch {
	case takenBack:
		w.isvc.Status.Rollout.ActiveRun.TargetRevisions = targets
		unit.Canary = nil
		unit.RolloutPhase = w.phaseWithNoRecord()
		w.canaryStep = -1
	case w.canaryStep < 0:
		unit.Canary = &v1beta1.CanaryStatus{CurrentStep: 0}
		unit.RolloutPhase = v1beta1.RolloutPhaseCanarying
		w.canaryStep = 0
	default:
		return
	}
	w.isvc.Status.Components[v1beta1.EngineComponent] = unit
}

// phaseWithNoRecord is the phase a canary unit with no record reads
// (canary/holds.go idlePhase): RollingBack while an Instance of the pair
// still runs a revision other than the one the spec names for its side,
// the Instances of a roll the operator took back coming back with no
// step; Stable once none does.
func (w *scaleWorld) phaseWithNoRecord() v1beta1.RolloutPhase {
	for _, s := range w.pair() {
		rev := s.revisions[s.image]
		if rev == nil {
			continue
		}
		for _, r := range w.rows(s) {
			if r.RunningRevision != "" && r.RunningRevision != rev.Name {
				return v1beta1.RolloutPhaseRollingBack
			}
		}
	}
	return v1beta1.RolloutPhaseStable
}

// pushHeld pushes an image no pod can pull on the decode side and version
// 2 on the prefill side; where the decoder is named to move first, both
// sides get the unpullable image, so the prefill side waits on a held
// decoder with its own push still to make.
func (w *scaleWorld) pushHeld() {
	w.t.Helper()
	w.noteStable()
	both := w.cfg.settings.firstSide == v1beta1.DecoderComponent
	for _, s := range w.sides {
		s.image = fixedImage
		if s == w.heldSide() || (both && s.comp == workloadtypes.ComponentEngine) {
			s.image = badImage
		}
		w.render(s)
	}
	w.recordPlan()
	switch {
	case both:
		w.path = append(w.path, "push an image no pod can pull, the decoder first")
	case w.decode() == nil:
		w.path = append(w.path, "push an image no pod can pull")
	default:
		w.path = append(w.path, "push v2 with the decoder's image unpullable")
	}
}

// heldSide is the side the held roll pushes to an image no pod can pull:
// the decoder, or the engine of an engine-only service.
func (w *scaleWorld) heldSide() *scaleSide {
	if d := w.decode(); d != nil {
		return d
	}
	return w.prefill()
}

// rollCounts reads a side's rows as the moments are named: attempts open,
// Instances promoted, Instances still on the old version, and whether a
// pod of an open attempt is being deleted.
func (w *scaleWorld) rollCounts(s *scaleSide) (open, promoted, old int32, draining bool) {
	deleting := map[int32]bool{}
	for _, pod := range w.pods(s) {
		if idx, ok := query.InstanceIdxFromLabels(pod); ok && pod.DeletionTimestamp != nil {
			deleting[idx] = true
		}
	}
	for _, r := range w.rows(s) {
		switch {
		case r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationUpdate:
			open++
			if deleting[r.Index] {
				draining = true
			}
		case r.Phase == workloadtypes.InstancePhaseReady && r.Operation == nil && r.RunningRevision == s.h.target.Name:
			promoted++
		case r.Phase == workloadtypes.InstancePhaseReady && r.Operation == nil:
			old++
		}
	}
	return open, promoted, old, draining
}

// momentReached reports whether the prefill side's roll stands at the
// moment: settling before any Instance serves the new version, rotating
// with new and old Instances beside an open attempt, draining while an old
// pod leaves, the last Instance moving, or the decoder held on a spent
// retry schedule.
func (w *scaleWorld) momentReached(m scaleMoment) bool {
	if m == scaleAtHeld {
		d := w.heldSide()
		return d.h.heldOn(d.h.target.Name)
	}
	s := w.prefill()
	open, promoted, old, draining := w.rollCounts(s)
	held := int32(len(w.heldIndices(s)))
	switch m {
	case scaleAtSettling:
		return open > 0 && promoted == 0
	case scaleAtRotating:
		return open > 0 && promoted > 0 && old > held
	case scaleAtDraining:
		return draining
	case scaleAtLast:
		return open > 0 && old <= held && promoted > 0
	}
	return false
}

// captureMoments plays the plain roll from idle to its end and keeps a
// snapshot at the first tick of each moment, then plays the held roll from
// idle. A plain roll that does not finish is a finding of its own.
func (w *scaleWorld) captureMoments(maxTicks int) map[scaleMoment]*scaleSnapshot {
	w.t.Helper()
	snaps := w.captureRollMoments(maxTicks)
	w.captureHeldMoment(snaps, maxTicks)
	return snaps
}

// captureRollMoments plays the plain roll from idle to its end and keeps
// a snapshot at the first tick of each moment, the canary step included
// when the pair rolls as a canary unit.
func (w *scaleWorld) captureRollMoments(maxTicks int) map[scaleMoment]*scaleSnapshot {
	w.t.Helper()
	snaps := map[scaleMoment]*scaleSnapshot{scaleAtIdle: w.snapshot()}
	w.push(fixedImage)
	settling := w.startSettling()
	settled := false
	for i := 0; i < maxTicks && !settled; i++ {
		for _, m := range []scaleMoment{scaleAtSettling, scaleAtRotating, scaleAtDraining, scaleAtLast} {
			if snaps[m] == nil && w.momentReached(m) {
				w.path = append(w.path, "at "+m.String())
				snaps[m] = w.snapshot()
				w.path = w.path[:len(w.path)-1]
			}
		}
		settled = settling.observe(w, w.tick())
	}
	w.path = append(w.path, "plain roll")
	w.classifyEnd(settled)
	if w.canaryStep >= 0 && settled && w.complete() {
		w.path = []string{"idle on v1", "push v2"}
		snaps[scaleAtCanaryStep] = w.snapshot()
	}
	return snaps
}

// captureHeldMoment plays the held roll from the idle snapshot and keeps
// the state where the held side's retry schedule is spent.
func (w *scaleWorld) captureHeldMoment(snaps map[scaleMoment]*scaleSnapshot, maxTicks int) {
	w.t.Helper()
	w.restore(snaps[scaleAtIdle])
	w.pushHeld()
	settled := w.playOut(maxTicks)
	if settled && w.momentReached(scaleAtHeld) {
		snaps[scaleAtHeld] = w.snapshot()
	} else {
		w.note("held-not-reached", "the held side's retry schedule was not spent within the bound: %s", w.picture())
	}
}

// placedNode is the node of the first placed pod among pods, or empty.
func placedNode(pods []*corev1.Pod) string {
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			return pod.Spec.NodeName
		}
	}
	return ""
}

// stepOf is a side's step at the configuration's budget of n, rounded up
// as the budgets round.
func (w *scaleWorld) stepOf(n int32) int32 {
	budget := intstr.Parse(w.cfg.settings.budgetText())
	return max(utils.ScaledCountFromIntOrString(&budget, n, true), 1)
}

// loss picks the Instances a world move touches on a side: the lowest
// Instance still on the old version and the highest promoted one, falling
// back to any Instance with a pod.
func (w *scaleWorld) lossIndices(s *scaleSide) []int32 {
	var oldest, newest int32 = -1, -1
	byIndex := w.livePodsByIndex(s)
	for _, r := range w.rows(s) {
		if len(byIndex[r.Index]) == 0 {
			continue
		}
		if r.RunningRevision == s.h.target.Name {
			newest = r.Index
		} else if oldest < 0 {
			oldest = r.Index
		}
	}
	var out []int32
	if oldest >= 0 {
		out = append(out, oldest)
	}
	if newest >= 0 && newest != oldest {
		out = append(out, newest)
	}
	return out
}

// movesAt lists the moves tried from a moment of this configuration: the
// whole list in full mode, the bounded list otherwise.
func (w *scaleWorld) movesAt(m scaleMoment, full bool) []scaleMove {
	if w.moves != nil {
		return w.moves(m, full)
	}
	if full {
		return w.allMoves(m)
	}
	return w.boundedMoves(m)
}

// boundedMoves keeps, per moment, the moves operators make
// most, so the bounded run stays inside its time.
func (w *scaleWorld) boundedMoves(m scaleMoment) []scaleMove {
	settings := w.cfg.settings
	prefill, decode := w.prefill(), w.decode()
	stepP := w.stepOf(prefill.h.replicas)
	up := func(delta int32) scaleMove {
		if settings.policy {
			return scaleMove{kind: moveAnchorScale, delta: delta}
		}
		return scaleMove{kind: moveScale, side: 0, delta: delta}
	}
	down := func() scaleMove {
		if settings.policy {
			return scaleMove{kind: moveAnchorScale, delta: -prefill.h.replicas / 2}
		}
		return scaleMove{kind: moveScale, side: 1, delta: -1}
	}
	var out []scaleMove
	switch m {
	case scaleAtIdle:
		out = append(out, up(stepP), down(), scaleMove{kind: movePause, side: 0, delta: 1})
		if !settings.policy {
			out = append(out, scaleMove{kind: moveScaleRatio, delta: stepP, delta2: -1})
		}
	case scaleAtSettling, scaleAtLast:
		out = append(out, up(1), down(), scaleMove{kind: moveRollBack})
	case scaleAtRotating:
		out = append(out, up(stepP), scaleMove{kind: moveScale, side: 0, delta: -prefill.h.replicas / 2},
			scaleMove{kind: moveScale, side: 1, delta: 1}, scaleMove{kind: movePause, side: 0, delta: 1}, scaleMove{kind: moveRollBack})
		if !settings.policy {
			out = append(out, scaleMove{kind: moveScaleBoth, delta: -1})
		}
		if settings.partition > 0 {
			out = append(out, scaleMove{kind: movePartition, side: 0, delta: 0}, scaleMove{kind: movePartition, side: 0, delta: settings.partition + 1})
		}
		if settings.canary {
			out = append(out, scaleMove{kind: moveCanaryPromote})
		}
	case scaleAtDraining:
		out = append(out, scaleMove{kind: moveScale, side: 0, delta: -1}, scaleMove{kind: moveScale, side: 1, delta: 1})
	case scaleAtHeld:
		out = append(out, up(1), scaleMove{kind: moveScale, side: 1, delta: 1})
	case scaleAtCanaryStep:
		out = append(out, up(stepP), down(), scaleMove{kind: moveCanaryPromote})
	}
	if m == scaleAtIdle || m == scaleAtRotating {
		for _, idx := range w.lossIndices(decode) {
			out = append(out, scaleMove{kind: moveLosePod, side: 1, index: idx})
			break
		}
	}
	return out
}

// allMoves lists every move of a moment: every scale of one side, both
// sides and the ratio by one, a step, half and the whole count; the same
// through the anchor of a policy; the policy and the ratio rule added,
// edited or removed; the partition set, raised, lowered and cleared; the
// pause, the roll back, the second push; the world's losses on both
// sides; the canary promoted.
func (w *scaleWorld) allMoves(m scaleMoment) []scaleMove {
	settings := w.cfg.settings
	prefill, decode := w.prefill(), w.decode()
	stepP, stepD := w.stepOf(prefill.h.replicas), w.stepOf(decode.h.replicas)
	var out []scaleMove
	scales := func(side int, n, step int32) {
		out = append(out,
			scaleMove{kind: moveScale, side: side, delta: 1}, scaleMove{kind: moveScale, side: side, delta: -1},
			scaleMove{kind: moveScale, side: side, delta: step}, scaleMove{kind: moveScale, side: side, delta: -step},
			scaleMove{kind: moveScale, side: side, delta: n / 2}, scaleMove{kind: moveScale, side: side, delta: -n / 2},
			scaleMove{kind: moveScale, side: side, delta: n})
	}
	if settings.policy {
		out = append(out,
			scaleMove{kind: moveAnchorScale, delta: 1}, scaleMove{kind: moveAnchorScale, delta: -1},
			scaleMove{kind: moveAnchorScale, delta: stepP}, scaleMove{kind: moveAnchorScale, delta: -prefill.h.replicas / 2},
			scaleMove{kind: moveAnchorScale, delta: prefill.h.replicas},
			scaleMove{kind: movePolicyEdit}, scaleMove{kind: movePolicyRemove})
		scales(1, decode.h.replicas, stepD)
	} else {
		scales(0, prefill.h.replicas, stepP)
		scales(1, decode.h.replicas, stepD)
		out = append(out,
			scaleMove{kind: moveScaleBoth, delta: 1}, scaleMove{kind: moveScaleBoth, delta: -1},
			scaleMove{kind: moveScaleRatio, delta: stepP, delta2: -1}, scaleMove{kind: moveScaleRatio, delta: -stepP, delta2: 1},
			scaleMove{kind: movePolicyAdd})
	}
	if settings.partition > 0 {
		out = append(out,
			scaleMove{kind: movePartition, side: 0, delta: 0},
			scaleMove{kind: movePartition, side: 0, delta: settings.partition + 1},
			scaleMove{kind: movePartition, side: 0, delta: settings.partition - 1})
	} else {
		out = append(out, scaleMove{kind: movePartition, side: 0, delta: 2})
	}
	switch {
	case settings.maintainRatio:
		out = append(out, scaleMove{kind: moveRatioRuleRemove})
	case !settings.canary:
		out = append(out, scaleMove{kind: moveRatioRuleAdd})
	}
	out = append(out, scaleMove{kind: movePause, side: 0, delta: 1})
	if m != scaleAtIdle {
		out = append(out, scaleMove{kind: moveRollBack}, scaleMove{kind: movePush})
	}
	if settings.canary {
		out = append(out, scaleMove{kind: moveCanaryPromote})
	}
	for side, s := range []*scaleSide{prefill, decode} {
		for _, idx := range w.lossIndices(s) {
			out = append(out, scaleMove{kind: moveLosePod, side: side, index: idx}, scaleMove{kind: moveLoseNode, side: side, index: idx})
			if w.cfg.shape.pods > 1 {
				out = append(out, scaleMove{kind: moveLoseMember, side: side, index: idx})
			}
		}
	}
	return out
}

// apply makes one move on the world.
func (w *scaleWorld) apply(mv scaleMove) {
	w.t.Helper()
	w.path = append(w.path, mv.String())
	switch mv.kind {
	case moveScale:
		s := w.sides[mv.side]
		w.setReplicas(s, s.h.replicas+mv.delta)
	case moveScaleBoth:
		for _, s := range []*scaleSide{w.prefill(), w.decode()} {
			w.setReplicas(s, s.h.replicas+mv.delta)
		}
	case moveScaleRatio:
		w.setReplicas(w.prefill(), w.prefill().h.replicas+mv.delta)
		w.setReplicas(w.decode(), w.decode().h.replicas+mv.delta2)
	case moveAnchorScale:
		w.setReplicas(w.prefill(), w.prefill().h.replicas+mv.delta)
	case movePartition:
		w.setPartition(w.sides[mv.side], mv.delta)
	case movePartitionTwice:
		w.setPartition(w.sides[mv.side], mv.delta)
		w.playOut(scaleTickBound(false))
		w.setPartition(w.sides[mv.side], mv.delta2)
	case movePolicyAdd:
		w.setPolicy(ptr.To(w.shapeRatio()))
	case movePolicyEdit:
		ratio := w.policy.Proportional.Ratios[v1beta1.DecoderComponent]
		w.setPolicy(resource.NewMilliQuantity(ratio.MilliValue()*3/2, resource.DecimalSI))
	case movePolicyRemove:
		w.setPolicy(nil)
	case moveRatioRuleAdd, moveRatioRuleRemove:
		w.maintainRatio = mv.kind == moveRatioRuleAdd
		w.applyRatioRule(w.isvc)
	case movePause:
		// The operator pauses, scales under the pause, and resumes one
		// pass later.
		w.setPaused(true)
		s := w.sides[mv.side]
		w.setReplicas(s, s.h.replicas+mv.delta)
		w.tick()
		w.setPaused(false)
	case moveResume:
		w.setPaused(false)
	case moveRollBack:
		w.push(goodImage)
	case movePush:
		w.push(scaleRollThirdImage)
	case moveLosePod:
		w.markFaulted(w.sides[mv.side], mv.index)
		w.sides[mv.side].h.losePods(mv.index)
	case moveLoseNode:
		// The node under the Instance's first placed pod; an Instance
		// whose pods are not placed yet loses the node under any placed
		// pod of its side. Every Instance with a pod on that node is hit.
		s := w.sides[mv.side]
		byIndex := w.livePodsByIndex(s)
		node := placedNode(byIndex[mv.index])
		for idx := range byIndex {
			if node != "" {
				break
			}
			node = placedNode(byIndex[idx])
		}
		if node != "" {
			for idx, pods := range byIndex {
				for _, pod := range pods {
					if pod.Spec.NodeName == node {
						w.markFaulted(s, idx)
						break
					}
				}
			}
			s.h.failNode(node)
		}
	case moveLoseMember:
		w.markFaulted(w.sides[mv.side], mv.index)
		w.sides[mv.side].h.loseRunner(mv.index, "worker")
	case moveStrategy:
		w.setStrategy(w.sides[mv.side], mv.strategy, mv.budgets)
	case moveStrategyTwice:
		w.setStrategy(w.sides[mv.side], mv.strategy, mv.budgets)
		w.playOut(scaleTickBound(false))
		w.setStrategy(w.sides[mv.side], mv.strategy2, scaleRolling{})
	case moveBudget:
		w.setBudgets(w.sides[mv.side], mv.budgets)
	case moveGroupBudget:
		w.setGroupBudget(mv.text)
	case moveCanaryPromote:
		// The step counter moves past the last step, the value under which
		// the canary holds nothing; a unit with no record has no step to
		// move.
		unit := w.isvc.Status.Components[v1beta1.EngineComponent]
		if unit.Canary == nil {
			return
		}
		w.canaryStep = int32(len(w.isvc.Spec.Rollout.Groups[0].Canary.Steps))
		unit.Canary.CurrentStep = w.canaryStep
		w.isvc.Status.Components[v1beta1.EngineComponent] = unit
	}
}

// setPartition writes the operator's partition on a side; zero clears it.
func (w *scaleWorld) setPartition(s *scaleSide, partition int32) {
	s.partition = max(partition, 0)
	w.render(s)
}

// markFaulted records an Instance a fault of this run took pods from.
func (w *scaleWorld) markFaulted(s *scaleSide, index int32) {
	if w.faulted[s] == nil {
		w.faulted[s] = map[int32]bool{}
	}
	w.faulted[s][index] = true
}

// rebuiltOnTarget reports an Instance a fault took pods from that now
// stands toward the target: the repair rebuilt it on the new version, a
// start the batch plan never made.
func (w *scaleWorld) rebuiltOnTarget() bool {
	for _, s := range w.sides {
		started := w.startedIndices(s)
		for index := range w.faulted[s] {
			if started[index] {
				return true
			}
		}
	}
	return false
}

// endedBatchUnseen reports a side whose attempt toward the target failed
// and left the rows: its retry record names the target and no row reads
// toward it, open, failed or retried. The batch step settled that batch
// when it ended, so the reader credits the side one step for it.
func (w *scaleWorld) endedBatchUnseen(s *scaleSide) bool {
	recorded := false
	for _, blk := range s.h.blocks {
		if blk.TargetRevision == s.h.target.Name && blk.AttemptsStarted > 0 {
			recorded = true
		}
	}
	if !recorded {
		return false
	}
	for _, r := range w.rows(s) {
		if r.TargetRevision == s.h.target.Name || (r.Operation != nil && r.Operation.TargetRevision == s.h.target.Name) {
			return false
		}
	}
	return true
}

// setPaused writes the rollout pause on every side.
func (w *scaleWorld) setPaused(paused bool) {
	for _, s := range w.sides {
		s.paused = paused
		w.render(s)
	}
}

// check runs the per-tick checks: the budgets and the version mix.
func (w *scaleWorld) check() {
	if !w.checking {
		return
	}
	w.recordBuilds()
	w.checkBudgets()
	w.checkVersionMix()
	w.checkSourcesKept()
	w.checkScaleUpBuiltAtOnce()
	if w.moreChecks != nil {
		w.moreChecks()
	}
}

// surgingSources reads the side's rows for the sources whose replacement
// is coming up: an update at the Surge step of a source that has not
// failed.
func (w *scaleWorld) surgingSources(s *scaleSide) map[int32]struct{} {
	out := map[int32]struct{}{}
	for _, r := range w.rows(s) {
		if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationUpdate && r.Operation.Step == workloadtypes.UpdateStepSurge && r.Phase != workloadtypes.InstancePhaseFailed {
			out[r.Index] = struct{}{}
		}
	}
	return out
}

// checkSourcesKept holds the rule that a scale-down keeps a source
// whose replacement is coming up until that surge finishes, so a source
// that was surging at the last check is never a scale-down's delete at
// this one.
func (w *scaleWorld) checkSourcesKept() {
	for _, s := range w.sides {
		deleting := map[int32]bool{}
		for _, r := range w.rows(s) {
			if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationDelete {
				deleting[r.Index] = true
			}
		}
		for index := range w.surging[s] {
			if deleting[index] {
				w.note("source-removed-under-its-surge", "%s Instance %d was a source with its replacement coming up and a scale-down removes it: %s", s.comp, index, w.picture())
			}
		}
		w.surging[s] = w.surgingSources(s)
	}
}

// checkScaleUpBuiltAtOnce holds the rule that a scale-up is built at
// once, by a Sequential chain's waiting member too, so a side below its
// count has its new rows within three checks unless its retry schedule
// is spent.
func (w *scaleWorld) checkScaleUpBuiltAtOnce() {
	for _, s := range w.sides {
		have := int32(0)
		for _, r := range w.rows(s) {
			if !replacementOrLeaving(r) {
				have++
			}
		}
		if have >= s.h.replicas || s.h.heldOn(s.h.target.Name) {
			w.belowCount[s] = 0
			continue
		}
		w.belowCount[s]++
		if w.belowCount[s] == 3 {
			w.note("scale-up-not-built-at-once", "%s stands at %d Instances for %d wanted three checks after the scale: %s", s.comp, have, s.h.replicas, w.picture())
		}
	}
}

// recordBuilds counts, per revision, the new Instances the create pass
// built in this run (a rebuild of an Instance that ran before is not a
// scale-up's build): what a reader of the report wants to know about a
// scale-up beside a roll.
func (w *scaleWorld) recordBuilds() {
	for _, s := range w.sides {
		for _, r := range w.rows(s) {
			if r.Operation == nil || r.Operation.Type != workloadtypes.InstanceOperationCreate || r.RunningRevision != "" {
				continue
			}
			key := fmt.Sprintf("%s/%d/%d", s.comp, r.Index, r.Incarnation)
			if _, seen := w.builtSeen[key]; seen {
				continue
			}
			w.builtSeen[key] = struct{}{}
			version := w.short(s, r.Operation.TargetRevision)
			switch {
			case r.Operation.TargetRevision == s.h.target.Name:
				w.builtOn["target"]++
			case r.Operation.TargetRevision == s.h.currentRevision:
				w.builtOn["current"]++
			default:
				w.builtOn[version]++
			}
		}
	}
}

// checkBudgets holds every side to its own budget on the count its strategy
// uses, read the way the engine charges it. A breach right after a scale
// is reported in a class of its own: the count the budget is a share of
// just moved under attempts already open.
func (w *scaleWorld) checkBudgets() {
	class := "budget"
	if w.scaled {
		class = "budget-after-scale"
	}
	for _, s := range w.sides {
		rows := w.rows(s)
		rolling := s.h.lifecycle.UpdateStrategy.RollingUpdate
		if s.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
			budget := escalation.PerComponentMaxSurgeBudget(rolling, s.h.replicas)
			if open := escalation.CurrentSurgeInFlight(rows); budget != escalation.BudgetNoLimit && open > budget+w.allowedPastBudget(s, true, budget) {
				w.note(class, "%s has %d surges in flight against its budget of %d at %d Instances", s.comp, open, budget, s.h.replicas)
			}
			continue
		}
		budget := escalation.PerComponentMaxUnavailableBudget(rolling, s.h.replicas)
		if open := escalation.CurrentUnavailableInFlight(rows); budget != escalation.BudgetNoLimit && open > budget+w.allowedPastBudget(s, false, budget) {
			w.note(class, "%s has %d Instances out of rotation against its budget of %d at %d Instances", s.comp, open, budget, s.h.replicas)
		}
	}
}

// allowedPastBudget is how many attempts beyond budget a hunt allows a
// side in flight on one budget kind; none unless the hunt says so.
func (w *scaleWorld) allowedPastBudget(s *scaleSide, surge bool, budget int32) int32 {
	if w.pastBudget == nil {
		return 0
	}
	return w.pastBudget(s, surge, budget)
}

// startedIndices is the set of a side's Instances that have moved toward
// the target, the partition's held ones left out: promoted, open toward
// it, failed toward it, or built on it. A replacement gang's slot and a
// row a scale-down is deleting are not Instances of the count.
func (w *scaleWorld) startedIndices(s *scaleSide) map[int32]bool {
	held := w.heldIndices(s)
	out := map[int32]bool{}
	for _, r := range w.rows(s) {
		if held[r.Index] || replacementOrLeaving(r) {
			continue
		}
		toward := r.Operation != nil && r.Operation.TargetRevision == s.h.target.Name
		if r.RunningRevision == s.h.target.Name || r.TargetRevision == s.h.target.Name || toward {
			out[r.Index] = true
		}
	}
	return out
}

// replacementOrLeaving reports a row that is not an Instance of the count:
// a gang surge's replacement slot, or a row the scale-down is deleting.
func replacementOrLeaving(r workloadtypes.InstanceStatus) bool {
	if r.Operation == nil {
		return false
	}
	if r.Operation.Type == workloadtypes.InstanceOperationDelete {
		return true
	}
	return r.Operation.Step == workloadtypes.UpdateStepGangSurgeTarget || r.Operation.Step == workloadtypes.UpdateStepGangSurgeTargetCleanup
}

// mixGap is how far apart the two sides' new-version shares are, beside
// the gap a ratio rule allows (the tolerance plus one step of each side).
// With preScaleOnly the shares are read over the Instances each side had
// when the last scale landed, so the Instances a scale-up built are left
// out.
func (w *scaleWorld) mixGap(tolerance float64, preScaleOnly bool) (gap, allowed float64, ok bool) {
	allowed = tolerance
	var shares, steps []float64
	for _, s := range []*scaleSide{w.prefill(), w.decode()} {
		held := w.heldIndices(s)
		started := w.startedIndices(s)
		existed := map[int32]bool{}
		for _, idx := range w.indicesAtScale[s] {
			existed[idx] = true
		}
		var movable, moved int32
		for _, r := range w.rows(s) {
			if held[r.Index] || replacementOrLeaving(r) || (preScaleOnly && !existed[r.Index]) {
				continue
			}
			movable++
			if started[r.Index] {
				moved++
			}
		}
		if movable <= 0 {
			return 0, 0, false
		}
		if w.endedBatchUnseen(s) {
			moved = min(moved+w.stepOf(movable), movable)
		}
		shares = append(shares, float64(moved)/float64(movable))
		steps = append(steps, float64(w.stepOf(movable))/float64(movable))
	}
	gap, allowed = shares[0]-shares[1], allowed+steps[0]
	if gap < 0 {
		gap, allowed = -gap, tolerance+steps[1]
	}
	return gap, allowed, true
}

// startedSinceScale reports a start the gates admitted since the last
// scale on an Instance that was there when it landed.
func (w *scaleWorld) startedSinceScale() bool {
	for _, s := range []*scaleSide{w.prefill(), w.decode()} {
		before := w.startedAtScale[s]
		existed := map[int32]bool{}
		for _, idx := range w.indicesAtScale[s] {
			existed[idx] = true
		}
		for idx := range w.startedIndices(s) {
			if existed[idx] && !before[idx] {
				return true
			}
		}
	}
	return false
}

// checkVersionMix holds the two sides' new-version shares within the ratio
// rule's tolerance plus one step of each other while a ratio rule governs
// the pair. A gap a scale opened is named by its cause: the Instances a
// scale-up built on the target, or the Instances a scale-down removed;
// a gap a start widened past the rule is the gates' own.
func (w *scaleWorld) checkVersionMix() {
	if w.cfg.settings.canary || w.cfg.settings.sequential || w.decode() == nil {
		return
	}
	tolerance := float64(0)
	switch {
	case w.recordedRatioRule():
		tolerance = float64(scaleRollTolerance) / 100
	case w.cfg.settings.keepRatio:
		tolerance = float64(scaleRollKeepRatioTolerance) / 100
	default:
		return
	}
	gap, allowed, ok := w.mixGap(tolerance, false)
	if !ok || gap <= allowed+1e-9 {
		return
	}
	class := "version-mix"
	switch {
	case w.rebuiltOnTarget():
		class = "version-mix-after-a-rebuild"
	case !w.cfg.settings.keepRatio:
		class = "version-mix-by-batch-number"
	}
	if w.scaled {
		switch preGap, preAllowed, preOK := w.mixGap(tolerance, true); {
		case preOK && preGap <= preAllowed+1e-9:
			class = "version-mix-from-a-scale-up"
		case !w.startedSinceScale():
			class = "version-mix-from-the-scale"
		}
	}
	w.note(class, "the two sides' new-version shares are %.0f%% apart, more than the tolerance of %.0f%% plus one step of the leading side (%.0f%%)", gap*100, tolerance*100, allowed*100)
}

// classifyEnd reads a settled state: the roll complete, or stopped for a
// reason the operator can see, or a finding.
func (w *scaleWorld) classifyEnd(settled bool) {
	if !settled {
		w.note("did-not-settle", "the world kept changing to the end of the bound: %s", w.picture())
		return
	}
	if w.complete() {
		if w.endChecks != nil {
			w.endChecks()
		}
		return
	}
	countOK := true
	for _, s := range w.sides {
		if int32(len(w.rows(s))) != s.h.replicas {
			countOK = false
		}
	}
	for _, s := range w.sides {
		if s.paused {
			w.note("ends-paused", "the run ends paused: %s", w.picture())
			return
		}
	}
	for _, s := range w.sides {
		if !s.h.heldOn(s.h.target.Name) {
			continue
		}
		if countOK {
			w.note("held", "the retry schedule is spent and the roll holds: %s", w.picture())
		} else {
			w.note("held-with-the-scale-incomplete", "the roll holds on a spent retry schedule and a side is not at its count: %s", w.picture())
		}
		return
	}
	if w.heldByThePartition() {
		w.note("held-by-the-partition", "the partition holds an Instance whose attempt toward the target failed, and the hold names the exits: %s", w.picture())
		return
	}
	var waiting []string
	peers, withoutHold := 0, 0
	for _, s := range w.sides {
		if w.sideComplete(s) {
			continue
		}
		hold := s.h.hold
		if hold == nil {
			withoutHold++
			waiting = append(waiting, fmt.Sprintf("%s stands still with no hold", s.comp))
			continue
		}
		switch hold.Gate {
		case workloadtypes.RolloutHoldGateRatio, workloadtypes.RolloutHoldGatePairing, workloadtypes.RolloutHoldGateSequential:
			peers++
		}
		waiting = append(waiting, fmt.Sprintf("%s holds on %s: %s", s.comp, hold.Gate, hold.Reason))
	}
	class := "holds-for-good"
	switch {
	case !countOK:
		class = "scale-never-completes"
	case withoutHold > 0:
		class = "stands-still-without-a-hold"
	case peers >= 2:
		class = "two-gates-wait-on-each-other"
	}
	if w.endClass != nil {
		class = w.endClass(class)
	}
	w.note(class, "%s; %s", strings.Join(waiting, "; "), w.picture())
}

// heldByThePartition reports a settled end where a side holds on the
// partition's own hold (an Instance it holds whose attempt toward the
// target failed; the hold names lowering the partition and pushing
// another revision as the exits) and every other side still rolling
// holds on a gate of its own, waiting on that side.
func (w *scaleWorld) heldByThePartition() bool {
	onPartition := false
	for _, s := range w.sides {
		if w.sideComplete(s) {
			continue
		}
		hold := s.h.hold
		if hold == nil {
			return false
		}
		if hold.Gate == workloadtypes.RolloutHoldGateRetryBlock && strings.Contains(hold.Reason, "which the partition of") {
			onPartition = true
		}
	}
	return onPartition
}

// scaleTickBound is the most ticks one play may take.
func scaleTickBound(full bool) int {
	if full {
		return 200
	}
	return 120
}

// scaleTickBudget is the most ticks one configuration may spend; the moves
// left when it is spent are counted as skipped, so a run on a slower
// machine reads the same first moves.
func scaleTickBudget(full bool) int {
	if full {
		return 8000
	}
	return 4000
}

// budget is the tick budget of this world: the hunt's, else the mode's.
func (w *scaleWorld) budget(full bool) int {
	if w.tickBudget > 0 {
		return w.tickBudget
	}
	return scaleTickBudget(full)
}

// explore runs one configuration: the plain roll with its moments kept,
// every move from every moment played to its end, then the random runs.
func (w *scaleWorld) explore(full bool, seed int64, runs int) {
	w.t.Helper()
	maxTicks := scaleTickBound(full)
	w.reachIdle()
	snaps := w.captureMoments(maxTicks)
	for _, m := range []scaleMoment{scaleAtIdle, scaleAtSettling, scaleAtRotating, scaleAtDraining, scaleAtLast, scaleAtHeld, scaleAtCanaryStep} {
		snap := snaps[m]
		if snap == nil {
			continue
		}
		w.restore(snap)
		moves := w.movesAt(m, full)
		for _, mv := range moves {
			if w.ticks >= w.budget(full) {
				w.skipped++
				continue
			}
			w.restore(snap)
			w.path = append(w.path, "at "+m.String())
			w.apply(mv)
			if m == scaleAtIdle {
				w.push(fixedImage)
			}
			w.probes++
			w.classifyEnd(w.playOut(maxTicks))
		}
	}
	w.randomRuns(snaps[scaleAtIdle], seed, runs, maxTicks, full)
}

// randomRuns plays the roll from idle with up to three moves at random
// ticks, under a fixed seed.
func (w *scaleWorld) randomRuns(idle *scaleSnapshot, seed int64, runs, maxTicks int, full bool) {
	w.t.Helper()
	rng := rand.New(rand.NewSource(seed))
	for run := 0; run < runs; run++ {
		if w.ticks >= w.budget(full) {
			w.skipped++
			continue
		}
		w.restore(idle)
		w.path = append(w.path, fmt.Sprintf("random run %d", run+1))
		w.push(fixedImage)
		w.probes++
		left := 3
		settling := w.startSettling()
		settled := false
		for i := 0; i < maxTicks && !settled; i++ {
			if left > 0 && rng.Intn(5) == 0 {
				moves := w.movesAt(scaleAtRotating, full)
				w.apply(moves[rng.Intn(len(moves))])
				left--
				settling = w.startSettling()
			}
			settled = settling.observe(w, w.tick())
		}
		w.classifyEnd(settled)
	}
}

// scaleReferenceConfigs are the configurations every run explores: the
// reference shapes under their reference settings, one shape per
// setting, the gangs on the smallest shape, so the bounded run stays
// under five minutes.
func scaleReferenceConfigs() []scaleConfig {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	return []scaleConfig{
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true}},
		{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}},
		{shape: scaleShape{12, 4, 1}, settings: scaleSettings{prefill: surge, decode: surge, policy: true}},
		{shape: scaleShape{12, 4, 2}, settings: scaleSettings{prefill: recreate, decode: recreate, keepRatio: true, firstSide: v1beta1.DecoderComponent}},
		{shape: scaleShape{24, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, partition: 2}},
		{shape: scaleShape{30, 10, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, policy: true, followerMax: true}},
		{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: surge, canary: true}},
		{shape: scaleShape{12, 4, 4}, settings: scaleSettings{prefill: surge, decode: surge, policy: true}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, router: true}},
		{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, sequential: true}},
	}
}

// scaleConfigs lists the configurations of a run: the reference ones, and
// in full mode a seeded sample of the whole product of shapes, gang sizes,
// strategy pairs and settings.
func scaleConfigs(full bool) []scaleConfig {
	configs := scaleReferenceConfigs()
	if !full {
		return configs
	}
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	shapes := [][2]int32{{12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}
	pods := []int32{1, 2, 4, 8}
	strategies := [][2]workloadtypes.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}}
	variants := []scaleSettings{
		{},
		{maintainRatio: true},
		{keepRatio: true},
		{keepRatio: true, firstSide: v1beta1.DecoderComponent},
		{keepRatio: true, firstSide: v1beta1.EngineComponent},
		{partition: 2},
		{canary: true},
		{policy: true},
		{policy: true, followerMax: true},
		{router: true},
		{sequential: true},
		{maintainRatio: true, policy: true},
		{keepRatio: true, policy: true},
		{partition: 2, policy: true},
	}
	var all []scaleConfig
	seen := map[string]bool{}
	for _, cfg := range configs {
		seen[cfg.String()] = true
	}
	for _, shape := range shapes {
		for _, p := range pods {
			if (shape[0]+shape[1])*p > scaleFullPodCeiling {
				continue
			}
			for _, pair := range strategies {
				for _, variant := range variants {
					settings := variant
					settings.prefill, settings.decode = pair[0], pair[1]
					cfg := scaleConfig{shape: scaleShape{shape[0], shape[1], p}, settings: settings}
					if !seen[cfg.String()] {
						all = append(all, cfg)
					}
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if len(all) > scaleFullSample {
		all = all[:scaleFullSample]
	}
	return append(configs, all...)
}

// scaleFullSample is how many sampled configurations the full run adds to
// the reference ones; scaleFullPodCeiling keeps the sample to fleets the
// fake cluster rolls in the run's time (a tick costs about a millisecond
// per pod and a configuration spends thousands of ticks).
const (
	scaleFullSample     = 30
	scaleFullPodCeiling = 160
)

// scaleExpectedEnds are the ends a run may reach that are not findings: a
// spent retry schedule holds the roll by design, a run that ends paused
// waits for the operator, and the partition's hold on a failed attempt
// names its exits.
var scaleExpectedEnds = map[string]bool{"held": true, "ends-paused": true, "held-by-the-partition": true}

// knownScaleFindingClasses are the finding classes the model shows on
// main today, each with the picture behind it. A finding in a known class
// is reported and does not fail the run; a change of behaviour removes its entry and the
// lock test that holds the picture.
var knownScaleFindingClasses = map[string]string{
	"budget-after-scale":             "a scale-down lands under attempts already open: the engine finishes them rather than stop them, so the share the new count allows is exceeded until they serve",
	"held-with-the-scale-incomplete": "a scale-up of a side whose retry schedule is spent, or of the side waiting on it, builds nothing or builds on the broken image: the slot stays empty and the hold does not name it",
	"version-mix-from-the-scale":     "a scale-down removes Instances by index and rank, not by version, so the shares the two sides have moved can end further apart than the tolerance with no start admitted; the gates re-plan from the new counts and the gap closes as the roll goes on",
	"version-mix-from-a-scale-up":    "a scale-up builds its new Instances on the target revision with no gate asked, so the scaled side's share jumps past the tolerance",
	"version-mix":                    "under keepRatio a move that changes the plan's inputs mid-roll (a resize, a partition, a roll back) re-plans the batches from the new counts, and a side's started count can fall a batch behind in the new plan, so a side the step was holding opens one more batch than it allowed before (held by TestScaleBesideARoll_ADoubledPrefillBesideAHeldDecoderOpensABatchTheStepHadRefused)",
	"version-mix-after-a-rebuild":    "an Instance that lost its pods is rebuilt on the target revision, the repair's rule, and counts as a start the batch plan never made, so its side stands one Instance past its batch",
	"version-mix-by-batch-number":    "under maintainRatio without keepRatio the batch step pairs batch numbers, each side on its own ten-percent step, and the lockstep is not consulted for the members it governs, so wherever the two sides' shares per batch differ (after a resize, under a partition, by shape) their versions drift apart by the difference every batch, past the tolerance with no hold (held by TestScaleBesideARoll_ADecoderScaledToElevenOutrunsTheEngineUnderTheRatioRule)",
}

type scaleExample struct {
	cfg     scaleConfig
	finding scaleFinding
}

// scaleResult is what one configuration's exploration found.
type scaleResult struct {
	cfg                                               scaleConfig
	states, ticks, passes, gateCalls, probes, skipped int
	counts                                            map[string]int
	findings                                          map[string]scaleFinding
	builtOn                                           map[string]int
	took                                              time.Duration
}

// scaleHunt is one exploration of the shared world: its configurations,
// what it installs on each world (its own move lists and checks), and the
// classes it reads as expected ends or as known pictures of the current engine.
type scaleHunt struct {
	full         bool
	runs         int
	configs      []scaleConfig
	setup        func(w *scaleWorld)
	expectedEnds map[string]bool
	known        map[string]string
}

// scaleHuntMode reads a hunt's environment: full mode and the text a
// configuration's name must carry.
func scaleHuntMode(fullEnv, onlyEnv string) (full bool, only string) {
	return os.Getenv(fullEnv) == "full", os.Getenv(onlyEnv)
}

// scaleHuntRuns is the random runs per configuration: six in full mode.
func scaleHuntRuns(full bool) int {
	if full {
		return 6
	}
	return 1
}

// keepScaleConfigs keeps the configurations keep admits whose name carries
// only, when only is set.
func keepScaleConfigs(configs []scaleConfig, keep func(scaleConfig) bool, only string) []scaleConfig {
	var out []scaleConfig
	for _, cfg := range configs {
		if keep(cfg) && (only == "" || strings.Contains(cfg.String(), only)) {
			out = append(out, cfg)
		}
	}
	return out
}

// scaleExplore runs the scaling hunt over the configurations keep admits.
func scaleExplore(t *testing.T, keep func(scaleConfig) bool) {
	t.Helper()
	full, only := scaleHuntMode(scaleRollFullEnv, scaleRollOnlyEnv)
	runScaleHunt(t, scaleHunt{
		full:         full,
		runs:         scaleHuntRuns(full),
		configs:      keepScaleConfigs(scaleConfigs(full), keep, only),
		expectedEnds: scaleExpectedEnds,
		known:        knownScaleFindingClasses,
	})
}

// runScaleHunt runs a hunt's configurations, one subtest per configuration
// in parallel, and reports every class, failing on the classes that are
// neither expected ends nor known.
func runScaleHunt(t *testing.T, hunt scaleHunt) {
	t.Helper()
	start := time.Now()
	results := make([]scaleResult, len(hunt.configs))
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range hunt.configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				w := newScaleWorld(t, cfg)
				if hunt.setup != nil {
					hunt.setup(w)
				}
				w.explore(hunt.full, 1, hunt.runs)
				results[i] = scaleResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, passes: w.passes, gateCalls: w.gateCalls, probes: w.probes, skipped: w.skipped,
					counts: w.counts, findings: w.findings, builtOn: w.builtOn, took: time.Since(began)}
			})
		}
	})
	reportScaleHunt(t, hunt, results, time.Since(start))
}

// scaleHuntTotals sums the counts of every configuration's result.
type scaleHuntTotals struct {
	states, ticks, passes, gateCalls, probes, skipped int
	classes                                           map[string]int
	configurations                                    map[string]int
	builtOn                                           map[string]int
	examples                                          map[string][]scaleExample
}

// reportScaleHunt logs every configuration and every class with its first
// pictures, and fails on a class that is neither an expected end nor known.
func reportScaleHunt(t *testing.T, hunt scaleHunt, results []scaleResult, took time.Duration) {
	t.Helper()
	totals := scaleHuntTotals{classes: map[string]int{}, configurations: map[string]int{}, builtOn: map[string]int{}, examples: map[string][]scaleExample{}}
	for n, res := range results {
		totals.states += res.states
		totals.ticks += res.ticks
		totals.passes += res.passes
		totals.gateCalls += res.gateCalls
		totals.probes += res.probes
		totals.skipped += res.skipped
		var parts []string
		for class, c := range res.counts {
			totals.classes[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		for revision, c := range res.builtOn {
			totals.builtOn[revision] += c
		}
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d probes (%d skipped past the tick budget), built %v, %s: %s", n+1, len(results), res.cfg, res.states, res.ticks, res.probes, res.skipped, res.builtOn, res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			totals.configurations[class]++
			if len(totals.examples[class]) < 6 {
				totals.examples[class] = append(totals.examples[class], scaleExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals.classes))
	for class := range totals.classes {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d gate questions, %d probes (%d skipped past the tick budget), scale-up Instances built on %v, %s", len(results), totals.states, totals.ticks, totals.passes, totals.gateCalls, totals.probes, totals.skipped, totals.builtOn, took.Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, totals.classes[class], totals.configurations[class])
		for _, ex := range totals.examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		switch {
		case hunt.expectedEnds[class]:
		case hunt.known[class] != "":
			t.Logf("  known: %s", hunt.known[class])
		default:
			t.Errorf("%s: %d states (first: %s: %s)", class, totals.classes[class], totals.examples[class][0].cfg, totals.examples[class][0].finding.message)
		}
	}
}

// TestScaleBesideARoll_EveryMoveLeavesTheRollAWayOut explores every
// configuration: under every shape, strategy pair and setting, a scale, a
// partition change, a policy change, a pause, a roll back, a second push
// or a loss at any moment of a roll leaves a roll that finishes, holds on
// a spent retry schedule, or waits paused, with every side at its count,
// within its budgets, and with the version mix inside the ratio rule.
func TestScaleBesideARoll_EveryMoveLeavesTheRollAWayOut(t *testing.T) {
	scaleExplore(t, func(scaleConfig) bool { return true })
}

// TestScaleBesideARoll_AScaleUpOfAHeldSideLeavesAnEmptySlotTheHoldDoesNotName
// holds the picture the model found: 12 prefill and 8 decode under
// RecreatePod at ten percent, the decoder pushed to an image no pod can
// pull until its retry schedule is spent, then scaled to 9. The ninth
// Instance is never built, which is right while the revision is broken,
// and the decoder's hold says nothing about it. A hold that names the
// empty slot fails this test, and the known class goes with it.
func TestScaleBesideARoll_AScaleUpOfAHeldSideLeavesAnEmptySlotTheHoldDoesNotName(t *testing.T) {
	cfg := scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod, decode: workloadtypes.UpdateStrategyRecreatePod}}
	w := newScaleWorld(t, cfg)
	w.reachIdle()
	w.pushHeld()
	if !w.playOut(scaleTickBound(false)) || !w.momentReached(scaleAtHeld) {
		t.Fatalf("the decoder's retry schedule was not spent: %s", w.picture())
	}
	w.apply(scaleMove{kind: moveScale, side: 1, delta: 1})
	if !w.playOut(scaleTickBound(false)) {
		t.Fatalf("the world did not settle after the scale: %s", w.picture())
	}
	decode := w.decode()
	if rows := len(w.rows(decode)); rows != 8 {
		t.Fatalf("the picture moved: the decoder has %d Instances for 9 wanted; if the ninth now waits as a named slot, remove the known class and this lock: %s", rows, w.picture())
	}
	hold := decode.h.hold
	if hold == nil || strings.Contains(hold.Reason, "slot") {
		t.Fatalf("the picture moved: the decoder's hold reads %v; if it names the empty slot now, remove the known class and this lock", hold)
	}
	t.Logf("the ninth decoder Instance is not built while the revision is broken, and the hold reads: %s", hold.Reason)
}

// TestScaleBesideARoll_ADoubledPrefillBesideAHeldDecoderOpensABatchTheStepHadRefused
// holds the picture the model found: 12 prefill and 8 decode under
// SurgeThenDrain at ten percent with keepRatio, the decoder pushed to an
// image no pod can pull until its retry schedule is spent, the prefill
// side held at three new Instances ("engine batch 3 waits for decoder
// batch 2"). The operator scales the prefill side to 24. The batch step
// re-plans from 24 and 8, reads the three as the first batch of three,
// and admits three more surges at once; with the twelve new Instances
// built on version 2 the prefill side then stands at 18 of 24 beside a
// decoder at 0 of 8. When the step keeps the side where it stood, this
// test fails, and the known class and this lock go with it.
func TestScaleBesideARoll_ADoubledPrefillBesideAHeldDecoderOpensABatchTheStepHadRefused(t *testing.T) {
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	cfg := scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true}}
	w := newScaleWorld(t, cfg)
	w.reachIdle()
	w.pushHeld()
	if !w.playOut(scaleTickBound(false)) || !w.momentReached(scaleAtHeld) {
		t.Fatalf("the decoder's retry schedule was not spent: %s", w.picture())
	}
	prefill := w.prefill()
	ask := func() (bool, string) {
		w.consult++
		allowed, _, reason := coordination.EvaluateUpdateGate(w.ctx, w.reader(), w.isvc, v1beta1.EngineComponent, prefill.h.revV1.Name, prefill.h.target, nil, w.defaults, surge, 0, 0)
		return allowed, reason
	}
	if allowed, reason := ask(); allowed {
		t.Fatalf("the step admits a prefill start beside the held decoder before the scale: %s", reason)
	}
	w.apply(scaleMove{kind: moveScale, side: 0, delta: 12})
	allowed, reason := ask()
	if !allowed {
		t.Fatalf("the picture moved: the step keeps the prefill side where it stood after the scale (%s); remove the known class and this lock", reason)
	}
	w.tick()
	_, _, _, _ = w.rollCounts(prefill)
	if open := escalation.CurrentSurgeInFlight(w.rows(prefill)); open != 3 {
		t.Fatalf("the picture moved: %d prefill surges open after the scale, not three: %s", open, w.picture())
	}
	t.Logf("after the scale the step admits a prefill start it refused before, and the pass opens three surges beside the held decoder: %s", w.picture())
}

// TestScaleBesideARoll_ADecoderScaledToElevenOutrunsTheEngineUnderTheRatioRule
// holds the picture the model found: 20 prefill and 10 decode under
// RecreatePod at ten percent with maintainRatio at a tolerance of 20. The
// operator scales the decoder to 11 while the first batch is open. From
// then on each batch moves 2 of 20 on the prefill side and 2 of 11 on the
// decode side, the batch step pairs the batch numbers, and the lockstep
// is not asked for the members the step governs: the decoder finishes
// its roll while the prefill side stands at 10 of 20, 50 percent apart,
// with no hold on the way. When the step keeps the shares within the
// tolerance, this test fails, and the known class and this lock go with
// it.
func TestScaleBesideARoll_ADecoderScaledToElevenOutrunsTheEngineUnderTheRatioRule(t *testing.T) {
	recreate := workloadtypes.UpdateStrategyRecreatePod
	cfg := scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, maintainRatio: true}}
	w := newScaleWorld(t, cfg)
	w.reachIdle()
	w.push(fixedImage)
	for i := 0; i < 40 && !w.momentReached(scaleAtSettling); i++ {
		w.tick()
	}
	if !w.momentReached(scaleAtSettling) {
		t.Fatalf("the first batch did not open: %s", w.picture())
	}
	w.apply(scaleMove{kind: moveScale, side: 1, delta: 1})
	tolerance := float64(scaleRollTolerance) / 100
	widest, holds := 0.0, 0
	for i := 0; i < 80 && !w.complete(); i++ {
		w.tick()
		if hold := w.decode().h.hold; hold != nil && hold.Gate == workloadtypes.RolloutHoldGateRatio {
			holds++
		}
		if gap, _, ok := w.mixGap(tolerance, false); ok && gap > widest {
			widest = gap
		}
	}
	if widest <= tolerance+2.0/11+1e-9 {
		t.Fatalf("the picture moved: the two sides stayed within %.0f%% of each other after the scale; if the step now pairs the shares, remove the known class and this lock: %s", widest*100, w.picture())
	}
	t.Logf("after the scale the two sides' new-version shares drift %.0f%% apart with %d ratio holds on the decoder on the way: %s", widest*100, holds, w.picture())
}
