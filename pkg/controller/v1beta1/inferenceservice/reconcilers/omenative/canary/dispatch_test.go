package canary

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// mustSecondaryReady calls secondaryCapacityReady and fails the test on a read
// error, so gate assertions stay one-liners.
func mustSecondaryReady(t *testing.T, ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, perRev, readyPerRev map[v1beta1.ComponentType]map[string]int32, primary v1beta1.ComponentType) bool {
	t.Helper()
	observedPods := make(map[v1beta1.ComponentType][]*corev1.Pod, len(readyPerRev))
	for component, revisions := range readyPerRev {
		for hash, count := range revisions {
			for index := int32(0); index < count; index++ {
				name := fmt.Sprintf("%s-%s-%s-%d", isvc.Name, component, hash, index)
				pod := canaryRunnerPod(isvc.Namespace, isvc.Name, string(component), hash, name, v1beta1.RunnerNameDefault, "0")
				pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(int(index))
				observedPods[component] = append(observedPods[component], pod)
			}
		}
	}
	ok, fresh, err := secondaryCapacityReady(ctx, reads, isvc, perRev, readyPerRev, observedPods, primary)
	if err != nil {
		t.Fatalf("secondaryCapacityReady error: %v", err)
	}
	if !fresh {
		t.Fatal("secondaryCapacityReady unexpectedly observed stale IR status")
	}
	return ok
}

// ir builds an InferenceReplica whose UpdateRevision names the per-Component
// canary target observed by Dispatch.
func ir(ns, isvc string, comp v1beta1.ComponentType, hash string) *v1beta1.InferenceReplica {
	r := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: isvc + "-" + string(comp),
	}}
	r.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	r.Status.UpdateRevision = isvc + "-" + string(comp) + "-" + hash
	return r
}

func TestSecondaryCapacityReady(t *testing.T) {
	ns := "default"
	n4 := 4
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	ctx := context.Background()

	// Single component (engine only) → no secondaries → always ready. The canary
	// group covers just [engine].
	engPlan := &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}
	eng := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "e1"}, Spec: v1beta1.InferenceServiceSpec{Rollout: engPlan}}
	eng.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	engRead := fake.NewClientBuilder().WithScheme(canaryScheme(t)).Build()
	if !mustSecondaryReady(t, ctx, engRead, eng,
		map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: {"eng": 1}},
		map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: {"eng": 1}},
		v1beta1.EngineComponent) {
		t.Fatal("single-component canary has no secondaries → must be ready")
	}

	// PD: router primary + engine secondary; step 0 newCount = 50% of 4 = 2. The
	// canary group couples [router, engine]. Revision hashes are PER-Component:
	// router=rtr, engine=eng — distinct, as in production.
	pdPlan := &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}
	mkPD := func() *v1beta1.InferenceService {
		pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}, Spec: v1beta1.InferenceServiceSpec{Rollout: pdPlan}}
		pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
		pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
		return pd
	}
	// IRs name the per-Component canary targets; engine has a stable revision
	// "engOld" too, so a roll is genuinely in progress for the secondary.
	engineTarget := ir(ns, "pd", v1beta1.EngineComponent, "eng")
	engineTarget.Status.CurrentRevision = "pd-engine-engOld"
	pdReads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(
			ir(ns, "pd", v1beta1.RouterComponent, "rtr"),
			engineTarget,
		).Build()

	below := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 2},
		v1beta1.EngineComponent: {"eng": 1, "engOld": 3},
	}
	if mustSecondaryReady(t, ctx, pdReads, mkPD(), below, below, v1beta1.RouterComponent) {
		t.Fatal("engine secondary below its step newCount → not ready")
	}
	targetOnlyShort := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 2},
		v1beta1.EngineComponent: {"eng": 1},
	}
	if mustSecondaryReady(t, ctx, pdReads, mkPD(), targetOnlyShort, targetOnlyShort, v1beta1.RouterComponent) {
		t.Fatal("distinct IR revisions must enforce capacity after stable pods disappear")
	}
	targetOnly := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 2},
		v1beta1.EngineComponent: {"eng": 2},
	}
	readyShort := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 2},
		v1beta1.EngineComponent: {"eng": 1},
	}
	if mustSecondaryReady(t, ctx, pdReads, mkPD(), targetOnly, readyShort, v1beta1.RouterComponent) {
		t.Fatal("ready Instance count must not bypass the ready Pod gate")
	}
	if !mustSecondaryReady(t, ctx, pdReads, mkPD(), targetOnly, targetOnly, v1beta1.RouterComponent) {
		t.Fatal("target-only secondary at both readiness thresholds → ready")
	}
}

func TestSecondaryCapacityReadyRequiresAuthoritativeIR(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{
			Capacity: intstr.FromString("50%"), Traffic: 50,
		}}},
	}}}
	perRev := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent: {"old": 2, "new": 2},
	}
	for _, tc := range []struct {
		name string
		ir   *v1beta1.InferenceReplica
	}{
		{name: "missing"},
		{name: "empty-status", ir: func() *v1beta1.InferenceReplica {
			empty := ir(ns, isvc.Name, v1beta1.EngineComponent, "unused")
			empty.Generation = 1
			empty.Status = v1beta1.InferenceReplicaStatus{ObservedGeneration: 1}
			return empty
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := []runtime.Object{}
			if tc.ir != nil {
				objects = append(objects, tc.ir)
			}
			reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objects...).Build()
			ready, fresh, err := secondaryCapacityReady(context.Background(), reads, isvc, perRev, perRev, nil, v1beta1.RouterComponent)
			if err != nil {
				t.Fatalf("secondaryCapacityReady: %v", err)
			}
			if ready || fresh {
				t.Fatalf("non-authoritative secondary must fail closed: ready=%v fresh=%v", ready, fresh)
			}
		})
	}
}

// TestSecondaryCapacityReady_PerComponentHashes reproduces the PD-canary stall:
// revision hashes are PER-Component, so gating a secondary on the PRIMARY's hash
// (the old behavior) found 0 secondary canary pods and blocked Pending forever
// even though the secondary's OWN canary capacity was fully up. The gate must
// resolve each secondary's own target hash.
func TestSecondaryCapacityReady_PerComponentHashes(t *testing.T) {
	ns := "default"
	n4 := 4
	ctx := context.Background()
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "p302"}}
	pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}

	reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(
			ir(ns, "p302", v1beta1.RouterComponent, "53db7c07"),
			ir(ns, "p302", v1beta1.EngineComponent, "73e1335d"),
			ir(ns, "p302", v1beta1.DecoderComponent, "f8f90d6d"),
		).Build()

	// Every Component's OWN canary capacity is up (2 of 4 = step-0 50%), each on
	// its own per-Component hash, with the stable revision still present.
	ready := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent:  {"53db7c07": 2, "c6f33d7b": 2},
		v1beta1.EngineComponent:  {"73e1335d": 2, "engOld": 2},
		v1beta1.DecoderComponent: {"f8f90d6d": 2, "decOld": 2},
	}
	if !mustSecondaryReady(t, ctx, reads, pd, ready, ready, v1beta1.RouterComponent) {
		t.Fatal("secondaries at their own per-Component canary capacity → must be ready (was stuck Pending on the primary's hash)")
	}
	// A secondary still short on its own canary capacity must still gate.
	short := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent:  {"53db7c07": 2, "c6f33d7b": 2},
		v1beta1.EngineComponent:  {"73e1335d": 1, "engOld": 3},
		v1beta1.DecoderComponent: {"f8f90d6d": 2, "decOld": 2},
	}
	if mustSecondaryReady(t, ctx, reads, pd, short, short, v1beta1.RouterComponent) {
		t.Fatal("engine secondary below its own step newCount → not ready")
	}
}

// TestSecondaryCapacityReady_UnbumpedSecondary covers the primary-only-bumped PD
// canary: only the router template changed, so engine and
// decoder have no canary revision distinct from stable. Those secondaries have
// nothing to stage and must NOT block the rollout.
func TestSecondaryCapacityReady_UnbumpedSecondary(t *testing.T) {
	ns := "default"
	n4 := 4
	ctx := context.Background()
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "p302"}}
	pd.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pd.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: steps},
	}}}

	// Only the router was bumped: engine/decoder IRs name their stable revision as
	// the (only) target, and only that one revision has pods.
	reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		ir(ns, "p302", v1beta1.RouterComponent, "53db7c07"),
		ir(ns, "p302", v1beta1.EngineComponent, "engStable"),
		ir(ns, "p302", v1beta1.DecoderComponent, "decStable"),
	).Build()

	pods := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent:  {"53db7c07": 2, "c6f33d7b": 2},
		v1beta1.EngineComponent:  {"engStable": 4},
		v1beta1.DecoderComponent: {"decStable": 4},
	}
	if !mustSecondaryReady(t, ctx, reads, pd, pods, pods, v1beta1.RouterComponent) {
		t.Fatal("unbumped secondaries have no canary revision to stage → must be ready (was stuck Pending forever)")
	}
}

// canaryRunnerPorts mirrors what the controller threads in: the merged
// effective serving ports of every Component a canary group may touch. The
// per-revision routing Service publishes the `http` one.
func canaryRunnerPorts() map[v1beta1.ComponentType][]corev1.ContainerPort {
	ports := []corev1.ContainerPort{{Name: "http", ContainerPort: 8000, Protocol: corev1.ProtocolTCP}}
	return map[v1beta1.ComponentType][]corev1.ContainerPort{
		v1beta1.EngineComponent:  ports,
		v1beta1.DecoderComponent: ports,
		v1beta1.RouterComponent:  ports,
	}
}

func canaryScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// canaryControllerRevision builds a ControllerRevision carrying the OMENative
// revision label set the rollback-target lookup selects on, named like a real
// per-(ISVC, Component) CR (`<isvc>-<component>-<hash>`) with a monotonic
// .Revision. Seeded directly into the fake client so the rollback signal has
// revision history to resolve the stable target from.
func canaryControllerRevision(ns, isvc, comp, hash string, rev int64) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      isvc + "-" + comp + "-" + hash,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           comp,
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		Revision: rev,
	}
}

// canaryPod builds a Running + Ready (serving) OMENative pod. The canary capacity
// gate counts only READY pods, so fixtures that represent live capacity must be
// Ready — a pod that merely exists does not satisfy the gate.
func canaryPod(ns, isvc, comp, hash, name string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           comp,
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelRevisionHash:               hash,
			},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	pod.Labels[query.LabelRunner] = string(v1beta1.RunnerNameDefault)
	pod.Labels[query.LabelPodOrdinal] = "0"
	if split := strings.LastIndexByte(name, '-'); split >= 0 {
		if _, err := strconv.Atoi(name[split+1:]); err == nil {
			pod.Labels[query.LabelInstanceIdx] = name[split+1:]
		}
	}
	return pod
}

func canaryRunnerPod(ns, isvc, comp, hash, name string, runner v1beta1.RunnerName, ordinal string) *corev1.Pod {
	pod := canaryPod(ns, isvc, comp, hash, name)
	pod.Labels[query.LabelRunner] = string(runner)
	if ordinal != "" {
		pod.Labels[query.LabelPodOrdinal] = ordinal
	}
	return pod
}

func canaryRoutingSelectorMatches(selector, podLabels map[string]string) bool {
	return labels.SelectorFromSet(selector).Matches(labels.Set(podLabels))
}

func TestReadyTargetInstanceCount(t *testing.T) {
	gang := observedCanaryRevisions{
		targetHash:  "target",
		runners:     []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 1}},
		fromIR:      true,
		statusFresh: true,
	}
	makePod := func(index int, runner v1beta1.RunnerName, ordinal, suffix string) *corev1.Pod {
		pod := canaryRunnerPod("default", "svc", "engine", "target", fmt.Sprintf("pod-%d-%s", index, suffix), runner, ordinal)
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(index)
		return pod
	}
	assertCount := func(t *testing.T, observation observedCanaryRevisions, pods []*corev1.Pod, want int32) {
		t.Helper()
		got := observation.readyTargetInstanceCount(pods)
		if got == nil || *got != want {
			t.Fatalf("ready target Instances = %v, want %d", got, want)
		}
	}

	t.Run("one ready pod per gang is incomplete", func(t *testing.T) {
		pods := make([]*corev1.Pod, 0, 8)
		for index := 0; index < 8; index++ {
			pods = append(pods, makePod(index, v1beta1.RunnerNameLeader, "0", "leader"))
		}
		assertCount(t, gang, pods, 0)
	})
	t.Run("complete gang", func(t *testing.T) {
		assertCount(t, gang, []*corev1.Pod{
			makePod(0, v1beta1.RunnerNameLeader, "0", "leader"),
			makePod(0, v1beta1.RunnerNameWorker, "0", "worker"),
		}, 1)
	})
	t.Run("missing runner", func(t *testing.T) {
		assertCount(t, gang, []*corev1.Pod{makePod(0, v1beta1.RunnerNameLeader, "0", "leader")}, 0)
	})
	t.Run("duplicate runner ordinal", func(t *testing.T) {
		workers := observedCanaryRevisions{
			targetHash:  "target",
			runners:     []v1beta1.Runner{{Name: v1beta1.RunnerNameWorker, Size: 2}},
			fromIR:      true,
			statusFresh: true,
		}
		assertCount(t, workers, []*corev1.Pod{
			makePod(0, v1beta1.RunnerNameWorker, "0", "worker-a"),
			makePod(0, v1beta1.RunnerNameWorker, "0", "worker-b"),
		}, 0)
	})
	t.Run("legacy pods share ordinal zero", func(t *testing.T) {
		workers := observedCanaryRevisions{
			targetHash: "target", runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameWorker, Size: 2}}, fromIR: true,
		}
		first := makePod(0, v1beta1.RunnerNameWorker, "0", "worker-a")
		second := makePod(0, v1beta1.RunnerNameWorker, "0", "worker-b")
		delete(first.Labels, query.LabelPodOrdinal)
		delete(second.Labels, query.LabelPodOrdinal)
		assertCount(t, workers, []*corev1.Pod{first, second}, 0)
	})
	t.Run("malformed ordinal", func(t *testing.T) {
		one := observedCanaryRevisions{
			targetHash: "target", runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}, fromIR: true,
		}
		assertCount(t, one, []*corev1.Pod{makePod(0, v1beta1.RunnerNameDefault, "bad", "default")}, 0)
	})
	t.Run("numeric ordinals outside runner slots", func(t *testing.T) {
		workers := observedCanaryRevisions{
			targetHash: "target", runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameWorker, Size: 2}}, fromIR: true,
		}
		assertCount(t, workers, []*corev1.Pod{
			makePod(0, v1beta1.RunnerNameWorker, "2", "worker-a"),
			makePod(0, v1beta1.RunnerNameWorker, "3", "worker-b"),
		}, 0)
		one := observedCanaryRevisions{
			targetHash: "target", runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}, fromIR: true,
		}
		assertCount(t, one, []*corev1.Pod{makePod(0, v1beta1.RunnerNameDefault, "2", "default")}, 0)
	})
	t.Run("valid runner slot boundaries", func(t *testing.T) {
		workers := observedCanaryRevisions{
			targetHash: "target", runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameWorker, Size: 2}}, fromIR: true,
		}
		assertCount(t, workers, []*corev1.Pod{
			makePod(0, v1beta1.RunnerNameWorker, "0", "worker-a"),
			makePod(0, v1beta1.RunnerNameWorker, "1", "worker-b"),
		}, 1)
		one := observedCanaryRevisions{
			targetHash: "target", runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}, fromIR: true,
		}
		assertCount(t, one, []*corev1.Pod{makePod(0, v1beta1.RunnerNameDefault, "1", "default")}, 1)
	})
	t.Run("ineligible pods", func(t *testing.T) {
		one := observedCanaryRevisions{
			targetHash:  "target",
			runners:     []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}},
			fromIR:      true,
			statusFresh: true,
		}
		valid := makePod(0, v1beta1.RunnerNameDefault, "0", "valid")
		wrongRevision := makePod(1, v1beta1.RunnerNameDefault, "0", "wrong-revision")
		wrongRevision.Labels[query.LabelRevisionHash] = "other"
		missingIndex := makePod(2, v1beta1.RunnerNameDefault, "0", "missing-index")
		delete(missingIndex.Labels, query.LabelInstanceIdx)
		terminating := makePod(3, v1beta1.RunnerNameDefault, "0", "terminating")
		deletedAt := metav1.Now()
		terminating.DeletionTimestamp = &deletedAt
		notReady := makePod(4, v1beta1.RunnerNameDefault, "0", "not-ready")
		notReady.Status.Conditions[0].Status = corev1.ConditionFalse
		assertCount(t, one, []*corev1.Pod{valid, wrongRevision, missingIndex, terminating, notReady}, 1)
	})
	t.Run("default single pod", func(t *testing.T) {
		one := observedCanaryRevisions{
			targetHash:  "target",
			runners:     []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}},
			fromIR:      true,
			statusFresh: true,
		}
		assertCount(t, one, []*corev1.Pod{makePod(0, v1beta1.RunnerNameDefault, "0", "default")}, 1)
	})
}

func TestDispatch_HoldsUntilIRIdentifiesCanaryTarget(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "target-lag"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)

	engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "target-lag-engine", Generation: 2}}
	engineIR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	engineIR.Status.CurrentRevision = "target-lag-engine-stable"
	engineIR.Status.UpdateRevision = "target-lag-engine-stable"
	engineIR.Status.ObservedGeneration = 1
	c := fake.NewClientBuilder().
		WithScheme(canaryScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(
			isvc,
			engineIR,
			canaryPod(ns, isvc.Name, "engine", "stable", "stable-0"),
			canaryPod(ns, isvc.Name, "engine", "stable", "stable-1"),
			canaryPod(ns, isvc.Name, "engine", "target", "target-2"),
			canaryPod(ns, isvc.Name, "engine", "target", "target-3"),
			canaryControllerRevision(ns, isvc.Name, "engine", "stable", 1),
			canaryControllerRevision(ns, isvc.Name, "engine", "target", 2),
		).
		Build()
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}

	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch with lagging IR status: %v", err)
	}
	if isvc.Status.Canary != nil {
		t.Fatalf("lagging IR status must not initialize the canary with inverted identities: %+v", isvc.Status.Canary)
	}

	liveIR := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: ns, Name: engineIR.Name}
	if err := c.Get(ctx, key, liveIR); err != nil {
		t.Fatal(err)
	}
	liveIR.Status.UpdateRevision = "target-lag-engine-target"
	liveIR.Status.ObservedGeneration = liveIR.Generation
	if err := c.Status().Update(ctx, liveIR); err != nil {
		t.Fatalf("publish target revision: %v", err)
	}

	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch with observed IR target: %v", err)
	}
	if isvc.Status.Canary == nil || isvc.Status.Canary.CanaryRevisionHash != "target" || isvc.Status.Canary.StableRevisionHash != "stable" {
		t.Fatalf("IR revision pair must bind target and stable identities, got %+v", isvc.Status.Canary)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("ready step-zero capacity should pause, got %q", got)
	}

	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch rollback: %v", err)
	}
	if got := isvc.Status.Canary.RolledBackRevisionHash; got != "target" {
		t.Fatalf("rollback must reject target, got %q", got)
	}
	traffic := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	wantStable := coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "stable")
	if len(traffic) != 1 || traffic[0].RevisionName != wantStable || traffic[0].Percent != 100 {
		t.Fatalf("rollback traffic must target stable: %+v", traffic)
	}
	if err := c.Get(ctx, key, liveIR); err != nil {
		t.Fatal(err)
	}
	if liveIR.Spec.Pacing == nil || liveIR.Spec.Pacing.RollbackToRevision == nil || *liveIR.Spec.Pacing.RollbackToRevision != "target-lag-engine-stable" {
		t.Fatalf("rollback must select the stable ControllerRevision, got %+v", liveIR.Spec.Pacing)
	}
}

func TestDispatch_StalePrimaryRetargetDoesNotResetCanary(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "stale-retarget"
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "middle"}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "middle",
		StableRevisionHash: "stable",
		CurrentStep:        0,
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePaused},
	}
	pinActiveRun(isvc)
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "middle")
	engineIR.Generation = 2
	engineIR.Status.ObservedGeneration = 1
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-stable"
	c := fake.NewClientBuilder().
		WithScheme(canaryScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(
			isvc,
			engineIR,
			canaryPod(ns, isvc.Name, "engine", "stable", "stable-0"),
			canaryPod(ns, isvc.Name, "engine", "stable", "stable-1"),
			canaryPod(ns, isvc.Name, "engine", "middle", "middle-2"),
			canaryPod(ns, isvc.Name, "engine", "middle", "middle-3"),
			canaryPod(ns, isvc.Name, "engine", "next", "next-4"),
			canaryPod(ns, isvc.Name, "engine", "next", "next-5"),
		).
		Build()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}

	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch with stale retarget: %v", err)
	}
	if got := isvc.Status.Canary; got.CanaryRevisionHash != "middle" || got.StableRevisionHash != "stable" || got.CurrentStep != 0 {
		t.Fatalf("stale retarget must leave the active canary unchanged: %+v", got)
	}
	if isvc.Annotations[constants.RolloutPromoteAnnotation] != "middle" {
		t.Fatal("stale retarget must not consume operator commands")
	}

	liveIR := &v1beta1.InferenceReplica{}
	key := types.NamespacedName{Namespace: ns, Name: engineIR.Name}
	if err := c.Get(context.Background(), key, liveIR); err != nil {
		t.Fatal(err)
	}
	liveIR.Status.UpdateRevision = isvc.Name + "-engine-next"
	liveIR.Status.ObservedGeneration = liveIR.Generation
	if err := c.Status().Update(context.Background(), liveIR); err != nil {
		t.Fatalf("publish retarget: %v", err)
	}
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch with fresh retarget: %v", err)
	}
	if got := isvc.Status.Canary; got.CanaryRevisionHash != "next" || got.StableRevisionHash != "stable" {
		t.Fatalf("fresh retarget must preserve stable and bind the new target: %+v", got)
	}
}

func TestDispatch_MissingOrEmptyPrimaryIRDefersCommands(t *testing.T) {
	for _, tc := range []struct {
		name  string
		empty bool
	}{
		{name: "missing"},
		{name: "empty-status", empty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := "default"
			n4 := 4
			isvc := canaryISVC(twoStep(), nil)
			isvc.Namespace = ns
			isvc.Name = "primary-" + tc.name
			isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
			isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
			isvc.Status.Canary = &v1beta1.CanaryStatus{
				CanaryRevisionHash: "target",
				StableRevisionHash: "stable",
				CurrentStep:        0,
			}
			isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePaused},
			}
			objects := []runtime.Object{
				isvc,
				canaryPod(ns, isvc.Name, "engine", "stable", "stable-0"),
				canaryPod(ns, isvc.Name, "engine", "stable", "stable-1"),
				canaryPod(ns, isvc.Name, "engine", "target", "target-2"),
				canaryPod(ns, isvc.Name, "engine", "target", "target-3"),
			}
			if tc.empty {
				emptyIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "unused")
				emptyIR.Generation = 1
				emptyIR.Status = v1beta1.InferenceReplicaStatus{ObservedGeneration: 1}
				objects = append(objects, emptyIR)
			}
			c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objects...).Build()

			if _, err := Dispatch(context.Background(), DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if got := isvc.Status.Canary; got.CanaryRevisionHash != "target" || got.StableRevisionHash != "stable" || got.RolledBackRevisionHash != "" {
				t.Fatalf("non-authoritative primary must not mutate canary status: %+v", got)
			}
			if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" {
				t.Fatal("non-authoritative primary must not consume rollback")
			}
			if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
				t.Fatalf("non-authoritative primary must preserve phase, got %q", got)
			}
		})
	}
}

func TestDispatch_StaleSecondaryDefersRollback(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns,
		Name:      "stale-secondary",
		Annotations: map[string]string{
			constants.RolloutRollbackAnnotation: "true",
		},
	}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "router-new",
		StableRevisionHash: "router-old",
		CurrentStep:        0,
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.RouterComponent: {RolloutPhase: v1beta1.RolloutPhasePaused},
	}
	routerIR := ir(ns, isvc.Name, v1beta1.RouterComponent, "router-new")
	routerIR.Generation = 1
	routerIR.Status.ObservedGeneration = 1
	routerIR.Status.CurrentRevision = isvc.Name + "-router-router-old"
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "engine-old")
	engineIR.Generation = 2
	engineIR.Status.ObservedGeneration = 1
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-engine-old"
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc,
		routerIR,
		engineIR,
		canaryPod(ns, isvc.Name, "router", "router-old", "router-old-0"),
		canaryPod(ns, isvc.Name, "router", "router-new", "router-new-1"),
		canaryPod(ns, isvc.Name, "engine", "engine-old", "engine-old-0"),
		canaryPod(ns, isvc.Name, "engine", "engine-new", "engine-new-1"),
	).Build()

	if _, err := Dispatch(context.Background(), DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if isvc.Status.Canary.RolledBackRevisionHash != "" {
		t.Fatalf("stale secondary must defer rollback: %+v", isvc.Status.Canary)
	}
	if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" {
		t.Fatal("stale secondary must not consume the rollback command")
	}
	for _, name := range []string{routerIR.Name, engineIR.Name} {
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing != nil && got.Spec.Pacing.RollbackToRevision != nil {
			t.Fatalf("stale secondary must not signal rollback on %s: %+v", name, got.Spec.Pacing)
		}
	}
}

func TestDispatch_RepairsInvertedStableIdentity(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "repair-identity"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "target",
		StableRevisionHash: "target",
		CurrentStep:        0,
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePending},
	}
	engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "repair-identity-engine"}}
	engineIR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	engineIR.Status.CurrentRevision = "repair-identity-engine-stable"
	engineIR.Status.UpdateRevision = "repair-identity-engine-target"
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc,
		engineIR,
		canaryPod(ns, isvc.Name, "engine", "stable", "stable-0"),
		canaryPod(ns, isvc.Name, "engine", "stable", "stable-1"),
		canaryPod(ns, isvc.Name, "engine", "target", "target-2"),
		canaryPod(ns, isvc.Name, "engine", "target", "target-3"),
	).Build()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}

	isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch while paused: %v", err)
	}
	if got := isvc.Status.Canary.StableRevisionHash; got != "target" {
		t.Fatalf("global pause must defer status repair, got %q", got)
	}

	// The freeze depth must hold the canary identically to a plain pause.
	isvc.Annotations[constants.PausedRolloutAnnotation] = constants.PausedRolloutFreezeValue
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch while frozen: %v", err)
	}
	if got := isvc.Status.Canary.StableRevisionHash; got != "target" {
		t.Fatalf("freeze must defer status repair like a plain pause, got %q", got)
	}

	delete(isvc.Annotations, constants.PausedRolloutAnnotation)
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := isvc.Status.Canary.StableRevisionHash; got != "stable" {
		t.Fatalf("authoritative IR current revision must repair stable identity, got %q", got)
	}
	traffic := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	if len(traffic) != 2 {
		t.Fatalf("repaired canary must retain distinct stable and target traffic: %+v", traffic)
	}
}

func TestDispatch_MultiPodSecondaryCapacityCountsInstances(t *testing.T) {
	ns := "default"
	n2, n8 := 2, 8
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "gang-capacity"}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n8}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "rtrtarget",
		StableRevisionHash: "rtrstable",
		CurrentStep:        1,
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.RouterComponent: {RolloutPhase: v1beta1.RolloutPhaseCanarying},
	}
	routerIR := ir(ns, isvc.Name, v1beta1.RouterComponent, "rtrtarget")
	routerIR.Status.CurrentRevision = isvc.Name + "-router-rtrstable"
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "engtarget")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-engstable"
	engineIR.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1},
		{Name: v1beta1.RunnerNameWorker, Size: 1},
	}
	objects := []runtime.Object{isvc, routerIR, engineIR}
	addPod := func(comp v1beta1.ComponentType, hash string, instance int, runner v1beta1.RunnerName) {
		name := fmt.Sprintf("%s-%s-%s-%d", isvc.Name, comp, runner, instance)
		pod := canaryRunnerPod(ns, isvc.Name, string(comp), hash, name, runner, "0")
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(instance)
		objects = append(objects, pod)
	}
	addPod(v1beta1.RouterComponent, "rtrtarget", 0, v1beta1.RunnerNameDefault)
	addPod(v1beta1.RouterComponent, "rtrtarget", 1, v1beta1.RunnerNameDefault)
	addPod(v1beta1.RouterComponent, "rtrstable", 2, v1beta1.RunnerNameDefault)
	for i := 0; i < 3; i++ {
		addPod(v1beta1.EngineComponent, "engstable", i, v1beta1.RunnerNameLeader)
		addPod(v1beta1.EngineComponent, "engstable", i, v1beta1.RunnerNameWorker)
	}
	for i := 3; i < 8; i++ {
		addPod(v1beta1.EngineComponent, "engtarget", i, v1beta1.RunnerNameLeader)
		addPod(v1beta1.EngineComponent, "engtarget", i, v1beta1.RunnerNameWorker)
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objects...).Build()

	if _, err := Dispatch(context.Background(), DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.RouterComponent].RolloutPhase; got != v1beta1.RolloutPhasePending {
		t.Fatalf("five ready gangs must not satisfy eight-instance capacity, got %q", got)
	}
	if got := isvc.Status.Canary.CurrentStep; got != 1 {
		t.Fatalf("capacity shortfall must hold the final step, got %d", got)
	}
}

func TestDispatch_ReadyTargetHashResyncsBeforePromotion(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "ready-target"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "stable",
		CurrentStep:        0,
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePending},
	}
	pinActiveRun(isvc)

	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ready-target-engine"}}
	ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	ir.Status.UpdateRevision = "ready-target-engine-target"
	ir.Status.CurrentRevision = "ready-target-engine-stable"
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc,
		ir,
		canaryPod(ns, isvc.Name, "engine", "target", "target-0"),
		canaryPod(ns, isvc.Name, "engine", "target", "target-1"),
		canaryPod(ns, isvc.Name, "engine", "stable", "stable-0"),
		canaryPod(ns, isvc.Name, "engine", "stable", "stable-1"),
	).Build()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}

	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}
	if got := isvc.Status.Canary.CanaryRevisionHash; got != "target" {
		t.Fatalf("ready live target must be published before the pause, got %q", got)
	}
	if got := isvc.Status.Canary.StableRevisionHash; got != "stable" {
		t.Fatalf("stable identity must remain stable, got %q", got)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("ready target should enter the manual pause, got %q", got)
	}

	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: isvc.Status.Canary.CanaryRevisionHash}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("promoting Dispatch error: %v", err)
	}
	if got := isvc.Status.Canary.CurrentStep; got != 1 {
		t.Fatalf("promotion of the published target must advance, got step %d", got)
	}
}

func TestDispatch_UsesObservedRunnerShapeForRevisionServices(t *testing.T) {
	ns := "default"
	n2 := 2
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "runtime-shape"}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{
			Capacity: intstr.FromString("50%"),
			Traffic:  50,
		}}},
	}}}
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "newhash")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-stable0"
	multiLeader := canaryRunnerPod(ns, isvc.Name, "engine", "newhash", isvc.Name+"-engine-1", v1beta1.RunnerNameLeader, "0")
	multiWorker := canaryRunnerPod(ns, isvc.Name, "engine", "newhash", isvc.Name+"-engine-2", v1beta1.RunnerNameWorker, "0")
	stablePod := canaryRunnerPod(ns, isvc.Name, "engine", "stable0", isvc.Name+"-engine-0", v1beta1.RunnerNameDefault, "0")
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc,
		engineIR,
		multiLeader,
		multiWorker,
		stablePod,
	).Build()
	ctx := context.Background()

	if _, err := Dispatch(ctx, DispatchDeps{
		Client:               c,
		Reader:               c,
		ISVC:                 isvc,
		ComponentRunnerPorts: canaryRunnerPorts(),
	}); err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}

	for _, tc := range []struct {
		hash     string
		multiPod bool
	}{
		{hash: "stable0", multiPod: false},
		{hash: "newhash", multiPod: true},
	} {
		routing := &corev1.Service{}
		routingKey := types.NamespacedName{
			Namespace: ns,
			Name:      coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, tc.hash),
		}
		if err := c.Get(ctx, routingKey, routing); err != nil {
			t.Fatalf("routing Service for %s missing: %v", tc.hash, err)
		}
		if tc.multiPod {
			if got := routing.Spec.Selector[query.LabelRunner]; got != string(v1beta1.RunnerNameLeader) {
				t.Errorf("multi-pod routing Service selector %s: got %q want %q", query.LabelRunner, got, v1beta1.RunnerNameLeader)
			}
			if got := routing.Spec.Selector[query.LabelPodOrdinal]; got != "0" {
				t.Errorf("multi-pod routing Service selector %s: got %q want 0", query.LabelPodOrdinal, got)
			}
			if !canaryRoutingSelectorMatches(routing.Spec.Selector, multiLeader.Labels) {
				t.Errorf("multi-pod leader must match routing selector: selector=%v labels=%v", routing.Spec.Selector, multiLeader.Labels)
			}
			if canaryRoutingSelectorMatches(routing.Spec.Selector, multiWorker.Labels) {
				t.Errorf("multi-pod worker must not match routing selector: selector=%v labels=%v", routing.Spec.Selector, multiWorker.Labels)
			}
		} else {
			if _, ok := routing.Spec.Selector[query.LabelRunner]; ok {
				t.Errorf("single-pod routing Service selector must not include %s: %v", query.LabelRunner, routing.Spec.Selector)
			}
			if _, ok := routing.Spec.Selector[query.LabelPodOrdinal]; ok {
				t.Errorf("single-pod routing Service selector must not include %s: %v", query.LabelPodOrdinal, routing.Spec.Selector)
			}
			if !canaryRoutingSelectorMatches(routing.Spec.Selector, stablePod.Labels) {
				t.Errorf("single-pod revision must match broad routing selector: selector=%v labels=%v", routing.Spec.Selector, stablePod.Labels)
			}
		}

		headless := &corev1.Service{}
		headlessKey := types.NamespacedName{
			Namespace: ns,
			Name:      coordination.PerRevisionHeadlessServiceName(isvc.Name, v1beta1.EngineComponent, tc.hash),
		}
		if err := c.Get(ctx, headlessKey, headless); err != nil {
			t.Fatalf("headless Service for %s missing: %v", tc.hash, err)
		}
		if _, ok := headless.Spec.Selector[query.LabelRunner]; ok {
			t.Errorf("headless Service for %s selector must not include %s: %v", tc.hash, query.LabelRunner, headless.Spec.Selector)
		}
		if _, ok := headless.Spec.Selector[query.LabelPodOrdinal]; ok {
			t.Errorf("headless Service for %s selector must not include %s: %v", tc.hash, query.LabelPodOrdinal, headless.Spec.Selector)
		}
	}
}

// TestDispatch_ServicesHashAndRollbackWarning exercises the controller-side entry
// point: per-revision Service ensure, IR-sourced canary hash, and the deferred-
// rollback warning event.
func TestDispatch_ServicesHashAndRollbackWarning(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1"}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{
			Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
	}}}
	pinActiveRun(isvc)
	// IR names the canary target (its UpdateRevision) — Dispatch must source the
	// hash from here, not the (stale) ISVC aggregate.
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1-engine"}}
	ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	ir.Status.UpdateRevision = "d1-engine-newhash"
	ir.Status.CurrentRevision = "d1-engine-stable0" // the stable revision rollback reverts to

	objs := []runtime.Object{isvc, ir,
		canaryPod(ns, "d1", "engine", "newhash", "d1-engine-2"),
		canaryPod(ns, "d1", "engine", "newhash", "d1-engine-3"),
		canaryPod(ns, "d1", "engine", "stable0", "d1-engine-0"),
		canaryPod(ns, "d1", "engine", "stable0", "d1-engine-1"),
		// Revision history the rollback signal resolves the stable target from.
		canaryControllerRevision(ns, "d1", "engine", "stable0", 1),
		canaryControllerRevision(ns, "d1", "engine", "newhash", 2),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()

	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}
	// IR-sourced hash recorded.
	if isvc.Status.Canary == nil || isvc.Status.Canary.CanaryRevisionHash != "newhash" {
		t.Fatalf("canary hash should come from the IR UpdateRevision (newhash), got %+v", isvc.Status.Canary)
	}
	// Per-revision Services created for both live revisions.
	for _, h := range []string{"newhash", "stable0"} {
		svc := &corev1.Service{}
		key := types.NamespacedName{Namespace: ns, Name: coordination.PerRevisionServiceName("d1", v1beta1.EngineComponent, h)}
		if err := c.Get(ctx, key, svc); err != nil {
			t.Fatalf("per-revision Service for %s should exist: %v", h, err)
		}
	}
	// Capacity met (2 new >= 50%% of 4) → Manual pause.
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("expected Paused (capacity met, Manual), got %q", got)
	}

	// Rollback: the executor records the rejected hash + Dispatch points the IR at
	// the stable revision (Pacing.RollbackToRevision = IR CurrentRevision).
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("Dispatch (rollback) error: %v", err)
	}
	if isvc.Status.Canary == nil || isvc.Status.Canary.RolledBackRevisionHash != "newhash" {
		t.Fatalf("rollback should record the rejected hash (newhash), got %+v", isvc.Status.Canary)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("expected RollingBack (canary pods still present), got %q", got)
	}
	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "d1-engine"}, gotIR); err != nil {
		t.Fatal(err)
	}
	if gotIR.Spec.Pacing == nil || gotIR.Spec.Pacing.RollbackToRevision == nil ||
		*gotIR.Spec.Pacing.RollbackToRevision != "d1-engine-stable0" {
		t.Fatalf("IR should be signaled to roll back to the stable revision, got pacing=%+v", gotIR.Spec.Pacing)
	}
}

// TestDispatch_PDRouterRollbackTargetsStable is the PD-canary rollback regression:
// the router (the PD primary, often a 1-replica proxy) fully rolls forward to its
// canary revision before the rollback fires — so the router IR's CurrentRevision is
// PROMOTED to the canary hash (RolloutComplete: all Instances on target). The
// rollback signal must NOT echo CurrentRevision back as the roll target (that would
// "roll back" the router to the revision it is already on — a no-op — so the canary
// pods never drain and the rollout wedges in RollingBack forever). It must resolve
// the STABLE pre-canary revision from the run's persisted per-Component identity.
func TestDispatch_PDRouterRollbackTargetsStable(t *testing.T) {
	ns := "default"
	n1 := 1
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}}
	// Router is the PD primary (router > engine > decoder). 1 replica is the common
	// router shape: a 50% step rounds its newCount up to 1, partition 0 — so the
	// single router pod rolls fully to canary at EVERY step.
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	// Canary in progress at the final step (breach about to roll back). The router
	// has shifted 100% traffic to its canary.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "rtrNew", CurrentStep: 1, ObservedTrafficWeight: 100}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: "rtrNew", StableRevision: "rtrStable"},
		{Component: v1beta1.EngineComponent, Revision: "engNew", StableRevision: "engStable"},
		{Component: v1beta1.DecoderComponent, Revision: "decNew", StableRevision: "decStable"},
	}
	// Router IR fully rolled to canary: CurrentRevision == UpdateRevision == canary CR.
	rtrIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd-router"}}
	rtrIR.Status.UpdateRevision = "pd-router-rtrNew"
	rtrIR.Status.CurrentRevision = "pd-router-rtrNew"
	engIR := ir(ns, "pd", v1beta1.EngineComponent, "engNew")
	decIR := ir(ns, "pd", v1beta1.DecoderComponent, "decNew")

	// Engine/decoder IRs also fully rolled to their own per-Component canary.
	engIR.Status.CurrentRevision = "pd-engine-engNew"
	decIR.Status.CurrentRevision = "pd-decoder-decNew"

	objs := []runtime.Object{isvc, rtrIR, engIR, decIR,
		// Every Component fully on its canary revision — no stable pods remain.
		canaryPod(ns, "pd", "router", "rtrNew", "pd-router-0"),
		canaryPod(ns, "pd", "engine", "engNew", "pd-engine-0"),
		canaryPod(ns, "pd", "decoder", "decNew", "pd-decoder-0"),
		// The exact persisted hashes resolve to these retained ControllerRevisions,
		// even though no stable pods are live. Hashes are per-Component.
		canaryControllerRevision(ns, "pd", "router", "rtrStable", 1),
		canaryControllerRevision(ns, "pd", "router", "rtrNew", 2),
		canaryControllerRevision(ns, "pd", "engine", "engStable", 1),
		canaryControllerRevision(ns, "pd", "engine", "engNew", 2),
		canaryControllerRevision(ns, "pd", "decoder", "decStable", 1),
		canaryControllerRevision(ns, "pd", "decoder", "decNew", 2),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()

	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("Dispatch (rollback) error: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.RouterComponent].RolloutPhase; got != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("router still has canary pods → RollingBack, got %q", got)
	}
	// Every group Component's IR must be signaled to roll back to its OWN stable
	// pre-canary revision — never to the canary it is already on (a no-op that
	// would wedge the drain). A PD rollback that signals only the router leaves
	// the engine/decoder serving the rejected revision behind the rolled-back router.
	for comp, want := range map[v1beta1.ComponentType]string{
		v1beta1.RouterComponent:  "pd-router-rtrStable",
		v1beta1.EngineComponent:  "pd-engine-engStable",
		v1beta1.DecoderComponent: "pd-decoder-decStable",
	} {
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "pd-" + string(comp)}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil {
			t.Fatalf("%s IR must be signaled to roll back, got pacing=%+v", comp, got.Spec.Pacing)
		}
		if *got.Spec.Pacing.RollbackToRevision != want {
			t.Fatalf("%s rollback target must be its stable revision %q (not the canary no-op), got %q",
				comp, want, *got.Spec.Pacing.RollbackToRevision)
		}
	}
}

func TestDispatch_SecondaryOnlyTargetStartsCanary(t *testing.T) {
	ns := "default"
	n1, n4 := 1, 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "secondary-only"}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 25, Pause: &v1beta1.RolloutPause{}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.RunID = "engine-run"
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: "router", StableRevision: "router"},
		{Component: v1beta1.EngineComponent, Revision: "enginenew", StableRevision: "engineold"},
	}
	routerIR := ir(ns, isvc.Name, v1beta1.RouterComponent, "router")
	routerIR.Status.CurrentRevision = "secondary-only-router-router"
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "enginenew")
	engineIR.Status.CurrentRevision = "secondary-only-engine-engineold"
	objects := []runtime.Object{
		isvc,
		routerIR,
		engineIR,
		canaryPod(ns, isvc.Name, "router", "router", "secondary-only-router-0"),
		canaryPod(ns, isvc.Name, "engine", "engineold", "secondary-only-engine-0"),
		canaryPod(ns, isvc.Name, "engine", "engineold", "secondary-only-engine-1"),
		canaryPod(ns, isvc.Name, "engine", "enginenew", "secondary-only-engine-2"),
		canaryPod(ns, isvc.Name, "engine", "enginenew", "secondary-only-engine-3"),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objects...).Build()

	if _, err := Dispatch(context.Background(), DispatchDeps{
		Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(),
	}); err != nil {
		t.Fatal(err)
	}
	if isvc.Status.Canary == nil || isvc.Status.Canary.CurrentStep != 0 {
		t.Fatalf("secondary-only target must initialize canary step 0, got %+v", isvc.Status.Canary)
	}
	if got := isvc.Status.Components[v1beta1.RouterComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("secondary capacity reached step 0; phase = %q, want Paused", got)
	}
	traffic := isvc.Status.Components[v1beta1.RouterComponent].Traffic
	if len(traffic) != 1 || traffic[0].Percent != 100 || query.RevisionFromName(traffic[0].RevisionName).Hash() != "router" {
		t.Fatalf("unchanged primary traffic = %+v, want one 100%% router target", traffic)
	}
}

// TestDispatch_RollbackTargetsPersistedStableAfterRetarget pins the
// controller-side rollback resolution: after a stable-A → partial-B →
// retarget-C sequence, the rollback signal must point the IR at A's
// ControllerRevision — the persisted stable identity — even though B is both
// the most-populated live non-canary revision AND the highest-numbered
// non-rejected ControllerRevision (every inference-based resolution names B).
func TestDispatch_RollbackTargetsPersistedStableAfterRetarget(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "rt1"}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	// Mid-canary toward C after the retarget: the persisted stable is A.
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash:    "revC",
		StableRevisionHash:    "revA",
		CurrentStep:           0,
		ObservedTrafficWeight: 50,
	}
	objs := []runtime.Object{isvc, ir(ns, "rt1", v1beta1.EngineComponent, "revC"),
		canaryPod(ns, "rt1", "engine", "revA", "rt1-engine-0"),
		canaryPod(ns, "rt1", "engine", "revB", "rt1-engine-1"),
		canaryPod(ns, "rt1", "engine", "revB", "rt1-engine-2"),
		canaryPod(ns, "rt1", "engine", "revC", "rt1-engine-3"),
		canaryControllerRevision(ns, "rt1", "engine", "revA", 1),
		canaryControllerRevision(ns, "rt1", "engine", "revB", 2),
		canaryControllerRevision(ns, "rt1", "engine", "revC", 3),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()

	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("Dispatch (rollback) error: %v", err)
	}
	if isvc.Status.Canary.RolledBackRevisionHash != "revC" {
		t.Fatalf("rollback should reject C, got %+v", isvc.Status.Canary)
	}
	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "rt1-engine"}, gotIR); err != nil {
		t.Fatal(err)
	}
	if gotIR.Spec.Pacing == nil || gotIR.Spec.Pacing.RollbackToRevision == nil ||
		*gotIR.Spec.Pacing.RollbackToRevision != "rt1-engine-revA" {
		t.Fatalf("rollback must target the persisted stable A, got pacing=%+v", gotIR.Spec.Pacing)
	}
	tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	wantStable := coordination.PerRevisionServiceName("rt1", v1beta1.EngineComponent, "revA")
	if len(tr) != 1 || tr[0].Percent != 100 || tr[0].RevisionName != wantStable {
		t.Fatalf("rollback traffic must go 100%% to %s, got %+v", wantStable, tr)
	}
}

// TestStableRevisionName_PersistedIdentity pins exact rollback resolution: a
// missing identity or missing ControllerRevision resolves to no target rather
// than selecting a revision by history order.
func TestStableRevisionName_PersistedIdentity(t *testing.T) {
	ns := "default"
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "sr1"}}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		canaryControllerRevision(ns, "sr1", "engine", "revA", 1),
		canaryControllerRevision(ns, "sr1", "engine", "revB", 2),
		canaryControllerRevision(ns, "sr1", "engine", "revC", 3),
	).Build()
	ctx := context.Background()

	got, err := stableRevisionName(ctx, c, isvc, v1beta1.EngineComponent, "revA")
	if err != nil || got != "sr1-engine-revA" {
		t.Fatalf("persisted identity must resolve exactly: got %q err=%v", got, err)
	}
	got, err = stableRevisionName(ctx, c, isvc, v1beta1.EngineComponent, "")
	if err != nil || got != "" {
		t.Fatalf("no persisted identity must not guess: got %q err=%v", got, err)
	}
	got, err = stableRevisionName(ctx, c, isvc, v1beta1.EngineComponent, "gone0000")
	if err != nil || got != "" {
		t.Fatalf("persisted identity not retained → no target (no guessing), got %q err=%v", got, err)
	}
}

func TestComponentStableRevisionHashPrecedence(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Status.Canary = &v1beta1.CanaryStatus{StableRevisionHash: "primary-legacy"}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {LatestRolledoutRevision: "model-engine-secondary-legacy"},
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{
		LastRun: &v1beta1.RolloutRunRecord{
			Outcome: v1beta1.RolloutRunRolledBack,
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, StableRevision: "secondary-recorded"},
			},
		},
	}

	if got := componentStableRevisionHash(isvc, v1beta1.EngineComponent, v1beta1.RouterComponent); got != "secondary-recorded" {
		t.Fatalf("closed-run secondary stable = %q, want secondary-recorded", got)
	}
	isvc.Status.Rollout.ActiveRun = &v1beta1.RolloutRun{TargetRevisions: []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, StableRevision: "secondary-active"},
	}}
	if got := componentStableRevisionHash(isvc, v1beta1.EngineComponent, v1beta1.RouterComponent); got != "secondary-active" {
		t.Fatalf("active-run secondary stable = %q, want secondary-active", got)
	}
	if got := componentStableRevisionHash(isvc, v1beta1.RouterComponent, v1beta1.RouterComponent); got != "primary-legacy" {
		t.Fatalf("primary compatibility stable = %q, want primary-legacy", got)
	}
	isvc.Status.Rollout = nil
	if got := componentStableRevisionHash(isvc, v1beta1.EngineComponent, v1beta1.RouterComponent); got != "legacy" {
		t.Fatalf("secondary compatibility stable = %q, want legacy", got)
	}
}

// TestRollbackSignal_UnknownStableKeepsTarget pins the compatibility behavior
// for objects without an exact stable identity: never replace or clear an
// existing rollback signal by guessing from ControllerRevision ordering.
func TestRollbackSignal_UnknownStableKeepsTarget(t *testing.T) {
	ns := "default"
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1"}}
	rbIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1-engine"}}
	rbIR.Status.UpdateRevision = "d1-engine-newhash"
	hold := "d1-engine-stable0"
	rbIR.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &hold}

	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(isvc, rbIR,
		canaryControllerRevision(ns, "d1", "engine", "stable0", 1),
		canaryControllerRevision(ns, "d1", "engine", "newhash", 2),
	).Build()

	if err := reconcileRollbackSignal(context.Background(), c, c, isvc, v1beta1.EngineComponent, "", true); err != nil {
		t.Fatal(err)
	}
	if err := reconcileRollbackSignal(context.Background(), c, c, isvc, v1beta1.EngineComponent, "missing", true); err != nil {
		t.Fatal(err)
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "d1-engine"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != hold {
		t.Fatalf("RollbackToRevision must survive an unknown stable identity, got pacing=%+v", got.Spec.Pacing)
	}
}

// TestDispatch_EnsuresPerRevisionServicesForSecondary pins that a multi-Component
// canary ensures per-revision Services for EVERY group Component, not just the
// primary. coordination skips canary-owned Components, so the canary engine is
// their sole producer; ensuring only the primary's would leave the secondary's
// per-revision Service missing for the weighted-route consumer.
func TestDispatch_EnsuresPerRevisionServicesForSecondary(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd1"}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary: &v1beta1.GroupCanary{
			Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
		},
	}}}
	objs := []runtime.Object{isvc,
		canaryPod(ns, "pd1", "engine", "newhash", "pd1-engine-0"),
		canaryPod(ns, "pd1", "decoder", "newhash", "pd1-decoder-0"),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}
	svc := &corev1.Service{}
	key := types.NamespacedName{Namespace: ns, Name: coordination.PerRevisionServiceName("pd1", v1beta1.DecoderComponent, "newhash")}
	if err := c.Get(ctx, key, svc); err != nil {
		t.Fatalf("per-revision Service for the decoder secondary should exist (else its peer revision endpoint dangles): %v", err)
	}
}

// TestDispatch_RecreatesDeletedServiceWhenConverged is the canary-side
// self-heal regression test: a per-revision Service deleted out-of-band must
// be recreated on the next Dispatch even when the live revision-hash set
// already matches the recorded Traffic set. Ensure runs unconditionally so
// the weighted-route consumer never routes to a dead backend.
func TestDispatch_RecreatesDeletedServiceWhenConverged(t *testing.T) {
	ns := "default"
	n1 := 1
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "cv1"}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	// A single revision present, recorded as the engine's Traffic set — the
	// live and recorded hash sets agree, which is the state the deleted
	// Service must still be recreated in.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Traffic: []v1beta1.ComponentTrafficTarget{{
			RevisionName: coordination.PerRevisionServiceName("cv1", v1beta1.EngineComponent, "stable0"),
			Percent:      100, LatestRevision: true,
		}}},
	}
	objs := []runtime.Object{isvc, ir(ns, "cv1", v1beta1.EngineComponent, "stable0"),
		canaryPod(ns, "cv1", "engine", "stable0", "cv1-engine-0"),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()

	// First Dispatch ensures the per-revision Service.
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("first Dispatch error: %v", err)
	}
	key := types.NamespacedName{Namespace: ns, Name: coordination.PerRevisionServiceName("cv1", v1beta1.EngineComponent, "stable0")}
	if err := c.Get(ctx, key, &corev1.Service{}); err != nil {
		t.Fatalf("setup: routing Service should exist after first Dispatch: %v", err)
	}

	// Delete the Service out-of-band while the revision set stays converged.
	if err := c.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}); err != nil {
		t.Fatalf("delete out-of-band: %v", err)
	}
	if err := c.Get(ctx, key, &corev1.Service{}); err == nil {
		t.Fatalf("setup: routing Service should be gone after delete")
	}

	// Second Dispatch: converged, but the deleted Service must be recreated.
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts()}); err != nil {
		t.Fatalf("second Dispatch error: %v", err)
	}
	if err := c.Get(ctx, key, &corev1.Service{}); err != nil {
		t.Errorf("converged revision set: deleted per-revision Service must be recreated, got: %v", err)
	}
}

// TestPrimaryComponent pins the router>engine>decoder priority for the externally-
// routed Component that carries the canary's traffic weight. In v2 the primary is
// chosen among the canary GROUP's Components (not the raw Spec.* presence), so the
// fixture sets the group membership directly.
func TestPrimaryComponent(t *testing.T) {
	mk := func(comps ...v1beta1.ComponentType) *v1beta1.InferenceService {
		isvc := &v1beta1.InferenceService{}
		if len(comps) > 0 {
			isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: comps,
				Canary:     &v1beta1.GroupCanary{},
			}}}
		}
		return isvc
	}
	if got := primaryComponent(mk(v1beta1.EngineComponent)); got != v1beta1.EngineComponent {
		t.Fatalf("engine-only → engine, got %q", got)
	}
	if got := primaryComponent(mk(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent)); got != v1beta1.RouterComponent {
		t.Fatalf("PD with router → router, got %q", got)
	}
	if got := primaryComponent(mk(v1beta1.EngineComponent, v1beta1.DecoderComponent)); got != v1beta1.EngineComponent {
		t.Fatalf("engine+decoder (no router) → engine, got %q", got)
	}
	if got := primaryComponent(mk()); got != "" {
		t.Fatalf("no canary group → empty, got %q", got)
	}
}

func TestOtherRevision(t *testing.T) {
	if got := otherRevision(map[string]int32{"new": 2, "old": 3}, "new"); got != "old" {
		t.Fatalf("want old, got %q", got)
	}
	if got := otherRevision(map[string]int32{"new": 4}, "new"); got != "" {
		t.Fatalf("only canary present → empty, got %q", got)
	}
	if got := otherRevision(map[string]int32{"a": 1, "b": 5, "new": 2}, "new"); got != "b" {
		t.Fatalf("most-populated non-canary → b, got %q", got)
	}
}

func TestComponentReplicas(t *testing.T) {
	n := 4
	isvc := &v1beta1.InferenceService{}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n, MaxReplicas: 6}}
	if got := componentReplicas(isvc, v1beta1.EngineComponent); got != 4 {
		t.Fatalf("MinReplicas wins → 4, got %d", got)
	}
	isvc.Spec.Engine.MinReplicas = nil
	if got := componentReplicas(isvc, v1beta1.EngineComponent); got != 6 {
		t.Fatalf("fallback MaxReplicas → 6, got %d", got)
	}
	if got := componentReplicas(isvc, v1beta1.DecoderComponent); got != 0 {
		t.Fatalf("absent component → 0, got %d", got)
	}
}
