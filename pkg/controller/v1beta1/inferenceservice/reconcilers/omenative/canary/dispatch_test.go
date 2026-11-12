package canary

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// mustSecondaryReady runs secondaryCapacityReady over the replicas reads
// holds, each seeded with the per-Instance rows the gate counts: one Ready
// row per Instance of each revision in perRev, the first readyPerRev of them
// with their pod in rotation. It fails the test on a read error, so gate
// assertions stay one-liners.
func mustSecondaryReady(t *testing.T, ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, perRev, readyPerRev map[v1beta1.ComponentType]map[string]int32, primary v1beta1.ComponentType) bool {
	t.Helper()
	list := &v1beta1.InferenceReplicaList{}
	if err := reads.List(ctx, list); err != nil {
		t.Fatalf("list replicas: %v", err)
	}
	objects := make([]runtime.Object, 0, len(list.Items))
	for i := range list.Items {
		r := &list.Items[i]
		objects = append(objects, withInstanceRows(r, perRev[r.Spec.Component], readyPerRev[r.Spec.Component]))
	}
	seeded := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objects...).Build()
	ok, fresh, _, _, err := secondaryCapacityReady(ctx, seeded, isvc, perRev, nil, primary, rollout.CanaryGroup(isvc, rollout.Policies{}), time.Unix(1000, 0))
	if err != nil {
		t.Fatalf("secondaryCapacityReady error: %v", err)
	}
	if !fresh {
		t.Fatal("secondaryCapacityReady unexpectedly observed stale IR status")
	}
	return ok
}

// withInstanceRows returns a copy of the replica carrying one Ready row per
// Instance of each revision in total, the first serving[hash] of them with
// their pod in rotation and the rest with it out of rotation.
func withInstanceRows(r *v1beta1.InferenceReplica, total, serving map[string]int32) *v1beta1.InferenceReplica {
	out := r.DeepCopy()
	out.ResourceVersion = ""
	out.Status.InstanceStatuses = nil
	hashes := make([]string, 0, len(total))
	for hash := range total {
		hashes = append(hashes, hash)
	}
	slices.Sort(hashes)
	index := int32(0)
	for _, hash := range hashes {
		for i := int32(0); i < total[hash]; i++ {
			inRotation := int32(0)
			if i < serving[hash] {
				inRotation = 1
			}
			out.Status.InstanceStatuses = append(out.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
				Index:             index,
				Phase:             v1beta1.OMENativeInstanceReady,
				RunningRevision:   out.Name + "-" + hash,
				PodCount:          1,
				ServingPodCount:   inRotation,
				AvailablePodCount: inRotation,
			})
			index++
		}
	}
	return out
}

// syncInstanceRows publishes on each Component's replica the per-Instance
// rows the replica controller would publish for the pods the fake client
// holds: one row per Instance index, on the revision of its serving target
// pod (else its first serving pod, else its first pod), its pods counted and
// the ones Ready with the serving gate and not terminating in rotation. A
// row the fixture already holds keeps its phase, anchors and record and
// takes the revision and the counts; an Instance whose pods are all gone
// keeps its row at zero pods. Fixtures that move pods between passes call it
// before the next dispatch. A Component without a replica is left alone.
func syncInstanceRows(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, components ...v1beta1.ComponentType) {
	t.Helper()
	ctx := context.Background()
	for _, comp := range components {
		ir := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, irprojector.RoleReplicaKey(isvc, comp), ir); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			t.Fatal(err)
		}
		pods := &corev1.PodList{}
		if err := c.List(ctx, pods, client.InNamespace(isvc.Namespace), client.MatchingLabels{
			constants.InferenceServicePodLabelKey: irprojector.RoleReplicaPrefix(isvc, comp),
			constants.OMEComponentLabel:           string(comp),
		}); err != nil {
			t.Fatal(err)
		}
		ir.Status.InstanceStatuses = instanceRowsFromPods(ir, pods.Items)
		// A fake that registers the status subresource takes the rows there;
		// one that does not takes them on the object.
		if err := c.Status().Update(ctx, ir); err != nil {
			if err := c.Update(ctx, ir); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// instanceRowsFromPods is the replica's rows for its pods: the fixture's own
// rows refreshed from the pods at their index, then a Ready row for every
// index the fixture has no row for, in index order.
func instanceRowsFromPods(ir *v1beta1.InferenceReplica, pods []corev1.Pod) []v1beta1.OMENativeInstanceStatus {
	target := query.RevisionFromName(ir.Status.UpdateRevision).Hash()
	byIndex := map[int32][]*corev1.Pod{}
	for i := range pods {
		pod := &pods[i]
		index, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		byIndex[index] = append(byIndex[index], pod)
	}
	seen := map[int32]bool{}
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(ir.Status.InstanceStatuses)+len(byIndex))
	for _, row := range ir.Status.InstanceStatuses {
		seen[row.Index] = true
		if group := byIndex[row.Index]; len(group) > 0 {
			row.RunningRevision = ir.Name + "-" + instanceRevision(group, target)
		}
		row.PodCount, row.ServingPodCount = countInstancePods(byIndex[row.Index])
		row.AvailablePodCount = row.ServingPodCount
		rows = append(rows, row)
	}
	for index, group := range byIndex {
		if seen[index] {
			continue
		}
		total, serving := countInstancePods(group)
		rows = append(rows, v1beta1.OMENativeInstanceStatus{
			Index:             index,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   ir.Name + "-" + instanceRevision(group, target),
			PodCount:          total,
			ServingPodCount:   serving,
			AvailablePodCount: serving,
		})
	}
	slices.SortFunc(rows, func(a, b v1beta1.OMENativeInstanceStatus) int { return int(a.Index - b.Index) })
	return rows
}

// instanceRevision is the revision hash an Instance's pods run: its serving
// pod on the target revision, else its first serving pod, else its first
// pod, by name.
func instanceRevision(group []*corev1.Pod, target string) string {
	slices.SortFunc(group, func(a, b *corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	revision, serving := "", ""
	for _, pod := range group {
		hash := pod.Labels[query.LabelRevisionHash]
		if revision == "" {
			revision = hash
		}
		if !coordination.PodReadyAndServing(pod) {
			continue
		}
		if serving == "" || hash == target {
			serving = hash
		}
	}
	if serving != "" {
		return serving
	}
	return revision
}

// countInstancePods counts an Instance's pods and the ones in rotation.
func countInstancePods(pods []*corev1.Pod) (total, serving int32) {
	for _, pod := range pods {
		total++
		if coordination.PodReadyAndServing(pod) {
			serving++
		}
	}
	return total, serving
}

// ir builds an InferenceReplica whose UpdateRevision names the per-Component
// canary target observed by Dispatch.
func ir(ns, isvc string, comp v1beta1.ComponentType, hash string) *v1beta1.InferenceReplica {
	name := isvc + "-" + string(comp)
	r := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: name, UID: canaryIRUID(name),
	}}
	r.Spec.Component = comp
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
	// router=rtr, engine=eng — distinct, as in a deployed service.
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
			ready, fresh, _, _, err := secondaryCapacityReady(context.Background(), reads, isvc, perRev, nil, v1beta1.RouterComponent, rollout.CanaryGroup(isvc, rollout.Policies{}), time.Unix(1000, 0))
			if err != nil {
				t.Fatalf("secondaryCapacityReady: %v", err)
			}
			if ready || fresh {
				t.Fatalf("non-authoritative secondary must fail closed: ready=%v fresh=%v", ready, fresh)
			}
		})
	}
}

// TestSecondaryCapacityReady_PerComponentHashes pins per-Component hash
// resolution: revision hashes are PER-Component, so gating a secondary on the
// PRIMARY's hash would find 0 secondary canary pods and block Pending forever
// even though the secondary's OWN canary capacity is fully up. The gate must
// resolve each secondary's own target hash.
func TestSecondaryCapacityReady_PerComponentHashes(t *testing.T) {
	ns := "default"
	n4 := 4
	ctx := context.Background()
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd-canary"}}
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
			ir(ns, "pd-canary", v1beta1.RouterComponent, "53db7c07"),
			ir(ns, "pd-canary", v1beta1.EngineComponent, "73e1335d"),
			ir(ns, "pd-canary", v1beta1.DecoderComponent, "f8f90d6d"),
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
	pd := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd-canary"}}
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
		ir(ns, "pd-canary", v1beta1.RouterComponent, "53db7c07"),
		ir(ns, "pd-canary", v1beta1.EngineComponent, "engStable"),
		ir(ns, "pd-canary", v1beta1.DecoderComponent, "decStable"),
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

// canaryIRUID is the UID a fixture InferenceReplica carries, so a revision can
// name that replica as its controller.
func canaryIRUID(name string) types.UID {
	return types.UID(name + "-uid")
}

// canaryControllerRevision builds a ControllerRevision carrying the OMENative
// revision label set the rollback-target lookup selects on, named like a real
// per-(ISVC, Component) CR (`<isvc>-<component>-<hash>`) with a monotonic
// .Revision and controlled by the Component's replica. Seeded directly into
// the fake client so the rollback signal has revision history to resolve the
// stable target from.
func canaryControllerRevision(ns, isvc, comp, hash string, rev int64) *appsv1.ControllerRevision {
	replica := &metav1.ObjectMeta{Name: isvc + "-" + comp, UID: canaryIRUID(isvc + "-" + comp)}
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      isvc + "-" + comp + "-" + hash,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc,
				constants.OMEComponentLabel:           comp,
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(replica, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica")),
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

// The capacity a revision has is its serving Instances as the replica
// reports them: a gang counts once and only whole, a pod out of rotation
// does not count, rows on another revision are not the target's capacity,
// and under a readiness window an Instance counts only once the replica
// also reports it available.
func TestServingTargetInstances(t *testing.T) {
	const stable, target = "svc-engine-stable", "svc-engine-target"
	row := func(index int32, revision string, pods, serving, available int32) v1beta1.OMENativeInstanceStatus {
		return v1beta1.OMENativeInstanceStatus{
			Index: index, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revision,
			PodCount: pods, ServingPodCount: serving, AvailablePodCount: available,
		}
	}
	gang := []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 3}}
	single := []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	leadersOnly := make([]v1beta1.OMENativeInstanceStatus, 0, 8)
	for i := int32(0); i < 8; i++ {
		leadersOnly = append(leadersOnly, row(i, target, 1, 1, 1))
	}
	for _, tc := range []struct {
		name    string
		runners []v1beta1.Runner
		window  int32
		rows    []v1beta1.OMENativeInstanceStatus
		want    int32
	}{
		{"a whole serving gang counts once", gang, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 4, 4, 4)}, 1},
		{"a gang with a pod out of rotation does not count", gang, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 4, 3, 3)}, 0},
		{"a partial gang does not count", gang, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 2, 2, 2)}, 0},
		{"eight serving leaders are no gang", gang, 0, leadersOnly, 0},
		{"two whole gangs count twice", gang, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 4, 4, 4), row(1, target, 4, 4, 4), row(2, stable, 4, 4, 4)}, 2},
		{"stable rows are not canary capacity", single, 0, []v1beta1.OMENativeInstanceStatus{row(0, stable, 1, 1, 1), row(1, target, 1, 1, 1)}, 1},
		{"a surge pod beside the serving pod counts once", single, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 2, 1, 1)}, 1},
		{"a single pod out of rotation does not count", single, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 1, 0, 0)}, 0},
		{"an undeclared shape needs every pod serving", nil, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 2, 1, 1)}, 0},
		{"an undeclared shape with every pod serving", nil, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 2, 2, 2)}, 1},
		{"no rows is no capacity", single, 0, nil, 0},
		{"no window reads rotation alone", single, 0, []v1beta1.OMENativeInstanceStatus{row(0, target, 1, 1, 0)}, 1},
		{"under a window a serving pod inside it does not count", single, 30, []v1beta1.OMENativeInstanceStatus{row(0, target, 1, 1, 0)}, 0},
		{"under a window an available pod counts", single, 30, []v1beta1.OMENativeInstanceStatus{row(0, target, 1, 1, 1)}, 1},
		{"under a window a gang waits for its last member", gang, 30, []v1beta1.OMENativeInstanceStatus{row(0, target, 4, 4, 3)}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation := observedCanaryRevisions{targetHash: "target", runners: tc.runners, minReadySeconds: tc.window, rows: tc.rows, fromIR: true, statusFresh: true}
			got := observation.servingTargetInstances()
			if got == nil || *got != tc.want {
				t.Fatalf("serving target Instances = %v, want %d", got, tc.want)
			}
		})
	}
	if got := (observedCanaryRevisions{}).servingTargetInstances(); got != nil {
		t.Fatalf("with no replica observed the pod count stands in, got %d", *got)
	}
}

func TestDispatch_HoldsUntilIRIdentifiesCanaryTarget(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "target-lag"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)

	engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "target-lag-engine", UID: canaryIRUID("target-lag-engine"), Generation: 2}}
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
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
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

// A plan and a new target that land in one write before the IR promotes its
// current revision leave every stable-revision authority empty, and the staged
// partition then keeps the promotion from ever happening. The pods still on
// the non-target revision must supply the stable identity so the canary arms.
func TestDispatch_ArmsFromLivePodsWhenCurrentRevisionUnpromoted(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "unpromoted"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)

	engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "unpromoted-engine", UID: canaryIRUID("unpromoted-engine"), Generation: 2}}
	engineIR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	engineIR.Status.CurrentRevision = ""
	engineIR.Status.UpdateRevision = "unpromoted-engine-target"
	engineIR.Status.ObservedGeneration = engineIR.Generation
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
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch with unpromoted current revision: %v", err)
	}
	if isvc.Status.Canary == nil || isvc.Status.Canary.CanaryRevisionHash != "target" || isvc.Status.Canary.StableRevisionHash != "stable" {
		t.Fatalf("live non-target pods must supply the stable identity, got %+v", isvc.Status.Canary)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhasePaused {
		t.Fatalf("ready step-zero capacity should pause, got %q", got)
	}

	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch rollback: %v", err)
	}
	traffic := isvc.Status.Components[v1beta1.EngineComponent].Traffic
	wantStable := coordination.PerRevisionServiceName(isvc.Name, v1beta1.EngineComponent, "stable")
	if len(traffic) != 1 || traffic[0].RevisionName != wantStable || traffic[0].Percent != 100 {
		t.Fatalf("rollback traffic must target the derived stable, got %+v", traffic)
	}
	liveIR := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: engineIR.Name}, liveIR); err != nil {
		t.Fatal(err)
	}
	if liveIR.Spec.Pacing == nil || liveIR.Spec.Pacing.RollbackToRevision == nil || *liveIR.Spec.Pacing.RollbackToRevision != "unpromoted-engine-stable" {
		t.Fatalf("rollback must select the derived stable ControllerRevision, got %+v", liveIR.Spec.Pacing)
	}
}

// A single live revision never arms a canary: the first rollout of a service
// has no stable identity to shift traffic away from.
func TestDispatch_SingleRevisionDoesNotArmWithoutCurrentRevision(t *testing.T) {
	ns := "default"
	n2 := 2
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "first-roll"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	pinActiveRun(isvc)

	engineIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "first-roll-engine", Generation: 1}}
	engineIR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	engineIR.Status.UpdateRevision = "first-roll-engine-target"
	engineIR.Status.ObservedGeneration = engineIR.Generation
	c := fake.NewClientBuilder().
		WithScheme(canaryScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(
			isvc,
			engineIR,
			canaryPod(ns, isvc.Name, "engine", "target", "target-0"),
			canaryControllerRevision(ns, isvc.Name, "engine", "target", 1),
		).
		Build()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch on a single revision: %v", err)
	}
	if isvc.Status.Canary != nil {
		t.Fatalf("a single live revision must not arm a canary, got %+v", isvc.Status.Canary)
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
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

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

			if _, err := Dispatch(context.Background(), DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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

	if _, err := Dispatch(context.Background(), DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	pinActiveRun(isvc) // a live canary steps only under a pinned run
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
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
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
		// The final step was just entered: it is staging until its capacity is met.
		v1beta1.RouterComponent: {RolloutPhase: v1beta1.RolloutPhasePending},
	}
	pinActiveRun(isvc) // a live canary steps only under a pinned run
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

	if _, err := Dispatch(context.Background(), DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
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
		ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1-engine", UID: canaryIRUID("d1-engine")}}
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

	syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	rtrIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd-router", UID: canaryIRUID("pd-router")}}
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
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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

	syncInstanceRows(t, c, isvc, v1beta1.RouterComponent, v1beta1.EngineComponent)
	if _, err := Dispatch(context.Background(), DispatchDeps{
		Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	replica := ir(ns, "sr1", v1beta1.EngineComponent, "revC")
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		canaryControllerRevision(ns, "sr1", "engine", "revA", 1),
		canaryControllerRevision(ns, "sr1", "engine", "revB", 2),
		canaryControllerRevision(ns, "sr1", "engine", "revC", 3),
	).Build()
	ctx := context.Background()

	got, err := stableRevisionName(ctx, c, isvc, v1beta1.EngineComponent, replica, "revA")
	if err != nil || got != "sr1-engine-revA" {
		t.Fatalf("persisted identity must resolve exactly: got %q err=%v", got, err)
	}
	got, err = stableRevisionName(ctx, c, isvc, v1beta1.EngineComponent, replica, "")
	if err != nil || got != "" {
		t.Fatalf("no persisted identity must not guess: got %q err=%v", got, err)
	}
	got, err = stableRevisionName(ctx, c, isvc, v1beta1.EngineComponent, replica, "gone0000")
	if err != nil || got != "" {
		t.Fatalf("persisted identity not retained → no target (no guessing), got %q err=%v", got, err)
	}
}

// TestRollbackSignal_PinsOnlyARevisionTheReplicaControls pins the ownership
// filter on the rollback target: the revision labels also match revisions
// another object controls, and the replica refuses a rollback target it does
// not control, so only a revision the replica controls may be pinned.
func TestRollbackSignal_PinsOnlyARevisionTheReplicaControls(t *testing.T) {
	ns := "default"
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "sr1"}}
	own := canaryControllerRevision(ns, "sr1", "engine", "revA", 2)
	// Same labels and hash as the replica's own revision, controlled by another
	// replica. It lists ahead of the replica's own revision, so the lookup must
	// skip it rather than take the first match.
	foreign := canaryControllerRevision(ns, "sr1", "engine", "revA", 1)
	foreign.Name = "sr1-engine-copy-revA"
	foreign.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(
		&metav1.ObjectMeta{Name: "pool-a", UID: canaryIRUID("pool-a")},
		v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
	)}

	// pinned runs the rollback signal against the given revisions and returns
	// the replica's RollbackToRevision, "" when unset.
	pinned := func(t *testing.T, revisions ...runtime.Object) string {
		t.Helper()
		replica := ir(ns, "sr1", v1beta1.EngineComponent, "revB")
		c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).
			WithRuntimeObjects(append([]runtime.Object{isvc, replica}, revisions...)...).Build()
		ctx := context.Background()
		if _, err := reconcileRollbackSignal(ctx, c, c, isvc, v1beta1.EngineComponent, nil, "revA", true, false); err != nil {
			t.Fatal(err)
		}
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: replica.Name}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil {
			return ""
		}
		return *got.Spec.Pacing.RollbackToRevision
	}

	if got := pinned(t, foreign, own); got != own.Name {
		t.Fatalf("rollback must pin the revision the replica controls (%s), got %q", own.Name, got)
	}
	if got := pinned(t, foreign); got != "" {
		t.Fatalf("a revision another object controls must not be pinned, got %q", got)
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
	rbIR := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "d1-engine", UID: canaryIRUID("d1-engine")}}
	rbIR.Status.UpdateRevision = "d1-engine-newhash"
	hold := "d1-engine-stable0"
	rbIR.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &hold}

	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(isvc, rbIR,
		canaryControllerRevision(ns, "d1", "engine", "stable0", 1),
		canaryControllerRevision(ns, "d1", "engine", "newhash", 2),
	).Build()

	if _, err := reconcileRollbackSignal(context.Background(), c, c, isvc, v1beta1.EngineComponent, nil, "", true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcileRollbackSignal(context.Background(), c, c, isvc, v1beta1.EngineComponent, nil, "missing", true, false); err != nil {
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
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}); err != nil {
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
	if got := primaryComponentOf(rollout.CanaryGroup(mk(v1beta1.EngineComponent), rollout.Policies{})); got != v1beta1.EngineComponent {
		t.Fatalf("engine-only → engine, got %q", got)
	}
	if got := primaryComponentOf(rollout.CanaryGroup(mk(v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent), rollout.Policies{})); got != v1beta1.RouterComponent {
		t.Fatalf("PD with router → router, got %q", got)
	}
	if got := primaryComponentOf(rollout.CanaryGroup(mk(v1beta1.EngineComponent, v1beta1.DecoderComponent), rollout.Policies{})); got != v1beta1.EngineComponent {
		t.Fatalf("engine+decoder (no router) → engine, got %q", got)
	}
	if got := primaryComponentOf(rollout.CanaryGroup(mk(), rollout.Policies{})); got != "" {
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

// missingStableFixture seeds a single-engine canary serving its first step
// whose stable revision has no retained ControllerRevision: only the canary
// revision's is present. It returns the working copy and the store.
func missingStableFixture(t *testing.T, name string) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", StableRevisionHash: "old", CurrentStep: 0, ObservedTrafficWeight: 50}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePaused},
	}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = name + "-engine-old"
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc, engineIR,
		canaryPod(ns, name, "engine", "old", "old-0"),
		canaryPod(ns, name, "engine", "old", "old-1"),
		canaryPod(ns, name, "engine", "new", "new-2"),
		canaryPod(ns, name, "engine", "new", "new-3"),
		// Only the canary revision is retained; the stable one was pruned.
		canaryControllerRevision(ns, name, "engine", "new", 2),
	).Build()
	return isvc, c
}

// manualRollbacks reads the engine unit's manual rollback counter.
func manualRollbacks(isvc *v1beta1.InferenceService) float64 {
	return testutil.ToFloat64(canaryRollbackTotal.WithLabelValues(isvc.Namespace, isvc.Name, string(v1beta1.EngineComponent), "manual"))
}

// countEvents drains the recorder and counts the events carrying reason.
func countEvents(rec *record.FakeRecorder, reason string) int {
	n := 0
	for _, e := range eventsFrom(rec) {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}

// A rollback whose stable revision has no retained ControllerRevision cannot
// be carried out: the IR gets no rollback target, and the canary parks Failed
// with the reason instead of reporting a revert that never completes.
func TestDispatch_RollbackWithoutStableRevisionParksFailed(t *testing.T) {
	isvc, c := missingStableFixture(t, "no-stable")
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	for pass := 0; pass < 2; pass++ {
		if _, err := Dispatch(context.Background(), deps); err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if cs == nil || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing {
			t.Fatalf("pass %d: expected a StableRevisionMissing park, got %+v", pass, cs)
		}
		if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhaseFailed {
			t.Fatalf("pass %d: phase %q, want Failed", pass, got)
		}
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing != nil && got.Spec.Pacing.RollbackToRevision != nil {
			t.Fatalf("pass %d: no rollback target must be signaled when the stable revision is gone: %+v", pass, got.Spec.Pacing)
		}
	}
}

// A rollback request on a unit with no run open parks on a missing stable
// revision. A canary of version 2 is half done when the run that pinned it
// closes and no run opens again, because the plan stopped composing; the
// inline body still executes, so the unit is dispatched and holds at the
// run-lost hold. The executor reads the operator's rollback request before
// that hold: it records version 2 as rejected and starts the revert, and
// with no retained ControllerRevision for version 1 the same pass parks the
// unit Failed with the reason StableRevisionMissing, writes no revert target
// on the replica and hands the request back for removal. The pass after
// finds the park as it was and hands the request back again.
func TestDispatch_ARollbackRequestWithNoRunOpenParksOnAMissingStableRevision(t *testing.T) {
	isvc, c := missingStableFixture(t, "no-run-no-stable")
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome:         v1beta1.RolloutRunSuperseded,
		TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"}},
	}}
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	for pass := 0; pass < 2; pass++ {
		out, err := Dispatch(context.Background(), deps)
		if err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		if v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("pass %d: the dispatch opened a run", pass)
		}
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if cs == nil || cs.RolledBackRevisionHash != "new" || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing {
			t.Fatalf("pass %d: want version 2 recorded as rejected and the park for want of its stable revision, got %+v", pass, cs)
		}
		if got := phaseOf(isvc); got != v1beta1.RolloutPhaseFailed {
			t.Fatalf("pass %d: phase %q, want Failed", pass, got)
		}
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing != nil && got.Spec.Pacing.RollbackToRevision != nil {
			t.Fatalf("pass %d: no revert target must be written for a revision the replica does not retain: %+v", pass, got.Spec.Pacing)
		}
		if !slices.Contains(out.Consume, constants.RolloutRollbackAnnotation) {
			t.Fatalf("pass %d must hand the request back for removal, got %v", pass, out.Consume)
		}
	}
}

// A rollback request is applied once. The pass that finds no stable revision
// to return to records the rejected hash and parks; while the copy each later
// pass reads still carries the request, the park stays as it was: its clock
// is not re-stamped, the manual rollback counter does not move and the event
// is not repeated. Every pass hands the request back for removal after the
// flush.
func TestDispatch_MissingStableParkAppliesTheRollbackOnce(t *testing.T) {
	isvc, c := missingStableFixture(t, "park-once")
	rec := record.NewFakeRecorder(16)
	start := time.Unix(1000, 0)
	deps := DispatchDeps{Client: c, Reader: c, Recorder: rec, ISVC: isvc, Now: start, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

	rollbacksBefore := manualRollbacks(isvc)
	parkEvents := 0
	var parkedAt *metav1.Time
	for pass := 0; pass < 4; pass++ {
		annotate(isvc, constants.RolloutRollbackAnnotation, "true")
		deps.Now = start.Add(time.Duration(pass) * time.Minute)
		out, err := Dispatch(context.Background(), deps)
		if err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		parkEvents += countEvents(rec, EventReasonCanaryStableRevisionMissing)
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if cs == nil || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing || cs.Failed.Time == nil {
			t.Fatalf("pass %d: expected a StableRevisionMissing park, got %+v", pass, cs)
		}
		if parkedAt == nil {
			parkedAt = cs.Failed.Time.DeepCopy()
		} else if !cs.Failed.Time.Equal(parkedAt) {
			t.Errorf("pass %d re-stamped the park: %v -> %v", pass, parkedAt.Time, cs.Failed.Time.Time)
		}
		if cs.RolledBackRevisionHash != "new" {
			t.Errorf("pass %d: the rejected hash must stay recorded, got %+v", pass, cs)
		}
		if got := manualRollbacks(isvc) - rollbacksBefore; got != 1 {
			t.Errorf("pass %d: manual rollbacks %v, want 1", pass, got)
		}
		if parkEvents != 1 {
			t.Errorf("pass %d: %d %s events, want 1", pass, parkEvents, EventReasonCanaryStableRevisionMissing)
		}
		if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
			t.Errorf("pass %d: phase %q, want Failed", pass, phaseOf(isvc))
		}
		if !slices.Contains(out.Consume, constants.RolloutRollbackAnnotation) {
			t.Errorf("pass %d: the park must hand the request back for removal, got %v", pass, out.Consume)
		}
	}
}

// The controller removes the request once the flush that carries the park
// has landed. A request applied again finds the park and is refused the same
// way, without moving it; a new target re-arms the unit, and a copy of the
// request still stored at that moment leaves with the rejected hash instead
// of following the new canary.
func TestDispatch_MissingStableParkRefusesARepeatAndRearms(t *testing.T) {
	isvc, c := missingStableFixture(t, "park-repeat")
	rec := record.NewFakeRecorder(16)
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, Recorder: rec, ISVC: isvc, Now: time.Unix(1000, 0), ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	dispatch := func(pass string) Outcome {
		t.Helper()
		out, err := Dispatch(ctx, deps)
		if err != nil {
			t.Fatalf("Dispatch (%s): %v", pass, err)
		}
		return out
	}
	status := func() *v1beta1.CanaryStatus { return rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent) }
	requestStored := func() bool {
		_, ok := storedAnnotations(t, c, isvc)[constants.RolloutRollbackAnnotation]
		return ok
	}
	// The controller's removal after the status flush; the next pass reads
	// the object as the store then has it.
	removeTaken := func(out Outcome) {
		t.Helper()
		if err := ConsumeAnnotations(ctx, c, isvc, out.Consume); err != nil {
			t.Fatal(err)
		}
		isvc.Annotations = storedAnnotations(t, c, isvc)
	}

	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	rollbacksBefore := manualRollbacks(isvc)
	out := dispatch("park")
	cs := status()
	if cs == nil || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing || cs.Failed.Time == nil {
		t.Fatalf("expected a StableRevisionMissing park, got %+v", cs)
	}
	parkedAt := cs.Failed.Time.DeepCopy()
	if got := countEvents(rec, EventReasonCanaryStableRevisionMissing); got != 1 {
		t.Fatalf("%d %s events, want 1", got, EventReasonCanaryStableRevisionMissing)
	}
	removeTaken(out)
	if requestStored() {
		t.Fatal("the request must be removed once the park is persisted")
	}

	out = dispatch("hold")
	if len(out.Consume) != 0 {
		t.Fatalf("a pass that reads no request takes nothing, got %v", out.Consume)
	}

	// The operator applies the verb again: refused, the park unmoved.
	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	out = dispatch("repeat")
	cs = status()
	if cs.Failed == nil || !cs.Failed.Time.Equal(parkedAt) || cs.RolledBackRevisionHash != "new" || phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("a repeated request must not move the park, got %+v phase=%q", cs, phaseOf(isvc))
	}
	if got := manualRollbacks(isvc) - rollbacksBefore; got != 1 {
		t.Fatalf("manual rollbacks %v, want 1", got)
	}
	if got := countEvents(rec, EventReasonCanaryStableRevisionMissing); got != 0 {
		t.Fatalf("the park event must not repeat, got %d", got)
	}
	if !slices.Contains(out.Consume, constants.RolloutRollbackAnnotation) {
		t.Fatalf("the repeated request must be handed back for removal, got %v", out.Consume)
	}

	// A new target arrives while the repeated request is still stored: the
	// run is retargeted toward it and the executor re-arms the unit. The
	// request leaves with the rejected hash, stored and in memory.
	isvc.Annotations = storedAnnotations(t, c, isvc)
	if !requestStored() {
		t.Fatal("setup: the repeated request must still be stored")
	}
	isvc.Status.Rollout.ActiveRun.TargetRevisions[0].Revision = "v3"
	irKey := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}
	updateIR(t, c, irKey, func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = isvc.Name + "-engine-v3" })
	if err := c.Create(ctx, canaryControllerRevision(isvc.Namespace, isvc.Name, "engine", "v3", 3)); err != nil {
		t.Fatal(err)
	}
	dispatch("retarget")
	cs = status()
	if cs.RolledBackRevisionHash != "" || cs.Failed != nil || cs.CanaryRevisionHash != "v3" || cs.CurrentStep != 0 {
		t.Fatalf("the new target must arm a fresh canary at step 0, got %+v", cs)
	}
	if _, ok := isvc.Annotations[constants.RolloutRollbackAnnotation]; ok || requestStored() {
		t.Fatal("the request must leave with the rejected hash")
	}
	for _, pass := range []string{"stage", "stage again"} {
		dispatch(pass)
		if cs := status(); cs.RolledBackRevisionHash != "" || phaseOf(isvc) != v1beta1.RolloutPhasePending {
			t.Fatalf("%s: a later pass rolled the new target back: %+v phase=%q", pass, cs, phaseOf(isvc))
		}
	}
}

// TestGroupRollbackInputs_EveryMember pins that the rollback completion check
// is handed every configured member's view: its pods per revision, ready or
// not, and its persisted stable revision, primary included.
func TestGroupRollbackInputs_EveryMember(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: "rtrNew", StableRevision: "rtrStable"},
		{Component: v1beta1.EngineComponent, Revision: "engNew", StableRevision: "engStable"},
		{Component: v1beta1.DecoderComponent, Revision: "decNew", StableRevision: "decStable"},
	}
	totalRev := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent:  {"rtrStable": 1},
		v1beta1.EngineComponent:  {"engStable": 3, "engNew": 1},
		v1beta1.DecoderComponent: {},
	}
	wantStable := map[v1beta1.ComponentType]string{
		v1beta1.RouterComponent:  "rtrStable",
		v1beta1.EngineComponent:  "engStable",
		v1beta1.DecoderComponent: "decStable",
	}
	pods, stable := groupRollbackInputs(isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), v1beta1.RouterComponent, totalRev)
	if len(pods) != len(totalRev) || len(stable) != len(wantStable) {
		t.Fatalf("every configured member must be present: pods=%v stable=%v", pods, stable)
	}
	for comp, want := range totalRev {
		got, ok := pods[comp]
		if !ok || len(got) != len(want) {
			t.Fatalf("%s pods: got %v (present=%v), want %v", comp, got, ok, want)
		}
		for hash, n := range want {
			if got[hash] != n {
				t.Fatalf("%s pods[%s]: got %d, want %d", comp, hash, got[hash], n)
			}
		}
		if stable[comp] != wantStable[comp] {
			t.Fatalf("%s stable: got %q, want %q", comp, stable[comp], wantStable[comp])
		}
	}
}

// TestDispatch_PDRollbackWaitsForEveryMember pins that a group's rollback is
// complete for the group, not for the primary alone, and counts rejected pods
// whether or not they are ready. The router (primary) and the decoder are
// back on stable, but one engine pod, not ready, still exists on the engine's
// rejected revision, so the unit stays RollingBack; the run closes on the
// primary's phase, so it stays open for the engine's revert. Once the engine
// is back on stable the unit reads RolledBack.
func TestDispatch_PDRollbackWaitsForEveryMember(t *testing.T) {
	ns := "default"
	n1 := 1
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	}}}
	// The rollback is in flight: the router's rejected revision is recorded
	// and the router itself has already drained back to stable.
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "rtrNew", StableRevisionHash: "rtrStable", RolledBackRevisionHash: "rtrNew", CurrentStep: 1}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.RouterComponent: {RolloutPhase: v1beta1.RolloutPhaseRollingBack},
	}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: "rtrNew", StableRevision: "rtrStable"},
		{Component: v1beta1.EngineComponent, Revision: "engNew", StableRevision: "engStable"},
		{Component: v1beta1.DecoderComponent, Revision: "decNew", StableRevision: "decStable"},
	}
	rtrIR := ir(ns, "pd", v1beta1.RouterComponent, "rtrNew")
	rtrIR.Status.CurrentRevision = "pd-router-rtrStable"
	engIR := ir(ns, "pd", v1beta1.EngineComponent, "engNew")
	engIR.Status.CurrentRevision = "pd-engine-engStable"
	decIR := ir(ns, "pd", v1beta1.DecoderComponent, "decNew")
	decIR.Status.CurrentRevision = "pd-decoder-decStable"
	// The straggler exists but is not ready (a crashlooping rejected pod):
	// the ready view does not see it; the total view must.
	straggler := canaryPod(ns, "pd", "engine", "engNew", "pd-engine-0")
	straggler.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	objs := []runtime.Object{isvc, rtrIR, engIR, decIR,
		canaryPod(ns, "pd", "router", "rtrStable", "pd-router-0"),
		straggler,
		canaryPod(ns, "pd", "decoder", "decStable", "pd-decoder-0"),
		canaryControllerRevision(ns, "pd", "router", "rtrStable", 1),
		canaryControllerRevision(ns, "pd", "router", "rtrNew", 2),
		canaryControllerRevision(ns, "pd", "engine", "engStable", 1),
		canaryControllerRevision(ns, "pd", "engine", "engNew", 2),
		canaryControllerRevision(ns, "pd", "decoder", "decStable", 1),
		canaryControllerRevision(ns, "pd", "decoder", "decNew", 2),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}

	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.RouterComponent].RolloutPhase; got != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("an engine pod still exists on the rejected revision: phase %q, want RollingBack", got)
	}
	gotIR := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "pd-engine"}, gotIR); err != nil {
		t.Fatal(err)
	}
	if gotIR.Spec.Pacing == nil || gotIR.Spec.Pacing.RollbackToRevision == nil || *gotIR.Spec.Pacing.RollbackToRevision != "pd-engine-engStable" {
		t.Fatalf("the engine must stay signaled to revert while the unit waits on it, got pacing=%+v", gotIR.Spec.Pacing)
	}

	// The straggler is replaced by a stable pod: every member is back on stable.
	if err := c.Delete(ctx, straggler); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, canaryPod(ns, "pd", "engine", "engStable", "pd-engine-0")); err != nil {
		t.Fatal(err)
	}
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := isvc.Status.Components[v1beta1.RouterComponent].RolloutPhase; got != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("every member back on stable: phase %q, want RolledBack", got)
	}
}

// annotateStored sets an operator annotation the way an operator does: on the
// stored object, and on the copy the pass reads.
func annotateStored(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, key, value string) {
	t.Helper()
	annotate(isvc, key, value)
	patch := []byte(fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, value))
	if err := c.Patch(context.Background(), isvc.DeepCopy(), client.RawPatch(types.MergePatchType, patch)); err != nil {
		t.Fatal(err)
	}
}

// reopenRun models the run layer superseding the active run with a fresh one
// toward new pinned targets, over the same pinned plan.
func reopenRun(isvc *v1beta1.InferenceService, targets ...v1beta1.RolloutRunTarget) {
	closed := isvc.Status.Rollout.ActiveRun
	isvc.Status.Rollout.ActiveRun = &v1beta1.RolloutRun{
		RunID:           closed.RunID + "-next",
		OpenedAt:        metav1.Now(),
		PinnedAt:        metav1.Now(),
		TargetRevisions: targets,
		Plan:            closed.Plan,
	}
}

// updateIR applies one mutation to the stored InferenceReplica.
func updateIR(t *testing.T, c client.Client, key types.NamespacedName, mutate func(*v1beta1.InferenceReplica)) {
	t.Helper()
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	mutate(got)
	if err := c.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
}

// canaryOfVersionTwoUnderARun is 4 engines half way into a canary of
// version 2 under a run recorded toward it: 2 Instances on version 1, 2 on
// version 2, both ControllerRevisions retained, the replica's current
// revision on version 1. It returns the service and every object to seed
// the client with.
func canaryOfVersionTwoUnderARun(t *testing.T, name string) (*v1beta1.InferenceService, []runtime.Object) {
	t.Helper()
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = name + "-engine-old"
	return isvc, []runtime.Object{
		isvc, engineIR,
		canaryPod(ns, name, "engine", "old", "old-0"),
		canaryPod(ns, name, "engine", "old", "old-1"),
		canaryPod(ns, name, "engine", "new", "new-2"),
		canaryPod(ns, name, "engine", "new", "new-3"),
		canaryControllerRevision(ns, name, "engine", "old", 1),
		canaryControllerRevision(ns, name, "engine", "new", 2),
	}
}

// A rollback request is spent once it is recorded against a target. When the
// hold completes and a run opens toward a new target, the controller binds
// the unit to the run before the executor's pass: the request leaves with the
// rejected hash, the new target arms, and no later pass rolls it back.
func TestDispatch_RunReopenedAfterRollbackArmsTheNewTarget(t *testing.T) {
	ns := "default"
	isvc, objs := canaryOfVersionTwoUnderARun(t, "reopen")
	irKey := types.NamespacedName{Namespace: ns, Name: isvc.Name + "-engine"}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	dispatch := func(pass string) {
		t.Helper()
		syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
		if _, err := Dispatch(ctx, deps); err != nil {
			t.Fatalf("Dispatch (%s): %v", pass, err)
		}
	}
	status := func() *v1beta1.CanaryStatus { return rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent) }
	rollbackTarget := func() *string {
		t.Helper()
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, irKey, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing == nil {
			return nil
		}
		return got.Spec.Pacing.RollbackToRevision
	}

	dispatch("arm")
	if cs := status(); cs == nil || cs.CanaryRevisionHash != "new" || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the canary must serve its first step, got %+v phase=%q", cs, phaseOf(isvc))
	}

	// The operator rolls the canary back. The request stays on the object
	// through the hold; the revert drains the rejected pods.
	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	dispatch("rollback")
	if cs := status(); cs.RolledBackRevisionHash != "new" || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rollback must reject the canary revision, got %+v phase=%q", cs, phaseOf(isvc))
	}
	if got := rollbackTarget(); got == nil || *got != isvc.Name+"-engine-old" {
		t.Fatalf("the IR must be pointed at the stable revision, got %v", got)
	}
	for _, name := range []string{"new-2", "new-3"} {
		if err := c.Delete(ctx, canaryPod(ns, isvc.Name, "engine", "new", name)); err != nil {
			t.Fatal(err)
		}
	}
	updateIR(t, c, irKey, func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = isvc.Name + "-engine-old" })
	dispatch("drain")
	if phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("the hold must complete once the rejected pods are gone, got %q", phaseOf(isvc))
	}
	if _, ok := storedAnnotations(t, c, isvc)[constants.RolloutRollbackAnnotation]; !ok {
		t.Fatal("the request must stay on the object through the hold")
	}

	// A new target: the run layer opens a fresh run toward it and the
	// controller binds the unit to that run before the executor's pass.
	reopenRun(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "v3", StableRevision: "old"})
	deps.Group = rollout.CanaryGroup(isvc, rollout.Policies{})
	updateIR(t, c, irKey, func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = isvc.Name + "-engine-v3" })
	if err := c.Create(ctx, canaryControllerRevision(ns, isvc.Name, "engine", "v3", 3)); err != nil {
		t.Fatal(err)
	}
	if err := BindRun(ctx, c, isvc, deps.Group, RunOpen{}); err != nil {
		t.Fatal(err)
	}
	dispatch("reopen")
	if _, ok := isvc.Annotations[constants.RolloutRollbackAnnotation]; ok {
		t.Fatal("the spent request is still visible to the executor")
	}
	if _, ok := storedAnnotations(t, c, isvc)[constants.RolloutRollbackAnnotation]; ok {
		t.Fatal("the spent request is still stored on the object")
	}
	if cs := status(); cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" || cs.StableRevisionHash != "old" || cs.CurrentStep != 0 {
		t.Fatalf("the new target must arm a fresh canary at step 0, got %+v", cs)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("the new target has no capacity yet and must stage, got %q", phaseOf(isvc))
	}
	if got := rollbackTarget(); got != nil {
		t.Fatalf("the IR's rollback target must clear with the hold, got %q", *got)
	}

	// The following passes step the new target; none rolls it back.
	dispatch("stage")
	if cs := status(); cs.RolledBackRevisionHash != "" || phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a later pass rolled the new target back: %+v phase=%q", cs, phaseOf(isvc))
	}
	for _, name := range []string{"v3-2", "v3-3"} {
		if err := c.Create(ctx, canaryPod(ns, isvc.Name, "engine", "v3", name)); err != nil {
			t.Fatal(err)
		}
	}
	dispatch("serve")
	if cs := status(); cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the new target must serve its first step, got %+v phase=%q", cs, phaseOf(isvc))
	}
}

// A run reopened toward a new target while the rejected revision is still
// draining re-arms the unit at run open: the spent request leaves with the
// rejected hash, the new target arms while the old pods finish draining, and
// no later pass rolls it back.
func TestDispatch_RunReopenedWhileRollingBackArmsTheNewTarget(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "reopen-draining"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-old"
	irKey := types.NamespacedName{Namespace: ns, Name: engineIR.Name}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc, engineIR,
		canaryPod(ns, isvc.Name, "engine", "old", "old-0"),
		canaryPod(ns, isvc.Name, "engine", "old", "old-1"),
		canaryPod(ns, isvc.Name, "engine", "new", "new-2"),
		canaryPod(ns, isvc.Name, "engine", "new", "new-3"),
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "new", 2),
	).Build()
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	dispatch := func(pass string) {
		t.Helper()
		syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
		if _, err := Dispatch(ctx, deps); err != nil {
			t.Fatalf("Dispatch (%s): %v", pass, err)
		}
	}
	status := func() *v1beta1.CanaryStatus { return rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent) }
	rollbackTarget := func() *string {
		t.Helper()
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, irKey, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing == nil {
			return nil
		}
		return got.Spec.Pacing.RollbackToRevision
	}

	dispatch("arm")
	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	dispatch("rollback")
	if cs := status(); cs.RolledBackRevisionHash != "new" || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rollback must reject the canary revision and drain it, got %+v phase=%q", cs, phaseOf(isvc))
	}
	if got := rollbackTarget(); got == nil || *got != isvc.Name+"-engine-old" {
		t.Fatalf("the IR must be pointed at the stable revision, got %v", got)
	}

	// The new target arrives before the rejected pods are gone: the run layer
	// opens a fresh run toward it and the controller binds the unit first.
	reopenRun(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "v3", StableRevision: "old"})
	deps.Group = rollout.CanaryGroup(isvc, rollout.Policies{})
	updateIR(t, c, irKey, func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = isvc.Name + "-engine-v3" })
	if err := c.Create(ctx, canaryControllerRevision(ns, isvc.Name, "engine", "v3", 3)); err != nil {
		t.Fatal(err)
	}
	if err := BindRun(ctx, c, isvc, deps.Group, RunOpen{}); err != nil {
		t.Fatal(err)
	}
	dispatch("reopen")
	if _, ok := isvc.Annotations[constants.RolloutRollbackAnnotation]; ok {
		t.Fatal("the spent request is still visible to the executor")
	}
	if _, ok := storedAnnotations(t, c, isvc)[constants.RolloutRollbackAnnotation]; ok {
		t.Fatal("the spent request is still stored on the object")
	}
	if cs := status(); cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" || cs.StableRevisionHash != "old" || cs.CurrentStep != 0 {
		t.Fatalf("the new target must arm a fresh canary at step 0, got %+v", cs)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("the new target has no capacity yet and must stage, got %q", phaseOf(isvc))
	}
	if got := rollbackTarget(); got != nil {
		t.Fatalf("the IR's rollback target must clear with the hold, got %q", *got)
	}

	// The rejected pods drain under the new run; no pass rolls the target back.
	dispatch("stage")
	for _, name := range []string{"new-2", "new-3"} {
		if err := c.Delete(ctx, canaryPod(ns, isvc.Name, "engine", "new", name)); err != nil {
			t.Fatal(err)
		}
	}
	dispatch("drained")
	if cs := status(); cs.RolledBackRevisionHash != "" || phaseOf(isvc) != v1beta1.RolloutPhasePending {
		t.Fatalf("a later pass rolled the new target back: %+v phase=%q", cs, phaseOf(isvc))
	}
	for _, name := range []string{"v3-2", "v3-3"} {
		if err := c.Create(ctx, canaryPod(ns, isvc.Name, "engine", "v3", name)); err != nil {
			t.Fatal(err)
		}
	}
	dispatch("serve")
	if cs := status(); cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the new target must serve its first step, got %+v phase=%q", cs, phaseOf(isvc))
	}
}

// The marker is the record of a park with no stable revision to return to.
// A status write that drops it leaves the rejected hash and the Failed
// phase, which the executor reads as a revert in flight; the same pass finds
// the stable revision still unretained and parks the unit again before
// anything is flushed. The park survives the write; only its time is new.
func TestDispatch_MissingStableParkSurvivesADroppedMarker(t *testing.T) {
	isvc, c := missingStableFixture(t, "park-marker")
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, Now: time.Unix(1000, 0), ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (park): %v", err)
	}
	delete(isvc.Annotations, constants.RolloutRollbackAnnotation)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs == nil || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing {
		t.Fatalf("setup: expected a StableRevisionMissing park, got %+v", cs)
	}

	// The marker leaves the record; the phase and the rejected hash stay.
	cs.Failed = nil
	deps.Now = deps.Now.Add(time.Minute)
	out, err := Dispatch(ctx, deps)
	if err != nil {
		t.Fatalf("Dispatch (dropped marker): %v", err)
	}
	cs = rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs == nil || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing || cs.Failed.Time == nil || !cs.Failed.Time.Time.Equal(deps.Now) {
		t.Fatalf("the pass must park the unit again, got %+v", cs)
	}
	if cs.RolledBackRevisionHash != "new" {
		t.Errorf("the rejected hash must stay recorded, got %+v", cs)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Errorf("phase %q, want Failed", phaseOf(isvc))
	}
	if len(out.Consume) != 0 {
		t.Errorf("a pass that reads no request takes nothing, got %v", out.Consume)
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Pacing != nil && got.Spec.Pacing.RollbackToRevision != nil {
		t.Errorf("no rollback target must be signaled when the stable revision is gone: %+v", got.Spec.Pacing)
	}
}

// TestWindowWait_RunsFromPodReady: under minReadySeconds the capacity
// gate's wake is how long until the soonest serving target pod has held
// Ready for the window, measured from the Ready condition's transition and
// never from ContainersReady. Containers up at T, the pod Ready at T+60,
// window 30: nothing waits at T+30 (the pod is not Ready), 30s remain at
// T+60, 28s at T+62 and nothing at T+90. A gang waits for its last member; a
// pod that was never Ready raises no wait; no window waits for nothing.
func TestWindowWait_RunsFromPodReady(t *testing.T) {
	containersUp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	readyAt := containersUp.Add(60 * time.Second)
	window := int32(30)
	single := observedCanaryRevisions{
		targetHash:      "target",
		runners:         []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}},
		minReadySeconds: window,
		fromIR:          true,
		statusFresh:     true,
	}
	noWindow := single
	noWindow.minReadySeconds = 0
	gang := single
	gang.runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 1}}
	// windowPod is a target pod of Instance 0 whose containers came up at
	// containersUp and whose Ready condition turned True at ready; a zero
	// ready leaves the pod not Ready.
	windowPod := func(runner v1beta1.RunnerName, ready time.Time) *corev1.Pod {
		pod := canaryRunnerPod("default", "svc", "engine", "target", "pod-0-"+string(runner), runner, "0")
		pod.Labels[query.LabelInstanceIdx] = "0"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(containersUp)},
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(containersUp)},
		}
		if !ready.IsZero() {
			pod.Status.Conditions[1] = corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(ready)}
		}
		return pod
	}
	gangPods := []*corev1.Pod{
		windowPod(v1beta1.RunnerNameWorker, readyAt.Add(-20*time.Second)),
		windowPod(v1beta1.RunnerNameLeader, readyAt),
	}
	cases := []struct {
		name        string
		observation observedCanaryRevisions
		pods        []*corev1.Pod
		now         time.Time
		wantWait    time.Duration
	}{
		{"containers up for the window but not yet Ready", single, []*corev1.Pod{windowPod(v1beta1.RunnerNameDefault, time.Time{})}, containersUp.Add(30 * time.Second), 0},
		{"Ready just now is inside the window", single, []*corev1.Pod{windowPod(v1beta1.RunnerNameDefault, readyAt)}, readyAt, 30 * time.Second},
		{"Ready for 2s is inside the window", single, []*corev1.Pod{windowPod(v1beta1.RunnerNameDefault, readyAt)}, readyAt.Add(2 * time.Second), 28 * time.Second},
		{"Ready for the window waits no longer", single, []*corev1.Pod{windowPod(v1beta1.RunnerNameDefault, readyAt)}, readyAt.Add(30 * time.Second), 0},
		{"never Ready never waits", single, []*corev1.Pod{windowPod(v1beta1.RunnerNameDefault, time.Time{})}, readyAt.Add(30 * time.Second), 0},
		{"no window waits for nothing", noWindow, []*corev1.Pod{windowPod(v1beta1.RunnerNameDefault, readyAt)}, readyAt, 0},
		{"a gang waits for its last member", gang, gangPods, readyAt.Add(20 * time.Second), 10 * time.Second},
		{"a gang clear of the window waits no longer", gang, gangPods, readyAt.Add(30 * time.Second), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if wait := tc.observation.windowWait(tc.pods, tc.now); wait != tc.wantWait {
				t.Fatalf("wait until the next pod clears its window = %s, want %s", wait, tc.wantWait)
			}
		})
	}
}

// TestSecondaryCapacityReady_WindowRunsFromPodReady: a secondary member's
// capacity gate counts its canary Instances on the same bar as the
// primary's, as the member's replica accounts them: in rotation and, under
// the member's minReadySeconds, available. Two engine canary Instances in
// rotation but not yet available under a 30-second window leave the step
// short, with the wait until their pods clear the window; once the replica
// reports them available, they fill it.
func TestSecondaryCapacityReady_WindowRunsFromPodReady(t *testing.T) {
	ns := "default"
	n4 := 4
	readyAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pd"}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	engine := ir(ns, "pd", v1beta1.EngineComponent, "eng")
	engine.Status.CurrentRevision = "pd-engine-engOld"
	engine.Spec.MinReadySeconds = 30
	// rows is the engine replica's account with its two canary Instances in
	// rotation and available pods of them reported available.
	rows := func(available int32) []v1beta1.OMENativeInstanceStatus {
		return []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "pd-engine-eng", PodCount: 1, ServingPodCount: 1, AvailablePodCount: available},
			{Index: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "pd-engine-eng", PodCount: 1, ServingPodCount: 1, AvailablePodCount: available},
			{Index: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "pd-engine-engOld", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1},
			{Index: 3, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "pd-engine-engOld", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1},
		}
	}
	engine.Status.InstanceStatuses = rows(0)
	reads := fake.NewClientBuilder().WithScheme(canaryScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithRuntimeObjects(ir(ns, "pd", v1beta1.RouterComponent, "rtr"), engine).Build()
	perRev := map[v1beta1.ComponentType]map[string]int32{
		v1beta1.RouterComponent: {"rtr": 2},
		v1beta1.EngineComponent: {"eng": 2, "engOld": 2},
	}
	observedPods := map[v1beta1.ComponentType][]*corev1.Pod{}
	for index := 0; index < 2; index++ {
		pod := canaryRunnerPod(ns, "pd", "engine", "eng", fmt.Sprintf("pd-engine-%d", index), v1beta1.RunnerNameDefault, "0")
		pod.Labels[query.LabelInstanceIdx] = strconv.Itoa(index)
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readyAt)}}
		observedPods[v1beta1.EngineComponent] = append(observedPods[v1beta1.EngineComponent], pod)
	}
	group := rollout.CanaryGroup(isvc, rollout.Policies{})

	ready, fresh, _, wait, err := secondaryCapacityReady(context.Background(), reads, isvc, perRev, observedPods, v1beta1.RouterComponent, group, readyAt.Add(2*time.Second))
	if err != nil || !fresh {
		t.Fatalf("secondaryCapacityReady: fresh=%v err=%v", fresh, err)
	}
	if ready {
		t.Fatal("engine canary Instances in rotation but not yet available under a 30s window must leave the step short")
	}
	if wait != 28*time.Second {
		t.Fatalf("wait until the pods clear their window = %s, want 28s", wait)
	}

	// The replica reports the Instances available once their pods have held
	// Ready for the window.
	published := &v1beta1.InferenceReplica{}
	if err := reads.Get(context.Background(), client.ObjectKeyFromObject(engine), published); err != nil {
		t.Fatal(err)
	}
	published.Status.InstanceStatuses = rows(1)
	if err := reads.Status().Update(context.Background(), published); err != nil {
		t.Fatal(err)
	}
	ready, fresh, _, wait, err = secondaryCapacityReady(context.Background(), reads, isvc, perRev, observedPods, v1beta1.RouterComponent, group, readyAt.Add(30*time.Second))
	if err != nil || !fresh {
		t.Fatalf("secondaryCapacityReady: fresh=%v err=%v", fresh, err)
	}
	if !ready || wait != 0 {
		t.Fatalf("engine canary Instances the replica reports available must fill the step: ready=%v wait=%s", ready, wait)
	}
}

// settledHoldFixture is a unit whose canary rejected "new" and whose revert
// completed with no run left open: the run closed RolledBack, every engine
// pod is back on "old", the engine's record is level with it, and the
// replica still names "new" as its spec target with no revert signal on it.
// stableRetained says whether the replica controls a ControllerRevision for
// "old"; otherwise only a copy another replica controls carries the hash.
func settledHoldFixture(t *testing.T, name string, stableRetained bool) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new", StableRevisionHash: "old", RolledBackRevisionHash: "new",
	})
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	engine.LatestRolledoutRevision = coordination.PerRevisionServiceName(name, v1beta1.EngineComponent, "old")
	engine.LatestReadyRevision = engine.LatestRolledoutRevision
	isvc.Status.Components[v1beta1.EngineComponent] = engine
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome:         v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"}},
	}}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = name + "-engine-old"
	stable := canaryControllerRevision(ns, name, "engine", "old", 1)
	if !stableRetained {
		stable.Name = name + "-engine-copy-old"
		stable.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(
			&metav1.ObjectMeta{Name: "pool-a", UID: canaryIRUID("pool-a")},
			v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
		)}
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc, engineIR,
		canaryPod(ns, name, "engine", "old", "old-0"),
		canaryPod(ns, name, "engine", "old", "old-1"),
		canaryPod(ns, name, "engine", "old", "old-2"),
		canaryPod(ns, name, "engine", "old", "old-3"),
		stable,
		canaryControllerRevision(ns, name, "engine", "new", 2),
	).Build()
	return isvc, c
}

// A settled hold keeps its revert signal with no run open. The rejected
// revision stays the replica's spec target for as long as the hold stands,
// so every pass resolves the member's stable revision from the rolled-back
// run's record and reads the replica's revert target against it; a target
// that names the stable revision is left as it is, so the replica stays
// pointed at the ControllerRevision it controls. The record, level with the
// stable revision, is not consulted and not moved.
func TestDispatch_ASettledHoldKeepsItsRevertSignalWithNoRunOpen(t *testing.T) {
	isvc, c := settledHoldFixture(t, "settled-hold", true)
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	record := isvc.Status.Components[v1beta1.EngineComponent].LatestRolledoutRevision
	for pass := 0; pass < 2; pass++ {
		if _, err := Dispatch(context.Background(), deps); err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		if got := phaseOf(isvc); got != v1beta1.RolloutPhaseRolledBack {
			t.Fatalf("pass %d: phase %q, want RolledBack", pass, got)
		}
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != isvc.Name+"-engine-old" {
			t.Fatalf("pass %d: the replica must stay pointed at the stable revision's ControllerRevision, got pacing=%+v", pass, got.Spec.Pacing)
		}
		if cur := isvc.Status.Components[v1beta1.EngineComponent]; cur.LatestRolledoutRevision != record || cur.PreviousRolledoutRevision != "" {
			t.Fatalf("pass %d: the hold moved the record: %+v", pass, cur)
		}
	}
}

// A settled hold whose stable revision leaves the replica's history fails.
// The hold resolves its stable revision on every pass; once no retained
// ControllerRevision of the replica's history (one it controls, or one its
// service owns) carries it, the dispatch stops the unit with the Failed
// reason StableRevisionMissing rather than keep a revert it cannot point the
// replica at, and the rejected hash stays recorded.
func TestDispatch_ASettledHoldFailsWhenItsStableRevisionLeavesTheHistory(t *testing.T) {
	isvc, c := settledHoldFixture(t, "settled-gone", false)
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	for pass := 0; pass < 2; pass++ {
		if _, err := Dispatch(context.Background(), deps); err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if cs == nil || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing || cs.RolledBackRevisionHash != "new" {
			t.Fatalf("pass %d: expected the Failed reason StableRevisionMissing with the rejection kept, got %+v", pass, cs)
		}
		if got := phaseOf(isvc); got != v1beta1.RolloutPhaseFailed {
			t.Fatalf("pass %d: phase %q, want Failed", pass, got)
		}
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing != nil && got.Spec.Pacing.RollbackToRevision != nil {
			t.Fatalf("pass %d: no revert target must be written for a revision the replica does not control: %+v", pass, got.Spec.Pacing)
		}
	}
}

// revertSignalLog counts what the revert signal costs over a run of passes:
// the lookups of the replica's revision history and the writes that moved
// the replica's revert target, each as "<from>-><to>".
type revertSignalLog struct {
	lookups int
	writes  []string
}

// loggingClient wraps a fake client so a test can tell a pass that read the
// replica's revert target from one that resolved and wrote it.
func loggingClient(base client.WithWatch) (client.Client, *revertSignalLog) {
	log := &revertSignalLog{}
	target := func(r *v1beta1.InferenceReplica) string {
		if r.Spec.Pacing == nil || r.Spec.Pacing.RollbackToRevision == nil {
			return "nil"
		}
		return *r.Spec.Pacing.RollbackToRevision
	}
	c := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*appsv1.ControllerRevisionList); ok {
				log.lookups++
			}
			return cl.List(ctx, list, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if r, ok := obj.(*v1beta1.InferenceReplica); ok {
				stored := &v1beta1.InferenceReplica{}
				if err := cl.Get(ctx, client.ObjectKeyFromObject(r), stored); err != nil {
					return err
				}
				if from, to := target(stored), target(r); from != to {
					log.writes = append(log.writes, from+"->"+to)
				}
			}
			return cl.Update(ctx, obj, opts...)
		},
	})
	return c, log
}

// A settled hold writes its revert signal once. 4 engines serve version 1
// after a canary of version 2 was rolled back and its run closed, and the
// spec still names version 2. The first pass resolves the stable revision's
// ControllerRevision from the replica's history and points the replica at
// it. Every later pass reads the target off the replica and the retained
// ControllerRevision it names, and neither looks the history up nor writes
// the replica again.
func TestDispatch_ASettledHoldWritesItsRevertSignalOnce(t *testing.T) {
	isvc, base := settledHoldFixture(t, "settled-once", true)
	c, log := loggingClient(base.(client.WithWatch))
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	stable := isvc.Name + "-engine-old"
	for pass := 1; pass <= 3; pass++ {
		if _, err := Dispatch(context.Background(), deps); err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		if got := phaseOf(isvc); got != v1beta1.RolloutPhaseRolledBack {
			t.Fatalf("pass %d: phase %q, want RolledBack", pass, got)
		}
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != stable {
			t.Fatalf("pass %d: the replica must stay pointed at the stable revision's ControllerRevision, got pacing=%+v", pass, got.Spec.Pacing)
		}
		if log.lookups != 1 || len(log.writes) != 1 {
			t.Fatalf("after pass %d the hold had looked its stable revision up in the history %d times and moved the replica's target %v; want one lookup and one write for the whole hold", pass, log.lookups, log.writes)
		}
	}
}

// A new hold after the release writes the revert signal again. 4 engines:
// the operator rolls a canary of version 2 back, and the hold points the
// replica at version 1 once. A push of version 3 ends the hold and clears
// the target. The operator rolls version 3 back too: the new hold looks the
// stable revision up again and points the replica at version 1 again.
func TestDispatch_ANewHoldAfterTheReleaseWritesTheRevertSignalAgain(t *testing.T) {
	ns := "default"
	isvc, objs := canaryOfVersionTwoUnderARun(t, "hold-again")
	irKey := types.NamespacedName{Namespace: ns, Name: isvc.Name + "-engine"}
	c, log := loggingClient(fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build())
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	dispatch := func(pass string) {
		t.Helper()
		syncInstanceRows(t, c, isvc, v1beta1.EngineComponent)
		if _, err := Dispatch(ctx, deps); err != nil {
			t.Fatalf("Dispatch (%s): %v", pass, err)
		}
	}
	stable := isvc.Name + "-engine-old"
	expectTarget := func(at string, want string) {
		t.Helper()
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, irKey, got); err != nil {
			t.Fatal(err)
		}
		switch {
		case want == "" && got.Spec.Pacing != nil && got.Spec.Pacing.RollbackToRevision != nil:
			t.Fatalf("%s: the replica's revert target must be clear, got %q", at, *got.Spec.Pacing.RollbackToRevision)
		case want != "" && (got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != want):
			t.Fatalf("%s: the replica must be pointed at %s, got pacing=%+v", at, want, got.Spec.Pacing)
		}
	}

	dispatch("first step")
	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	dispatch("rollback")
	if phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rollback must start the revert, got %q", phaseOf(isvc))
	}
	expectTarget("rollback", stable)
	for _, name := range []string{"new-2", "new-3"} {
		if err := c.Delete(ctx, canaryPod(ns, isvc.Name, "engine", "new", name)); err != nil {
			t.Fatal(err)
		}
	}
	updateIR(t, c, irKey, func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = stable })
	for _, pass := range []string{"drain", "hold", "hold again"} {
		dispatch(pass)
		if phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
			t.Fatalf("%s: the hold must stand RolledBack, got %q", pass, phaseOf(isvc))
		}
		expectTarget(pass, stable)
	}
	if want := []string{"nil->" + stable}; !slices.Equal(log.writes, want) {
		t.Fatalf("the hold moved the replica's target %v, want %v", log.writes, want)
	}

	// The release: a push of version 3 opens a run toward it and the
	// controller binds the unit to that run before the executor's pass.
	reopenRun(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "v3", StableRevision: "old"})
	deps.Group = rollout.CanaryGroup(isvc, rollout.Policies{})
	updateIR(t, c, irKey, func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = isvc.Name + "-engine-v3" })
	if err := c.Create(ctx, canaryControllerRevision(ns, isvc.Name, "engine", "v3", 3)); err != nil {
		t.Fatal(err)
	}
	if err := BindRun(ctx, c, isvc, deps.Group, RunOpen{}); err != nil {
		t.Fatal(err)
	}
	dispatch("release")
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" {
		t.Fatalf("the push of version 3 must end the hold and start the new target, got %+v", cs)
	}
	expectTarget("release", "")
	for _, name := range []string{"v3-2", "v3-3"} {
		if err := c.Create(ctx, canaryPod(ns, isvc.Name, "engine", "v3", name)); err != nil {
			t.Fatal(err)
		}
	}
	dispatch("serve")
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("version 3 must serve its first step before the second rollback, got %q", phaseOf(isvc))
	}

	// The second rollback is a new hold: it resolves the stable revision
	// again and writes the target again.
	lookupsBefore := log.lookups
	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	dispatch("rollback again")
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.RolledBackRevisionHash != "v3" || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the second rollback must reject version 3, got %+v phase=%q", cs, phaseOf(isvc))
	}
	expectTarget("rollback again", stable)
	if log.lookups <= lookupsBefore {
		t.Fatal("the new hold must look its stable revision up in the history again")
	}
	if want := []string{"nil->" + stable, stable + "->nil", "nil->" + stable}; !slices.Equal(log.writes, want) {
		t.Fatalf("the two holds moved the replica's target %v, want %v", log.writes, want)
	}
}

// A revert target that already names the stable revision's retained
// ControllerRevision is read and left: no history lookup and no write,
// however many passes run. A target naming another revision is resolved
// and rewritten, and an absent one is written, each once. A target whose
// ControllerRevision is gone sends every pass back to the history, which
// finds none and reports the stable revision missing, as a replica with
// no target would.
func TestRollbackSignal_ATargetNamingTheStableRevisionIsLeftAlone(t *testing.T) {
	ns := "default"
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "once"}}
	stable := "once-engine-stable0"
	for _, tc := range []struct {
		name, target string
		stableGone   bool
		wantLookups  int
		wantWrites   []string
		wantMissing  bool
		wantTarget   string
	}{
		{name: "names the stable revision", target: stable, wantTarget: stable},
		{name: "names another revision", target: "once-engine-other", wantLookups: 1, wantWrites: []string{"once-engine-other->" + stable}, wantTarget: stable},
		{name: "absent", wantLookups: 1, wantWrites: []string{"nil->" + stable}, wantTarget: stable},
		{name: "names the stable revision whose ControllerRevision is gone", target: stable, stableGone: true, wantLookups: 3, wantMissing: true, wantTarget: stable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replica := ir(ns, "once", v1beta1.EngineComponent, "newhash")
			if tc.target != "" {
				target := tc.target
				replica.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: &target}
			}
			objs := []runtime.Object{isvc, replica, canaryControllerRevision(ns, "once", "engine", "newhash", 2)}
			if !tc.stableGone {
				objs = append(objs, canaryControllerRevision(ns, "once", "engine", "stable0", 1))
			}
			c, log := loggingClient(fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build())
			ctx := context.Background()
			for pass := 1; pass <= 3; pass++ {
				missing, err := reconcileRollbackSignal(ctx, c, c, isvc, v1beta1.EngineComponent, nil, "stable0", true, false)
				if err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
				if missing != tc.wantMissing {
					t.Fatalf("pass %d reported the stable revision missing=%v, want %v", pass, missing, tc.wantMissing)
				}
			}
			got := &v1beta1.InferenceReplica{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: replica.Name}, got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != tc.wantTarget {
				t.Fatalf("the replica must be pointed at %s, got pacing=%+v", tc.wantTarget, got.Spec.Pacing)
			}
			if log.lookups != tc.wantLookups || !slices.Equal(log.writes, tc.wantWrites) {
				t.Fatalf("three passes looked the history up %d times and moved the target %v; want %d lookups and %v", log.lookups, log.writes, tc.wantLookups, tc.wantWrites)
			}
		})
	}
}

// settledHoldWithAStableLessDecoderFixture is a settled hold on a unit of
// an engine and a decoder: the engine's canary of version 2 was rolled back
// onto version 1, and the decoder was created in the same change that put
// it in the group, so the run recorded no stable revision for it and the
// revert wrote nothing on it. The engine's stable ControllerRevision is
// retained; the decoder runs its only revision.
func settledHoldWithAStableLessDecoderFixture(t *testing.T, name string) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	ns := "default"
	n4, one := 4, 1
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = name
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one}}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new", StableRevisionHash: "old", RolledBackRevisionHash: "new",
	})
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	engine.LatestRolledoutRevision = coordination.PerRevisionServiceName(name, v1beta1.EngineComponent, "old")
	engine.LatestReadyRevision = engine.LatestRolledoutRevision
	isvc.Status.Components[v1beta1.EngineComponent] = engine
	isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
			{Component: v1beta1.DecoderComponent, Revision: "dec"},
		},
	}}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = name + "-engine-old"
	decoderIR := ir(ns, name, v1beta1.DecoderComponent, "dec")
	decoderIR.Status.CurrentRevision = name + "-decoder-dec"
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc, engineIR, decoderIR,
		canaryPod(ns, name, "engine", "old", "old-0"),
		canaryPod(ns, name, "engine", "old", "old-1"),
		canaryPod(ns, name, "engine", "old", "old-2"),
		canaryPod(ns, name, "engine", "old", "old-3"),
		canaryPod(ns, name, "decoder", "dec", "dec-0"),
		canaryControllerRevision(ns, name, "engine", "old", 1),
		canaryControllerRevision(ns, name, "engine", "new", 2),
		canaryControllerRevision(ns, name, "decoder", "dec", 1),
	).Build()
	return isvc, c
}

// A settled hold records a member with no stable revision on the revision
// its replica settled on. The engine's canary of version 2 was rolled back
// onto version 1, and the decoder, created in the same change that put it
// in the group, has no stable revision the run or the revert could have
// recorded: the completed revert's write leaves its empty record empty,
// and the rolled-back run names it with no stable. A unit resting on a
// rejection has no roll active, so the settled write that keeps an idle
// unit's records level with its replicas runs under the hold too: the
// decoder, settled on its only revision with a Ready pod, is recorded on
// it in the first pass of the hold, and from then on the hold resolves
// that revision as its stable and points its replica at the revision it
// already runs. The engine's stable still comes from the rolled-back
// run's record and its replica stays pointed at that ControllerRevision.
func TestDispatch_ASettledHoldRecordsAMemberWithNoStableRevisionOnTheRevisionItsReplicaSettledOn(t *testing.T) {
	isvc, c := settledHoldWithAStableLessDecoderFixture(t, "settled-pd")
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	settled := revisionServiceName(isvc, v1beta1.DecoderComponent, "dec")
	for pass := 0; pass < 2; pass++ {
		if _, err := Dispatch(context.Background(), deps); err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		if got := phaseOf(isvc); got != v1beta1.RolloutPhaseRolledBack {
			t.Fatalf("pass %d: phase %q, want RolledBack", pass, got)
		}
		if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.Failed != nil {
			t.Fatalf("pass %d: the member with no stable revision parked the unit: %+v", pass, cs.Failed)
		}
		engine := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-engine"}, engine); err != nil {
			t.Fatal(err)
		}
		if engine.Spec.Pacing == nil || engine.Spec.Pacing.RollbackToRevision == nil || *engine.Spec.Pacing.RollbackToRevision != isvc.Name+"-engine-old" {
			t.Fatalf("pass %d: the engine must stay pointed at its stable revision, got pacing=%+v", pass, engine.Spec.Pacing)
		}
		rec := isvc.Status.Components[v1beta1.DecoderComponent]
		if rec.LatestRolledoutRevision != settled || rec.PreviousRolledoutRevision != "" || rec.LatestReadyRevision != settled {
			t.Fatalf("pass %d: the hold must record the member with no stable revision on the revision its replica settled on (%s), got %+v", pass, settled, rec)
		}
		if got := componentStableRevisionHash(isvc, v1beta1.DecoderComponent, v1beta1.EngineComponent); got != "dec" {
			t.Fatalf("pass %d: the recorded member resolved %q as its stable, want the revision it runs", pass, got)
		}
		decoder := &v1beta1.InferenceReplica{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-decoder"}, decoder); err != nil {
			t.Fatal(err)
		}
		if decoder.Spec.Pacing == nil || decoder.Spec.Pacing.RollbackToRevision == nil || *decoder.Spec.Pacing.RollbackToRevision != isvc.Name+"-decoder-dec" {
			t.Fatalf("pass %d: the hold points a recorded member at the revision it already runs, got pacing=%+v", pass, decoder.Spec.Pacing)
		}
	}
}

// With no run open, a member's stable revision for the revert check is
// read from the records: the primary's from its canary record once the last
// run on file is not a rollback, a secondary's from its own rolled-out
// revision, and a secondary with neither resolves to nothing.
func TestComponentStableRevisionHashWithNoRunOpen(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc"}}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{StableRevisionHash: "rtrstable"})
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.LatestRolledoutRevision = coordination.PerRevisionServiceName("svc", v1beta1.EngineComponent, "engstable")
	isvc.Status.Components[v1beta1.EngineComponent] = engine
	for name, rollout := range map[string]*v1beta1.RolloutStatus{
		"no run on file":              nil,
		"the last run on file closed": {LastRun: &v1beta1.RolloutRunRecord{Outcome: v1beta1.RolloutRunCompleted, TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, StableRevision: "completedstable"}}}},
	} {
		isvc.Status.Rollout = rollout
		if got := componentStableRevisionHash(isvc, v1beta1.RouterComponent, v1beta1.RouterComponent); got != "rtrstable" {
			t.Fatalf("%s: the primary's stable = %q, want its canary record's rtrstable", name, got)
		}
		if got := componentStableRevisionHash(isvc, v1beta1.EngineComponent, v1beta1.RouterComponent); got != "engstable" {
			t.Fatalf("%s: the secondary's stable = %q, want its rolled-out revision engstable", name, got)
		}
		if got := componentStableRevisionHash(isvc, v1beta1.DecoderComponent, v1beta1.RouterComponent); got != "" {
			t.Fatalf("%s: a secondary with no record resolves %q, want nothing", name, got)
		}
	}
}

// A unit with no record under a run that records it at its stable revision,
// one of its pods still on another revision: the pass runs the executor,
// which reads the Instances rolling back after a push the operator took
// back and projects RollingBack, instead of waiting for the replica's
// revision pair as it does for a unit whose record is still to be written.
func TestDispatch_UnitWithNoRecordUnderARunAtItsStableDoesNotWaitForThePair(t *testing.T) {
	ns, name := "default", "taken-back"
	n4 := 4
	replicas := int32(4)
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace, isvc.Name = ns, name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"},
	}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "old")
	engineIR.Status.CurrentRevision = name + "-engine-old"
	engineIR.Spec.Replicas = &replicas
	for i := int32(0); i < 4; i++ {
		revision := name + "-engine-old"
		if i == 3 {
			revision = name + "-engine-new"
		}
		engineIR.Status.InstanceStatuses = append(engineIR.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: revision,
			PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1,
		})
	}
	objs := []client.Object{isvc, engineIR,
		canaryPod(ns, name, "engine", "old", name+"-engine-0"),
		canaryPod(ns, name, "engine", "old", name+"-engine-1"),
		canaryPod(ns, name, "engine", "old", name+"-engine-2"),
		canaryPod(ns, name, "engine", "new", name+"-engine-3"),
		canaryControllerRevision(ns, name, "engine", "old", 1),
		canaryControllerRevision(ns, name, "engine", "new", 2),
	}
	fc := fakeclient.New(t, fakeclient.WithObjects(objs...))
	deps := DispatchDeps{
		Client: fc, Reader: fc, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{}),
		Requeue: reconcileRequeue, ParkedRequeue: failedRequeue, Now: time.Unix(1000, 0),
	}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs != nil {
		t.Fatalf("a run that records the unit at its stable revision starts no record, got %+v", cs)
	}
	if got := phaseOf(isvc); got != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the pass must read the Instance still on the other revision rolling back, got phase %q", got)
	}
}
