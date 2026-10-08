package placement

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// TestAllResumedRetargetKeepsServingHomeUntilReplacementServes drives a full
// home that was provisioned on its own while a peer's initial inventory was
// unknown, retargets the service to that peer while the inventory is still
// pending, and then lets the inventory arrive. The resumed move must keep the
// serving home, even across an observation it cannot read, until the
// replacement serves its whole floor within the shared allowance.
func TestAllResumedRetargetKeepsServingHomeUntilReplacementServes(t *testing.T) {
	const replicas = 3
	f := newBackendFixture(t, v1beta1.PlacementModeAll)
	key := client.ObjectKeyFromObject(f.source)
	if err := f.reconciler.Get(t.Context(), key, f.source); err != nil {
		t.Fatal(err)
	}
	f.source.Spec.Engine.MinReplicas = ptr.To(replicas)
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	live := func() *v1beta1.InferenceService {
		t.Helper()
		out := &v1beta1.InferenceService{}
		if err := f.reconciler.Get(t.Context(), key, out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	allocationOf := func(source *v1beta1.InferenceService, name string) *v1beta1.CandidateAllocationStatus {
		t.Helper()
		for _, candidate := range source.Status.Placement.Candidates {
			if candidate.Cluster == name {
				return candidate.Allocation
			}
		}
		t.Fatalf("%s has no allocation", name)
		return nil
	}
	setReady := func(name string, ready bool) {
		t.Helper()
		cluster := &v1beta1.WorkloadCluster{}
		if err := f.reconciler.Get(t.Context(), client.ObjectKey{Name: name}, cluster); err != nil {
			t.Fatal(err)
		}
		status := metav1.ConditionFalse
		if ready {
			status = metav1.ConditionTrue
		}
		cluster.Status.Conditions[0].Status = status
		if err := f.reconciler.Update(t.Context(), cluster); err != nil {
			t.Fatal(err)
		}
	}
	var peak int32
	// reconcile runs one pass and records the largest floor the fleet carried.
	reconcile := func() *v1beta1.InferenceService {
		t.Helper()
		current := f.reconcile(t)
		var total int32
		for _, candidate := range current.Status.Placement.Candidates {
			total += candidate.Allocation.CurrentReplicas
		}
		peak = max(peak, total)
		return current
	}
	retained := func(stage string, current *v1beta1.InferenceService) {
		t.Helper()
		a := allocationOf(current, "member-a")
		_, present := allMemberOn(t, f, "member-a")
		if a.CurrentReplicas != replicas || a.DrainRequested || !present {
			t.Fatalf("%s: the serving home was trimmed before its replacement served (current=%d drain=%t member=%t)", stage, a.CurrentReplicas, a.DrainRequested, present)
		}
	}

	// The peer is unreachable, so the first home is provisioned on its own.
	connected := f.connections.m["member-b"]
	delete(f.connections.m, "member-b")
	setReady("member-b", false)
	initial := reconcile()
	adoption := initial.Status.Placement.Plan.AdoptionDigest
	if adoption == "" {
		t.Fatal("unknown home inventory was treated as complete")
	}
	if diff := cmp.Diff(int32(replicas), allocationOf(initial, "member-a").CurrentReplicas); diff != "" {
		t.Fatalf("independent home floor (-want +got):\n%s", diff)
	}
	projectAllHome(t, f, "member-a", replicas)
	serveAllHome(t, f, "member-a", replicas)
	publishAllRouting(t, f, map[string]int32{"member-a": replicas})
	retained("serving independent home", reconcile())

	// A retarget to the unknown peer, with room for a whole home, holds.
	source := live()
	source.Spec.Placement.ClusterAffinity = testAffinity("metadata.name=member-b")
	source.Generation++
	if err := f.reconciler.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	held := reconcile()
	if diff := cmp.Diff(adoption, held.Status.Placement.Plan.AdoptionDigest); diff != "" {
		t.Fatalf("retarget did not wait for the initial inventory (-want +got):\n%s", diff)
	}
	if condition := held.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Reason != "AwaitingHomeInputs" {
		t.Fatalf("hold condition = %+v", condition)
	}
	a := allocationOf(held, "member-a")
	if diff := cmp.Diff([]int32{replicas, 0}, []int32{a.CurrentReplicas, a.DesiredReplicas}); diff != "" {
		t.Fatalf("held floors (-want +got):\n%s", diff)
	}
	retained("inventory hold", held)
	projectAllHome(t, f, "member-a", replicas)
	publishAllRouting(t, f, map[string]int32{"member-a": replicas})
	retained("acknowledged hold", reconcile())

	// The peer's inventory arrives: the move resumes and places the replacement.
	f.connections.m["member-b"] = connected
	setReady("member-b", true)
	resumed := reconcile()
	if diff := cmp.Diff("", resumed.Status.Placement.Plan.AdoptionDigest); diff != "" {
		t.Fatalf("inventory hold survived recovery (-want +got):\n%s", diff)
	}
	retained("resume", resumed)
	projectAllHome(t, f, "member-a", replicas)
	publishAllRouting(t, f, map[string]int32{"member-a": replicas})
	placed := reconcile()
	retained("replacement placement", placed)
	if diff := cmp.Diff(int32(replicas), allocationOf(placed, "member-b").CurrentReplicas); diff != "" {
		t.Fatalf("replacement floor (-want +got):\n%s", diff)
	}
	if _, ok := allMemberOn(t, f, "member-b"); !ok {
		t.Fatal("replacement member was not created")
	}
	projectAllHome(t, f, "member-a", replicas)
	projectAllHome(t, f, "member-b", replicas)
	publishAllRouting(t, f, map[string]int32{"member-a": replicas})
	retained("replacement projected", reconcile())

	// One unreadable observation reports the serving home at zero readiness
	// while it stays addressable, so the publisher withdraws its weight. The
	// home keeps its acknowledged plan behind an unknown observation, and that
	// kept acknowledgement is not fresh evidence: an unknown home cannot
	// acknowledge the surge pause, so the move still holds.
	projected := candidateOf(t, live(), "member-a")
	if !projected.ObservationKnown || projected.AppliedPlanID == "" {
		t.Fatalf("serving home has no acknowledged plan before the unreadable pass: %+v", projected)
	}
	f.connections.m["member-a"] = unreadableAllMember(f, "member-a")
	unknown := reconcile()
	retained("unreadable observation", unknown)
	candidate := candidateOf(t, unknown, "member-a")
	if candidate.ObservationKnown || candidate.ReadyReplicas != 0 || candidate.Phase != v1beta1.CandidatePhaseAdmitted || candidate.Endpoint == nil {
		t.Fatalf("unreadable home reported as %+v", candidate)
	}
	if diff := cmp.Diff(projected.AppliedPlanID, candidate.AppliedPlanID); diff != "" {
		t.Fatalf("unreadable observation changed the acknowledged plan (-want +got):\n%s", diff)
	}
	if condition := unknown.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Status != corev1.ConditionFalse || condition.Reason != "AwaitingSurgePause" {
		t.Fatalf("kept acknowledgement advanced the move: %+v", condition)
	}
	if condition := unknown.Status.GetCondition(v1beta1.PlacementSatisfied); condition != nil && condition.Status == corev1.ConditionTrue {
		t.Fatalf("kept acknowledgement satisfied the plan: %+v", condition)
	}
	f.connections.m["member-a"] = workloadcluster.NewNeverCachingClient(f.workers["member-a"])
	publishAllRouting(t, f, map[string]int32{"member-a": 0})
	retained("unrouted serving home", reconcile())
	publishAllRouting(t, f, map[string]int32{"member-a": replicas})
	retained("routing restored", reconcile())

	// Only a replacement serving its whole floor releases the original home.
	serveAllHome(t, f, "member-b", replicas)
	projectAllHome(t, f, "member-b", replicas)
	retained("replacement serving before publication", reconcile())
	publishAllRouting(t, f, map[string]int32{"member-a": replicas, "member-b": replicas})
	draining := reconcile()
	if a := allocationOf(draining, "member-a"); !a.DrainRequested || a.CurrentReplicas != replicas {
		t.Fatalf("serving replacement did not request the original drain: %+v", a)
	}
	projectAllHome(t, f, "member-a", replicas)
	projectAllHome(t, f, "member-b", replicas)
	publishAllRouting(t, f, map[string]int32{"member-a": 0, "member-b": replicas})
	released := reconcile()
	if diff := cmp.Diff(int32(0), allocationOf(released, "member-a").CurrentReplicas); diff != "" {
		t.Fatalf("drained floor (-want +got):\n%s", diff)
	}
	projectAllHome(t, f, "member-b", replicas)
	publishAllRouting(t, f, map[string]int32{"member-a": 0, "member-b": replicas})
	reconcile()
	if _, ok := allMemberOn(t, f, "member-a"); ok {
		t.Fatal("drained original member remains")
	}
	if diff := cmp.Diff(int32(replicas), allocationOf(live(), "member-b").CurrentReplicas); diff != "" {
		t.Fatalf("replacement floor after the move (-want +got):\n%s", diff)
	}
	if peak > 2*replicas {
		t.Fatalf("the move carried %d replicas, more than one whole home above the floor", peak)
	}
}

// candidateOf returns the status candidate recorded for name.
func candidateOf(t *testing.T, source *v1beta1.InferenceService, name string) v1beta1.CandidatePlacement {
	t.Helper()
	for _, candidate := range source.Status.Placement.Candidates {
		if candidate.Cluster == name {
			return candidate
		}
	}
	t.Fatalf("%s has no candidate", name)
	return v1beta1.CandidatePlacement{}
}

// allMemberOn reads the member copy of the source on the named home.
func allMemberOn(t *testing.T, f *backendFixture, name string) (*v1beta1.InferenceService, bool) {
	t.Helper()
	member := &v1beta1.InferenceService{}
	err := f.workers[name].Get(t.Context(), client.ObjectKeyFromObject(f.source), member)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return member, true
}

func allEngineKey(f *backendFixture) client.ObjectKey {
	return client.ObjectKey{Namespace: f.source.Namespace, Name: f.source.Name + "-engine"}
}

// projectAllHome acknowledges the member's current execution policy on its
// engine component, creating the component without any serving instance first.
func projectAllHome(t *testing.T, f *backendFixture, name string, replicas int32) {
	t.Helper()
	member, ok := allMemberOn(t, f, name)
	if !ok {
		t.Fatalf("%s has no member to project", name)
	}
	policy, err := protocol.FromDerived(member)
	if err != nil {
		t.Fatal(err)
	}
	irKey := allEngineKey(f)
	ir := &v1beta1.InferenceReplica{}
	err = f.workers[name].Get(t.Context(), irKey, ir)
	fresh := apierrors.IsNotFound(err)
	if err != nil && !fresh {
		t.Fatal(err)
	}
	if fresh {
		ir = &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: irKey.Namespace, Name: irKey.Name, UID: types.UID("engine-" + name),
				Labels:          map[string]string{constants.InferenceServicePodLabelKey: f.source.Name},
				Annotations:     map[string]string{},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(member, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))},
			},
			Spec: v1beta1.InferenceReplicaSpec{
				Component: v1beta1.EngineComponent, ParentRef: &v1beta1.ParentReference{Name: f.source.Name},
				Replicas: ptr.To(replicas), Runners: []v1beta1.Runner{{Name: "default", Size: 1}},
			},
		}
		for index := range replicas {
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{Index: index, Phase: v1beta1.OMENativeInstanceCreating})
		}
	}
	ir.Generation++
	ir.Status.ObservedGeneration, ir.Status.PlacementObservedGeneration = ir.Generation, ir.Generation
	ir.Spec.PlacementExecution, ir.Spec.PlacementReplicaLimit = policy, ptr.To(replicas)
	ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = strconv.FormatInt(member.Generation, 10)
	if fresh {
		err = f.workers[name].Create(t.Context(), ir)
	} else {
		err = f.workers[name].Update(t.Context(), ir)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// serveAllHome materializes every engine instance as a ready, serving Pod and
// publishes the member's endpoint.
func serveAllHome(t *testing.T, f *backendFixture, name string, replicas int32) {
	t.Helper()
	member, ok := allMemberOn(t, f, name)
	if !ok {
		t.Fatalf("%s has no member to serve", name)
	}
	irKey := allEngineKey(f)
	ir := &v1beta1.InferenceReplica{}
	if err := f.workers[name].Get(t.Context(), irKey, ir); err != nil {
		t.Fatal(err)
	}
	ir.Status.ReadyReplicas = replicas
	for i := range ir.Status.InstanceStatuses {
		row := &ir.Status.InstanceStatuses[i]
		row.Phase, row.Admitted, row.RunningRevision, row.PodCount, row.ServingPodCount = v1beta1.OMENativeInstanceReady, true, irKey.Name+"-a", 1, 1
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: irKey.Namespace, Name: fmt.Sprintf("%s-%d", irKey.Name, row.Index), UID: types.UID(fmt.Sprintf("pod-%s-%d", name, row.Index)),
				Labels:          map[string]string{constants.InferenceServicePodLabelKey: f.source.Name, query.LabelInstanceIdx: strconv.Itoa(int(row.Index)), query.LabelRevisionHash: "a"},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ir, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
			}},
		}
		if err := f.workers[name].Create(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.workers[name].Update(t.Context(), ir); err != nil {
		t.Fatal(err)
	}
	member.Status.URL = &apis.URL{Scheme: "https", Host: name + ".example.com"}
	member.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	if err := f.workers[name].Status().Update(t.Context(), member); err != nil {
		t.Fatal(err)
	}
}

// publishAllRouting writes the routing intent for the accepted plan: every
// listed home that reports an endpoint carries the given weight.
func publishAllRouting(t *testing.T, f *backendFixture, weights map[string]int32) {
	t.Helper()
	key := client.ObjectKeyFromObject(f.source)
	current := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), key, current); err != nil {
		t.Fatal(err)
	}
	trafficMap := &v1beta1.TrafficMap{}
	err := f.reconciler.Get(t.Context(), key, trafficMap)
	fresh := apierrors.IsNotFound(err)
	if err != nil && !fresh {
		t.Fatal(err)
	}
	if fresh {
		trafficMap.ObjectMeta = metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: "traffic-map", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(current, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}}
	}
	trafficMap.Generation++
	trafficMap.Spec.PlacementPlanID, trafficMap.Spec.ObservedISVCGeneration, trafficMap.Spec.Entries = current.Status.Placement.Plan.ID, current.Generation, nil
	for _, candidate := range current.Status.Placement.Candidates {
		weight, listed := weights[candidate.Cluster]
		if !listed || candidate.Endpoint == nil {
			continue
		}
		trafficMap.Spec.Entries = append(trafficMap.Spec.Entries, v1beta1.TrafficMapEntry{Cluster: candidate.Cluster, Weight: weight, Endpoint: candidate.Endpoint.DeepCopy()})
	}
	trafficMap.Status.Published, trafficMap.Status.SourceUID, trafficMap.Status.ObservedTrafficMapGeneration = true, current.UID, trafficMap.Generation
	if fresh {
		err = f.reconciler.Create(t.Context(), trafficMap)
	} else {
		err = f.reconciler.Update(t.Context(), trafficMap)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// unreadableAllMember returns a connection to the named home whose component
// inventory cannot be listed, so every planned observation of it is unknown.
func unreadableAllMember(f *backendFixture, name string) workloadcluster.SelectivelyCachingClient {
	unreadable := interceptor.NewClient(f.workers[name], interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*v1beta1.InferenceReplicaList); ok {
			return errors.New("member inventory unavailable")
		}
		return cl.List(ctx, list, opts...)
	}})
	return workloadcluster.NewNeverCachingClient(unreadable)
}
