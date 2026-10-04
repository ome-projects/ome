package placement

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

type plannedObservationFixture struct {
	source       *v1beta1.InferenceService
	member       *v1beta1.InferenceService
	resources    memberResourceFixture
	components   []v1beta1.ComponentType
	intercept    interceptor.Funcs
	decodeBound  uint64
	extraObjects []client.Object
}

func observationFixture(t *testing.T) plannedObservationFixture {
	t.Helper()
	source := plannedTestSource()
	source.Status.Placement.Plan.PauseSurge = true
	source.Status.Placement.Candidates[0].Allocation.CurrentReplicas = 1
	source.Status.Placement.Candidates[0].Allocation.DesiredReplicas = 1
	member := DeriveISVC(source, "", "")
	member.UID, member.Generation = "member-service", 3
	member.Status.URL = &apis.URL{Scheme: "https", Host: "member.example.com"}
	member.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	policy := executionPolicy(source, source.Status.Placement.Candidates[0].Allocation)
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	member.Annotations[constants.PlacementExecution] = raw
	resources := resourceFixture()
	ir := resources.ir
	ir.Namespace, ir.Generation = source.Namespace, 5
	ir.Labels = map[string]string{constants.InferenceServicePodLabelKey: source.Name}
	ir.Annotations = map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "3"}
	ir.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(member, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}
	ir.Spec.Component, ir.Spec.ParentRef = v1beta1.EngineComponent, &v1beta1.ParentReference{Name: source.Name}
	ir.Spec.Replicas, ir.Spec.PlacementExecution = ptr.To[int32](1), policy
	ir.Spec.PlacementReplicaLimit = ptr.To[int32](1)
	ir.Status.ObservedGeneration = ir.Generation
	ir.Status.PlacementObservedGeneration = ir.Generation
	ir.Status.InstanceStatuses[0].Admitted = true
	resources.pods[0].Namespace = source.Namespace
	resources.pods[0].Labels[constants.InferenceServicePodLabelKey] = source.Name
	return plannedObservationFixture{source: source, member: member, resources: resources, components: []v1beta1.ComponentType{v1beta1.EngineComponent}}
}

func TestObservePlannedHome(t *testing.T) {
	type result struct {
		Home              allocation.Home
		Reserved          int32
		PauseAcknowledged bool
		Applied           string
		Ready, Admitted   int32
	}
	steady := result{Home: allocation.Home{Known: true, Applied: true, Occupied: 1, Ready: 1}, PauseAcknowledged: true, Applied: "plan-a", Ready: 1, Admitted: 1}
	for _, tt := range []struct {
		name    string
		edit    func(*plannedObservationFixture)
		want    result
		wantErr bool
	}{
		{name: "observed component acknowledges pause", want: steady},
		{name: "complete contracted policy acknowledges pause", edit: func(f *plannedObservationFixture) { f.attachContract(t) }, want: steady},
		{name: "pruned demand cannot acknowledge pause", edit: func(f *plannedObservationFixture) {
			f.attachContract(t)
			f.resources.ir.Spec.PlacementExecution.Demand = nil
		}, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "different component rendering cannot acknowledge pause", edit: func(f *plannedObservationFixture) {
			f.attachContract(t)
			f.resources.ir.Spec.PlacementExecution.Demand.Components[0].RenderingHash = strings.Repeat("c", 64)
		}, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "different demand fingerprint cannot acknowledge pause", edit: func(f *plannedObservationFixture) {
			f.attachContract(t)
			f.resources.ir.Spec.PlacementExecution.Demand.Fingerprint = strings.Repeat("c", 64)
		}, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "generic generation observation cannot acknowledge placement guards", edit: func(f *plannedObservationFixture) {
			f.resources.ir.Status.PlacementObservedGeneration = 0
		}, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "stale placement guard acknowledgement cannot grant authority", edit: func(f *plannedObservationFixture) {
			f.resources.ir.Status.PlacementObservedGeneration--
		}, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "gang width is verified through its PodGroup", edit: func(f *plannedObservationFixture) {
			f.resources.addGang()
			f.extraObjects = []client.Object{testMemberPodGroup(f.resources.ir)}
		}, want: steady},
		{name: "partial gang cannot claim whole replica admission", edit: func(f *plannedObservationFixture) {
			f.resources.addGang()
			f.extraObjects = []client.Object{testMemberPodGroup(f.resources.ir)}
			f.resources.pods = f.resources.pods[:1]
			f.resources.ir.Status.InstanceStatuses[0].PodCount = 1
			f.resources.ir.Status.InstanceStatuses[0].ServingPodCount = 1
		}, want: result{Home: allocation.Home{Known: true, Applied: true, Occupied: 1}, PauseAcknowledged: true, Applied: "plan-a"}},
		{name: "creating partial gang has no admitted replica", edit: func(f *plannedObservationFixture) {
			f.resources.addGang()
			f.extraObjects = []client.Object{testMemberPodGroup(f.resources.ir)}
			f.resources.pods = f.resources.pods[:1]
			f.resources.ir.Status.ReadyReplicas = 0
			f.resources.ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstanceCreating
			f.resources.ir.Status.InstanceStatuses[0].PodCount = 1
			f.resources.ir.Status.InstanceStatuses[0].ServingPodCount = 0
		}, want: result{Home: allocation.Home{Known: true, Applied: true, Occupied: 1}, PauseAcknowledged: true, Applied: "plan-a"}},
		{name: "unresolved desired replicas are unknown", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.Replicas = nil }, wantErr: true},
		{name: "router replicas remain a shared per home policy", edit: func(f *plannedObservationFixture) {
			ir := f.resources.ir.DeepCopy()
			ir.UID, ir.Name, ir.Spec.Component = "router-uid", "service-router", v1beta1.RouterComponent
			ir.Spec.Replicas = ptr.To[int32](8)
			pod := f.resources.pods[0].DeepCopy()
			pod.Name, pod.UID, pod.OwnerReferences[0].UID = "router-0", "router-pod", ir.UID
			f.extraObjects = []client.Object{ir, pod}
			f.components = append(f.components, v1beta1.RouterComponent)
		}, want: steady},
		{name: "router must be admitted before the home serves", edit: func(f *plannedObservationFixture) { f.components = append(f.components, v1beta1.RouterComponent) }, want: result{Home: allocation.Home{Known: true, Occupied: 1}}},
		{name: "engine and decoder share whole replica accounting", edit: func(f *plannedObservationFixture) {
			ir := f.resources.ir.DeepCopy()
			ir.UID, ir.Name, ir.Spec.Component = "decoder-uid", "service-decoder", v1beta1.DecoderComponent
			ir.Spec.Replicas = ptr.To[int32](2)
			ir.Spec.PlacementReplicaLimit = ptr.To[int32](2)
			pod := f.resources.pods[0].DeepCopy()
			pod.Name, pod.UID, pod.OwnerReferences[0].UID = "decoder-0", "decoder-pod", ir.UID
			f.extraObjects = []client.Object{ir, pod}
			f.components = append(f.components, v1beta1.DecoderComponent)
		}, want: result{Home: allocation.Home{Known: true, Applied: true, Occupied: 2, Ready: 1}, PauseAcknowledged: true, Applied: "plan-a", Ready: 1, Admitted: 1}},
		{name: "columnar component acknowledges pause", edit: func(f *plannedObservationFixture) {
			f.resources.ir = columnarTwin(t, f.resources.ir)
			f.decodeBound = 1
		}, want: steady},
		{name: "unbounded columnar status is unknown", edit: func(f *plannedObservationFixture) { f.resources.ir = columnarTwin(t, f.resources.ir) }, wantErr: true},
		{name: "autoscaler request reserves uncreated instances", edit: func(f *plannedObservationFixture) {
			f.resources.ir.Spec.Replicas = ptr.To[int32](3)
			f.resources.ir.Spec.PlacementReplicaLimit = ptr.To[int32](3)
		}, want: result{Home: allocation.Home{Known: true, Applied: true, Occupied: 3, Ready: 1}, PauseAcknowledged: true, Applied: "plan-a", Ready: 1, Admitted: 1}},
		{name: "deferred autoscaler growth does not expand authority", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.Replicas = ptr.To[int32](8) }, want: steady},
		{name: "unused limit remains committed", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.PlacementReplicaLimit = ptr.To[int32](3) }, want: result{Home: allocation.Home{Known: true, Applied: true, Occupied: 3, Ready: 1}, PauseAcknowledged: true, Applied: "plan-a", Ready: 1, Admitted: 1}},
		{name: "pruned growth limit cannot acknowledge pause", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.PlacementReplicaLimit = nil }, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "negative desired replicas are unknown", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.Replicas = ptr.To[int32](-1) }, wantErr: true},
		{name: "replaced source cannot inherit observations", edit: func(f *plannedObservationFixture) { f.source.Status.Placement.Plan.SourceUID = "older-source" }, wantErr: true},
		{name: "metadata annotation alone cannot acknowledge pause", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.PlacementExecution = nil }, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "stale component generation cannot acknowledge pause", edit: func(f *plannedObservationFixture) { f.resources.ir.Status.ObservedGeneration-- }, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "stale parent projection cannot acknowledge pause", edit: func(f *plannedObservationFixture) {
			f.resources.ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "2"
		}, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "source plan revision must reach every component", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.PlacementExecution.Revision-- }, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "scale floor must reach component", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.Replicas = ptr.To[int32](0) }, want: result{Home: allocation.Home{Known: true, Occupied: 1, Ready: 1}, Ready: 1, Admitted: 1}},
		{name: "inherited decoder is required before acknowledgement", edit: func(f *plannedObservationFixture) { f.components = append(f.components, v1beta1.DecoderComponent) }, want: result{Home: allocation.Home{Known: true, Occupied: 1}}},
		{name: "empty member is conclusively absent", edit: func(f *plannedObservationFixture) { f.member, f.resources.ir, f.resources.pods = nil, nil, nil }, want: result{Home: allocation.Home{Known: true, Absent: true}}},
		{name: "deleted service retains occupied components", edit: func(f *plannedObservationFixture) { f.member = nil }, want: result{Home: allocation.Home{Known: true, Occupied: 1}}},
		{name: "terminating service retains occupied components", edit: func(f *plannedObservationFixture) {
			now := metav1.Now()
			f.member.DeletionTimestamp = &now
			f.member.Finalizers = []string{"example.com/cleanup"}
		}, want: result{Home: allocation.Home{Known: true, Occupied: 1}}},
		{name: "ongoing surge preserves serving observation", edit: func(f *plannedObservationFixture) { f.resources.addSurge() }, want: result{Home: allocation.Home{Known: true, Applied: true, Occupied: 1}, Reserved: 1, PauseAcknowledged: true, Applied: "plan-a", Ready: 1, Admitted: 1}},
		{name: "ordinary local collision stays unverified", edit: func(f *plannedObservationFixture) { f.member.Annotations, f.member.Labels = nil, nil }, wantErr: true},
		{name: "orphan pod blocks absence proof", edit: func(f *plannedObservationFixture) { f.resources.ir, f.member = nil, nil }, wantErr: true},
		{name: "orphan component needs source provenance", edit: func(f *plannedObservationFixture) { f.member = nil; f.resources.ir.Spec.PlacementExecution = nil }, wantErr: true},
		{name: "component from another member is unknown", edit: func(f *plannedObservationFixture) { f.resources.ir.OwnerReferences[0].UID = "replaced-member" }, wantErr: true},
		{name: "unsupported component is unknown", edit: func(f *plannedObservationFixture) { f.resources.ir.Spec.Component = "unsupported" }, wantErr: true},
		{name: "unresolved component set cannot grant credit", edit: func(f *plannedObservationFixture) { f.components = nil }, wantErr: true},
		{name: "unreadable pod inventory is unknown", edit: func(f *plannedObservationFixture) {
			f.intercept.List = func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, pods := list.(*corev1.PodList); pods {
					return fmt.Errorf("pod inventory unavailable")
				}
				return c.List(ctx, list, opts...)
			}
		}, wantErr: true},
		{name: "unreadable inventory keeps the acknowledged plan", edit: func(f *plannedObservationFixture) {
			f.source.Status.Placement.Candidates[0].AppliedPlanID = "plan-a"
			f.source.Status.Placement.Candidates[0].ReadyReplicas = 1
			f.intercept.List = func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, components := list.(*v1beta1.InferenceReplicaList); components {
					return fmt.Errorf("member inventory unavailable")
				}
				return c.List(ctx, list, opts...)
			}
		}, wantErr: true},
		{name: "racing component version is unknown", edit: func(f *plannedObservationFixture) {
			f.source.Status.Placement.Candidates[0].ReadyReplicas = 1
			f.intercept.Get = func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if _, ir := obj.(*v1beta1.InferenceReplica); ir {
					obj.SetResourceVersion("changed")
				}
				return nil
			}
		}, wantErr: true},
		{name: "racing member version withholds previous ready capacity", edit: func(f *plannedObservationFixture) {
			f.source.Status.Placement.Candidates[0].ReadyReplicas = 1
			reads := 0
			f.intercept.Get = func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if _, member := obj.(*v1beta1.InferenceService); member {
					reads++
					if reads == 2 {
						obj.SetResourceVersion("changed")
					}
				}
				return nil
			}
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := observationFixture(t)
			if tt.edit != nil {
				tt.edit(&fixture)
			}
			objects := []client.Object{}
			objects = append(objects, fixture.extraObjects...)
			if fixture.member != nil {
				objects = append(objects, fixture.member)
			}
			if fixture.resources.ir != nil {
				objects = append(objects, fixture.resources.ir)
			}
			for i := range fixture.resources.pods {
				objects = append(objects, &fixture.resources.pods[i])
			}
			scheme := testScheme(t)
			worker := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(fixture.intercept).Build()
			clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, _ := newPlacer(scheme, clusters, fixture.source, plannedTestRegistration())
			r.InstanceStatusDecoder = irstatus.NewDecoder(fixture.decodeBound)
			observation, err := r.observePlannedHome(context.Background(), fixture.source, fixture.source.Status.Placement.Candidates[0], fixture.components)
			if (err != nil) != tt.wantErr {
				t.Fatalf("observation error = %v, want error %t", err, tt.wantErr)
			}
			if tt.wantErr {
				type evidence struct {
					AllocationKnown, ObservationKnown bool
					AppliedPlan                       string
					Ready                             int32
				}
				// An unverified pass grants no credit and keeps the plan it last acknowledged.
				want := evidence{AppliedPlan: fixture.source.Status.Placement.Candidates[0].AppliedPlanID}
				got := evidence{observation.Home.Known, observation.Candidate.ObservationKnown, observation.Candidate.AppliedPlanID, observation.Candidate.ReadyReplicas}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("unverified inventory evidence (-want +got):\n%s", diff)
				}
				return
			}
			got := result{Home: observation.Home, Reserved: observation.RolloutReserved, PauseAcknowledged: observation.PauseAcknowledged, Applied: observation.Candidate.AppliedPlanID, Ready: observation.Candidate.ReadyReplicas, Admitted: observation.Candidate.AdmittedReplicas}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("observation (-want +got):\n%s", diff)
			}
		})
	}
}

func (f *plannedObservationFixture) attachContract(t *testing.T) {
	t.Helper()
	attachCapacityContract(t, f.source)
	policy := executionPolicy(f.source, f.source.Status.Placement.Candidates[0].Allocation)
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	f.member.Annotations[constants.PlacementExecution] = raw
	f.resources.ir.Spec.PlacementExecution = policy.DeepCopy()
}

func TestPlannedTrafficEvidence(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.TrafficMap, *v1beta1.CandidatePlacement)
		want [3]bool
	}{
		{name: "published home", want: [3]bool{true, true, false}},
		{name: "published zero proves requested drain", edit: func(tm *v1beta1.TrafficMap, c *v1beta1.CandidatePlacement) {
			c.Allocation.DrainRequested = true
			tm.Spec.Entries[0].Weight = 0
		}, want: [3]bool{true, false, true}},
		{name: "published empty map proves requested drain", edit: func(tm *v1beta1.TrafficMap, c *v1beta1.CandidatePlacement) {
			c.Allocation.DrainRequested = true
			tm.Spec.Entries = nil
		}, want: [3]bool{true, false, true}},
		{name: "unpublished zero acknowledges requested drain", edit: func(tm *v1beta1.TrafficMap, c *v1beta1.CandidatePlacement) {
			c.Allocation.DrainRequested = true
			tm.Spec.Entries[0].Weight = 0
			tm.Status.Published = false
			tm.Status.ObservedTrafficMapGeneration = 0
		}, want: [3]bool{true, false, true}},
		{name: "automatic zero is not a removal instruction", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) { tm.Spec.Entries[0].Weight = 0 }, want: [3]bool{true, false, false}},
		{name: "previous plan cannot acknowledge drain", edit: func(tm *v1beta1.TrafficMap, c *v1beta1.CandidatePlacement) {
			tm.Spec.PlacementPlanID = "older"
			c.Allocation.DrainRequested = true
			tm.Spec.Entries = nil
		}},
		{name: "previous source generation cannot grant credit", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) { tm.Spec.ObservedISVCGeneration-- }},
		{name: "publisher acknowledgement is independent", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) { tm.Status.Published = false }, want: [3]bool{true, true, false}},
		{name: "publisher generation is independent", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) { tm.Status.ObservedTrafficMapGeneration-- }, want: [3]bool{true, true, false}},
		{name: "different source provenance is unknown", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) {
			tm.Status.SourceUID = types.UID("other-source")
		}},
		{name: "different source owner is unknown", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) {
			tm.OwnerReferences[0].UID = "other-source"
		}},
		{name: "different endpoint cannot fund a move", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) {
			tm.Spec.Entries[0].Endpoint.Host = "other.example.com"
		}},
		{name: "duplicate cluster entries are unknown", edit: func(tm *v1beta1.TrafficMap, _ *v1beta1.CandidatePlacement) {
			tm.Spec.Entries = append(tm.Spec.Entries, tm.Spec.Entries[0])
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedTestSource()
			candidate := *source.Status.Placement.Candidates[0].DeepCopy()
			candidate.Endpoint = &apis.URL{Scheme: "https", Host: "member.example.com"}
			tm := &v1beta1.TrafficMap{
				ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace, Generation: 4, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(source, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}},
				Spec:       v1beta1.TrafficMapSpec{PlacementPlanID: source.Status.Placement.Plan.ID, ObservedISVCGeneration: source.Generation, Entries: []v1beta1.TrafficMapEntry{{Cluster: candidate.Cluster, Weight: 1, Endpoint: candidate.Endpoint.DeepCopy()}}},
				Status:     v1beta1.TrafficMapStatus{SourceUID: source.UID, Published: true, ObservedTrafficMapGeneration: 4},
			}
			if tt.edit != nil {
				tt.edit(tm, &candidate)
			}
			known, routable, drained := plannedTrafficEvidence(source, tm, candidate)
			if diff := cmp.Diff(tt.want, [3]bool{known, routable, drained}); diff != "" {
				t.Errorf("traffic evidence (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFullHomeReadyRequiresEveryReplicaFloor(t *testing.T) {
	for _, tt := range []struct {
		name                                   string
		engine, router                         int32
		terminal, stale, ingressPending, ready bool
	}{
		{name: "complete engine", engine: 1, ready: true},
		{name: "partial engine", engine: 2},
		{name: "complete engine and router", engine: 1, router: 1, ready: true},
		{name: "partial router", engine: 1, router: 2},
		{name: "terminal member", engine: 1, terminal: true},
		{name: "stale component acknowledgement", engine: 1, stale: true},
		{name: "ingress is not ready", engine: 1, ingressPending: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := observationFixture(t)
			a := f.source.Status.Placement.Candidates[0].Allocation
			a.CurrentReplicas, a.DesiredReplicas = tt.engine, tt.engine
			a.CurrentHome = &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: tt.engine}}}
			if tt.router > 0 {
				a.CurrentHome.ReplicaFloors = append(a.CurrentHome.ReplicaFloors, v1beta1.PlacementComponentFloor{Component: v1beta1.RouterComponent, Replicas: tt.router})
			}
			a.DesiredHome = a.CurrentHome.DeepCopy()
			policy := executionPolicy(f.source, a)
			raw, err := protocol.Encode(policy)
			if err != nil {
				t.Fatal(err)
			}
			f.member.Annotations[constants.PlacementExecution] = raw
			if tt.ingressPending {
				f.member.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionFalse})
			}
			if tt.terminal {
				f.member.Status.ModelStatus.TransitionStatus = v1beta1.InvalidSpec
			}
			f.resources.ir.Spec.PlacementExecution = policy.DeepCopy()
			f.resources.ir.Spec.Replicas, f.resources.ir.Spec.PlacementReplicaLimit = ptr.To(tt.engine), ptr.To(tt.engine)
			if tt.stale {
				f.resources.ir.Status.PlacementObservedGeneration = 0
			}
			objects := []client.Object{f.member, f.resources.ir, &f.resources.pods[0]}
			if tt.router > 0 {
				ir := f.resources.ir.DeepCopy()
				ir.Name, ir.UID, ir.Spec.Component = "service-router", "router-uid", v1beta1.RouterComponent
				ir.Spec.Replicas, ir.Spec.PlacementReplicaLimit = ptr.To(tt.router), ptr.To(tt.router)
				pod := f.resources.pods[0].DeepCopy()
				pod.Name, pod.UID, pod.OwnerReferences[0].UID = "router-0", "router-pod", ir.UID
				f.components = append(f.components, v1beta1.RouterComponent)
				objects = append(objects, ir, pod)
			}
			scheme := testScheme(t)
			worker := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, _ := newPlacer(scheme, clusters, f.source, plannedTestRegistration())
			got, err := r.observePlannedHome(t.Context(), f.source, f.source.Status.Placement.Candidates[0], f.components)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.ready, got.FullHomeReady); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.terminal, got.Terminal); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
