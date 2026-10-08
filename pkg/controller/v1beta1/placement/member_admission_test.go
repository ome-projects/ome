package placement

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestVerifiedMemberAdmission(t *testing.T) {
	type result struct {
		Admitted []bool
		Ready    int32
	}
	complete := result{Admitted: []bool{true}, Ready: 1}
	pending := result{Admitted: []bool{true}}
	partial := result{Admitted: []bool{false}}
	for _, tt := range []struct {
		name    string
		edit    func(*memberResourceFixture)
		want    result
		wantErr bool
	}{
		{name: "complete leader worker gang", want: complete},
		{name: "missing worker", edit: func(f *memberResourceFixture) { f.pods = f.pods[:1] }, want: partial},
		{name: "missing gang", edit: func(f *memberResourceFixture) { f.pods = nil }, want: partial},
		{name: "gated worker", edit: func(f *memberResourceFixture) {
			f.pods[1].Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/admission"}}
		}, want: partial},
		{name: "terminating worker", edit: func(f *memberResourceFixture) { now := metav1.Now(); f.pods[1].DeletionTimestamp = &now }, want: partial},
		{name: "failed worker", edit: func(f *memberResourceFixture) { f.pods[1].Status.Phase = corev1.PodFailed }, want: partial},
		{name: "completed worker", edit: func(f *memberResourceFixture) { f.pods[1].Status.Phase = corev1.PodSucceeded }, want: partial},
		{name: "pending ungated worker is admitted", edit: func(f *memberResourceFixture) { f.pods[1].Status = corev1.PodStatus{Phase: corev1.PodPending} }, want: pending},
		{name: "drained worker is not ready", edit: func(f *memberResourceFixture) { f.pods[1].Status.Conditions[1].Status = corev1.ConditionFalse }, want: pending},
		{name: "aggregate readiness still caps credit", edit: func(f *memberResourceFixture) { f.ir.Status.ReadyReplicas = 0 }, want: pending},
		{name: "admission report still required", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].Admitted = false }, want: partial},
		{name: "duplicate runner ordinal cannot replace worker", edit: func(f *memberResourceFixture) { f.pods[1].Labels[query.LabelRunner] = "leader" }, want: partial},
		{name: "runner identity required", edit: func(f *memberResourceFixture) { delete(f.pods[1].Labels, query.LabelRunner) }, want: partial},
		{name: "unknown gang width", edit: func(f *memberResourceFixture) { f.gangSizes = nil }, want: partial},
		{name: "mixed incarnations", edit: func(f *memberResourceFixture) { f.pods[1].Labels[query.LabelInstanceIncarnation] = "1" }, want: partial},
		{name: "mixed revisions", edit: func(f *memberResourceFixture) { f.pods[1].Labels[query.LabelRevisionHash] = "b" }, want: partial},
		{name: "mixed groups", edit: func(f *memberResourceFixture) {
			f.pods[1].Labels[query.LabelPodGroup] = "other-group"
			f.gangSizes["other-group"] = 2
		}, want: partial},
		{name: "old incarnation cannot supply admission", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].Incarnation = 1 }, want: partial},
		{name: "unrelated revision cannot supply admission", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].RunningRevision = "service-engine-b" }, want: partial},
		{name: "creating gang uses its target revision", edit: func(f *memberResourceFixture) {
			row := &f.ir.Status.InstanceStatuses[0]
			row.TargetRevision, row.RunningRevision = row.RunningRevision, ""
			row.Phase = v1beta1.OMENativeInstanceCreating
		}, want: pending},
		{name: "creating gang uses durable operation target", edit: func(f *memberResourceFixture) {
			row := &f.ir.Status.InstanceStatuses[0]
			row.Operation = &v1beta1.InstanceOperation{TargetRevision: row.RunningRevision}
			row.RunningRevision = ""
		}, want: pending},
		{name: "unresolved revision withholds admission", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].RunningRevision = "" }, want: partial},
		{name: "old gang survives desired singleton shape", edit: func(f *memberResourceFixture) {
			f.ir.Spec.Runners = []v1beta1.Runner{{Name: "default", Size: 1}}
			f.ir.Status.InstanceStatuses[0].TargetRevision = "service-engine-b"
		}, want: complete},
		{name: "old singleton survives desired gang shape", edit: func(f *memberResourceFixture) {
			*f = resourceFixture()
			f.ir.Spec.Runners = []v1beta1.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
			f.ir.Status.InstanceStatuses[0].TargetRevision = "service-engine-b"
		}, want: complete},
		{name: "ready old singleton survives rollout surge", edit: func(f *memberResourceFixture) {
			*f = resourceFixture()
			f.addSurge()
			pod := *f.pods[0].DeepCopy()
			pod.Name, pod.UID = "surge", "surge-uid"
			pod.Labels[query.LabelRevisionHash], pod.Labels[query.LabelPodOrdinal] = "b", "1"
			f.pods = append(f.pods, pod)
		}, want: complete},
		{name: "inactive singleton ordinal has no credit", edit: func(f *memberResourceFixture) {
			*f = resourceFixture()
			f.ir.Status.InstanceStatuses[0].ActiveOrdinal = 1
		}, want: partial},
		{name: "complete and partial gangs count separately", edit: func(f *memberResourceFixture) {
			row := f.ir.Status.InstanceStatuses[0]
			row.Index = 1
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, row)
			f.ir.Status.ReadyReplicas = 2
			pod := *f.pods[0].DeepCopy()
			pod.Name, pod.UID = "second-leader", "second-uid"
			pod.Labels[query.LabelInstanceIdx], pod.Labels[query.LabelPodGroup] = "1", "second-group"
			f.pods = append(f.pods, pod)
			f.gangSizes["second-group"] = 2
		}, want: result{Admitted: []bool{true, false}, Ready: 1}},
		{name: "unidentified component", edit: func(f *memberResourceFixture) { f.ir.UID = "" }, wantErr: true},
		{name: "negative readiness", edit: func(f *memberResourceFixture) { f.ir.Status.ReadyReplicas = -1 }, wantErr: true},
		{name: "foreign pod owner", edit: func(f *memberResourceFixture) { f.pods[1].OwnerReferences[0].UID = "foreign" }, wantErr: true},
		{name: "unidentified pod", edit: func(f *memberResourceFixture) { f.pods[1].UID = "" }, wantErr: true},
		{name: "invalid pod slot", edit: func(f *memberResourceFixture) { f.pods[1].Labels[query.LabelPodOrdinal] = "invalid" }, wantErr: true},
		{name: "negative row index", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].Index = -1 }, wantErr: true},
		{name: "duplicate row index", edit: func(f *memberResourceFixture) {
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, f.ir.Status.InstanceStatuses[0])
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := resourceFixture()
			f.addGang()
			if tt.edit != nil {
				tt.edit(&f)
			}
			before := f.ir.DeepCopy()
			status, err := verifiedMemberAdmission(f.ir, f.pods, f.gangSizes)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error (-want +got):\n%s; %v", diff, err)
			}
			if diff := cmp.Diff(before, f.ir); diff != "" {
				t.Fatalf("verification mutated the reported status:\n%s", diff)
			}
			if err != nil {
				return
			}
			got := result{Ready: status.ReadyReplicas}
			for _, row := range status.InstanceStatuses {
				got.Admitted = append(got.Admitted, row.Admitted)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("whole-replica credit (-want +got):\n%s", diff)
			}
		})
	}
}

// busyMemberReads matches the reads of a member whose service and components
// are rewritten between any two reads of them.
func busyMemberReads(obj client.Object) bool {
	switch obj.(type) {
	case *v1beta1.InferenceReplica, *metav1.PartialObjectMetadata, *v1beta1.InferenceService:
		return true
	}
	return false
}

func TestObserveHomeWholeGangAdmission(t *testing.T) {
	for _, tt := range []struct {
		name     string
		edit     func(*plannedObservationFixture)
		admitted int32
		unknown  bool
		errText  string
	}{
		{name: "complete gang", admitted: 1},
		{name: "missing worker", edit: func(f *plannedObservationFixture) { f.resources.pods = f.resources.pods[:1] }},
		{name: "gated worker", edit: func(f *plannedObservationFixture) {
			f.resources.pods[1].Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/admission"}}
		}},
		{name: "busy component is observed", edit: func(f *plannedObservationFixture) {
			f.intercept = busyReads(0, busyMemberReads)
		}, admitted: 1},
		{name: "worker created after the component snapshot completes the gang", edit: func(f *plannedObservationFixture) {
			worker := f.resources.pods[1].DeepCopy()
			f.resources.pods = f.resources.pods[:1]
			f.afterFirstComponentRead(func(ctx context.Context, c client.WithWatch) error { return c.Create(ctx, worker) })
		}, admitted: 1},
		{name: "worker gone before the pod list withholds credit", edit: func(f *plannedObservationFixture) {
			gone := client.ObjectKeyFromObject(&f.resources.pods[1])
			f.afterFirstComponentRead(func(ctx context.Context, c client.WithWatch) error {
				return c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: gone.Name, Namespace: gone.Namespace}})
			})
		}},
		{name: "foreign component", edit: func(f *plannedObservationFixture) { f.resources.ir.OwnerReferences[0].UID = "foreign" }, unknown: true, errText: "has unverified service ownership"},
		{name: "busy foreign component stays unknown", edit: func(f *plannedObservationFixture) {
			f.resources.ir.OwnerReferences[0].UID = "foreign"
			f.intercept = busyReads(0, busyMemberReads)
		}, unknown: true, errText: "has unverified service ownership"},
		{name: "pod owned through another kind", edit: func(f *plannedObservationFixture) { f.resources.pods[1].OwnerReferences[0].Kind = "Deployment" }, unknown: true, errText: "has unverified component ownership"},
		{name: "unverified gang", edit: func(f *plannedObservationFixture) { f.extraObjects = nil }, unknown: true},
		{name: "gang owned by another component", edit: func(f *plannedObservationFixture) {
			group := testMemberPodGroup(f.resources.ir)
			owner := group.GetOwnerReferences()
			owner[0].UID = "foreign"
			group.SetOwnerReferences(owner)
			f.extraObjects = []client.Object{group}
		}, unknown: true, errText: "has unverified component identity"},
		{name: "unreadable pod inventory", edit: func(f *plannedObservationFixture) {
			f.intercept.List = func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return fmt.Errorf("pod inventory unavailable")
			}
		}, unknown: true},
		{name: "partial pod inventory", edit: func(f *plannedObservationFixture) {
			f.intercept.List = func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := c.List(ctx, list, opts...); err != nil {
					return err
				}
				list.SetContinue("more")
				return nil
			}
		}, unknown: true, errText: "member pod inventory is incomplete"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := observationFixture(t)
			f.resources.addGang()
			f.resources.ir.Name = f.member.Name + "-engine"
			for i := range f.resources.pods {
				f.resources.pods[i].OwnerReferences[0].Name = f.resources.ir.Name
			}
			f.extraObjects = []client.Object{testMemberPodGroup(f.resources.ir)}
			if tt.edit != nil {
				tt.edit(&f)
			}
			objects := append(f.extraObjects, f.member, f.resources.ir)
			for i := range f.resources.pods {
				objects = append(objects, &f.resources.pods[i])
			}
			scheme := testScheme(t)
			worker := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(f.intercept).Build()
			clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, _ := newPlacer(scheme, clusters, f.source, plannedTestRegistration())
			got := r.observeHome(t.Context(), f.source, f.source.Status.Placement.Candidates[0], true)
			if diff := cmp.Diff(tt.unknown, got.state == homeUnknown); diff != "" {
				t.Fatalf("observation (-want +got):\n%s; %v", diff, got.err)
			}
			if tt.errText != "" && (got.err == nil || !strings.Contains(got.err.Error(), tt.errText)) {
				t.Fatalf("observation error = %v, want %q", got.err, tt.errText)
			}
			if diff := cmp.Diff(tt.admitted, got.candidate.AdmittedReplicas); diff != "" {
				t.Errorf("admission (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.admitted, got.candidate.ReadyReplicas); diff != "" {
				t.Errorf("readiness (-want +got):\n%s", diff)
			}
		})
	}
}

// TestComponentIRStatusesListsPodsAfterEveryComponentRead pins the read order
// admission is verified in: one read per component, then one Pod list, so the
// cohort is at least as current as every status that claims admission.
func TestComponentIRStatusesListsPodsAfterEveryComponentRead(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc", UID: "member-uid"}}
	declareComponent(isvc, v1beta1.EngineComponent)
	declareComponent(isvc, v1beta1.DecoderComponent)
	engine := placementIR(v1beta1.EngineComponent, v1beta1.OMENativeInstanceReady, true)
	decoder := placementIR(v1beta1.DecoderComponent, v1beta1.OMENativeInstanceReady, true)
	var reads []string
	record := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			reads = append(reads, fmt.Sprintf("%T %s", obj, key.Name))
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			reads = append(reads, fmt.Sprintf("%T", list))
			return c.List(ctx, list, opts...)
		},
	}
	worker := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(observedWorkerObjects(isvc, engine, decoder)...).WithInterceptorFuncs(record).Build()
	statuses, err := componentIRStatuses(t.Context(), (&Reconciler{}).instanceStatusReader(worker), isvc)
	if err != nil {
		t.Fatal(err)
	}
	if !AllComponentsAdmitted(isvc, statuses) {
		t.Fatal("fixture does not exercise admission verification")
	}
	want := []string{"*v1beta1.InferenceReplica svc-engine", "*v1beta1.InferenceReplica svc-decoder", "*v1.PodList"}
	if diff := cmp.Diff(want, reads); diff != "" {
		t.Errorf("member reads (-want +got):\n%s", diff)
	}
}

// TestStandingObservationOfBusyMembers runs the standing pass over members
// whose service and every component are rewritten between any two reads of
// them. Every home is present and admitted, and the source renders Ready.
func TestStandingObservationOfBusyMembers(t *testing.T) {
	f := observationFixture(t)
	container := corev1.Container{Name: "ome-container", Image: "img"}
	f.source.Spec.Decoder = &v1beta1.DecoderSpec{PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container}}}
	f.source.Spec.Router = &v1beta1.RouterSpec{PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{container}}}
	members := []string{"member-a", "member-b", "member-c"}
	f.source.Status.Placement.Candidates = nil
	for _, name := range members {
		f.source.Status.Placement.Candidates = append(f.source.Status.Placement.Candidates, v1beta1.CandidatePlacement{
			Cluster: name, Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: types.UID(name + "-uid"), CurrentReplicas: 1, DesiredReplicas: 1},
		})
	}
	scheme := testScheme(t)
	connections := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}
	controlPlane := []client.Object{f.source}
	var registrations []v1beta1.WorkloadCluster
	for _, name := range members {
		member := DeriveISVC(f.source, "", "")
		member.UID, member.Generation = types.UID(name+"-service"), 3
		member.Status.URL = &apis.URL{Scheme: "https", Host: name + ".example.com"}
		member.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
		objects := []client.Object{member}
		for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
			ir := f.resources.ir.DeepCopy()
			ir.Name, ir.UID, ir.Spec.Component = fmt.Sprintf("%s-%s", member.Name, component), types.UID(fmt.Sprintf("%s-%s", name, component)), component
			ir.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(member, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}
			pod := f.resources.pods[0].DeepCopy()
			pod.Name, pod.UID = fmt.Sprintf("%s-0", component), types.UID(fmt.Sprintf("%s-%s-pod", name, component))
			pod.OwnerReferences[0].Name, pod.OwnerReferences[0].UID = ir.Name, ir.UID
			objects = append(objects, ir, pod)
		}
		worker := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(busyReads(0, busyMemberReads)).Build()
		connections.m[name] = workloadcluster.NewNeverCachingClient(worker)
		registration := readyWC(name, nil)
		controlPlane = append(controlPlane, registration)
		registrations = append(registrations, *registration)
	}
	r, root := newPlacer(scheme, connections, controlPlane...)
	observations := r.observeStandingHomes(t.Context(), f.source, registrations)
	for _, name := range members {
		type home struct {
			Present, Serving bool
			Phase            v1beta1.CandidatePlacementPhase
			Admitted, Ready  int32
		}
		observed := observations.homes[name]
		got := home{observed.state == homePresent, observed.serving, observed.candidate.Phase, observed.candidate.AdmittedReplicas, observed.candidate.ReadyReplicas}
		if diff := cmp.Diff(home{Present: true, Serving: true, Phase: v1beta1.CandidatePhaseAdmitted, Admitted: 1, Ready: 1}, got); diff != "" {
			t.Errorf("%s (-want +got):\n%s; %v", name, diff, observed.err)
		}
	}
	source := &v1beta1.InferenceService{}
	if err := root.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	if _, err := r.writeObservedPlacement(t.Context(), source, observations); err != nil {
		t.Fatal(err)
	}
	if err := root.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	ready := source.Status.GetCondition(apis.ConditionReady)
	if ready == nil || ready.Status != corev1.ConditionTrue {
		t.Errorf("source readiness = %+v, want True", ready)
	}
}

func TestSingleAdmissionLossRetainsWinner(t *testing.T) {
	for _, tt := range []struct {
		name  string
		gated bool
	}{
		{name: "missing Pod"},
		{name: "gated Pod", gated: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := singleFixture(t, false)
			f.reconcile(t)
			seedSingleAdmission(t, f, "member-a", v1beta1.EngineComponent)
			accepted := f.reconcile(t).Status.Placement.Plan.DeepCopy()
			worker := f.workers["member-a"]
			member := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), member); err != nil {
				t.Fatal(err)
			}
			pods := &corev1.PodList{}
			if err := worker.List(t.Context(), pods, client.MatchingLabels{query.LabelInstanceIdx: "0"}); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(1, len(pods.Items)); diff != "" {
				t.Fatal(diff)
			}
			pod := &pods.Items[0]
			if tt.gated {
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/admission"}}
				if err := worker.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			} else if err := worker.Delete(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			got := f.reconcile(t)
			if diff := cmp.Diff(accepted, got.Status.Placement.Plan); diff != "" {
				t.Fatalf("admission gap changed placement authority:\n%s", diff)
			}
			if diff := cmp.Diff("member-a", got.Status.Placement.Cluster); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(v1beta1.PlacementPhaseAdmitting, got.Status.Placement.Phase); diff != "" {
				t.Fatal(diff)
			}
			live := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(member), live); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(member.UID, live.UID); diff != "" {
				t.Fatalf("admission gap recreated the member:\n%s", diff)
			}
			if err := f.workers["member-b"].Get(t.Context(), client.ObjectKeyFromObject(f.source), &v1beta1.InferenceService{}); !apierrors.IsNotFound(err) {
				t.Fatalf("admission gap reopened the losing home: %v", err)
			}
		})
	}
}
