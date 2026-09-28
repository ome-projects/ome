package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

type memberResourceFixture struct {
	ir        *v1beta1.InferenceReplica
	pods      []corev1.Pod
	gangSizes map[string]int32
}

func resourceFixture() memberResourceFixture {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "service-engine", UID: "engine-uid"},
		Spec:       v1beta1.InferenceReplicaSpec{Runners: []v1beta1.Runner{{Name: "default", Size: 1}}},
		Status: v1beta1.InferenceReplicaStatus{
			ReadyReplicas: 1,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
				Index: 0, Phase: v1beta1.OMENativeInstanceReady, Admitted: true,
				RunningRevision: "service-engine-a", PodCount: 1, ServingPodCount: 1,
			}},
		},
	}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0", UID: "pod-a", Labels: map[string]string{
			query.LabelInstanceIdx: "0", query.LabelRevisionHash: "a",
		}, OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica", Name: ir.Name, UID: ir.UID, Controller: ptr.To(true),
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
		}},
	}
	return memberResourceFixture{ir: ir, pods: []corev1.Pod{pod}}
}

func (f *memberResourceFixture) addSurge() {
	f.ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{
		Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepSurge, TargetRevision: "service-engine-b",
	}
}

func (f *memberResourceFixture) addGang() {
	f.gangSizes = map[string]int32{"engine-0-a": 2}
	f.ir.Spec.Runners = []v1beta1.Runner{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	f.ir.Status.InstanceStatuses[0].PodCount = 2
	f.ir.Status.InstanceStatuses[0].ServingPodCount = 2
	f.pods[0].Labels[query.LabelPodGroup] = "engine-0-a"
	f.pods[0].Labels[query.LabelRunner] = "leader"
	worker := *f.pods[0].DeepCopy()
	worker.UID, worker.Name = "pod-worker", "engine-worker-0"
	worker.Labels[query.LabelRunner] = "worker"
	f.pods = append(f.pods, worker)
}

func TestCountMemberResources(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*memberResourceFixture)
		want    memberResourceCount
		wantErr bool
	}{
		{name: "steady serving instance", want: memberResourceCount{Occupied: 1, Ready: 1}},
		{name: "admission observation required for readiness credit", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].Admitted = false }, want: memberResourceCount{Occupied: 1}},
		{name: "empty component", edit: func(f *memberResourceFixture) { f.pods = nil }},
		{name: "pending pod consumes capacity", edit: func(f *memberResourceFixture) { f.pods[0].Status.Phase = corev1.PodPending }, want: memberResourceCount{Occupied: 1}},
		{name: "terminating pod consumes capacity", edit: func(f *memberResourceFixture) { now := metav1.Now(); f.pods[0].DeletionTimestamp = &now }, want: memberResourceCount{Occupied: 1}},
		{name: "drained pod consumes capacity", edit: func(f *memberResourceFixture) { f.pods[0].Status.Conditions[1].Status = corev1.ConditionFalse }, want: memberResourceCount{Occupied: 1}},
		{name: "unready pod consumes capacity", edit: func(f *memberResourceFixture) { f.pods[0].Status.Conditions[0].Status = corev1.ConditionFalse }, want: memberResourceCount{Occupied: 1}},
		{name: "pod without conditions consumes capacity", edit: func(f *memberResourceFixture) { f.pods[0].Status.Conditions = nil }, want: memberResourceCount{Occupied: 1}},
		{name: "aggregate readiness caps credit", edit: func(f *memberResourceFixture) { f.ir.Status.ReadyReplicas = 0 }, want: memberResourceCount{Occupied: 1}},
		{name: "status incarnation mismatch cannot fund a move", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].Incarnation = 1 }, want: memberResourceCount{Occupied: 1}},
		{name: "status revision mismatch cannot fund a move", edit: func(f *memberResourceFixture) { f.ir.Status.InstanceStatuses[0].RunningRevision = "service-engine-b" }, want: memberResourceCount{Occupied: 1}},
		{name: "surge reserves a missing pod", edit: (*memberResourceFixture).addSurge, want: memberResourceCount{Occupied: 1, Reserved: 1}},
		{name: "surge drain retains its reservation", edit: func(f *memberResourceFixture) {
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.Step = workloadtypes.UpdateStepSurgeDrain
		}, want: memberResourceCount{Occupied: 1, Reserved: 1}},
		{name: "surge drain settle retains its reservation", edit: func(f *memberResourceFixture) {
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.Step = workloadtypes.UpdateStepSurgeDrainSettle
		}, want: memberResourceCount{Occupied: 1, Reserved: 1}},
		{name: "non surge update has no reservation", edit: func(f *memberResourceFixture) {
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.Step = "Recreate"
		}, want: memberResourceCount{Occupied: 1}},
		{name: "materialized surge is counted once", edit: func(f *memberResourceFixture) {
			f.addSurge()
			pod := *f.pods[0].DeepCopy()
			pod.UID, pod.Name = "surge-uid", "surge"
			pod.Labels[query.LabelPodOrdinal], pod.Labels[query.LabelRevisionHash] = "1", "b"
			f.pods = append(f.pods, pod)
		}, want: memberResourceCount{Occupied: 2}},
		{name: "same revision surge retains both physical pods", edit: func(f *memberResourceFixture) {
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.TargetRevision = "service-engine-a"
			pod := *f.pods[0].DeepCopy()
			pod.UID, pod.Name, pod.Labels[query.LabelPodOrdinal] = "surge-uid", "surge", "1"
			f.pods = append(f.pods, pod)
		}, want: memberResourceCount{Occupied: 2}},
		{name: "alternating surge reserves ordinal zero", edit: func(f *memberResourceFixture) {
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].ActiveOrdinal = 1
			f.pods[0].Labels[query.LabelPodOrdinal] = "1"
		}, want: memberResourceCount{Occupied: 1, Reserved: 1}},
		{name: "complete gang is one ready replica", edit: (*memberResourceFixture).addGang, want: memberResourceCount{Occupied: 1, Ready: 1}},
		{name: "partial gang cannot borrow aggregate readiness", edit: func(f *memberResourceFixture) {
			f.addGang()
			f.pods = f.pods[:1]
			f.ir.Status.InstanceStatuses[0].PodCount = 1
			f.ir.Status.InstanceStatuses[0].ServingPodCount = 1
		}, want: memberResourceCount{Occupied: 1}},
		{name: "unresolved gang width cannot grant readiness", edit: func(f *memberResourceFixture) { f.addGang(); f.gangSizes = nil }, want: memberResourceCount{Occupied: 1}},
		{name: "partial gang still occupies whole replica", edit: func(f *memberResourceFixture) { f.addGang(); f.pods = f.pods[:1] }, want: memberResourceCount{Occupied: 1}},
		{name: "terminating gang member prevents readiness credit", edit: func(f *memberResourceFixture) { f.addGang(); now := metav1.Now(); f.pods[1].DeletionTimestamp = &now }, want: memberResourceCount{Occupied: 1}},
		{name: "old gang remains whole after single pod spec change", edit: func(f *memberResourceFixture) {
			f.addGang()
			f.ir.Spec.Runners = []v1beta1.Runner{{Name: "default", Size: 1}}
		}, want: memberResourceCount{Occupied: 1, Ready: 1}},
		{name: "old single pods stay separate after gang spec change", edit: func(f *memberResourceFixture) {
			f.ir.Spec.Runners = []v1beta1.Runner{{Name: "worker", Size: 2}}
			pod := *f.pods[0].DeepCopy()
			pod.UID, pod.Name, pod.Labels[query.LabelPodOrdinal] = "surge-uid", "surge", "1"
			f.pods = append(f.pods, pod)
		}, want: memberResourceCount{Occupied: 2}},
		{name: "gang target reservation", edit: func(f *memberResourceFixture) {
			f.addGang()
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.SurgeIndex = ptr.To[int32](1)
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{Index: 1, Incarnation: 2})
		}, want: memberResourceCount{Occupied: 1, Reserved: 1}},
		{name: "partial target gang consumes reservation", edit: func(f *memberResourceFixture) {
			f.addGang()
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.SurgeIndex = ptr.To[int32](1)
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{Index: 1})
			pod := *f.pods[0].DeepCopy()
			pod.UID, pod.Name = "target-uid", "target-leader"
			pod.Labels[query.LabelInstanceIdx], pod.Labels[query.LabelRevisionHash], pod.Labels[query.LabelPodGroup] = "1", "b", "target-gang"
			f.pods = append(f.pods, pod)
		}, want: memberResourceCount{Occupied: 2}},
		{name: "queued migration has no reservation", edit: func(f *memberResourceFixture) {
			f.ir.Status.Migrations = []v1beta1.MigrationStatus{{Phase: v1beta1.MigrationPhaseAccepted}}
		}, want: memberResourceCount{Occupied: 1, Ready: 1}},
		{name: "terminal migration has no reservation", edit: func(f *memberResourceFixture) {
			f.ir.Status.Migrations = []v1beta1.MigrationStatus{{Phase: v1beta1.MigrationPhaseCompleted, SurgeInstance: ptr.To[int32](1)}}
		}, want: memberResourceCount{Occupied: 1, Ready: 1}},
		{name: "allocated migration reserves before target revision stamp", edit: func(f *memberResourceFixture) {
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{Index: 1})
			f.ir.Status.Migrations = []v1beta1.MigrationStatus{{Phase: v1beta1.MigrationPhaseSurgePending, SourceInstance: 0, SurgeInstance: ptr.To[int32](1)}}
		}, want: memberResourceCount{Occupied: 1, Reserved: 1, Ready: 1}},
		{name: "migration and update cannot double count a shared target", edit: func(f *memberResourceFixture) {
			f.addGang()
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.SurgeIndex = ptr.To[int32](1)
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{Index: 1, RunningRevision: "service-engine-b"})
			f.ir.Status.Migrations = []v1beta1.MigrationStatus{{Phase: v1beta1.MigrationPhaseSurgePending, SourceInstance: 0, SurgeInstance: ptr.To[int32](1)}}
		}, want: memberResourceCount{Occupied: 1, Reserved: 1}},
		{name: "missing component", edit: func(f *memberResourceFixture) { f.ir = nil }, wantErr: true},
		{name: "active migration cannot lose its reservation", edit: func(f *memberResourceFixture) {
			f.ir.Status.Migrations = []v1beta1.MigrationStatus{{Phase: v1beta1.MigrationPhaseSurgePending}}
		}, wantErr: true},
		{name: "unidentified component", edit: func(f *memberResourceFixture) { f.ir.UID = "" }, wantErr: true},
		{name: "missing runner shape", edit: func(f *memberResourceFixture) { f.ir.Spec.Runners = nil }, wantErr: true},
		{name: "unresolved runner size", edit: func(f *memberResourceFixture) { f.ir.Spec.Runners[0].Size = 0 }, wantErr: true},
		{name: "negative readiness", edit: func(f *memberResourceFixture) { f.ir.Status.ReadyReplicas = -1 }, wantErr: true},
		{name: "pod without UID", edit: func(f *memberResourceFixture) { f.pods[0].UID = "" }, wantErr: true},
		{name: "pod without owner", edit: func(f *memberResourceFixture) { f.pods[0].OwnerReferences = nil }, wantErr: true},
		{name: "replaced component owner", edit: func(f *memberResourceFixture) { f.pods[0].OwnerReferences[0].UID = "other-uid" }, wantErr: true},
		{name: "wrong component owner kind", edit: func(f *memberResourceFixture) { f.pods[0].OwnerReferences[0].Kind = "InferenceService" }, wantErr: true},
		{name: "duplicate instance rows", edit: func(f *memberResourceFixture) {
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, f.ir.Status.InstanceStatuses[0])
		}, wantErr: true},
		{name: "unresolved target revision", edit: func(f *memberResourceFixture) {
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.TargetRevision = ""
		}, wantErr: true},
		{name: "unresolved surge target", edit: func(f *memberResourceFixture) {
			f.addSurge()
			f.ir.Status.InstanceStatuses[0].Operation.SurgeIndex = ptr.To[int32](1)
		}, wantErr: true},
		{name: "unresolved migration target", edit: func(f *memberResourceFixture) {
			f.ir.Status.Migrations = []v1beta1.MigrationStatus{{Phase: v1beta1.MigrationPhaseSurgePending, SurgeInstance: ptr.To[int32](1)}}
		}, wantErr: true},
		{name: "invalid active ordinal", edit: func(f *memberResourceFixture) { f.addSurge(); f.ir.Status.InstanceStatuses[0].ActiveOrdinal = 2 }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := resourceFixture()
			if tt.edit != nil {
				tt.edit(&fixture)
			}
			got, err := countMemberResources(fixture.ir, fixture.pods, fixture.gangSizes)
			if (err != nil) != tt.wantErr {
				t.Fatalf("count error = %v, want error %t", err, tt.wantErr)
			}
			if !tt.wantErr {
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("resources (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestMemberSlotRejectsUnresolvedLabels(t *testing.T) {
	for _, tt := range []struct{ name, key, value string }{
		{name: "missing index", key: query.LabelInstanceIdx},
		{name: "negative index", key: query.LabelInstanceIdx, value: "-1"},
		{name: "overflow index", key: query.LabelInstanceIdx, value: "2147483648"},
		{name: "invalid incarnation", key: query.LabelInstanceIncarnation, value: "bad"},
		{name: "negative incarnation", key: query.LabelInstanceIncarnation, value: "-1"},
		{name: "negative ordinal", key: query.LabelPodOrdinal, value: "-1"},
		{name: "invalid ordinal", key: query.LabelPodOrdinal, value: "bad"},
		{name: "missing revision", key: query.LabelRevisionHash},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := resourceFixture()
			fixture.pods[0].Labels[tt.key] = tt.value
			if _, err := countMemberResources(fixture.ir, fixture.pods, fixture.gangSizes); err == nil {
				t.Fatal("unresolved pod identity authorized resource accounting")
			}
		})
	}
}

func testMemberPodGroup(ir *v1beta1.InferenceReplica) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroup",
		"metadata": map[string]interface{}{"name": "engine-0-a", "namespace": ir.Namespace, "uid": "group-a", "ownerReferences": []interface{}{map[string]interface{}{
			"apiVersion": v1beta1.SchemeGroupVersion.String(), "kind": "InferenceReplica", "name": ir.Name, "uid": string(ir.UID), "controller": true,
		}}},
		"spec": map[string]interface{}{"minMember": int64(2)},
	}}
}

func TestMemberGangSizes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*unstructured.Unstructured)
		missing bool
		wantErr bool
	}{
		{name: "owned live gang"},
		{name: "missing gang is unknown", missing: true, wantErr: true},
		{name: "unidentified gang is unknown", edit: func(g *unstructured.Unstructured) { g.SetUID("") }, wantErr: true},
		{name: "foreign gang is unknown", edit: func(g *unstructured.Unstructured) {
			owners := g.GetOwnerReferences()
			owners[0].UID = "other-component"
			g.SetOwnerReferences(owners)
		}, wantErr: true},
		{name: "ownerless gang is unknown", edit: func(g *unstructured.Unstructured) { g.SetOwnerReferences(nil) }, wantErr: true},
		{name: "missing gang size is unknown", edit: func(g *unstructured.Unstructured) { unstructured.RemoveNestedField(g.Object, "spec", "minMember") }, wantErr: true},
		{name: "invalid gang size is unknown", edit: func(g *unstructured.Unstructured) { g.Object["spec"] = map[string]interface{}{"minMember": "two"} }, wantErr: true},
		{name: "non gang size is unknown", edit: func(g *unstructured.Unstructured) { g.Object["spec"] = map[string]interface{}{"minMember": int64(1)} }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := resourceFixture()
			fixture.addGang()
			group := testMemberPodGroup(fixture.ir)
			if tt.edit != nil {
				tt.edit(group)
			}
			objects := []client.Object{}
			if !tt.missing {
				objects = append(objects, group)
			}
			cl := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
			got, err := memberGangSizes(t.Context(), cl, fixture.ir, fixture.pods)
			if (err != nil) != tt.wantErr {
				t.Fatalf("gang read error = %v, want error %t", err, tt.wantErr)
			}
			if !tt.wantErr {
				if diff := cmp.Diff(fixture.gangSizes, got); diff != "" {
					t.Errorf("gang widths (-want +got):\n%s", diff)
				}
			}
		})
	}
}
