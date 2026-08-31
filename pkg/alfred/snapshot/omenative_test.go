package snapshot

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

func TestBuildOMENativeDenseAndColumnarParity(t *testing.T) {
	dense := newOMENativeFixture()
	dense.ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{ID: "op-3"}

	columnar := newOMENativeFixture()
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	admitted := "3"
	columnar.ir.Status.InstanceStatuses = nil
	columnar.ir.Status.InstanceStatusEncoding = &encoding
	columnar.ir.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{
		Members: "3",
		Phases: []v1beta1.InstanceStatusPhaseGroup{{
			Value: v1beta1.OMENativeInstanceReady, Indexes: "3",
		}},
		RunningRevisions:   []v1beta1.InstanceStatusStringGroup{{Value: "rev-a", Indexes: "3"}},
		Incarnations:       []v1beta1.InstanceStatusInt64Group{{Value: 1, Indexes: "3"}},
		PodCounts:          []v1beta1.InstanceStatusCountGroup{{Value: 1, Indexes: "3"}},
		ServingPodCounts:   []v1beta1.InstanceStatusCountGroup{{Value: 1, Indexes: "3"}},
		AvailablePodCounts: []v1beta1.InstanceStatusCountGroup{{Value: 1, Indexes: "3"}},
		Admitted:           &admitted,
		Entries: []v1beta1.InstanceStatusColumnEntry{{
			Index: 3, Operation: &v1beta1.InstanceOperation{ID: "op-3"},
		}},
	}

	denseComponent := buildOMENativeFixture(t, dense)
	columnarComponent := buildOMENativeFixture(t, columnar)
	if !denseComponent.ObservationValid || !columnarComponent.ObservationValid {
		t.Fatalf("observations invalid: dense=%q columnar=%q", denseComponent.ObservationReason, columnarComponent.ObservationReason)
	}
	if !denseComponent.StatusFresh || !columnarComponent.StatusFresh {
		t.Fatal("fresh IR statuses must be marked fresh")
	}
	if denseComponent.IR == nil || columnarComponent.IR == nil {
		t.Fatal("accepted components must retain the source IR")
	}
	if !reflect.DeepEqual(denseComponent.Instances, columnarComponent.Instances) {
		t.Fatalf("DenseV1 and ColumnarV2 differ:\ndense=%#v\ncolumnar=%#v", denseComponent.Instances, columnarComponent.Instances)
	}
	instance := denseComponent.Instances[0]
	if instance.Index != 3 || instance.Incarnation != 1 || instance.Phase != v1beta1.OMENativeInstanceReady ||
		instance.RunningRevision != "rev-a" || instance.DesiredPods != 1 || instance.ObservedPods != 1 ||
		instance.ServingPods != 1 || instance.AvailablePods != 1 || instance.ReadyPods != 1 ||
		instance.ActiveOrdinal != 0 || len(instance.Pods) != 1 || instance.NodesSet["node-a"] != 1 {
		t.Fatalf("normalized instance = %+v", instance)
	}
	instance.Operation.ID = "mutated"
	if dense.ir.Status.InstanceStatuses[0].Operation.ID != "op-3" {
		t.Fatal("snapshot operation aliases the source IR status")
	}
}

func TestBuildOMENativePreservesSparseRowOrderAndSinglePodActiveOrdinal(t *testing.T) {
	fixture := newOMENativeFixture()
	fixture.ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		readyOMERow(7, 1, 1),
		readyOMERow(2, 1, 1),
	}
	fixture.pods = []*corev1.Pod{
		omeNativePod(fixture, 7, 1, "default", 0, "node-a"),
		omeNativePod(fixture, 2, 1, "default", 1, "node-b"),
	}
	fixture.ir.Status.InstanceStatuses[1].ActiveOrdinal = 1

	component := buildOMENativeFixture(t, fixture)
	if !component.ObservationValid {
		t.Fatalf("ObservationReason = %q", component.ObservationReason)
	}
	if len(component.Instances) != 2 || component.Instances[0].Index != 7 || component.Instances[1].Index != 2 {
		t.Fatalf("row order/indexes = %+v", component.Instances)
	}
	if component.Instances[1].Pods[0].PodOrdinal != 1 || component.Instances[1].NodesSet["node-b"] != 1 {
		t.Fatalf("active ordinal membership = %+v", component.Instances[1])
	}
}

func TestBuildOMENativeExactGangMembershipAndLivePlacement(t *testing.T) {
	fixture := newOMENativeFixture()
	fixture.ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1},
		{Name: v1beta1.RunnerNameWorker, Size: 2},
	}
	fixture.ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{readyOMERow(3, 1, 3)}
	fixture.pods = []*corev1.Pod{
		omeNativePod(fixture, 3, 1, "leader", 0, "node-a"),
		omeNativePod(fixture, 3, 1, "worker", 0, "node-a"),
		omeNativePod(fixture, 3, 1, "worker", 1, "node-b"),
	}

	component := buildOMENativeFixture(t, fixture)
	if !component.ObservationValid {
		t.Fatalf("ObservationReason = %q", component.ObservationReason)
	}
	instance := component.Instances[0]
	if len(instance.Pods) != 3 || instance.NodesSet["node-a"] != 2 || instance.NodesSet["node-b"] != 1 || instance.ReadyPods != 3 {
		t.Fatalf("gang membership = %+v", instance)
	}
}

func TestBuildOMENativeRejectsInvalidIRAndPodEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omeNativeFixture)
	}{
		{name: "stale generation", mutate: func(f *omeNativeFixture) { f.ir.Status.ObservedGeneration-- }},
		{name: "missing IR", mutate: func(f *omeNativeFixture) { f.ir = nil }},
		{name: "duplicate IR", mutate: func(f *omeNativeFixture) {
			duplicate := f.ir.DeepCopy()
			duplicate.Name += "-duplicate"
			duplicate.UID += "-duplicate"
			duplicate.OwnerReferences[0].UID = "wrong-owner-on-duplicate"
			f.extra = append(f.extra, duplicate)
		}},
		{name: "wrong IR owner name", mutate: func(f *omeNativeFixture) { f.ir.OwnerReferences[0].Name = "other" }},
		{name: "wrong IR owner UID", mutate: func(f *omeNativeFixture) { f.ir.OwnerReferences[0].UID = "other-uid" }},
		{name: "wrong IR owner API group", mutate: func(f *omeNativeFixture) { f.ir.OwnerReferences[0].APIVersion = "apps/v1" }},
		{name: "wrong IR owner API version in same group", mutate: func(f *omeNativeFixture) {
			f.ir.OwnerReferences[0].APIVersion = v1beta1.SchemeGroupVersion.Group + "/v1alpha1"
		}},
		{name: "invalid encoding", mutate: func(f *omeNativeFixture) {
			encoding := v1beta1.InstanceStatusEncoding("FutureV3")
			f.ir.Status.InstanceStatusEncoding = &encoding
		}},
		{name: "missing identity label", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelPodOrdinal) }},
		{name: "missing instance index", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelInstanceIdx) }},
		{name: "missing incarnation", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelInstanceIncarnation) }},
		{name: "missing runner", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelRunner) }},
		{name: "malformed identity label", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIdx] = "secret-malformed" }},
		{name: "negative instance index", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIdx] = "-1" }},
		{name: "negative incarnation", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIncarnation] = "-1" }},
		{name: "wrong managed by", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelManagedBy] = "Deployment" }},
		{name: "wrong Pod controller UID", mutate: func(f *omeNativeFixture) { f.pods[0].OwnerReferences[0].UID = "other-ir" }},
		{name: "stale incarnation", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIncarnation] = "2" }},
		{name: "missing Pod", mutate: func(f *omeNativeFixture) { f.pods = nil }},
		{name: "extra Pod", mutate: func(f *omeNativeFixture) { f.pods = append(f.pods, omeNativePod(f, 3, 1, "default", 1, "node-b")) }},
		{name: "duplicate Pod identity", mutate: func(f *omeNativeFixture) {
			duplicate := f.pods[0].DeepCopy()
			duplicate.Name += "-duplicate"
			f.pods = append(f.pods, duplicate)
		}},
		{name: "statusless Pod index", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIdx] = "9" }},
		{name: "terminating current Pod", mutate: func(f *omeNativeFixture) {
			now := metav1.Now()
			f.pods[0].DeletionTimestamp = &now
			f.pods[0].Finalizers = []string{"test/finalizer"}
		}},
		{name: "unknown runner", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelRunner] = "sidecar" }},
		{name: "negative pod ordinal", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelPodOrdinal] = "-1" }},
		{name: "unsupported runner layout", mutate: func(f *omeNativeFixture) {
			f.ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 2}}
		}},
		{name: "duplicate runner layout", mutate: func(f *omeNativeFixture) {
			f.ir.Spec.Runners = append(f.ir.Spec.Runners, v1beta1.Runner{Name: v1beta1.RunnerNameDefault, Size: 1})
		}},
		{name: "zero runner size", mutate: func(f *omeNativeFixture) { f.ir.Spec.Runners[0].Size = 0 }},
		{name: "negative runner size", mutate: func(f *omeNativeFixture) { f.ir.Spec.Runners[0].Size = -1 }},
		{name: "pod count mismatch", mutate: func(f *omeNativeFixture) { f.ir.Status.InstanceStatuses[0].PodCount = 2 }},
		{name: "serving count mismatch", mutate: func(f *omeNativeFixture) { f.ir.Status.InstanceStatuses[0].ServingPodCount = 0 }},
		{name: "available count mismatch", mutate: func(f *omeNativeFixture) { f.ir.Status.InstanceStatuses[0].AvailablePodCount = 0 }},
		{name: "live readiness mismatch", mutate: func(f *omeNativeFixture) { f.pods[0].Status.Conditions = nil }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			test.mutate(fixture)
			component := buildOMENativeFixture(t, fixture)
			assertInvalidOMENativeObservation(t, component)
		})
	}
}

func TestBuildOMENativeRejectsOwnedPodsWithInvalidComponentEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{
			name: "missing component label",
			mutate: func(pod *corev1.Pod) {
				delete(pod.Labels, constants.OMEComponentLabel)
			},
		},
		{
			name: "wrong component label",
			mutate: func(pod *corev1.Pod) {
				pod.Labels[constants.OMEComponentLabel] = string(v1beta1.DecoderComponent)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseline := buildOMENativeFixture(t, newOMENativeFixture())
			if !baseline.ObservationValid || len(baseline.Instances) != 1 ||
				baseline.Instances[0].Phase != v1beta1.OMENativeInstanceReady {
				t.Fatalf("baseline is not a steady OMENative observation: %+v", baseline)
			}

			fixture := newOMENativeFixture()
			adversarial := fixture.pods[0].DeepCopy()
			adversarial.Name += "-adversarial"
			test.mutate(adversarial)
			fixture.pods = append(fixture.pods, adversarial)

			component := buildOMENativeFixture(t, fixture)
			assertInvalidOMENativeObservation(t, component)
		})
	}
}

func TestBuildOMENativeRejectsAmbiguousOwnerUIDAcrossComponents(t *testing.T) {
	baselineFixture := newOMENativeFixture()
	addOMENativeDecoder(baselineFixture)
	baseline := buildOMENativeFixtureWorkload(t, baselineFixture)
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		if !baseline.Components[component].ObservationValid {
			t.Fatalf("baseline %s observation is invalid: %+v", component, baseline.Components[component])
		}
	}

	fixture := newOMENativeFixture()
	decoderIR, decoderPod := addOMENativeDecoder(fixture)
	decoderIR.UID = fixture.ir.UID
	decoderPod.OwnerReferences[0].UID = fixture.ir.UID

	workload := buildOMENativeFixtureWorkload(t, fixture)
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		t.Run(string(component), func(t *testing.T) {
			assertInvalidOMENativeObservation(t, workload.Components[component])
		})
	}
}

func TestBuildOMENativeRejectsUnresolvableOwnerAcrossComponents(t *testing.T) {
	fixture := newOMENativeFixture()
	addOMENativeDecoder(fixture)
	adversarial := fixture.pods[0].DeepCopy()
	adversarial.Name += "-unresolvable"
	adversarial.OwnerReferences[0].UID = "unknown-ir-uid"
	fixture.pods = append(fixture.pods, adversarial)

	workload := buildOMENativeFixtureWorkload(t, fixture)
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		t.Run(string(component), func(t *testing.T) {
			assertInvalidOMENativeObservation(t, workload.Components[component])
		})
	}
}

func TestBuildOMENativeIgnoresUnrelatedRawComponentPod(t *testing.T) {
	fixture := newOMENativeFixture()
	addOMENativeDecoder(fixture)
	fixture.pods = append(fixture.pods,
		omePod("prod", "svc-router", "node-a", "svc", "router", 0, true))

	workload := buildOMENativeFixtureWorkload(t, fixture)
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		if !workload.Components[component].ObservationValid {
			t.Fatalf("Raw router Pod poisoned %s observation: %+v", component, workload.Components[component])
		}
	}
}

func TestBuildOMENativeIgnoresCompatibilityObservationFields(t *testing.T) {
	fixture := newOMENativeFixture()
	row := &fixture.ir.Status.InstanceStatuses[0]
	row.ReadyPodCount = -100
	row.ScheduledPodCount = 1000
	row.NodesOccupied = []string{"fictional-secret-node"}

	component := buildOMENativeFixture(t, fixture)
	if !component.ObservationValid {
		t.Fatalf("compatibility fields affected observation: %q", component.ObservationReason)
	}
	instance := component.Instances[0]
	if instance.ReadyPods != 1 || !reflect.DeepEqual(instance.NodesSet, map[string]int{"node-a": 1}) {
		t.Fatalf("live readiness/placement not derived from Pods: %+v", instance)
	}
}

func TestBuildOMENativePreservesCoherentNonSteadyObservation(t *testing.T) {
	fixture := newOMENativeFixture()
	row := &fixture.ir.Status.InstanceStatuses[0]
	row.Phase = v1beta1.OMENativeInstanceMigrating
	row.Operation = &v1beta1.InstanceOperation{ID: "migration-3"}

	component := buildOMENativeFixture(t, fixture)
	if !component.ObservationValid || component.Instances[0].Phase != v1beta1.OMENativeInstanceMigrating {
		t.Fatalf("coherent non-steady observation = %+v", component)
	}
}

func TestBuildOMENativeKeepsStatusAndLivePodCountsSeparate(t *testing.T) {
	fixture := newOMENativeFixture()
	row := &fixture.ir.Status.InstanceStatuses[0]
	row.Phase = v1beta1.OMENativeInstanceMigrating
	row.Operation = &v1beta1.InstanceOperation{ID: "migration-3"}
	row.PodCount = 7
	fixture.pods = append(fixture.pods, omeNativePod(fixture, 3, 1, "default", 1, "node-b"))

	component := buildOMENativeFixture(t, fixture)
	instance := component.Instances[0]
	if !component.ObservationValid || instance.StatusPods != 7 || instance.ObservedPods != 2 {
		t.Fatalf("status/live pod counts alias: component=%+v instance=%+v", component, instance)
	}
}

func TestBuildOMENativeAllowsTrustworthyTransitionalMembership(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omeNativeFixture)
		want   int
	}{
		{
			name: "missing expected pod",
			mutate: func(f *omeNativeFixture) {
				f.pods = nil
			},
			want: 0,
		},
		{
			name: "both single-pod ordinals",
			mutate: func(f *omeNativeFixture) {
				f.pods = append(f.pods, omeNativePod(f, 3, 1, "default", 1, "node-b"))
			},
			want: 2,
		},
		{
			name: "terminating stale incarnation",
			mutate: func(f *omeNativeFixture) {
				f.pods[0].Labels[query.LabelInstanceIncarnation] = "2"
				now := metav1.Now()
				f.pods[0].DeletionTimestamp = &now
				f.pods[0].Finalizers = []string{"test/finalizer"}
			},
			want: 1,
		},
		{
			name: "current and terminating stale incarnation share member identity",
			mutate: func(f *omeNativeFixture) {
				stale := omeNativePod(f, 3, 2, "default", 0, "node-b")
				stale.Name += "-stale"
				now := metav1.Now()
				stale.DeletionTimestamp = &now
				stale.Finalizers = []string{"test/finalizer"}
				f.pods = append(f.pods, stale)
			},
			want: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			row := &fixture.ir.Status.InstanceStatuses[0]
			row.Phase = v1beta1.OMENativeInstanceMigrating
			row.Operation = &v1beta1.InstanceOperation{ID: "migration-3"}
			test.mutate(fixture)

			component := buildOMENativeFixture(t, fixture)
			if !component.ObservationValid || len(component.Instances) != 1 || len(component.Instances[0].Pods) != test.want {
				t.Fatalf("transitional observation = %+v", component)
			}
		})
	}
}

func TestBuildInferenceReplicaListErrorDegradesOnlyOMENative(t *testing.T) {
	fixture := newOMENativeFixture()
	rawMode := constants.RawDeployment
	rawISVC := fixture.isvc.DeepCopy()
	rawISVC.Name = "raw"
	rawISVC.UID = "raw-uid"
	rawISVC.Spec.DeploymentMode = &rawMode
	rawPod := omePod("prod", "raw-engine", "node-a", "raw", "engine", 0, true)

	scheme := testScheme(t)
	objects := fixture.runtimeObjects()
	objects = append(objects, rawISVC, rawPod)
	base := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	r := &inferenceReplicaListErrorReader{Reader: base}
	snap, err := Build(context.Background(), r, Options{Now: func() time.Time { return buildNow }})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	raw := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "raw"}].Components[v1beta1.EngineComponent]
	if len(raw.Instances) != 1 || raw.Instances[0].Pods[0].Name != "raw-engine" {
		t.Fatalf("Raw component lost on IR list error: %+v", raw)
	}
	ome := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: fixture.isvc.Name}].Components[v1beta1.EngineComponent]
	assertInvalidOMENativeObservation(t, ome)
}

type omeNativeFixture struct {
	isvc  *v1beta1.InferenceService
	ir    *v1beta1.InferenceReplica
	pods  []*corev1.Pod
	extra []runtime.Object
}

func newOMENativeFixture() *omeNativeFixture {
	mode := constants.OMENative
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc", UID: "isvc-uid", Generation: 5},
		Spec:       v1beta1.InferenceServiceSpec{DeploymentMode: &mode, Engine: &v1beta1.EngineSpec{}},
	}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "svc-engine", UID: "ir-uid", Generation: 3,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: "svc",
				UID: "isvc-uid", Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: "svc"},
			Component: v1beta1.EngineComponent,
			Runners:   []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}},
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 3,
			InstanceStatuses:   []v1beta1.OMENativeInstanceStatus{readyOMERow(3, 1, 1)},
		},
	}
	fixture := &omeNativeFixture{isvc: isvc, ir: ir}
	fixture.pods = []*corev1.Pod{omeNativePod(fixture, 3, 1, "default", 0, "node-a")}
	return fixture
}

func readyOMERow(index int32, incarnation int64, pods int32) v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index: index, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady,
		RunningRevision: "rev-a", PodCount: pods, ServingPodCount: pods,
		AvailablePodCount: pods, Admitted: true,
	}
}

func omeNativePod(f *omeNativeFixture, index int32, incarnation int64, runner string, ordinal int32, node string) *corev1.Pod {
	pod := omePod("prod", "svc-engine-pod", node, "svc", "engine", 1, true)
	pod.Name += "-" + runner + "-" + string(rune('0'+ordinal)) + "-" + string(rune('0'+index))
	pod.Labels[query.LabelManagedBy] = query.ManagedByOMENative
	pod.Labels[query.LabelInstanceIdx] = strconv.FormatInt(int64(index), 10)
	pod.Labels[query.LabelInstanceIncarnation] = strconv.FormatInt(incarnation, 10)
	pod.Labels[query.LabelRunner] = runner
	pod.Labels[query.LabelPodOrdinal] = strconv.FormatInt(int64(ordinal), 10)
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica", Name: f.ir.Name,
		UID: f.ir.UID, Controller: ptr.To(true),
	}}
	return pod
}

func addOMENativeDecoder(f *omeNativeFixture) (*v1beta1.InferenceReplica, *corev1.Pod) {
	f.isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	decoderIR := f.ir.DeepCopy()
	decoderIR.Name = "svc-decoder"
	decoderIR.UID = "decoder-ir-uid"
	decoderIR.Spec.Component = v1beta1.DecoderComponent
	decoderIR.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{readyOMERow(4, 1, 1)}
	f.extra = append(f.extra, decoderIR)

	decoderPod := omeNativePod(f, 4, 1, "default", 0, "node-b")
	decoderPod.Name = "svc-decoder-pod"
	decoderPod.Labels[constants.OMEComponentLabel] = string(v1beta1.DecoderComponent)
	decoderPod.OwnerReferences[0].Name = decoderIR.Name
	decoderPod.OwnerReferences[0].UID = decoderIR.UID
	f.pods = append(f.pods, decoderPod)
	return decoderIR, decoderPod
}

func (f *omeNativeFixture) runtimeObjects() []runtime.Object {
	objects := []runtime.Object{f.isvc}
	if f.ir != nil {
		objects = append(objects, f.ir)
	}
	for _, pod := range f.pods {
		objects = append(objects, pod)
	}
	return append(objects, f.extra...)
}

func buildOMENativeFixture(t *testing.T, fixture *omeNativeFixture) *Component {
	t.Helper()
	workload := buildOMENativeFixtureWorkload(t, fixture)
	return workload.Components[v1beta1.EngineComponent]
}

func buildOMENativeFixtureWorkload(t *testing.T, fixture *omeNativeFixture) *Workload {
	t.Helper()
	reader := fake.NewClientBuilder().WithScheme(testScheme(t)).WithRuntimeObjects(fixture.runtimeObjects()...).Build()
	snap, err := Build(context.Background(), reader, Options{Now: func() time.Time { return buildNow }})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	workload := snap.Workloads[types.NamespacedName{Namespace: fixture.isvc.Namespace, Name: fixture.isvc.Name}]
	if workload == nil || workload.Components[v1beta1.EngineComponent] == nil {
		t.Fatalf("engine component missing from snapshot: %+v", workload)
	}
	return workload
}

func assertInvalidOMENativeObservation(t *testing.T, component *Component) {
	t.Helper()
	if component.ObservationValid {
		t.Fatalf("ObservationValid = true, component = %+v", component)
	}
	if component.ObservationReason == "" || len(component.ObservationReason) > 128 {
		t.Fatalf("ObservationReason is not bounded: %q", component.ObservationReason)
	}
	for _, payload := range []string{"secret-malformed", "fictional-secret-node", "other-uid"} {
		if strings.Contains(component.ObservationReason, payload) {
			t.Fatalf("ObservationReason exposes source payload %q: %q", payload, component.ObservationReason)
		}
	}
}

type inferenceReplicaListErrorReader struct{ client.Reader }

func (r *inferenceReplicaListErrorReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1beta1.InferenceReplicaList); ok {
		return errors.New("synthetic IR list failure with secret payload")
	}
	return r.Reader.List(ctx, list, opts...)
}
