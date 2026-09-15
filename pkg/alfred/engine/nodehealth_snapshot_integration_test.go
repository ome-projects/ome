package engine

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/nodehealth"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestOwnerResolvedMalformedOMEPodPreventsFalseNodeDrained(t *testing.T) {
	const nodeName = "bad"
	workloadKey := types.NamespacedName{Namespace: "prod", Name: "svc"}
	mode := constants.OMENative
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName, UID: "node-uid"},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")},
			Conditions: []corev1.NodeCondition{{
				Type:               "GpuUnhealthy",
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(testNow.Add(-time.Minute)),
			}},
		},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: workloadKey.Namespace, Name: workloadKey.Name, UID: "isvc-uid"},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &v1beta1.EngineSpec{},
		},
	}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  workloadKey.Namespace,
			Name:       "svc-engine",
			UID:        "ir-uid",
			Generation: 1,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       workloadKey.Name,
				UID:        isvc.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: workloadKey.Name},
			Component: v1beta1.EngineComponent,
			Runners:   []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}},
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 1,
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
				Index: 3, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
				RunningRevision: "rev-a", PodCount: 1, ServingPodCount: 1,
				AvailablePodCount: 1, Admitted: true,
			}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workloadKey.Namespace,
			Name:      "svc-engine-pod",
			// The ISVC label is deliberately absent. Controller ownership proves
			// the canonical occupancy target, but the raw join must stay invalid.
			Labels: map[string]string{
				constants.OMEComponentLabel:   string(v1beta1.EngineComponent),
				"ome.io/managed-by":           "OMENative",
				"ome.io/instance-index":       "3",
				"ome.io/instance-incarnation": "1",
				"ome.io/runner":               string(v1beta1.RunnerNameDefault),
				"ome.io/pod-ordinal":          "0",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       ir.Name,
				UID:        ir.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name: "runner",
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				}},
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, isvc, ir, pod).Build()
	snap, err := snapshot.Build(context.Background(), reader, snapshot.Options{Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	observedNode := snap.Nodes[nodeName]
	if len(observedNode.OMEPods) != 1 || len(observedNode.OtherOccupants) != 0 ||
		observedNode.OMEPods[0].ISVC != workloadKey {
		t.Fatalf("canonical node occupancy = OME %+v, other %+v", observedNode.OMEPods, observedNode.OtherOccupants)
	}
	component := snap.Workloads[workloadKey].Components[v1beta1.EngineComponent]
	if component.ObservationValid {
		t.Fatalf("missing raw ISVC label repaired the checked OMENative join: %+v", component)
	}

	candidates := (&nodehealth.Policy{}).Evaluate(snap, config.Default())
	if len(candidates) != 2 {
		t.Fatalf("node-health candidates = %+v, want marker plus component-wide advisory", candidates)
	}
	marker := candidates[0].Remediation
	if marker == nil || len(marker.Workloads) != 1 || marker.Workloads[0] != workloadKey.String() {
		t.Fatalf("remediation marker = %+v, want canonical workload blocker", marker)
	}
	finding := candidates[1]
	if finding.Workload != workloadKey || finding.Component != v1beta1.EngineComponent ||
		finding.Instance != policy.ComponentWideInstance ||
		finding.AdvisoryReason != policy.AdvisoryOMENativeObservationInvalid {
		t.Fatalf("fallback finding = %+v", finding)
	}

	reporter, _, _, reportClient := newTestReporter(t, recommendationsCM(nil))
	recorder := &capturingRecorder{}
	reporter.Recorder = recorder
	cfg := config.Default()
	reporter.ReportCycle(context.Background(), candidates, nil, cfg, testNow)
	if got := recorder.count(eventNodeRepairNeeded); got != 1 {
		t.Fatalf("opening observation emitted %d repair-needed event(s), want 1", got)
	}
	reporter.ReportCycle(context.Background(), candidates, nil, cfg, testNow.Add(time.Minute))
	if got := recorder.count(eventNodeDrainedForRepair); got != 0 {
		t.Fatalf("live owner-resolved OME Pod emitted %d drained-for-repair event(s)", got)
	}
	record, ok := nodeRecord(t, reportClient, nodeName)
	if !ok || record.DrainedAt != nil || len(record.Workloads) != 1 || record.Workloads[0] != workloadKey.String() {
		t.Fatalf("node remediation record = %+v, present=%t", record, ok)
	}

	t.Run("absent IR keeps unnamed OME occupancy", func(t *testing.T) {
		orphanPod := pod.DeepCopy()
		delete(orphanPod.Labels, "ome.io/managed-by")
		orphanReader := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(node.DeepCopy(), isvc.DeepCopy(), orphanPod).Build()
		orphanSnap, err := snapshot.Build(context.Background(), orphanReader,
			snapshot.Options{Now: func() time.Time { return testNow }})
		if err != nil {
			t.Fatalf("Build() error = %v", err)
		}
		orphanNode := orphanSnap.Nodes[nodeName]
		if len(orphanNode.OMEPods) != 1 || len(orphanNode.OtherOccupants) != 0 ||
			orphanNode.OMEPods[0].ISVC.Name != "" || !orphanNode.OMEPods[0].ControllerOwnerValid {
			t.Fatalf("orphan IR node occupancy = OME %+v, other %+v", orphanNode.OMEPods, orphanNode.OtherOccupants)
		}

		orphanCandidates := (&nodehealth.Policy{}).Evaluate(orphanSnap, config.Default())
		if len(orphanCandidates) != 1 || orphanCandidates[0].Remediation == nil {
			t.Fatalf("orphan candidates = %+v, want one remediation marker", orphanCandidates)
		}
		orphanMarker := orphanCandidates[0].Remediation
		if len(orphanMarker.Workloads) != 0 || !orphanMarker.OMEGPUOccupantsPresent {
			t.Fatalf("orphan remediation marker = %+v", orphanMarker)
		}

		orphanReporter, _, _, orphanReportClient := newTestReporter(t, recommendationsCM(nil))
		orphanRecorder := &capturingRecorder{}
		orphanReporter.Recorder = orphanRecorder
		orphanCfg := config.Default()
		orphanReporter.ReportCycle(context.Background(), orphanCandidates, nil, orphanCfg, testNow)
		if got := orphanRecorder.count(eventNodeRepairNeeded); got != 1 {
			t.Fatalf("opening orphan observation emitted %d repair-needed event(s), want 1", got)
		}
		orphanReporter.ReportCycle(context.Background(), orphanCandidates, nil, orphanCfg, testNow.Add(time.Minute))
		if got := orphanRecorder.count(eventNodeDrainedForRepair); got != 0 {
			t.Fatalf("live orphan OME Pod emitted %d drained-for-repair event(s)", got)
		}
		orphanRecord, ok := nodeRecord(t, orphanReportClient, nodeName)
		if !ok || orphanRecord.DrainedAt != nil || len(orphanRecord.Workloads) != 0 ||
			!orphanRecord.OMEGPUOccupantsPresent {
			t.Fatalf("orphan node remediation record = %+v, present=%t", orphanRecord, ok)
		}
	})
}
