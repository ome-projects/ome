package snapshot

import (
	"context"
	"errors"
	"reflect"
	"sort"
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

func TestBuildOMENativeCopiesDenseStatus(t *testing.T) {
	fixture := newOMENativeFixture()
	fixture.ir.Status.InstanceStatuses[0].Operation = &v1beta1.InstanceOperation{ID: "op-3"}

	component := buildOMENativeFixture(t, fixture)
	if !component.ObservationValid {
		t.Fatalf("observation invalid: %q", component.ObservationReason)
	}
	if !component.StatusFresh {
		t.Fatal("fresh IR statuses must be marked fresh")
	}
	if component.IR == nil {
		t.Fatal("accepted components must retain the source IR")
	}
	instance := component.Instances[0]
	if instance.Index != 3 || instance.Incarnation != 1 || instance.Phase != v1beta1.OMENativeInstanceReady ||
		instance.RunningRevision != "rev-a" || instance.DesiredPods != 1 || instance.ObservedPods != 1 ||
		instance.ServingPods != 1 || instance.AvailablePods != 1 || instance.ReadyPods != 1 ||
		instance.ActiveOrdinal != 0 || len(instance.Pods) != 1 || instance.NodesSet["node-a"] != 1 {
		t.Fatalf("normalized instance = %+v", instance)
	}
	instance.Operation.ID = "mutated"
	if fixture.ir.Status.InstanceStatuses[0].Operation.ID != "op-3" {
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
		{name: "missing identity label", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelPodOrdinal) }},
		{name: "missing instance index", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelInstanceIdx) }},
		{name: "missing incarnation", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelInstanceIncarnation) }},
		{name: "missing runner", mutate: func(f *omeNativeFixture) { delete(f.pods[0].Labels, query.LabelRunner) }},
		{name: "malformed identity label", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIdx] = "secret-malformed" }},
		{name: "negative instance index", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIdx] = "-1" }},
		{name: "negative incarnation", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelInstanceIncarnation] = "-1" }},
		{name: "wrong managed by", mutate: func(f *omeNativeFixture) { f.pods[0].Labels[query.LabelManagedBy] = "Deployment" }},
		{name: "wrong Pod controller UID", mutate: func(f *omeNativeFixture) { f.pods[0].OwnerReferences[0].UID = "other-ir" }},
		{name: "wrong Pod controller name", mutate: func(f *omeNativeFixture) { f.pods[0].OwnerReferences[0].Name = "other-ir" }},
		{name: "wrong Pod controller kind", mutate: func(f *omeNativeFixture) { f.pods[0].OwnerReferences[0].Kind = "Deployment" }},
		{name: "wrong Pod controller API version", mutate: func(f *omeNativeFixture) {
			f.pods[0].OwnerReferences[0].APIVersion = "apps/v1"
		}},
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
		{name: "available count overflow", mutate: func(f *omeNativeFixture) { f.ir.Status.InstanceStatuses[0].AvailablePodCount = 2 }},
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

func TestBuildOMENativeAllowsAvailabilityToDwellBelowReady(t *testing.T) {
	fixture := newOMENativeFixture()
	fixture.ir.Status.InstanceStatuses[0].AvailablePodCount = 0

	component := buildOMENativeFixture(t, fixture)
	if !component.ObservationValid {
		t.Fatalf("minReadySeconds dwell invalidated observation: %q", component.ObservationReason)
	}
	if got := component.Instances[0].AvailablePods; got != 0 {
		t.Fatalf("AvailablePods = %d, want 0 during dwell", got)
	}
}

func TestValidatedDenseInstanceStatusesCardinalityBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		rowCount  int
		wantValid bool
	}{
		{name: "ten thousand rows", rowCount: 10_000, wantValid: true},
		{name: "ten thousand and one rows", rowCount: 10_001, wantValid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := make([]v1beta1.OMENativeInstanceStatus, test.rowCount)
			for i := range rows {
				rows[i].Index = int32(i)
				rows[i].Phase = v1beta1.OMENativeInstancePending
			}

			validated, valid := validatedDenseInstanceStatuses(rows)
			if valid != test.wantValid {
				t.Fatalf("validatedDenseInstanceStatuses(%d rows) valid = %t, want %t",
					test.rowCount, valid, test.wantValid)
			}
			if valid && len(validated) != test.rowCount {
				t.Fatalf("validated row count = %d, want %d", len(validated), test.rowCount)
			}
		})
	}
}

func TestBuildOMENativeRejectsMalformedDenseStatus(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omeNativeFixture)
	}{
		{name: "duplicate index", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses = append(f.ir.Status.InstanceStatuses, f.ir.Status.InstanceStatuses[0])
		}},
		{name: "negative index", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].Index = -1
		}},
		{name: "unknown phase", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstancePhase("Unknown")
		}},
		{name: "negative incarnation", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].Incarnation = -1
		}},
		{name: "negative pod count", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].PodCount = -1
		}},
		{name: "negative serving count", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].ServingPodCount = -1
		}},
		{name: "negative available count", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].AvailablePodCount = -1
		}},
		{name: "negative active ordinal", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].ActiveOrdinal = -1
		}},
		{name: "active ordinal above one", mutate: func(f *omeNativeFixture) {
			f.ir.Status.InstanceStatuses[0].ActiveOrdinal = 2
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			test.mutate(fixture)

			component := buildOMENativeFixture(t, fixture)
			if component.ObservationValid || component.ObservationReason != observationReasonIRStatus {
				t.Fatalf("malformed dense status accepted: %+v", component)
			}
		})
	}
}

func TestBuildOMENativeInvalidInstanceIndexDoesNotTargetZeroOrSibling(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{
			name: "missing",
			mutate: func(pod *corev1.Pod) {
				delete(pod.Labels, query.LabelInstanceIdx)
			},
		},
		{
			name: "malformed",
			mutate: func(pod *corev1.Pod) {
				pod.Labels[query.LabelInstanceIdx] = "not-an-index"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			fixture.ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{readyOMERow(0, 1, 1)}
			fixture.pods[0] = omeNativePod(fixture, 0, 1, "default", 0, "node-a")
			addOMENativeDecoder(fixture)

			adversarial := fixture.pods[0].DeepCopy()
			adversarial.Name += "-invalid-index"
			test.mutate(adversarial)
			fixture.pods = append(fixture.pods, adversarial)

			workload := buildOMENativeFixtureWorkload(t, fixture)
			engine := workload.Components[v1beta1.EngineComponent]
			assertInvalidOMENativeObservation(t, engine)
			if len(engine.Instances) != 1 {
				t.Fatalf("engine instances = %+v, want one instance", engine.Instances)
			}
			instance := engine.Instances[0]
			if !instance.ObservationValid || instance.ObservationReason != "" || len(instance.Pods) != 1 {
				t.Fatalf("invalid index was attributed to instance 0: %+v", instance)
			}

			decoder := workload.Components[v1beta1.DecoderComponent]
			if !decoder.ObservationValid {
				t.Fatalf("invalid engine index poisoned decoder: %+v", decoder)
			}
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

func TestBuildOMENativeRejectsOwnedPodsWithInvalidISVCEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omeNativeFixture, *corev1.Pod)
	}{
		{
			name: "missing ISVC label",
			mutate: func(_ *omeNativeFixture, pod *corev1.Pod) {
				delete(pod.Labels, constants.InferenceServicePodLabelKey)
			},
		},
		{
			name: "wrong ISVC label",
			mutate: func(fixture *omeNativeFixture, pod *corev1.Pod) {
				rawMode := constants.RawDeployment
				other := fixture.isvc.DeepCopy()
				other.Name = "other-svc"
				other.UID = "other-isvc-uid"
				other.Spec.DeploymentMode = &rawMode
				fixture.extra = append(fixture.extra, other)
				pod.Labels[constants.InferenceServicePodLabelKey] = other.Name
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			adversarial := fixture.pods[0].DeepCopy()
			adversarial.Name += "-adversarial-isvc"
			test.mutate(fixture, adversarial)
			fixture.pods = append(fixture.pods, adversarial)

			component := buildOMENativeFixture(t, fixture)
			assertInvalidOMENativeObservation(t, component)
		})
	}
}

func TestBuildProjectsOwnerResolvedIdentityIntoNodeOccupancy(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{
			name: "missing ISVC label",
			mutate: func(pod *corev1.Pod) {
				delete(pod.Labels, constants.InferenceServicePodLabelKey)
			},
		},
		{
			name: "wrong ISVC label",
			mutate: func(pod *corev1.Pod) {
				pod.Labels[constants.InferenceServicePodLabelKey] = "wrong-svc"
			},
		},
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
			fixture := newOMENativeFixture()
			test.mutate(fixture.pods[0])
			snap := buildOMENativeFixtureSnapshot(t, fixture, gpuNode("node-a", "8", nil))

			node := snap.Nodes["node-a"]
			if node.AllocatedGPUs != 1 || node.FreeGPUs != 7 {
				t.Fatalf("GPU accounting = %d allocated/%d free, want 1/7", node.AllocatedGPUs, node.FreeGPUs)
			}
			if len(node.OtherOccupants) != 0 || len(node.OMEPods) != 1 {
				t.Fatalf("occupants = OME %+v, other %+v, want one canonical OME occupant", node.OMEPods, node.OtherOccupants)
			}
			occupant := node.OMEPods[0]
			wantISVC := types.NamespacedName{Namespace: "prod", Name: "svc"}
			if occupant.ISVC != wantISVC || occupant.Component != v1beta1.EngineComponent {
				t.Fatalf("occupant identity = %s/%s, want %s/%s", occupant.ISVC, occupant.Component, wantISVC, v1beta1.EngineComponent)
			}

			component := snap.Workloads[wantISVC].Components[v1beta1.EngineComponent]
			assertInvalidOMENativeObservation(t, component)
		})
	}
}

func TestBuildProjectsEveryAmbiguousOwnerTargetWithoutDoubleCounting(t *testing.T) {
	fixture := newOMENativeFixture()
	otherISVC := fixture.isvc.DeepCopy()
	otherISVC.Name = "other-svc"
	otherISVC.UID = "other-isvc-uid"
	otherIR := fixture.ir.DeepCopy()
	otherIR.Spec.ParentRef.Name = otherISVC.Name
	otherIR.OwnerReferences[0].Name = otherISVC.Name
	otherIR.OwnerReferences[0].UID = otherISVC.UID
	// Deliberately retain the first IR's full name/UID identity while changing
	// its target. A corrupt duplicate list row makes both targets plausible.
	fixture.extra = append(fixture.extra, otherISVC)

	snap := buildOMENativeFixtureSnapshotWithIRList(t, fixture,
		[]v1beta1.InferenceReplica{*fixture.ir.DeepCopy(), *otherIR}, gpuNode("node-a", "8", nil))
	node := snap.Nodes["node-a"]
	if node.AllocatedGPUs != 1 || node.FreeGPUs != 7 {
		t.Fatalf("ambiguous Pod counted more than once: allocated/free = %d/%d", node.AllocatedGPUs, node.FreeGPUs)
	}
	if len(node.OtherOccupants) != 0 || len(node.OMEPods) != 2 {
		t.Fatalf("ambiguous occupants = OME %+v, other %+v, want two logical blockers", node.OMEPods, node.OtherOccupants)
	}
	got := []string{
		node.OMEPods[0].ISVC.String() + "/" + string(node.OMEPods[0].Component),
		node.OMEPods[1].ISVC.String() + "/" + string(node.OMEPods[1].Component),
	}
	sort.Strings(got)
	want := []string{"prod/other-svc/engine", "prod/svc/engine"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical blockers = %v, want %v", got, want)
	}
}

func TestBuildProjectsOwnerTargetWithoutDeclaredOMENativeLayout(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*omeNativeFixture)
		wantISVC  types.NamespacedName
		component v1beta1.ComponentType
	}{
		{
			name: "undeclared component",
			mutate: func(fixture *omeNativeFixture) {
				fixture.ir.Spec.Component = v1beta1.DecoderComponent
			},
			wantISVC:  types.NamespacedName{Namespace: "prod", Name: "svc"},
			component: v1beta1.DecoderComponent,
		},
		{
			name: "missing parent workload",
			mutate: func(fixture *omeNativeFixture) {
				fixture.ir.Spec.ParentRef.Name = "missing-svc"
			},
			wantISVC:  types.NamespacedName{Namespace: "prod", Name: "missing-svc"},
			component: v1beta1.EngineComponent,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			delete(fixture.pods[0].Labels, constants.InferenceServicePodLabelKey)
			test.mutate(fixture)
			snap := buildOMENativeFixtureSnapshot(t, fixture, gpuNode("node-a", "8", nil))

			node := snap.Nodes["node-a"]
			if node.AllocatedGPUs != 1 || node.FreeGPUs != 7 {
				t.Fatalf("GPU accounting = %d allocated/%d free, want 1/7", node.AllocatedGPUs, node.FreeGPUs)
			}
			if len(node.OtherOccupants) != 0 || len(node.OMEPods) != 1 {
				t.Fatalf("owner-proven occupant = OME %+v, other %+v", node.OMEPods, node.OtherOccupants)
			}
			occupant := node.OMEPods[0]
			if occupant.ISVC != test.wantISVC || occupant.Component != test.component {
				t.Fatalf("owner-proven identity = %s/%s, want %s/%s",
					occupant.ISVC, occupant.Component, test.wantISVC, test.component)
			}
		})
	}
}

func TestBuildDoesNotProjectMalformedNonInferenceReplicaOwner(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*metav1.OwnerReference)
	}{
		{
			name: "wrong API version",
			mutate: func(owner *metav1.OwnerReference) {
				owner.APIVersion = "apps/v1"
			},
		},
		{
			name: "wrong kind",
			mutate: func(owner *metav1.OwnerReference) {
				owner.Kind = "Deployment"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOMENativeFixture()
			delete(fixture.pods[0].Labels, constants.InferenceServicePodLabelKey)
			delete(fixture.pods[0].Labels, query.LabelManagedBy)
			test.mutate(&fixture.pods[0].OwnerReferences[0])
			snap := buildOMENativeFixtureSnapshot(t, fixture, gpuNode("node-a", "8", nil))

			node := snap.Nodes["node-a"]
			if node.AllocatedGPUs != 1 || node.FreeGPUs != 7 {
				t.Fatalf("GPU accounting = %d allocated/%d free, want 1/7", node.AllocatedGPUs, node.FreeGPUs)
			}
			if len(node.OMEPods) != 0 || len(node.OtherOccupants) != 1 {
				t.Fatalf("malformed non-IR owner was trusted: OME %+v, other %+v", node.OMEPods, node.OtherOccupants)
			}
		})
	}
}

func TestBuildDoesNotCanonicalizeUnmatchedInferenceReplicaOwnerName(t *testing.T) {
	fixture := newOMENativeFixture()
	delete(fixture.pods[0].Labels, constants.InferenceServicePodLabelKey)
	delete(fixture.pods[0].Labels, query.LabelManagedBy)
	fixture.pods[0].OwnerReferences[0].Name = "missing-ir"
	snap := buildOMENativeFixtureSnapshot(t, fixture, gpuNode("node-a", "8", nil))

	node := snap.Nodes["node-a"]
	if len(node.OMEPods) != 1 || len(node.OtherOccupants) != 0 {
		t.Fatalf("unmatched IR owner = OME %+v, other %+v", node.OMEPods, node.OtherOccupants)
	}
	occupant := node.OMEPods[0]
	if occupant.ISVC.Name != "" || occupant.Component != v1beta1.EngineComponent ||
		occupant.ControllerOwnerName != "missing-ir" {
		t.Fatalf("unmatched owner was canonicalized to a different IR: %+v", occupant)
	}
}

func TestBuildKeepsOrphanInferenceReplicaOwnerAsUnresolvedOMEOccupant(t *testing.T) {
	fixture := newOMENativeFixture()
	delete(fixture.pods[0].Labels, constants.InferenceServicePodLabelKey)
	delete(fixture.pods[0].Labels, query.LabelManagedBy)
	fixture.ir = nil
	snap := buildOMENativeFixtureSnapshot(t, fixture, gpuNode("node-a", "8", nil))

	node := snap.Nodes["node-a"]
	if node.AllocatedGPUs != 1 || node.FreeGPUs != 7 {
		t.Fatalf("GPU accounting = %d allocated/%d free, want 1/7", node.AllocatedGPUs, node.FreeGPUs)
	}
	if len(node.OMEPods) != 1 || len(node.OtherOccupants) != 0 {
		t.Fatalf("exact orphan IR owner = OME %+v, other %+v", node.OMEPods, node.OtherOccupants)
	}
	occupant := node.OMEPods[0]
	if occupant.ISVC.Name != "" || occupant.Component != v1beta1.EngineComponent ||
		!occupant.ControllerOwnerValid || occupant.ControllerOwnerName != "svc-engine" {
		t.Fatalf("unresolved OME identity = %+v", occupant)
	}
}

func TestBuildKeepsManagedByOMENativeAsUnresolvedOMEOccupant(t *testing.T) {
	fixture := newOMENativeFixture()
	delete(fixture.pods[0].Labels, constants.InferenceServicePodLabelKey)
	fixture.pods[0].OwnerReferences = nil
	snap := buildOMENativeFixtureSnapshot(t, fixture, gpuNode("node-a", "8", nil))

	node := snap.Nodes["node-a"]
	if node.AllocatedGPUs != 1 || node.FreeGPUs != 7 {
		t.Fatalf("GPU accounting = %d allocated/%d free, want 1/7", node.AllocatedGPUs, node.FreeGPUs)
	}
	if len(node.OMEPods) != 1 || len(node.OtherOccupants) != 0 {
		t.Fatalf("managed-by OMENative evidence = OME %+v, other %+v", node.OMEPods, node.OtherOccupants)
	}
	occupant := node.OMEPods[0]
	if occupant.ISVC.Name != "" || occupant.Component != v1beta1.EngineComponent ||
		occupant.ManagedBy != query.ManagedByOMENative || occupant.ControllerOwnerPresent {
		t.Fatalf("unresolved managed-by identity = %+v", occupant)
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
	decoderIR.Name = fixture.ir.Name
	decoderIR.UID = fixture.ir.UID
	decoderPod.OwnerReferences[0].Name = fixture.ir.Name
	decoderPod.OwnerReferences[0].UID = fixture.ir.UID
	// The decoder IR is supplied through an intentionally corrupt List result
	// because a real API store cannot contain duplicate namespace/name keys.
	fixture.extra = nil

	snap := buildOMENativeFixtureSnapshotWithIRList(t, fixture,
		[]v1beta1.InferenceReplica{*fixture.ir.DeepCopy(), *decoderIR})
	workload := snap.Workloads[types.NamespacedName{Namespace: fixture.isvc.Namespace, Name: fixture.isvc.Name}]
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		t.Run(string(component), func(t *testing.T) {
			assertInvalidOMENativeObservation(t, workload.Components[component])
		})
	}
}

func TestBuildOMENativeUsesDeclaredComponentForUnresolvableOwner(t *testing.T) {
	fixture := newOMENativeFixture()
	addOMENativeDecoder(fixture)
	adversarial := fixture.pods[0].DeepCopy()
	adversarial.Name += "-unresolvable"
	adversarial.OwnerReferences[0].UID = "unknown-ir-uid"
	fixture.pods = append(fixture.pods, adversarial)

	workload := buildOMENativeFixtureWorkload(t, fixture)
	assertInvalidOMENativeObservation(t, workload.Components[v1beta1.EngineComponent])
	decoder := workload.Components[v1beta1.DecoderComponent]
	if !decoder.ObservationValid {
		t.Fatalf("engine-labelled unresolved Pod poisoned decoder: %+v", decoder)
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
	snap := buildOMENativeFixtureSnapshot(t, fixture)
	workload := snap.Workloads[types.NamespacedName{Namespace: fixture.isvc.Namespace, Name: fixture.isvc.Name}]
	if workload == nil || workload.Components[v1beta1.EngineComponent] == nil {
		t.Fatalf("engine component missing from snapshot: %+v", workload)
	}
	return workload
}

func buildOMENativeFixtureSnapshot(t *testing.T, fixture *omeNativeFixture, extra ...runtime.Object) *ClusterSnapshot {
	t.Helper()
	objects := append(fixture.runtimeObjects(), extra...)
	reader := fake.NewClientBuilder().WithScheme(testScheme(t)).WithRuntimeObjects(objects...).Build()
	snap, err := Build(context.Background(), reader, Options{Now: func() time.Time { return buildNow }})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return snap
}

func buildOMENativeFixtureSnapshotWithIRList(t *testing.T, fixture *omeNativeFixture,
	items []v1beta1.InferenceReplica, extra ...runtime.Object) *ClusterSnapshot {
	t.Helper()
	objects := append(fixture.runtimeObjects(), extra...)
	base := fake.NewClientBuilder().WithScheme(testScheme(t)).WithRuntimeObjects(objects...).Build()
	reader := &inferenceReplicaListOverrideReader{Reader: base, items: items}
	snap, err := Build(context.Background(), reader, Options{Now: func() time.Time { return buildNow }})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return snap
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

type inferenceReplicaListOverrideReader struct {
	client.Reader
	items []v1beta1.InferenceReplica
}

func (r *inferenceReplicaListOverrideReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if irs, ok := list.(*v1beta1.InferenceReplicaList); ok {
		irs.Items = append([]v1beta1.InferenceReplica(nil), r.items...)
		return nil
	}
	return r.Reader.List(ctx, list, opts...)
}
