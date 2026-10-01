package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/webhook/admission/inferencereplica"
	"sigs.k8s.io/ome/pkg/webhook/admission/isvc"
)

// Check the exact merge patches against admission and runtime resolution.
// This does not start the image, simulate kubelet probes, or prove a rollout.
func TestRecoveryDocumentationPatches(t *testing.T) {
	dir := filepath.Join("..", "..", "config", "samples", "docs", "omenative-http")
	namespace := &corev1.Namespace{}
	rt := &v1beta1.ServingRuntime{}
	service := &v1beta1.InferenceService{}
	readWorkflowYAML(t, filepath.Join(dir, "namespace.yaml"), namespace)
	readWorkflowYAML(t, filepath.Join(dir, "servingruntime.yaml"), rt)
	readWorkflowYAML(t, filepath.Join(dir, "inferenceservice.yaml"), service)

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(namespace, rt).Build()
	selector := runtimeselector.New(cl)
	validator := &isvc.InferenceServiceValidator{Client: cl, Reader: cl, RuntimeSelector: selector}
	resolve := func(service *v1beta1.InferenceService) *v1beta1.EngineSpec {
		t.Helper()
		resolved, err := render.Resolve(t.Context(), render.Inputs{Service: service, Client: cl, Runtimes: selector, Log: logr.Discard()})
		if err != nil {
			t.Fatalf("resolve the recovery workload: %v", err)
		}
		if resolved.Model != nil || resolved.Specs.Engine == nil || resolved.Specs.Engine.Runner == nil {
			t.Fatal("the recovery exercise must stay model-free with an engine runner")
		}
		return resolved.Specs.Engine
	}
	patchFile := func(original *v1beta1.InferenceService, name string) *v1beta1.InferenceService {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		patched := patchWorkflowService(t, original, string(raw))
		if _, err := validator.ValidateUpdate(t.Context(), original, patched); err != nil {
			t.Fatalf("%s rejected by admission: %v", name, err)
		}
		return patched
	}

	// Start where the preceding guide finishes: three Instances on response v2.
	working := patchWorkflowService(t, service, `{"spec":{"engine":{"minReplicas":3,"maxReplicas":3,"runner":{"name":"ome-container","args":["-listen=:8080","-text=hello from OMENative v2"]}}}}`)
	if _, err := validator.ValidateCreate(t.Context(), working); err != nil {
		t.Fatalf("recovery prerequisite rejected: %v", err)
	}
	before := resolve(working)
	failed := patchFile(working, "recovery-fail.patch.json")
	broken := resolve(failed)
	if broken.Runner.Image != before.Runner.Image || !slices.Equal(broken.Runner.Args, before.Runner.Args) ||
		!reflect.DeepEqual(broken.Runner.Resources, before.Runner.Resources) {
		t.Fatal("failure must change readiness, not the image, listening arguments or resources")
	}
	if broken.Runner.ReadinessProbe == nil || broken.Runner.ReadinessProbe.HTTPGet == nil ||
		broken.Runner.ReadinessProbe.HTTPGet.Port != intstr.FromInt32(8081) ||
		broken.Runner.ReadinessProbe.PeriodSeconds != 2 {
		t.Fatal("failure must target the unused port and inherit the runtime's probe interval")
	}
	if broken.Lifecycle.InstanceReadyTimeout.Duration != 30*time.Second ||
		broken.Lifecycle.MigrationPolicy == nil || broken.Lifecycle.MigrationPolicy.Mode != v1beta1.MigrationPolicyModeNever {
		t.Fatal("failure must bound active readiness waiting and disable migration")
	}

	// Pause belongs on the parent. Both setting and removing the annotation
	// must pass the same service admission path as the user's commands.
	frozen := failed.DeepCopy()
	frozen.Annotations = map[string]string{constants.PausedRolloutAnnotation: "freeze"}
	if _, err := validator.ValidateUpdate(t.Context(), failed, frozen); err != nil {
		t.Fatalf("parent freeze rejected: %v", err)
	}
	fixed := patchFile(frozen, "recovery-fix.patch.json")
	if fixed.Annotations[constants.PausedRolloutAnnotation] != "freeze" {
		t.Fatal("applying the correction must not resume the workload")
	}
	if fixed.Spec.Engine.Lifecycle.MigrationPolicy != nil {
		t.Fatal("the recovery patch must remove the exercise's migration override")
	}
	recovered := resolve(fixed)
	if recovered.Runner.Image != before.Runner.Image ||
		!slices.Equal(recovered.Runner.Args, []string{"-listen=:8080", "-text=hello from OMENative v3"}) ||
		recovered.Runner.ReadinessProbe == nil || recovered.Runner.ReadinessProbe.HTTPGet == nil ||
		recovered.Runner.ReadinessProbe.HTTPGet.Port != intstr.FromInt32(8080) {
		t.Fatal("recovery must restore a probe on the listening port and publish response v3 using the same image")
	}
	if recovered.Lifecycle.InstanceReadyTimeout.Duration != 2*time.Minute ||
		fixed.Spec.Engine.MinReplicas == nil || *fixed.Spec.Engine.MinReplicas != 3 || fixed.Spec.Engine.MaxReplicas != 3 {
		t.Fatal("recovery must restore the lab timeout and preserve the three-Instance count")
	}
	if reflect.DeepEqual(broken.Runner, recovered.Runner) {
		t.Fatal("correction must change the rendered pod template, not just retry metadata")
	}
	resumed := fixed.DeepCopy()
	delete(resumed.Annotations, constants.PausedRolloutAnnotation)
	if _, err := validator.ValidateUpdate(t.Context(), fixed, resumed); err != nil {
		t.Fatalf("parent resume rejected: %v", err)
	}
}

// Feed the controller the kubelet evidence this exercise intends to create.
// Readiness limbo with migration disabled is not a revision-scoped retry hold.
func TestRecoveryDocumentationReadinessDisposition(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	target := "http-lab-engine-bbbbbbbb"
	row := workloadtypes.InstanceStatus{
		Index: 0,
		Phase: workloadtypes.InstancePhaseUpdating,
		Operation: &workloadtypes.InstanceOperation{
			Type:           workloadtypes.InstanceOperationUpdate,
			TargetRevision: target,
			Deadline:       metav1.NewTime(now.Add(-time.Second)),
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "http-lab-engine-0-default-1", Namespace: "ome-http-lab"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "ome-container",
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-time.Minute))},
				},
			}},
			Conditions: []corev1.PodCondition{{
				Type: corev1.ContainersReady, Status: corev1.ConditionFalse, Reason: evidence.ReasonContainersNotReady,
			}},
		},
	}
	blockWrites := 0
	input := workloadtypes.ReconcileInput{
		Clock: clocktesting.NewFakeClock(now),
		MutateInstance: func(_ context.Context, index int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
			if index != row.Index {
				t.Fatalf("mutating unrelated Instance %d", index)
			}
			mutate(&row)
			return nil
		},
		MutateRetryBlock: func(_ context.Context, _ string, _ func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
			blockWrites++
			return nil
		},
		WarnInstanceFailed: func(_ int32, _, _ string) {},
	}
	input.ObservedState.UpdateRevision = target
	outcome, err := escalation.DisposeExpiredAttempt(t.Context(), workloadtypes.Deps{}, input,
		workloadtypes.DispositionDeps{MigrationMode: workloadtypes.MigrationModeNever},
		row, []*corev1.Pod{pod}, "DeadlineExceeded: readiness attempt expired")
	if err != nil {
		t.Fatal(err)
	}
	if outcome != escalation.DispositionTerminal || blockWrites != 0 {
		t.Fatalf("readiness should fail without blaming the revision: outcome=%v retry writes=%d", outcome, blockWrites)
	}
	if row.Phase != workloadtypes.InstancePhaseFailed || row.Operation != nil || row.LastFailure == nil ||
		row.LastFailure.Reason != evidence.ReasonContainersNotReady || row.LastFailure.PodName != pod.Name {
		t.Fatalf("expected retained readiness evidence with the attempt cleared, got %+v", row)
	}
}

func TestRecoveryDocumentationResetAdmission(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	validator := &inferencereplica.Validator{Decoder: admission.NewDecoder(scheme)}
	original := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "http-lab-engine", Namespace: "ome-http-lab",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "http-lab",
				UID: "http-lab-owner", Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent, Replicas: ptr.To(int32(3))},
	}
	check := func(updated *v1beta1.InferenceReplica) admission.Response {
		t.Helper()
		oldRaw, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		newRaw, err := json.Marshal(updated)
		if err != nil {
			t.Fatal(err)
		}
		return validator.Handle(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Update, Namespace: original.Namespace, Name: original.Name,
			OldObject: runtime.RawExtension{Raw: oldRaw}, Object: runtime.RawExtension{Raw: newRaw},
		}})
	}
	for _, value := range []string{"0", "0,2", "all"} {
		t.Run(value, func(t *testing.T) {
			request := original.DeepCopy()
			request.Annotations = map[string]string{constants.ResetInstancesAnnotationKey: value}
			if response := check(request); !response.Allowed {
				t.Fatalf("metadata-only reset mailbox rejected: %v", response.Result)
			}
		})
	}
	// Admission allows the mailbox, not an operator edit of projected spec.
	// Parsing reset values and checking serving pods happen in reconciliation.
	specEdit := original.DeepCopy()
	specEdit.Annotations = map[string]string{constants.ResetInstancesAnnotationKey: "0"}
	specEdit.Spec.Replicas = ptr.To(int32(1))
	if response := check(specEdit); response.Allowed || response.Result == nil ||
		!strings.Contains(response.Result.Message, "edit the InferenceService instead") {
		t.Fatalf("reset annotation must not authorize a projected spec edit: %v", response.Result)
	}
}

func TestRecoveryDocumentationResetOwnership(t *testing.T) {
	for _, phase := range []workloadtypes.InstancePhase{workloadtypes.InstancePhaseFailed, workloadtypes.InstancePhaseReady} {
		for _, operation := range []workloadtypes.InstanceOperationType{
			"", workloadtypes.InstanceOperationCreate, workloadtypes.InstanceOperationRestart,
			workloadtypes.InstanceOperationUpdate, workloadtypes.InstanceOperationMigrate, workloadtypes.InstanceOperationDelete,
		} {
			t.Run(string(phase)+"/"+string(operation), func(t *testing.T) {
				failure := &workloadtypes.InstanceTermination{Reason: evidence.ReasonContainersNotReady}
				row := workloadtypes.InstanceStatus{Index: 0, Phase: phase, LastFailure: failure}
				if operation != "" {
					row.Operation = &workloadtypes.InstanceOperation{Type: operation}
				}
				mutate := func(_ context.Context, _ int32, mutation func(*workloadtypes.InstanceStatus) bool) error {
					mutation(&row)
					return nil
				}
				cleared, err := workloadstatus.ClearFailedInstanceOperation(t.Context(), mutate, 0)
				if err != nil {
					t.Fatal(err)
				}
				want := phase == workloadtypes.InstancePhaseFailed &&
					(operation == workloadtypes.InstanceOperationCreate || operation == workloadtypes.InstanceOperationRestart)
				if cleared != want || row.Phase != phase || row.LastFailure != failure {
					t.Fatalf("reset ownership/history mismatch: cleared=%v want=%v row=%+v", cleared, want, row)
				}
				if want && row.Operation != nil {
					t.Fatal("a reset-owned failed attempt must be cleared")
				}
				if !want && operation != "" && (row.Operation == nil || row.Operation.Type != operation) {
					t.Fatal("a reset must not clear another operation's continuation")
				}
			})
		}
	}
}
