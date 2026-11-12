package v1beta1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
)

// TestRuntimeReadyIsAdvisory_DoesNotClobberReady pins the core property of the
// RuntimeReady advisory condition: it is NOT a dependent of the aggregate
// Ready (conditionSet). A currently-serving ISVC (IngressReady + EngineReady
// True) whose spec.runtime is edited to a missing runtime gets
// RuntimeReady=False but MUST stay Ready=True, so its running pods aren't
// marked unhealthy over a bad spec edit that hasn't been applied.
func TestRuntimeReadyIsAdvisory_DoesNotClobberReady(t *testing.T) {
	ss := &InferenceServiceStatus{}
	// Make the ISVC healthy: both aggregate-Ready dependents True.
	ss.SetCondition(IngressReady, &apis.Condition{Type: IngressReady, Status: corev1.ConditionTrue})
	ss.SetCondition(EngineReady, &apis.Condition{Type: EngineReady, Status: corev1.ConditionTrue})
	if !ss.IsReady() {
		t.Fatalf("precondition: ISVC should be Ready with IngressReady+EngineReady True")
	}

	// Edit spec.runtime to a missing runtime → advisory False.
	ss.SetCondition(RuntimeReady, &apis.Condition{
		Type: RuntimeReady, Status: corev1.ConditionFalse,
		Reason: "RuntimeNotFound", Message: "runtime foo not found",
	})
	if !ss.IsReady() {
		t.Errorf("RuntimeReady=False must NOT flip the aggregate Ready (advisory only)")
	}
	rc := ss.GetCondition(RuntimeReady)
	if rc == nil || rc.Status != corev1.ConditionFalse || rc.Reason != "RuntimeNotFound" {
		t.Errorf("RuntimeReady advisory not recorded correctly: %+v", rc)
	}

	// Runtime later resolves → advisory cleared to True, Ready still True.
	ss.SetCondition(RuntimeReady, &apis.Condition{
		Type: RuntimeReady, Status: corev1.ConditionTrue, Reason: "RuntimeResolved",
	})
	if !ss.IsReady() {
		t.Errorf("clearing RuntimeReady must keep the aggregate Ready True")
	}
	if c := ss.GetCondition(RuntimeReady); c == nil || c.Status != corev1.ConditionTrue {
		t.Errorf("RuntimeReady should be cleared to True, got %+v", c)
	}
}

// TestRuntimeReadyDoesNotMakeUnreadyServiceReady is the guard for the inverse:
// the advisory must not accidentally *satisfy* readiness. An ISVC missing a
// dependent (EngineReady unset) is not Ready, and setting RuntimeReady=True
// must not change that.
func TestRuntimeReadyDoesNotMakeUnreadyServiceReady(t *testing.T) {
	ss := &InferenceServiceStatus{}
	ss.SetCondition(IngressReady, &apis.Condition{Type: IngressReady, Status: corev1.ConditionTrue})
	ss.SetCondition(RuntimeReady, &apis.Condition{Type: RuntimeReady, Status: corev1.ConditionTrue, Reason: "RuntimeResolved"})
	if ss.IsReady() {
		t.Errorf("RuntimeReady=True must not make a service Ready while EngineReady is unset")
	}
}

// TestReadyWaitsForEveryDeclaredComponent: a service that declares a decoder
// or a router is Ready only once that Component is Ready too. The engine and
// the ingress being up is not enough, and Ready names the Component it waits
// on.
func TestReadyWaitsForEveryDeclaredComponent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		component apis.ConditionType
		status    corev1.ConditionStatus
	}{
		{"decoder not serving", DecoderReady, corev1.ConditionFalse},
		{"encoder not serving", EncoderReady, corev1.ConditionFalse},
		{"router not serving", RouterReady, corev1.ConditionFalse},
		{"decoder readiness unknown", DecoderReady, corev1.ConditionUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ss := &InferenceServiceStatus{}
			ss.SetCondition(IngressReady, &apis.Condition{Type: IngressReady, Status: corev1.ConditionTrue})
			ss.SetCondition(EngineReady, &apis.Condition{Type: EngineReady, Status: corev1.ConditionTrue, Reason: "Ready"})
			ss.SetCondition(tc.component, &apis.Condition{
				Type: tc.component, Status: tc.status,
				Reason: "InsufficientAvailable", Message: "0/8 Instances serving, need 8",
			})
			if ss.IsReady() {
				t.Fatalf("%s=%s must keep the service from reading Ready=True", tc.component, tc.status)
			}
			ready := ss.GetCondition(apis.ConditionReady)
			if ready == nil || ready.Status != tc.status {
				t.Fatalf("Ready must follow %s=%s, got %+v", tc.component, tc.status, ready)
			}
			if ready.Reason != "InsufficientAvailable" || ready.Message != "0/8 Instances serving, need 8" {
				t.Errorf("Ready must carry the waiting Component's reason and message, got %+v", ready)
			}

			ss.SetCondition(tc.component, &apis.Condition{Type: tc.component, Status: corev1.ConditionTrue, Reason: "Ready"})
			if !ss.IsReady() {
				t.Errorf("the service must read Ready once %s is True, got %+v", tc.component, ss.GetCondition(apis.ConditionReady))
			}
		})
	}
}

// TestReadyWaitsForAComponentWrittenFirst: the order the conditions arrive in
// does not matter. A decoder reported before the engine and the ingress still
// holds Ready.
func TestReadyWaitsForAComponentWrittenFirst(t *testing.T) {
	ss := &InferenceServiceStatus{}
	ss.SetCondition(DecoderReady, &apis.Condition{Type: DecoderReady, Status: corev1.ConditionFalse, Reason: "NoReplicas"})
	ss.SetCondition(IngressReady, &apis.Condition{Type: IngressReady, Status: corev1.ConditionTrue})
	ss.SetCondition(EngineReady, &apis.Condition{Type: EngineReady, Status: corev1.ConditionTrue, Reason: "Ready"})
	if ss.IsReady() {
		t.Fatalf("a decoder reported before the engine must still hold Ready, got %+v", ss.GetCondition(apis.ConditionReady))
	}
}

// TestReadyIgnoresComponentsTheServiceDoesNotDeclare: an engine-only service
// reads as it always has. Ready is True on the ingress and the engine alone,
// and no condition is invented for a decoder or router it does not have.
func TestReadyIgnoresComponentsTheServiceDoesNotDeclare(t *testing.T) {
	ss := &InferenceServiceStatus{}
	ss.SetCondition(IngressReady, &apis.Condition{Type: IngressReady, Status: corev1.ConditionTrue})
	ss.SetCondition(EngineReady, &apis.Condition{Type: EngineReady, Status: corev1.ConditionTrue, Reason: "Ready"})
	if !ss.IsReady() {
		t.Fatalf("an engine-only service must be Ready on IngressReady and EngineReady, got %+v", ss.GetCondition(apis.ConditionReady))
	}
	for _, c := range optionalComponentReadyConditions {
		if ss.GetCondition(c) != nil {
			t.Errorf("%s must not appear on a service that declares no such Component", c)
		}
	}
	if len(ss.Status.Conditions) != 3 {
		t.Errorf("expected IngressReady, EngineReady and Ready only, got %+v", ss.Status.Conditions)
	}
}

// TestReadyDependsOnEveryRoleBesideTheEngine: the roles whose readiness can
// hold Ready are read from the roles table, so a role added there gates Ready
// without a second list to keep in step.
func TestReadyDependsOnEveryRoleBesideTheEngine(t *testing.T) {
	want := []apis.ConditionType{DecoderReady, EncoderReady, RouterReady}
	if got := optionalComponentReadyConditions; len(got) != len(want) {
		t.Fatalf("optional ready conditions = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("optional ready conditions = %v, want %v", got, want)
			}
		}
	}
}

// TestReadyAdoptsAComponentConditionWrittenBeforeItGatedReady: a decoder
// condition written when the decoder did not gate Ready carries the advisory
// severity. The next write reads it as a dependent, so Ready follows the
// decoder instead of reading Unknown with no reason.
func TestReadyAdoptsAComponentConditionWrittenBeforeItGatedReady(t *testing.T) {
	ss := &InferenceServiceStatus{}
	ss.Status.SetConditions(apis.Conditions{
		{Type: IngressReady, Status: corev1.ConditionTrue},
		{Type: EngineReady, Status: corev1.ConditionTrue, Reason: "Ready"},
		{Type: apis.ConditionReady, Status: corev1.ConditionTrue},
		{Type: DecoderReady, Status: corev1.ConditionFalse, Reason: "InsufficientAvailable", Message: "0/8 Instances serving, need 8", Severity: apis.ConditionSeverityInfo},
	})
	ss.SetCondition(EngineReady, &apis.Condition{Type: EngineReady, Status: corev1.ConditionTrue, Reason: "Ready"})
	ready := ss.GetCondition(apis.ConditionReady)
	if ready == nil || ready.Status != corev1.ConditionFalse || ready.Reason != "InsufficientAvailable" {
		t.Fatalf("Ready must follow the decoder written before it gated Ready, got %+v", ready)
	}
	if got := ss.GetCondition(DecoderReady); got == nil || got.Severity != apis.ConditionSeverityError {
		t.Errorf("DecoderReady must be read as a dependent of Ready, got %+v", got)
	}
}

// TestRemoveConditionReDerivesReady: a Component that leaves the spec takes
// its condition with it, and Ready is read again from the conditions that
// remain.
func TestRemoveConditionReDerivesReady(t *testing.T) {
	ss := &InferenceServiceStatus{}
	ss.SetCondition(IngressReady, &apis.Condition{Type: IngressReady, Status: corev1.ConditionTrue})
	ss.SetCondition(EngineReady, &apis.Condition{Type: EngineReady, Status: corev1.ConditionTrue, Reason: "Ready"})
	ss.SetCondition(DecoderReady, &apis.Condition{Type: DecoderReady, Status: corev1.ConditionFalse, Reason: "NoReplicas"})
	if ss.IsReady() {
		t.Fatalf("precondition: a decoder with no replicas must hold Ready")
	}

	ss.RemoveCondition(DecoderReady)
	if ss.GetCondition(DecoderReady) != nil {
		t.Errorf("DecoderReady must be gone after the decoder is removed")
	}
	if !ss.IsReady() {
		t.Errorf("Ready must be re-read from the ingress and the engine once the decoder is gone, got %+v", ss.GetCondition(apis.ConditionReady))
	}

	// Removing a condition the status does not carry changes nothing.
	before := len(ss.Status.Conditions)
	ss.RemoveCondition(RouterReady)
	if len(ss.Status.Conditions) != before || !ss.IsReady() {
		t.Errorf("removing an absent condition must change nothing, got %+v", ss.Status.Conditions)
	}

	// A dependent that is still not ready keeps Ready False after the removal.
	ss.SetCondition(EngineReady, &apis.Condition{Type: EngineReady, Status: corev1.ConditionFalse, Reason: "InsufficientAvailable", Message: "3/12 Instances serving, need 12"})
	ss.SetCondition(RouterReady, &apis.Condition{Type: RouterReady, Status: corev1.ConditionFalse, Reason: "NoReplicas"})
	ss.RemoveCondition(RouterReady)
	ready := ss.GetCondition(apis.ConditionReady)
	if ready == nil || ready.Status != corev1.ConditionFalse || ready.Reason != "InsufficientAvailable" {
		t.Errorf("Ready must still read the engine's shortfall after the router is removed, got %+v", ready)
	}
}
