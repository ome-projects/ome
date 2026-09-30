package types

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOperationCapacityProvisioning(t *testing.T) {
	if OperationCapacityProvisioning(nil) || OperationCapacityProvisioning(&InstanceOperation{}) {
		t.Error("only the provisioning token reports the hold")
	}
	if !OperationCapacityProvisioning(&InstanceOperation{Waiting: WaitingReasonCapacityProvisioning}) {
		t.Error("the token reads as provisioning")
	}
}

// Capacity a provisioner outside the workload has not readied is not the
// workload's failure: the wait must park InstanceReadyTimeout like every
// other environment-caused hold.
func TestCapacityProvisioningParksTheDeadline(t *testing.T) {
	op := &InstanceOperation{
		Type:    InstanceOperationUpdate,
		Step:    UpdateStepSurge,
		Waiting: WaitingReasonCapacityProvisioning,
	}
	if !OperationExternallyHeld(op) {
		t.Fatal("a provisioning wait must park the attempt deadline")
	}
}

// The record carries the moment the caller passes, never now:
// re-observing the same wait with the same moment must produce an
// identical record or the pass writes on every tick.
func TestCapacityProvisioningTerminationIsStableAcrossObservations(t *testing.T) {
	since := metav1.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	first := CapacityProvisioningTermination("svc-engine-0", "slice not active", since)
	second := CapacityProvisioningTermination("svc-engine-0", "slice not active", since)
	if *first != *second {
		t.Fatalf("two observations differ: %+v vs %+v", *first, *second)
	}
	if first.Reason != WaitingReasonCapacityProvisioning || first.PodName != "svc-engine-0" {
		t.Errorf("record must name the token and the pod: %+v", *first)
	}
	if !first.Time.Equal(&since) {
		t.Errorf("record moment: got %v want %v", first.Time, since)
	}
	for _, want := range []string{"svc-engine-0", "slice not active"} {
		if !strings.Contains(first.Message, want) {
			t.Errorf("message must carry %q: %q", want, first.Message)
		}
	}
}
