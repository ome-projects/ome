package transitionpreflight

import (
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// An inventory with three manager locators missing names them in the same
// order on every run, so two operators reading one file see one message.
func TestInventoryValidateNamesMissingLocatorsInOneOrder(t *testing.T) {
	inventory := &Inventory{
		ManagerImage:    testImage,
		OMENativeStatus: ExpectedStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1},
		Manager:         ManagerLocation{Container: "manager"},
		PageSize:        500,
		Clusters:        []Cluster{{Name: "east", Kubeconfig: "/etc/ome/east.kubeconfig", Context: "east"}},
	}
	want := "manager.namespace is required\nmanager.deployment is required\nmanager.configMap is required"
	// Enough runs that a walk over a map would show a second order.
	for run := 0; run < 100; run++ {
		err := inventory.Validate()
		if err == nil {
			t.Fatal("inventory with three missing locators accepted")
		}
		if got := err.Error(); got != want {
			t.Fatalf("run %d: message %q, want %q", run, got, want)
		}
	}
}
