package workload

import (
	"errors"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
)

// A zero-valued observation names no source, scope or epoch: the unknown
// value of each enumeration is its zero value. The engine refuses such an
// observation for a decision and for a publication, so a pass can only act
// on an observation one of the constructors built.
func TestZeroObservationIsNeitherDecisionNorPublicationGrade(t *testing.T) {
	if ObservationEpochUnknown != 0 || PodObservationSourceUnknown != 0 || PodObservationScopeUnknown != 0 {
		t.Fatalf("unknown values = epoch %d, source %d, scope %d, want the zero value of each", ObservationEpochUnknown, PodObservationSourceUnknown, PodObservationScopeUnknown)
	}
	var pods PodObservation
	if pods.Source() != PodObservationSourceUnknown || pods.Scope() != PodObservationScopeUnknown {
		t.Fatalf("zero pod observation reads source %d, scope %d, want unknown", pods.Source(), pods.Scope())
	}
	if pods.validForDecision() {
		t.Fatal("zero pod observation passes the decision check")
	}
	if _, err := NewDecisionObservation(nil, pods); !errors.Is(err, errDecisionObservationProvenance) {
		t.Fatalf("NewDecisionObservation(zero) = %v, want the provenance refusal", err)
	}
	if _, err := NewOwnedPublicationObservation(nil, pods, nil, status.AvailabilityWindow{}); !errors.Is(err, errPublicationObservationProvenance) {
		t.Fatalf("NewOwnedPublicationObservation(zero) = %v, want the provenance refusal", err)
	}
	var component ComponentObservation
	if component.Epoch() != ObservationEpochUnknown {
		t.Fatalf("zero component observation reads epoch %d, want unknown", component.Epoch())
	}
	if err := component.validatePublication(); !errors.Is(err, errPublicationObservationRequired) {
		t.Fatalf("validatePublication(zero) = %v, want the publication-observation refusal", err)
	}
}
