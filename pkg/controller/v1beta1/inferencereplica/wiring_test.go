package inferencereplica

// validateWiring is the composition-root guard: production setup must
// fail fast when the authoritative reader is missing instead of
// silently degrading every live read to the lagging cache, and when the
// status write target was never loaded from configuration.

import (
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestValidateWiring(t *testing.T) {
	r := &Reconciler{}
	if err := r.validateWiring(); err == nil {
		t.Fatal("nil APIReader must be rejected at setup")
	}
	r.APIReader = &podListFailingReader{}
	err := r.validateWiring()
	if err == nil || !strings.Contains(err.Error(), "InstanceStatusTarget") {
		t.Fatalf("an unset status write target must be rejected at setup, got %v", err)
	}
	r.InstanceStatusTarget = irstatus.Encoding("Sparse")
	if err := r.validateWiring(); err == nil {
		t.Fatal("an unknown status write target must be rejected at setup")
	}
	r.InstanceStatusTarget = irstatus.EncodingDenseV1
	if err := r.validateWiring(); err != nil {
		t.Fatalf("wired APIReader and DenseV1 target must pass: %v", err)
	}
	r.InstanceStatusTarget = irstatus.EncodingColumnarV2
	if err := r.validateWiring(); err == nil || !strings.Contains(err.Error(), "maxDecodedInstances") {
		t.Fatalf("a ColumnarV2 target without a decode bound must be rejected at setup, got %v", err)
	}
	r.InstanceStatusDecoder = irstatus.NewDecoder(1)
	if err := r.validateWiring(); err != nil {
		t.Fatalf("wired APIReader, ColumnarV2 target, and bound must pass: %v", err)
	}
}
