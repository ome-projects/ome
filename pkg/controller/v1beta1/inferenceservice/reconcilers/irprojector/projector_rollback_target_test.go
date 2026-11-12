package irprojector

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A replica created while a rollback holds its Component is created pointed
// at the revision the operator rolled back to, so its Instances come up on
// that revision and not on the spec's rejected one. A replica that already
// exists keeps whatever target the canary executor wrote on it: the
// projection neither writes nor clears the target on an update, and a
// projection with no hold creates a replica with no target.
func TestEnsureInferenceReplica_AFreshReplicaUnderARollbackHoldIsCreatedPointedAtTheStableRevision(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("llama", "prod")
	c := fakeclient.New(t)
	key := types.NamespacedName{Name: "llama-engine", Namespace: "prod"}

	held := minimalParams(t, isvc, c)
	held.PacingPartition = fixedPartition(0)
	held.RollbackToRevision = "llama-engine-stable"
	_, err := EnsureInferenceReplica(context.Background(), held)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	created := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, created)).To(gomega.Succeed())
	g.Expect(created.Spec.Pacing).NotTo(gomega.BeNil())
	g.Expect(created.Spec.Pacing.RollbackToRevision).To(gomega.HaveValue(gomega.Equal("llama-engine-stable")),
		"a replica created under a rollback hold is created pointed at the stable revision")
	g.Expect(created.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(0))))

	// The executor moves the target; the next projection keeps the live
	// value and does not write its own.
	created.Spec.Pacing.RollbackToRevision = ptr.To("llama-engine-other")
	g.Expect(c.Update(context.Background(), created)).To(gomega.Succeed())
	_, err = EnsureInferenceReplica(context.Background(), held)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	updated := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, updated)).To(gomega.Succeed())
	g.Expect(updated.Spec.Pacing.RollbackToRevision).To(gomega.HaveValue(gomega.Equal("llama-engine-other")),
		"an existing replica keeps the target the executor wrote")

	// No hold: a fresh replica carries no target.
	other := baselineISVC("vicuna", "prod")
	_, err = EnsureInferenceReplica(context.Background(), minimalParams(t, other, c))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	plain := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), types.NamespacedName{Name: "vicuna-engine", Namespace: "prod"}, plain)).To(gomega.Succeed())
	g.Expect(plain.Spec.Pacing).To(gomega.BeNil())
}
