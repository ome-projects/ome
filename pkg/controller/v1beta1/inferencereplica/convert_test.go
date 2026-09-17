package inferencereplica

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// buildRemoveInstance must Forget the SAME expectations bucket the
// workload ops populate: Key.OwnerName is the parent ISVC name
// (buildKey), not the IR name. Keyed on the IR name the Forget deletes
// nothing, so a reused index inherits stale counters until the TTL.
func TestBuildRemoveInstance_ForgetsParentKeyedExpectations(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := baselineIR("llama-engine", "default", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: v1beta1.OMENativeInstanceReady},
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		Build()

	exp := workload.NewExpectations()
	key := buildKey(ir)
	exp.ExpectCreates(key.Namespace, key.OwnerName, key.Component, 0, 1)
	g.Expect(exp.Satisfied(key.Namespace, key.OwnerName, key.Component, 0)).To(gomega.BeFalse(),
		"an in-flight create must block the bucket before removal")

	removed, err := buildRemoveInstance(testStatusWriter(c), c, ir, exp)(context.Background(), 0)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(removed).To(gomega.BeTrue())

	g.Expect(exp.Satisfied(key.Namespace, key.OwnerName, key.Component, 0)).To(gomega.BeTrue(),
		"Forget must clear the bucket ExpectCreates populated (OwnerName = parent ISVC name)")
}
