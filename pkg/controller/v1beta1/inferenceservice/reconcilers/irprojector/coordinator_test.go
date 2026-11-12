package irprojector

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestCoordinatorFollowerPreservesDesiredCount(t *testing.T) {
	minimum := 1
	five := int32(5)
	params := Params{ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: &minimum}, CoordinatorFollower: true}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}}
	ir.Spec.Replicas = &five
	if got := desiredReplicas(ir, params); got == nil || *got != five {
		t.Fatalf("coordinator follower count = %v; want 5", got)
	}
	params.CoordinatorFollower = false
	if got := desiredReplicas(ir, params); got == nil || *got != 1 {
		t.Fatalf("fixed-count projection = %v; want 1", got)
	}
	params.CoordinatorFollower = true
	ir.ResourceVersion = ""
	if got := desiredReplicas(ir, params); got == nil || *got != 1 {
		t.Fatalf("initial projection = %v; want floor 1", got)
	}
	// Scale-down preserves the lower desired count just as scale-up does.
	ir.ResourceVersion = "2"
	two := int32(2)
	ir.Spec.Replicas = &two
	if got := desiredReplicas(ir, params); got == nil || *got != 2 {
		t.Fatalf("decreased follower count = %v; want 2", got)
	}
}

// A follower's count is the coordinator's target, on update and on create,
// and the canary partition is sized against that count in the same spec.
func TestCoordinatorTargetIsWrittenBesideItsPartition(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := baselineISVC("pair", "serving")
	c := fakeclient.New(t)
	key := types.NamespacedName{Name: "pair-engine", Namespace: "serving"}
	// A 25 percent step holds every Instance but the quarter it stages.
	quarterStep := func(replicas int32) *int32 { return ptr.To(replicas - (replicas+3)/4) }

	created := minimalParams(t, isvc, c)
	created.CoordinatorFollower = true
	created.CoordinatorTarget = ptr.To(int32(8))
	created.PacingPartition = quarterStep
	_, err := EnsureInferenceReplica(context.Background(), created)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(8))), "a follower is created at the coordinator's count")
	g.Expect(got.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(6))))

	doubled := minimalParams(t, isvc, c)
	doubled.CoordinatorFollower = true
	doubled.CoordinatorTarget = ptr.To(int32(16))
	doubled.PacingPartition = quarterStep
	_, err = EnsureInferenceReplica(context.Background(), doubled)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(16))))
	g.Expect(got.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(12))),
		"the partition is sized against the count carried in the same write")

	held := minimalParams(t, isvc, c)
	held.CoordinatorFollower = true
	held.PacingPartition = quarterStep
	_, err = EnsureInferenceReplica(context.Background(), held)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(16))), "with no target the follower keeps its count")
	g.Expect(got.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(12))))

	// A target reaches only a Component the coordinator owns.
	unowned := minimalParams(t, isvc, c)
	unowned.CoordinatorTarget = ptr.To(int32(3))
	_, err = EnsureInferenceReplica(context.Background(), unowned)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(*isvc.Spec.Engine.MinReplicas))),
		"a Component the coordinator does not own is projected from its declared minimum")
}

// A Component that stopped following keeps the count it carries under
// autoscaler class None; the same Component with nothing marking it released
// is written from its minReplicas, and a fresh replica starts there.
func TestReleasedFollowerKeepsItsLiveCount(t *testing.T) {
	minimum := 2
	twelve := int32(12)
	params := Params{ComponentExt: &v1beta1.ComponentExtensionSpec{MinReplicas: &minimum},
		ResolvedAutoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}, FollowerReleased: true}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}}
	ir.Spec.Replicas = &twelve
	if got := ptr.Deref(desiredReplicas(ir, params), -1); got != 12 {
		t.Fatalf("released follower count = %d; want the 12 it carries", got)
	}
	params.FollowerReleased = false
	if got := ptr.Deref(desiredReplicas(ir, params), -1); got != 2 {
		t.Fatalf("fixed-count projection = %d; want minReplicas 2", got)
	}
	params.FollowerReleased = true
	ir.ResourceVersion = ""
	if got := ptr.Deref(desiredReplicas(ir, params), -1); got != 2 {
		t.Fatalf("initial projection = %d; want minReplicas 2", got)
	}
}
