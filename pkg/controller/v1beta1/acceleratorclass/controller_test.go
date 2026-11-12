package acceleratorclass

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestAcceleratorClass_Reconcile_AddsFinalizerAndUpdatesStatus(t *testing.T) {
	g := NewWithT(t)

	// Initial objects: one AcceleratorClass and one matching Node
	ac := &v1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-ac",
		},
		Spec: v1beta1.AcceleratorClassSpec{
			Discovery: v1beta1.AcceleratorDiscovery{
				NodeSelector: map[string]string{"accelerator": "nvidia"},
			},
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{"accelerator": "nvidia"},
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceMemory:                                resource.MustParse("64Gi"),
				corev1.ResourceName(constants.NvidiaGPUResourceType): resource.MustParse("1"),
			},
		},
	}

	c := fakeclient.New(t, fakeclient.WithObjects(ac, node), fakeclient.WithStatusSubresource(&v1beta1.AcceleratorClass{}))

	reconciler := &AcceleratorClassReconciler{
		Client: c,
		Log:    ctrl.Log.WithName("AcceleratorClassTest"),
	}

	// Reconcile
	ctx := context.TODO()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: ac.Name}})
	g.Expect(err).NotTo(HaveOccurred())

	// Validate finalizer added and status updated
	updated := &v1beta1.AcceleratorClass{}
	g.Expect(c.Get(ctx, types.NamespacedName{Name: ac.Name}, updated)).To(Succeed())
	g.Expect(updated.GetFinalizers()).To(ContainElement(constants.AcceleratorClassFinalizer))
	g.Expect(updated.Status.AvailableNodes).To(Equal(int32(1)))
	g.Expect(updated.Status.Nodes).To(ContainElement("node-1"))
	g.Expect(updated.Status.LastUpdated.IsZero()).To(BeFalse())
}

func TestAcceleratorClass_Reconcile_MatchDiscovery(t *testing.T) {
	g := NewWithT(t)

	ac := &v1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ac-discovery"},
		Spec: v1beta1.AcceleratorClassSpec{
			Discovery: v1beta1.AcceleratorDiscovery{NodeSelector: map[string]string{"accel": "nvidia"}},
		},
	}

	nodeA := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"accel": "nvidia"}},
	}
	nodeB := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"accel": "amd"}},
	}

	c := fakeclient.New(t, fakeclient.WithObjects(ac, nodeA, nodeB), fakeclient.WithStatusSubresource(&v1beta1.AcceleratorClass{}))

	reconciler := &AcceleratorClassReconciler{Client: c, Log: ctrl.Log.WithName("AcceleratorClassTest")}

	ctx := context.TODO()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: ac.Name}})
	g.Expect(err).NotTo(HaveOccurred())

	curr := &v1beta1.AcceleratorClass{}
	g.Expect(c.Get(ctx, types.NamespacedName{Name: ac.Name}, curr)).To(Succeed())
	g.Expect(curr.Status.AvailableNodes).To(Equal(int32(1)))
	g.Expect(curr.Status.Nodes).To(ContainElement("node-a"))
	g.Expect(curr.Status.Nodes).NotTo(ContainElement("node-b"))
}

func TestAcceleratorClass_Reconcile_MatchDeclaredResources(t *testing.T) {
	g := NewWithT(t)

	ac := &v1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ac-resources"},
		Spec: v1beta1.AcceleratorClassSpec{
			Discovery: v1beta1.AcceleratorDiscovery{NodeSelector: map[string]string{"accel": "nvidia"}},
			Resources: []v1beta1.AcceleratorResource{
				{Name: constants.NvidiaGPUResourceType, Quantity: resource.MustParse("1")},
			},
		},
	}

	// node-a exposes the declared GPU resource; node-b matches the discovery
	// labels but exposes no GPU capacity (mislabeled or drained hardware).
	nodeA := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"accel": "nvidia"}},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceName(constants.NvidiaGPUResourceType): resource.MustParse("1"),
				corev1.ResourceMemory:                                resource.MustParse("32Gi"),
			},
		},
	}
	nodeB := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"accel": "nvidia"}},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("8Gi"),
			},
		},
	}

	c := fakeclient.New(t, fakeclient.WithObjects(ac, nodeA, nodeB), fakeclient.WithStatusSubresource(&v1beta1.AcceleratorClass{}))

	reconciler := &AcceleratorClassReconciler{Client: c, Log: ctrl.Log.WithName("AcceleratorClassTest")}

	ctx := context.TODO()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: ac.Name}})
	g.Expect(err).NotTo(HaveOccurred())

	curr := &v1beta1.AcceleratorClass{}
	g.Expect(c.Get(ctx, types.NamespacedName{Name: ac.Name}, curr)).To(Succeed())
	g.Expect(curr.Status.AvailableNodes).To(Equal(int32(1)))
	g.Expect(curr.Status.Nodes).To(ContainElement("node-a"))
	g.Expect(curr.Status.Nodes).NotTo(ContainElement("node-b"))
}

func TestAcceleratorClass_Reconcile_DoesNotUpdateTimestampOnNoChange(t *testing.T) {
	g := NewWithT(t)

	ac := &v1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ac-ts"},
		Spec: v1beta1.AcceleratorClassSpec{
			Discovery: v1beta1.AcceleratorDiscovery{NodeSelector: map[string]string{"accel": "nvidia"}},
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"accel": "nvidia"}},
	}

	// LastUpdated is set only inside a status write, so a pass that writes
	// no status cannot move it; counting the writes proves that without a clock.
	statusPatches := fakeclient.CountingVerb(fakeclient.StatusPatch)
	statusUpdates := fakeclient.CountingVerb(fakeclient.StatusUpdate)
	statusWrites := func() int { return statusPatches.Count() + statusUpdates.Count() }
	c := fakeclient.New(t, fakeclient.WithObjects(ac, node), fakeclient.WithStatusSubresource(&v1beta1.AcceleratorClass{}), statusPatches, statusUpdates)

	reconciler := &AcceleratorClassReconciler{Client: c, Log: ctrl.Log.WithName("AcceleratorClassTest")}

	ctx := context.TODO()
	_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: ac.Name}})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(statusWrites()).To(Equal(1))

	curr := &v1beta1.AcceleratorClass{}
	g.Expect(c.Get(ctx, types.NamespacedName{Name: ac.Name}, curr)).To(Succeed())
	firstUpdate := curr.Status.LastUpdated
	g.Expect(firstUpdate.IsZero()).To(BeFalse())

	// The same nodes again: nothing to record, so no status write at all.
	_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: ac.Name}})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(statusWrites()).To(Equal(1))

	post := &v1beta1.AcceleratorClass{}
	g.Expect(c.Get(ctx, types.NamespacedName{Name: ac.Name}, post)).To(Succeed())
	g.Expect(post.Status.LastUpdated.Time.Equal(firstUpdate.Time)).To(BeTrue())
}
