package servingruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A runtime annotated as a profile must be disabled.
func TestValidateProfileMarker(t *testing.T) {
	profile := map[string]string{constants.RuntimeProfileAnnotationKey: "true"}
	tests := []struct {
		name        string
		annotations map[string]string
		disabled    *bool
		wantErr     bool
	}{
		{name: "no annotations"},
		{name: "no profile annotation, enabled", annotations: map[string]string{}, disabled: ptr.To(false)},
		{name: "profile annotation, disabled", annotations: profile, disabled: ptr.To(true)},
		{name: "profile annotation, disabled unset", annotations: profile, wantErr: true},
		{name: "profile annotation, enabled", annotations: profile, disabled: ptr.To(false), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			err := validateProfileMarker(tt.annotations, tt.disabled)
			if !tt.wantErr {
				g.Expect(err).To(gomega.Succeed())
				return
			}
			g.Expect(err).To(gomega.HaveOccurred())
			g.Expect(err.Error()).To(gomega.ContainSubstring("disabled: true"))
		})
	}
}

func TestValidateClusterRuntimeInheritance_NoAnnotation(t *testing.T) {
	g := gomega.NewWithT(t)
	c := fakeclient.New(t)

	csr := clusterServingRuntime("solo")
	g.Expect(validateClusterRuntimeInheritance(context.Background(), c, csr)).To(gomega.Succeed())
}

func TestValidateClusterRuntimeInheritance_ParentExists(t *testing.T) {
	g := gomega.NewWithT(t)
	profile := clusterServingRuntime("profile-infra", withProfileAnnotation(), withDisabled(true))
	c := fakeclient.New(t, fakeclient.WithObjects(profile))

	csr := clusterServingRuntime("rt", withParent("profile-infra"))
	g.Expect(validateClusterRuntimeInheritance(context.Background(), c, csr)).To(gomega.Succeed())
}

func TestValidateClusterRuntimeInheritance_ParentMissing(t *testing.T) {
	g := gomega.NewWithT(t)
	c := fakeclient.New(t)

	csr := clusterServingRuntime("rt", withParent("missing-parent"))
	err := validateClusterRuntimeInheritance(context.Background(), c, csr)
	g.Expect(err).To(gomega.HaveOccurred())
	var pnf *runtimeinheritance.ParentNotFoundError
	g.Expect(errors.As(err, &pnf)).To(gomega.BeTrue())
	g.Expect(pnf.Parent).To(gomega.Equal("missing-parent"))
}

func TestValidateClusterRuntimeInheritance_Cycle(t *testing.T) {
	g := gomega.NewWithT(t)
	b := clusterServingRuntime("b", withParent("a"))
	a := clusterServingRuntime("a", withParent("b"))
	c := fakeclient.New(t, fakeclient.WithObjects(a, b))

	err := validateClusterRuntimeInheritance(context.Background(), c, a)
	g.Expect(err).To(gomega.HaveOccurred())
	var ce *runtimeinheritance.CycleError
	g.Expect(errors.As(err, &ce)).To(gomega.BeTrue())
}

func TestValidateClusterRuntimeInheritance_DepthExceeded(t *testing.T) {
	g := gomega.NewWithT(t)
	// A chain of six, n0 to n5, and a runtime under n5: seven levels in all.
	objects := []client.Object{}
	prev := ""
	for i := 0; i < 6; i++ {
		name := "n" + string(rune('0'+i))
		objects = append(objects, clusterServingRuntime(name, withParent(prev)))
		prev = name
	}
	c := fakeclient.New(t, fakeclient.WithObjects(objects...))

	leaf := clusterServingRuntime("leaf", withParent("n5"))
	err := validateClusterRuntimeInheritance(context.Background(), c, leaf)
	g.Expect(err).To(gomega.HaveOccurred())
	var de *runtimeinheritance.MaxDepthExceededError
	g.Expect(errors.As(err, &de)).To(gomega.BeTrue())
}

func TestValidateNamespacedRuntimeInheritance_SameNamespace(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := servingRuntime("ns-profile", "team")
	c := fakeclient.New(t, fakeclient.WithObjects(parent))

	sr := servingRuntime("rt", "team", withParent("ns-profile"))
	g.Expect(validateNamespacedRuntimeInheritance(context.Background(), c, sr)).To(gomega.Succeed())
}

func TestValidateNamespacedRuntimeInheritance_FallsBackToCluster(t *testing.T) {
	g := gomega.NewWithT(t)
	parent := clusterServingRuntime("cluster-profile")
	c := fakeclient.New(t, fakeclient.WithObjects(parent))

	sr := servingRuntime("rt", "team", withParent("cluster-profile"))
	g.Expect(validateNamespacedRuntimeInheritance(context.Background(), c, sr)).To(gomega.Succeed())
}

// A parent is read from the runtime's own namespace and from cluster scope
// only, so a same-named runtime in another namespace is not found.
func TestValidateNamespacedRuntimeInheritance_CrossNamespaceRejected(t *testing.T) {
	g := gomega.NewWithT(t)
	otherNS := servingRuntime("shared", "other-team")
	c := fakeclient.New(t, fakeclient.WithObjects(otherNS))

	sr := servingRuntime("rt", "team", withParent("shared"))
	err := validateNamespacedRuntimeInheritance(context.Background(), c, sr)
	g.Expect(err).To(gomega.HaveOccurred())
	var pnf *runtimeinheritance.ParentNotFoundError
	g.Expect(errors.As(err, &pnf)).To(gomega.BeTrue())
}
