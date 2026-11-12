package acceleratorclassselector

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// Helper function to create resource.Quantity
func mustParseQuantity(value string) resource.Quantity {
	q, err := resource.ParseQuantity(value)
	if err != nil {
		panic(err)
	}
	return q
}

func TestNewDefaultAcceleratorFetcher(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Create a fake client
	c := fakeclient.New(t)

	// Create fetcher
	fetcher := NewDefaultAcceleratorFetcher(c)

	// Verify fetcher is not nil and implements AcceleratorFetcher interface
	g.Expect(fetcher).NotTo(gomega.BeNil())

	// Verify it implements the interface
	var _ AcceleratorFetcher = fetcher
}

func TestDefaultAcceleratorFetcher_GetAcceleratorClass(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                      string
		acceleratorClassName      string
		clusterAcceleratorClasses []v1beta1.AcceleratorClass
		expectFound               bool
		expectClusterScoped       bool
		expectError               bool
		validateSpec              func(*testing.T, *v1beta1.AcceleratorClass)
	}{
		{
			name:                 "Cluster-scoped accelerator class found",
			acceleratorClassName: "nvidia-h100",
			clusterAcceleratorClasses: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nvidia-h100",
					},
					Spec: v1beta1.AcceleratorClassSpec{
						Discovery: v1beta1.AcceleratorDiscovery{
							NodeSelector: map[string]string{
								"accelerator": "nvidia-h100",
							},
						},
						Capabilities: v1beta1.AcceleratorCapabilities{},
						Resources: []v1beta1.AcceleratorResource{
							{
								Name:     "nvidia.com/gpu",
								Quantity: mustParseQuantity("1"),
							},
						},
					},
				},
			},
			expectFound:         true,
			expectClusterScoped: true,
			expectError:         false,
			validateSpec: func(t *testing.T, ac *v1beta1.AcceleratorClass) {
				g.Expect(ac).NotTo(gomega.BeNil())
				g.Expect(ac.Spec.Discovery.NodeSelector).To(gomega.HaveKeyWithValue("accelerator", "nvidia-h100"))
				g.Expect(ac.Spec.Resources).To(gomega.HaveLen(1))
				g.Expect(ac.Spec.Resources[0].Name).To(gomega.Equal("nvidia.com/gpu"))
			},
		},
		{
			name:                 "Accelerator class not found",
			acceleratorClassName: "non-existent",
			clusterAcceleratorClasses: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nvidia-h100",
					},
					Spec: v1beta1.AcceleratorClassSpec{
						Discovery:    v1beta1.AcceleratorDiscovery{},
						Capabilities: v1beta1.AcceleratorCapabilities{},
					},
				},
			},
			expectFound:         false,
			expectClusterScoped: false,
			expectError:         false,
			validateSpec: func(t *testing.T, ac *v1beta1.AcceleratorClass) {
				g.Expect(ac).To(gomega.BeNil())
			},
		},
		{
			name:                      "No accelerator classes in cluster",
			acceleratorClassName:      "nvidia-a100",
			clusterAcceleratorClasses: []v1beta1.AcceleratorClass{},
			expectFound:               false,
			expectClusterScoped:       false,
			expectError:               false,
			validateSpec: func(t *testing.T, ac *v1beta1.AcceleratorClass) {
				g.Expect(ac).To(gomega.BeNil())
			},
		},
		{
			name:                 "Multiple accelerator classes - find specific one",
			acceleratorClassName: "amd-mi300x",
			clusterAcceleratorClasses: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nvidia-h100",
					},
					Spec: v1beta1.AcceleratorClassSpec{
						Discovery: v1beta1.AcceleratorDiscovery{
							NodeSelector: map[string]string{
								"accelerator": "nvidia-h100",
							},
						},
						Capabilities: v1beta1.AcceleratorCapabilities{},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "amd-mi300x",
					},
					Spec: v1beta1.AcceleratorClassSpec{
						Discovery: v1beta1.AcceleratorDiscovery{
							NodeSelector: map[string]string{
								"accelerator": "amd-mi300x",
							},
						},
						Capabilities: v1beta1.AcceleratorCapabilities{},
						Resources: []v1beta1.AcceleratorResource{
							{
								Name:     "amd.com/gpu",
								Quantity: mustParseQuantity("1"),
							},
						},
					},
				},
			},
			expectFound:         true,
			expectClusterScoped: true,
			expectError:         false,
			validateSpec: func(t *testing.T, ac *v1beta1.AcceleratorClass) {
				g.Expect(ac).NotTo(gomega.BeNil())
				g.Expect(ac.Spec.Discovery.NodeSelector).To(gomega.HaveKeyWithValue("accelerator", "amd-mi300x"))
				g.Expect(ac.Spec.Resources).To(gomega.HaveLen(1))
				g.Expect(ac.Spec.Resources[0].Name).To(gomega.Equal("amd.com/gpu"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create objects for fake client
			objects := make([]client.Object, 0)
			for i := range tt.clusterAcceleratorClasses {
				objects = append(objects, &tt.clusterAcceleratorClasses[i])
			}

			// Create fake client
			c := fakeclient.New(t, fakeclient.WithObjects(objects...))

			// Create fetcher
			fetcher := NewDefaultAcceleratorFetcher(c)

			// Get accelerator class
			spec, isClusterScoped, err := fetcher.GetAcceleratorClass(context.TODO(), tt.acceleratorClassName)

			if tt.expectError {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
			}

			if tt.expectFound {
				g.Expect(spec).NotTo(gomega.BeNil())
				g.Expect(isClusterScoped).To(gomega.Equal(tt.expectClusterScoped))
			} else {
				g.Expect(spec).To(gomega.BeNil())
			}

			// Run validation function if provided
			if tt.validateSpec != nil {
				tt.validateSpec(t, spec)
			}
		})
	}
}

func TestDefaultAcceleratorFetcher_GetAcceleratorClass_NotFoundContract(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Create fake client with no accelerator classes
	c := fakeclient.New(t)

	// Create fetcher
	fetcher := NewDefaultAcceleratorFetcher(c)

	// A missing class is found=false with a nil error — not-found is a valid
	// outcome (policy candidates skip it), distinct from a failed read.
	spec, found, err := fetcher.GetAcceleratorClass(context.TODO(), "non-existent")

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(spec).To(gomega.BeNil())
	g.Expect(found).To(gomega.BeFalse())
}
