package servingruntime

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// Every accelerator class a runtime names must exist; the refusal lists the
// unknown ones.
func TestValidateAcceleratorClasses(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithObjects(
		&v1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{Name: "nvidia-h100-80gb"}},
		&v1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{Name: "nvidia-a100-80gb"}},
	))
	requires := func(classes ...string) *v1beta1.ServingRuntimeSpec {
		return &v1beta1.ServingRuntimeSpec{
			AcceleratorRequirements: &v1beta1.AcceleratorRequirements{AcceleratorClasses: classes},
		}
	}

	testcases := []struct {
		name     string
		spec     *v1beta1.ServingRuntimeSpec
		errorMsg string
	}{
		{name: "No accelerator requirements", spec: &v1beta1.ServingRuntimeSpec{}},
		{name: "Empty accelerator classes list", spec: requires()},
		{name: "Valid accelerator class - nvidia-h100-80gb", spec: requires("nvidia-h100-80gb")},
		{name: "Unknown accelerator class", spec: requires("unknown-accelerator"), errorMsg: "unknown accelerator classes"},
		{name: "Multiple accelerator classes - all valid", spec: requires("nvidia-h100-80gb", "nvidia-a100-80gb")},
		{name: "Multiple accelerator classes - one invalid", spec: requires("nvidia-h100-80gb", "invalid-class"), errorMsg: "unknown accelerator classes"},
		{name: "Multiple accelerator classes - all invalid", spec: requires("invalid-class-1", "invalid-class-2"), errorMsg: "unknown accelerator classes"},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			err := validateAcceleratorClasses(context.Background(), c, tc.spec)

			if tc.errorMsg == "" {
				g.Expect(err).To(gomega.BeNil())
				return
			}
			g.Expect(err).ToNot(gomega.BeNil())
			g.Expect(err.Error()).To(gomega.ContainSubstring(tc.errorMsg))
		})
	}
}
