package v1beta1testing

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// AcceleratorClassWrapper carries a cluster-scoped AcceleratorClass
// plus chained setters. Only the fields RawDeployment / VirtualDeployment
// tests care about today; expand as needed.
type AcceleratorClassWrapper struct {
	v1beta1.AcceleratorClass
}

// MakeAcceleratorClass returns a wrapped scaffold. Discovery + Capabilities
// are required by the CRD validator; we set sensible empty defaults so
// callers can chain without nil-deref panics.
func MakeAcceleratorClass(name string) *AcceleratorClassWrapper {
	return &AcceleratorClassWrapper{
		AcceleratorClass: v1beta1.AcceleratorClass{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: v1beta1.AcceleratorClassSpec{
				Discovery: v1beta1.AcceleratorDiscovery{
					NodeSelector: map[string]string{},
				},
				Capabilities: v1beta1.AcceleratorCapabilities{},
			},
		},
	}
}

func (w *AcceleratorClassWrapper) Obj() *v1beta1.AcceleratorClass { return &w.AcceleratorClass }

// Vendor is a pass-through metadata setter.
func (w *AcceleratorClassWrapper) Vendor(v string) *AcceleratorClassWrapper {
	w.Spec.Vendor = v
	return w
}

// DiscoveryNodeSelector merges into spec.Discovery.NodeSelector.
func (w *AcceleratorClassWrapper) DiscoveryNodeSelector(k, v string) *AcceleratorClassWrapper {
	if w.Spec.Discovery.NodeSelector == nil {
		w.Spec.Discovery.NodeSelector = map[string]string{}
	}
	w.Spec.Discovery.NodeSelector[k] = v
	return w
}

// No init() / cleanup hook — AcceleratorClass is cluster-scoped so
// per-namespace cleanup doesn't apply. Tests that create ACs are
// responsible for deleting them in AfterEach.
