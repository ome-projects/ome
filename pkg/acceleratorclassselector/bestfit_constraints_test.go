package acceleratorclassselector

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestBestFitNilConstraintsMemoryScore verifies that omitted constraints yield a
// perfect memory fit without reading the candidate's memory specification.
func TestBestFitNilConstraintsMemoryScore(t *testing.T) {
	candidate := v1beta1.AcceleratorClass{}
	if got := calculateMemoryFitScore(candidate, nil); got != 1 {
		t.Fatalf("memory score without a requirement = %v, want 1", got)
	}
}

// TestBestFitNilConstraintsComputeScore verifies that omitted constraints preserve
// compute scoring for missing, empty, and populated performance data.
func TestBestFitNilConstraintsComputeScore(t *testing.T) {
	for _, tt := range []struct {
		name        string
		performance *v1beta1.AcceleratorPerformance
		want        float64
	}{
		{name: "missing performance"},
		{name: "empty performance", performance: &v1beta1.AcceleratorPerformance{}},
		{
			name:        "positive performance",
			performance: &v1beta1.AcceleratorPerformance{Fp16Tflops: int64Ptr(100)},
			want:        1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			candidate := v1beta1.AcceleratorClass{
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{Performance: tt.performance},
				},
			}
			if got := calculateComputePerformanceTFLOPSScore(candidate, nil); got != tt.want {
				t.Fatalf("compute score without constraints = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBestFitWithoutConstraints exercises public BestFit selection with multiple
// candidates when constraints are omitted or explicitly empty.
func TestBestFitWithoutConstraints(t *testing.T) {
	for _, tt := range []struct {
		name        string
		constraints *v1beta1.AcceleratorConstraints
	}{
		{name: "omitted"},
		{name: "empty", constraints: &v1beta1.AcceleratorConstraints{}},
	} {
		for _, withPerformance := range []bool{false, true} {
			name := tt.name + "/without performance"
			if withPerformance {
				name = tt.name + "/with performance"
			}
			t.Run(name, func(t *testing.T) {
				fetcher := newMockFetcher()
				fetcher.addAccelerator("no-performance", &v1beta1.AcceleratorClassSpec{})
				other := &v1beta1.AcceleratorClassSpec{}
				if withPerformance {
					other.Capabilities.Performance = &v1beta1.AcceleratorPerformance{
						Fp16Tflops: int64Ptr(100),
					}
				}
				fetcher.addAccelerator("other", other)
				selector := &defaultSelector{fetcher: fetcher}
				isvc := &v1beta1.InferenceService{
					Spec: v1beta1.InferenceServiceSpec{
						AcceleratorSelector: &v1beta1.AcceleratorSelector{
							Policy:      v1beta1.BestFitPolicy,
							Constraints: tt.constraints,
						},
					},
				}
				runtime := &v1beta1.ServingRuntimeSpec{
					AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
						AcceleratorClasses: []string{"no-performance", "other"},
					},
				}
				selected, name, err := selector.GetAcceleratorClass(context.Background(), isvc, runtime, v1beta1.EngineComponent)
				if err != nil {
					t.Fatalf("GetAcceleratorClass: %v", err)
				}
				if selected == nil || (name != "no-performance" && name != "other") {
					t.Fatalf("expected an eligible accelerator, got %q (%v)", name, selected)
				}
				if withPerformance && name != "other" {
					t.Fatalf("expected accelerator with performance data, got %q", name)
				}
			})
		}
	}
}
