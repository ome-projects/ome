package acceleratorclassselector

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// mockAcceleratorFetcher is a mock implementation of AcceleratorFetcher for testing
type mockAcceleratorFetcher struct {
	accelerators map[string]*v1beta1.AcceleratorClass
}

func newMockFetcher() *mockAcceleratorFetcher {
	return &mockAcceleratorFetcher{
		accelerators: make(map[string]*v1beta1.AcceleratorClass),
	}
}

func (m *mockAcceleratorFetcher) addAccelerator(name string, spec *v1beta1.AcceleratorClassSpec) {
	m.accelerators[name] = &v1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       *spec,
	}
}

func (m *mockAcceleratorFetcher) GetAcceleratorClass(ctx context.Context, name string) (*v1beta1.AcceleratorClass, bool, error) {
	ac, found := m.accelerators[name]
	return ac, found, nil
}

// createRealisticAccelerators creates H100, A100, and T4 AcceleratorClass specs with realistic data
func createRealisticAccelerators() map[string]*v1beta1.AcceleratorClassSpec {
	return map[string]*v1beta1.AcceleratorClassSpec{
		"nvidia-h100-80gb": {
			Vendor: "NVIDIA",
			Family: "Hopper",
			Model:  "H100",
			Capabilities: v1beta1.AcceleratorCapabilities{
				MemoryGB:            resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
				MemoryBandwidthGBps: resource.NewQuantity(3000, resource.DecimalSI),
				Performance: &v1beta1.AcceleratorPerformance{
					Fp32Tflops: ptr.To[int64](67),
					Fp16Tflops: ptr.To[int64](1979),
					Int8Tops:   ptr.To[int64](3958), // FP8 capable
					Int4Tops:   ptr.To[int64](7916),
				},
				Features:          []string{"cuda", "tensor-cores", "nvlink", "fp8"},
				ComputeCapability: "9.0",
			},
			Cost: &v1beta1.AcceleratorCost{
				PerHour:          resource.NewQuantity(4, resource.DecimalSI),
				SpotPerHour:      resource.NewQuantity(2, resource.DecimalSI),
				PerMillionTokens: resource.NewQuantity(100, resource.DecimalSI),
				Tier:             "high",
			},
		},
		"nvidia-a100-40gb": {
			Vendor: "NVIDIA",
			Family: "Ampere",
			Model:  "A100",
			Capabilities: v1beta1.AcceleratorCapabilities{
				MemoryGB:            resource.NewQuantity(40*1024*1024*1024, resource.BinarySI),
				MemoryBandwidthGBps: resource.NewQuantity(1555, resource.DecimalSI),
				Performance: &v1beta1.AcceleratorPerformance{
					Fp32Tflops: ptr.To[int64](19),
					Fp16Tflops: ptr.To[int64](312),
					Int8Tops:   ptr.To[int64](624),
				},
				Features:          []string{"cuda", "tensor-cores", "nvlink"},
				ComputeCapability: "8.0",
			},
			Cost: &v1beta1.AcceleratorCost{
				PerHour:          resource.NewQuantity(2, resource.DecimalSI),
				SpotPerHour:      resource.NewQuantity(1, resource.DecimalSI),
				PerMillionTokens: resource.NewQuantity(50, resource.DecimalSI),
				Tier:             "medium",
			},
		},
		"nvidia-t4": {
			Vendor: "NVIDIA",
			Family: "Turing",
			Model:  "T4",
			Capabilities: v1beta1.AcceleratorCapabilities{
				MemoryGB:            resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
				MemoryBandwidthGBps: resource.NewQuantity(320, resource.DecimalSI),
				Performance: &v1beta1.AcceleratorPerformance{
					Fp32Tflops: ptr.To[int64](8),
					Fp16Tflops: ptr.To[int64](65),
					Int8Tops:   ptr.To[int64](130),
				},
				Features:          []string{"cuda", "tensor-cores"},
				ComputeCapability: "7.5",
			},
			Cost: &v1beta1.AcceleratorCost{
				PerHour:          resource.NewQuantity(1, resource.DecimalSI),
				SpotPerHour:      resource.NewQuantity(0, resource.DecimalSI),
				PerMillionTokens: resource.NewQuantity(20, resource.DecimalSI),
				Tier:             "low",
			},
		},
	}
}

// TestSelectBestFit_Integration tests BestFit policy with realistic data
func TestSelectBestFit_Integration(t *testing.T) {
	tests := []struct {
		name           string
		constraints    *v1beta1.AcceleratorConstraints
		candidateNames []string
		expectedName   string
		description    string
	}{
		{
			name: "40GB requirement - A100 best fit",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory:           ptr.To[int64](40),
				PreferredPrecisions: []string{"fp16"},
			},
			candidateNames: []string{"nvidia-h100-80gb", "nvidia-a100-40gb"},
			expectedName:   "nvidia-a100-40gb",
			description:    "A100-40GB should be selected over H100-80GB to avoid over-provisioning",
		},
		{
			name: "80GB requirement - H100 only option",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory:           ptr.To[int64](80),
				PreferredPrecisions: []string{"fp16"},
			},
			candidateNames: []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
			expectedName:   "nvidia-h100-80gb",
			description:    "Only H100 meets 80GB requirement",
		},
		{
			name: "FP8 preference with high memory - H100 selected",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory:           ptr.To[int64](80),
				PreferredPrecisions: []string{"fp8", "fp16"},
			},
			candidateNames: []string{"nvidia-h100-80gb", "nvidia-a100-40gb"},
			expectedName:   "nvidia-h100-80gb",
			description:    "H100 exact memory match + fp8 support wins over A100",
		},
		{
			name: "NVLink required - filters out T4",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory:        ptr.To[int64](16),
				RequiredFeatures: []string{"nvlink"},
			},
			candidateNames: []string{"nvidia-a100-40gb", "nvidia-t4"},
			expectedName:   "nvidia-a100-40gb",
			description:    "T4 lacks nvlink, A100 selected",
		},
		{
			name: "Single candidate after filtering",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
				MaxMemory: ptr.To[int64](20),
			},
			candidateNames: []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
			expectedName:   "nvidia-t4",
			description:    "Only T4 within memory range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock fetcher
			mockFetcher := newMockFetcher()
			accelerators := createRealisticAccelerators()
			for name, spec := range accelerators {
				mockFetcher.addAccelerator(name, spec)
			}

			ctx := context.Background()

			// Fetch candidates
			candidates, err := candidatesFromNames(ctx, mockFetcher, tt.candidateNames)
			if err != nil {
				t.Fatalf("Failed to fetch candidates: %v", err)
			}

			// Filter candidates
			validCandidates := filterCandidates(ctx, candidates, tt.constraints)

			if len(validCandidates) == 0 {
				t.Fatalf("%s: No candidates passed filtering", tt.description)
			}

			// Select best fit
			selected := selectBestFit(ctx, validCandidates, tt.constraints)

			if selected == nil {
				t.Errorf("%s: selectBestFit() returned nil", tt.description)
			} else if *selected != tt.expectedName {
				t.Errorf("%s: selectBestFit() = %s, want %s", tt.description, *selected, tt.expectedName)
			}
		})
	}
}

// TestSelectCheapest_Integration tests Cheapest policy with realistic data
func TestSelectCheapest_Integration(t *testing.T) {
	tests := []struct {
		name           string
		constraints    *v1beta1.AcceleratorConstraints
		candidateNames []string
		expectedName   string
		description    string
	}{
		{
			name: "All meet requirements - T4 cheapest",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
			},
			candidateNames: []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
			expectedName:   "nvidia-t4",
			description:    "T4 has lowest cost ($1/hr vs $2/hr vs $4/hr)",
		},
		{
			name: "Spot pricing preferred",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](40),
			},
			candidateNames: []string{"nvidia-h100-80gb", "nvidia-a100-40gb"},
			expectedName:   "nvidia-a100-40gb",
			description:    "A100 spot ($1/hr) cheaper than H100 spot ($2/hr)",
		},
		{
			name: "High memory requirement - H100 vs A100",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](80),
			},
			candidateNames: []string{"nvidia-h100-80gb", "nvidia-a100-40gb"},
			expectedName:   "nvidia-h100-80gb",
			description:    "Only H100 meets requirement (A100 filtered out)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock fetcher
			mockFetcher := newMockFetcher()
			accelerators := createRealisticAccelerators()
			for name, spec := range accelerators {
				mockFetcher.addAccelerator(name, spec)
			}

			ctx := context.Background()

			// Fetch candidates
			candidates, err := candidatesFromNames(ctx, mockFetcher, tt.candidateNames)
			if err != nil {
				t.Fatalf("Failed to fetch candidates: %v", err)
			}

			// Filter candidates
			validCandidates := filterCandidates(ctx, candidates, tt.constraints)

			if len(validCandidates) == 0 {
				t.Fatalf("%s: No candidates passed filtering", tt.description)
			}

			// Select cheapest
			selected := selectCheapest(ctx, validCandidates)

			if selected == nil {
				t.Errorf("%s: selectCheapest() returned nil", tt.description)
			} else if *selected != tt.expectedName {
				t.Errorf("%s: selectCheapest() = %s, want %s", tt.description, *selected, tt.expectedName)
			}
		})
	}
}

// TestSelectCheapest_CostTypeGrouping tests that cost comparison groups same-unit costs together
func TestSelectCheapest_CostTypeGrouping(t *testing.T) {
	tests := []struct {
		name         string
		candidates   []v1beta1.AcceleratorClass
		expectedName string
		expectNil    bool
		description  string
	}{
		{
			name: "Mixed hourly types - spot vs on-demand same unit",
			candidates: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-a"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							SpotPerHour: resource.NewQuantity(5, resource.DecimalSI),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-b"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							PerHour: resource.NewQuantity(3, resource.DecimalSI),
						},
					},
				},
			},
			expectedName: "ac-b",
			description:  "B's on-demand $3 < A's spot $5, both in hourly group",
		},
		{
			name: "Hourly vs token - hourly group takes priority",
			candidates: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-hourly"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							PerHour: resource.NewQuantity(10, resource.DecimalSI),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-token"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							PerMillionTokens: resource.NewQuantity(1, resource.DecimalSI),
						},
					},
				},
			},
			expectedName: "ac-hourly",
			description:  "Hourly group wins over token group even if token value is lower",
		},
		{
			name: "Only token pricing",
			candidates: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-expensive-token"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							PerMillionTokens: resource.NewQuantity(50, resource.DecimalSI),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-cheap-token"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							PerMillionTokens: resource.NewQuantity(10, resource.DecimalSI),
						},
					},
				},
			},
			expectedName: "ac-cheap-token",
			description:  "When only token pricing exists, compare by tokens",
		},
		{
			name: "Only tier pricing",
			candidates: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-high"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							Tier: "high",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-low"},
					Spec: v1beta1.AcceleratorClassSpec{
						Cost: &v1beta1.AcceleratorCost{
							Tier: "low",
						},
					},
				},
			},
			expectedName: "ac-low",
			description:  "When only tier exists, pick lowest tier",
		},
		{
			name: "No cost data at all",
			candidates: []v1beta1.AcceleratorClass{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-no-cost-1"},
					Spec:       v1beta1.AcceleratorClassSpec{},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "ac-no-cost-2"},
					Spec:       v1beta1.AcceleratorClassSpec{},
				},
			},
			expectNil:   true,
			description: "No cost data returns nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			selected := selectCheapest(ctx, tt.candidates)

			if tt.expectNil {
				if selected != nil {
					t.Errorf("%s: expected nil, got %s", tt.description, *selected)
				}
				return
			}
			if selected == nil {
				t.Errorf("%s: selectCheapest() returned nil", tt.description)
			} else if *selected != tt.expectedName {
				t.Errorf("%s: selectCheapest() = %s, want %s", tt.description, *selected, tt.expectedName)
			}
		})
	}
}

// TestSelectMostCapable_Integration tests MostCapable policy with realistic data
func TestSelectMostCapable_Integration(t *testing.T) {
	tests := []struct {
		name                string
		constraints         *v1beta1.AcceleratorConstraints
		candidateNames      []string
		preferredPrecisions []string
		expectedName        string
		description         string
	}{
		{
			name: "FP8 workload - H100 most capable",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
			},
			candidateNames:      []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
			preferredPrecisions: []string{"fp8", "fp16"},
			expectedName:        "nvidia-h100-80gb",
			description:         "H100 has highest INT8 TOPS (3958) for fp8 workload",
		},
		{
			name: "FP16 workload - H100 most capable",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
			},
			candidateNames:      []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
			preferredPrecisions: []string{"fp16"},
			expectedName:        "nvidia-h100-80gb",
			description:         "H100 has highest FP16 TFLOPS (1979)",
		},
		{
			name: "FP32 workload - H100 most capable",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
			},
			candidateNames:      []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
			preferredPrecisions: []string{"fp32"},
			expectedName:        "nvidia-h100-80gb",
			description:         "H100 has highest FP32 TFLOPS (67)",
		},
		{
			name: "Memory constraint filters H100",
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
				MaxMemory: ptr.To[int64](50),
			},
			candidateNames:      []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
			preferredPrecisions: []string{"fp16"},
			expectedName:        "nvidia-a100-40gb",
			description:         "H100 filtered by MaxMemory, A100 most capable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock fetcher
			mockFetcher := newMockFetcher()
			accelerators := createRealisticAccelerators()
			for name, spec := range accelerators {
				mockFetcher.addAccelerator(name, spec)
			}

			ctx := context.Background()

			// Fetch candidates
			candidates, err := candidatesFromNames(ctx, mockFetcher, tt.candidateNames)
			if err != nil {
				t.Fatalf("Failed to fetch candidates: %v", err)
			}

			// Filter candidates
			validCandidates := filterCandidates(ctx, candidates, tt.constraints)

			if len(validCandidates) == 0 {
				t.Fatalf("%s: No candidates passed filtering", tt.description)
			}

			// Select most capable
			selected := selectMostCapable(ctx, validCandidates, tt.preferredPrecisions)

			if selected == nil {
				t.Errorf("%s: selectMostCapable() returned nil", tt.description)
			} else if *selected != tt.expectedName {
				t.Errorf("%s: selectMostCapable() = %s, want %s", tt.description, *selected, tt.expectedName)
			}
		})
	}
}

// TestGetAcceleratorClassByPolicy_AllPolicies tests the main policy routing logic
func TestGetAcceleratorClassByPolicy_AllPolicies(t *testing.T) {
	tests := []struct {
		name         string
		policy       v1beta1.AcceleratorSelectionPolicy
		constraints  *v1beta1.AcceleratorConstraints
		expectedName string
		description  string
	}{
		{
			name:   "BestFit policy - A100 for 40GB",
			policy: v1beta1.BestFitPolicy,
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory:           ptr.To[int64](40),
				PreferredPrecisions: []string{"fp16"},
			},
			expectedName: "nvidia-a100-40gb",
			description:  "BestFit should select A100 to avoid over-provisioning",
		},
		{
			name:   "Cheapest policy - T4 for 16GB",
			policy: v1beta1.CheapestPolicy,
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
			},
			expectedName: "nvidia-t4",
			description:  "Cheapest should select T4 ($1/hr)",
		},
		{
			name:   "MostCapable policy - H100 for fp8",
			policy: v1beta1.MostCapablePolicy,
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory:           ptr.To[int64](16),
				PreferredPrecisions: []string{"fp8"},
			},
			expectedName: "nvidia-h100-80gb",
			description:  "MostCapable should select H100 for fp8 workload",
		},
		{
			name:   "FirstAvailable policy - first in list",
			policy: v1beta1.FirstAvailablePolicy,
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](16),
			},
			expectedName: "nvidia-h100-80gb",
			description:  "FirstAvailable should select first in runtime list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock fetcher
			mockFetcher := newMockFetcher()
			accelerators := createRealisticAccelerators()
			for name, spec := range accelerators {
				mockFetcher.addAccelerator(name, spec)
			}

			// Create selector
			selector := &defaultSelector{fetcher: mockFetcher}

			// Create ServingRuntime
			runtime := &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"},
				},
			}

			ctx := context.Background()

			// Call getAcceleratorClassByPolicy
			selected, err := selector.getAcceleratorClassByPolicy(ctx, runtime, tt.policy, tt.constraints)
			if err != nil {
				t.Fatalf("%s: getAcceleratorClassByPolicy() error: %v", tt.description, err)
			}

			if selected == nil {
				t.Errorf("%s: getAcceleratorClassByPolicy() returned nil", tt.description)
			} else if *selected != tt.expectedName {
				t.Errorf("%s: getAcceleratorClassByPolicy() = %s, want %s",
					tt.description, *selected, tt.expectedName)
			}
		})
	}
}

// TestGetAcceleratorClassByPolicy_BestFitWithoutConstraints: a service that states no
// constraints, or an empty constraints block, gets a BestFit choice ranked as if nothing
// were required, never a panic.
func TestGetAcceleratorClassByPolicy_BestFitWithoutConstraints(t *testing.T) {
	tests := []struct {
		name        string
		constraints *v1beta1.AcceleratorConstraints
	}{
		{name: "nil constraints", constraints: nil},
		{name: "empty constraints", constraints: &v1beta1.AcceleratorConstraints{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The first listed class states no compute figures, so BestFit ranks the second above it.
			mockFetcher := newMockFetcher()
			mockFetcher.addAccelerator("memory-only", &v1beta1.AcceleratorClassSpec{
				Capabilities: v1beta1.AcceleratorCapabilities{
					MemoryGB: resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
				},
			})
			mockFetcher.addAccelerator("with-compute-figures", &v1beta1.AcceleratorClassSpec{
				Capabilities: v1beta1.AcceleratorCapabilities{
					MemoryGB:    resource.NewQuantity(40*1024*1024*1024, resource.BinarySI),
					Performance: &v1beta1.AcceleratorPerformance{Fp16Tflops: ptr.To[int64](312)},
				},
			})
			selector := &defaultSelector{fetcher: mockFetcher}

			runtime := &v1beta1.ServingRuntimeSpec{
				AcceleratorRequirements: &v1beta1.AcceleratorRequirements{
					AcceleratorClasses: []string{"memory-only", "with-compute-figures"},
				},
			}

			selected, err := selector.getAcceleratorClassByPolicy(context.Background(), runtime, v1beta1.BestFitPolicy, tt.constraints)
			if err != nil {
				t.Fatalf("getAcceleratorClassByPolicy() error: %v", err)
			}
			if selected == nil {
				t.Fatal("getAcceleratorClassByPolicy() returned nil, want with-compute-figures")
			}
			if *selected != "with-compute-figures" {
				t.Errorf("getAcceleratorClassByPolicy() = %s, want with-compute-figures", *selected)
			}
		})
	}
}

// realisticSelector is a selector over the H100, A100 and T4 classes.
func realisticSelector() *defaultSelector {
	mockFetcher := newMockFetcher()
	for name, spec := range createRealisticAccelerators() {
		mockFetcher.addAccelerator(name, spec)
	}
	return &defaultSelector{fetcher: mockFetcher}
}

// selectedEngineClass is the class GetAcceleratorClass names for the engine of a
// service with the given service-level selector and engine override, on a runtime
// that lists the classes in order; "" when it names none.
func selectedEngineClass(t *testing.T, selector *defaultSelector, service, override *v1beta1.AcceleratorSelector, classes []string) string {
	t.Helper()
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			AcceleratorSelector: service,
			Engine:              &v1beta1.EngineSpec{AcceleratorOverride: override},
		},
	}
	runtime := &v1beta1.ServingRuntimeSpec{
		AcceleratorRequirements: &v1beta1.AcceleratorRequirements{AcceleratorClasses: classes},
	}
	_, name, err := selector.GetAcceleratorClass(context.Background(), isvc, runtime, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("GetAcceleratorClass() error: %v", err)
	}
	return name
}

// TestGetAcceleratorClass_OverrideConstraintsGovernThePolicyChoice: the policy
// filters and scores the runtime's classes with the constraints of the first
// selector that states a block, the component's override before the service's
// selector; the block is taken whole, never merged with the service's, and an
// empty block counts as stated.
func TestGetAcceleratorClass_OverrideConstraintsGovernThePolicyChoice(t *testing.T) {
	minMemory := func(gb int64) *v1beta1.AcceleratorConstraints {
		return &v1beta1.AcceleratorConstraints{MinMemory: ptr.To(gb)}
	}
	tests := []struct {
		name     string
		service  *v1beta1.AcceleratorSelector
		override *v1beta1.AcceleratorSelector
		want     string
	}{
		{
			name:     "the override's minMemory excludes the class the service's block admits",
			service:  &v1beta1.AcceleratorSelector{Policy: v1beta1.CheapestPolicy, Constraints: minMemory(16)},
			override: &v1beta1.AcceleratorSelector{Constraints: minMemory(40)},
			want:     "nvidia-a100-40gb",
		},
		{
			name:     "an override without a block falls back to the service's block",
			service:  &v1beta1.AcceleratorSelector{Constraints: minMemory(40)},
			override: &v1beta1.AcceleratorSelector{Policy: v1beta1.CheapestPolicy},
			want:     "nvidia-a100-40gb",
		},
		{
			name:     "an empty override block clears the service's constraints for this component",
			service:  &v1beta1.AcceleratorSelector{Policy: v1beta1.CheapestPolicy, Constraints: minMemory(40)},
			override: &v1beta1.AcceleratorSelector{Constraints: &v1beta1.AcceleratorConstraints{}},
			want:     "nvidia-t4",
		},
		{
			name:     "the override's block replaces the service's whole and is not merged with it",
			service:  &v1beta1.AcceleratorSelector{Policy: v1beta1.CheapestPolicy, Constraints: minMemory(40)},
			override: &v1beta1.AcceleratorSelector{Constraints: &v1beta1.AcceleratorConstraints{ExcludedClasses: []string{"nvidia-a100-40gb"}}},
			want:     "nvidia-t4",
		},
		{
			name:     "BestFit scores the memory fit against the override's minMemory",
			service:  &v1beta1.AcceleratorSelector{Policy: v1beta1.BestFitPolicy},
			override: &v1beta1.AcceleratorSelector{Constraints: minMemory(40)},
			want:     "nvidia-a100-40gb",
		},
	}
	classes := []string{"nvidia-h100-80gb", "nvidia-a100-40gb", "nvidia-t4"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectedEngineClass(t, realisticSelector(), tt.service, tt.override, classes); got != tt.want {
				t.Errorf("GetAcceleratorClass() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestGetAcceleratorClass_OverridePrecisionsGovernMostCapable: MostCapable ranks
// compute at the precision the component's override prefers, not the one the
// service's selector prefers.
func TestGetAcceleratorClass_OverridePrecisionsGovernMostCapable(t *testing.T) {
	mockFetcher := newMockFetcher()
	mockFetcher.addAccelerator("fp32-class", &v1beta1.AcceleratorClassSpec{
		Capabilities: v1beta1.AcceleratorCapabilities{
			MemoryGB:    resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
			Performance: &v1beta1.AcceleratorPerformance{Fp32Tflops: ptr.To[int64](100)},
		},
	})
	mockFetcher.addAccelerator("fp8-class", &v1beta1.AcceleratorClassSpec{
		Capabilities: v1beta1.AcceleratorCapabilities{
			MemoryGB:    resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
			Performance: &v1beta1.AcceleratorPerformance{Int8Tops: ptr.To[int64](100)},
		},
	})
	selector := &defaultSelector{fetcher: mockFetcher}
	service := &v1beta1.AcceleratorSelector{
		Policy:      v1beta1.MostCapablePolicy,
		Constraints: &v1beta1.AcceleratorConstraints{PreferredPrecisions: []string{"fp32"}},
	}
	override := &v1beta1.AcceleratorSelector{
		Constraints: &v1beta1.AcceleratorConstraints{PreferredPrecisions: []string{"fp8"}},
	}
	if got := selectedEngineClass(t, selector, service, override, []string{"fp32-class", "fp8-class"}); got != "fp8-class" {
		t.Errorf("GetAcceleratorClass() = %q, want %q", got, "fp8-class")
	}
}

// TestEdgeCases tests edge cases for policy selection
func TestEdgeCases(t *testing.T) {
	t.Run("Empty candidate list", func(t *testing.T) {
		ctx := context.Background()
		validCandidates := []v1beta1.AcceleratorClass{}

		selected := selectBestFit(ctx, validCandidates, nil)
		if selected != nil {
			t.Errorf("Empty candidates should return nil, got %s", *selected)
		}
	})

	t.Run("All candidates filtered out", func(t *testing.T) {
		mockFetcher := newMockFetcher()
		accelerators := createRealisticAccelerators()
		for name, spec := range accelerators {
			mockFetcher.addAccelerator(name, spec)
		}

		ctx := context.Background()
		candidates, _ := candidatesFromNames(ctx, mockFetcher, []string{"nvidia-t4"})

		// Filter with impossible constraint
		constraints := &v1beta1.AcceleratorConstraints{
			MinMemory: ptr.To[int64](100), // T4 only has 16GB
		}
		validCandidates := filterCandidates(ctx, candidates, constraints)

		if len(validCandidates) != 0 {
			t.Errorf("Expected all candidates filtered, got %d", len(validCandidates))
		}
	})

	t.Run("Single candidate optimization", func(t *testing.T) {
		mockFetcher := newMockFetcher()
		accelerators := createRealisticAccelerators()
		for name, spec := range accelerators {
			mockFetcher.addAccelerator(name, spec)
		}

		ctx := context.Background()
		candidates, _ := candidatesFromNames(ctx, mockFetcher, []string{"nvidia-t4"})

		// Should return immediately without scoring
		selected := selectBestFit(ctx, candidates, nil)
		if selected == nil || *selected != "nvidia-t4" {
			t.Errorf("Single candidate should be selected immediately")
		}
	})

	t.Run("Cheapest with no cost data for multiple candidates", func(t *testing.T) {
		mockFetcher := newMockFetcher()
		// Add multiple accelerators without cost data
		mockFetcher.addAccelerator("no-cost-gpu-1", &v1beta1.AcceleratorClassSpec{
			Capabilities: v1beta1.AcceleratorCapabilities{
				MemoryGB: resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
			},
		})
		mockFetcher.addAccelerator("no-cost-gpu-2", &v1beta1.AcceleratorClassSpec{
			Capabilities: v1beta1.AcceleratorCapabilities{
				MemoryGB: resource.NewQuantity(32*1024*1024*1024, resource.BinarySI),
			},
		})

		ctx := context.Background()
		candidates, _ := candidatesFromNames(ctx, mockFetcher, []string{"no-cost-gpu-1", "no-cost-gpu-2"})

		selected := selectCheapest(ctx, candidates)
		if selected != nil {
			t.Errorf("Cheapest with no cost data should return nil, got %s", *selected)
		}
	})

	t.Run("Single candidate without cost data - still selected", func(t *testing.T) {
		mockFetcher := newMockFetcher()
		// Add single accelerator without cost data
		mockFetcher.addAccelerator("no-cost-gpu", &v1beta1.AcceleratorClassSpec{
			Capabilities: v1beta1.AcceleratorCapabilities{
				MemoryGB: resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
			},
		})

		ctx := context.Background()
		candidates, _ := candidatesFromNames(ctx, mockFetcher, []string{"no-cost-gpu"})

		selected := selectCheapest(ctx, candidates)
		if selected == nil || *selected != "no-cost-gpu" {
			t.Errorf("Single candidate should be selected even without cost data")
		}
	})
}
