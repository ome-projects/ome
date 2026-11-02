package acceleratorclassselector

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestScoreFromTFLOPS tests the scoreFromTFLOPS helper function
func TestScoreFromTFLOPS(t *testing.T) {
	tests := []struct {
		name           string
		tflops         int64
		requiredTFLOPS int64
		expectedScore  float64
	}{
		{
			name:           "Zero TFLOPS",
			tflops:         0,
			requiredTFLOPS: 100,
			expectedScore:  0.0,
		},
		{
			name:           "No requirement (zero required)",
			tflops:         100,
			requiredTFLOPS: 0,
			expectedScore:  1.0,
		},
		{
			name:           "Exact match",
			tflops:         100,
			requiredTFLOPS: 100,
			expectedScore:  1.0,
		},
		{
			name:           "Exceeds requirement",
			tflops:         200,
			requiredTFLOPS: 100,
			expectedScore:  1.0, // Capped at 1.0
		},
		{
			name:           "Half of requirement",
			tflops:         50,
			requiredTFLOPS: 100,
			expectedScore:  0.5,
		},
		{
			name:           "Quarter of requirement",
			tflops:         25,
			requiredTFLOPS: 100,
			expectedScore:  0.25,
		},
		{
			name:           "Slightly below requirement",
			tflops:         95,
			requiredTFLOPS: 100,
			expectedScore:  0.95,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := scoreFromTFLOPS(tt.tflops, tt.requiredTFLOPS)
			if score != tt.expectedScore {
				t.Errorf("scoreFromTFLOPS(%d, %d) = %f, want %f",
					tt.tflops, tt.requiredTFLOPS, score, tt.expectedScore)
			}
		})
	}
}

// TestGetTFLOPSForPrecision tests precision-based TFLOPS extraction
func TestGetTFLOPSForPrecision(t *testing.T) {
	perf := &v1beta1.AcceleratorPerformance{
		Fp32Tflops: ptr.To[int64](100),
		Fp16Tflops: ptr.To[int64](200),
		Int8Tops:   ptr.To[int64](400),
		Int4Tops:   ptr.To[int64](800),
	}

	tests := []struct {
		name           string
		perf           *v1beta1.AcceleratorPerformance
		precision      string
		expectedTFLOPS int64
	}{
		{
			name:           "FP32 precision",
			perf:           perf,
			precision:      "fp32",
			expectedTFLOPS: 100,
		},
		{
			name:           "FP16 precision",
			perf:           perf,
			precision:      "fp16",
			expectedTFLOPS: 200,
		},
		{
			name:           "INT8 precision",
			perf:           perf,
			precision:      "int8",
			expectedTFLOPS: 400,
		},
		{
			name:           "FP8 maps to INT8",
			perf:           perf,
			precision:      "fp8",
			expectedTFLOPS: 400,
		},
		{
			name:           "INT4 precision",
			perf:           perf,
			precision:      "int4",
			expectedTFLOPS: 800,
		},
		{
			name:           "Case insensitive - FP16",
			perf:           perf,
			precision:      "FP16",
			expectedTFLOPS: 200,
		},
		{
			name:           "Nil performance struct",
			perf:           nil,
			precision:      "fp16",
			expectedTFLOPS: 0,
		},
		{
			name: "Missing FP16 field",
			perf: &v1beta1.AcceleratorPerformance{
				Fp32Tflops: ptr.To[int64](100),
			},
			precision:      "fp16",
			expectedTFLOPS: 0,
		},
		{
			name:           "Unknown precision",
			perf:           perf,
			precision:      "unknown",
			expectedTFLOPS: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tflops := getTFLOPSForPrecision(tt.perf, tt.precision)
			if tflops != tt.expectedTFLOPS {
				t.Errorf("getTFLOPSForPrecision(%v, %s) = %d, want %d",
					tt.perf, tt.precision, tflops, tt.expectedTFLOPS)
			}
		})
	}
}

// TestGetMaxTFLOPS tests getting maximum TFLOPS across all precisions
func TestGetMaxTFLOPS(t *testing.T) {
	tests := []struct {
		name        string
		perf        *v1beta1.AcceleratorPerformance
		expectedMax int64
	}{
		{
			name: "INT4 is maximum",
			perf: &v1beta1.AcceleratorPerformance{
				Fp32Tflops: ptr.To[int64](100),
				Fp16Tflops: ptr.To[int64](200),
				Int8Tops:   ptr.To[int64](400),
				Int4Tops:   ptr.To[int64](800),
			},
			expectedMax: 800,
		},
		{
			name: "FP32 is maximum",
			perf: &v1beta1.AcceleratorPerformance{
				Fp32Tflops: ptr.To[int64](500),
				Fp16Tflops: ptr.To[int64](200),
			},
			expectedMax: 500,
		},
		{
			name: "Only FP16 available",
			perf: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](200),
			},
			expectedMax: 200,
		},
		{
			name:        "Nil performance struct",
			perf:        nil,
			expectedMax: 0,
		},
		{
			name:        "Empty performance struct",
			perf:        &v1beta1.AcceleratorPerformance{},
			expectedMax: 0,
		},
		{
			name: "All zero values",
			perf: &v1beta1.AcceleratorPerformance{
				Fp32Tflops: ptr.To[int64](0),
				Fp16Tflops: ptr.To[int64](0),
			},
			expectedMax: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			max := getMaxTFLOPS(tt.perf)
			if max != tt.expectedMax {
				t.Errorf("getMaxTFLOPS() = %d, want %d", max, tt.expectedMax)
			}
		})
	}
}

// TestCalculateMemoryFitScore tests memory fit scoring with over-provisioning penalty
func TestCalculateMemoryFitScore(t *testing.T) {
	tests := []struct {
		name           string
		candidateMemGB int64
		requiredMemGB  *int64
		expectedScore  float64
		description    string
	}{
		{
			name:           "No memory requirement",
			candidateMemGB: 40,
			requiredMemGB:  nil,
			expectedScore:  1.0,
			description:    "Should return perfect score when no requirement",
		},
		{
			name:           "Exact match",
			candidateMemGB: 40,
			requiredMemGB:  ptr.To[int64](40),
			expectedScore:  1.0,
			description:    "Exact match should score 1.0",
		},
		{
			name:           "2x over-provisioned",
			candidateMemGB: 80,
			requiredMemGB:  ptr.To[int64](40),
			expectedScore:  0.5,
			description:    "2x over = 0.5 score (1.0 / 2.0)",
		},
		{
			name:           "10x over-provisioned",
			candidateMemGB: 400,
			requiredMemGB:  ptr.To[int64](40),
			expectedScore:  0.1,
			description:    "10x over = 0.1 score (1.0 / 10.0), no cap",
		},
		{
			name:           "4x over-provisioned",
			candidateMemGB: 160,
			requiredMemGB:  ptr.To[int64](40),
			expectedScore:  0.25,
			description:    "4x over = 0.25 score",
		},
		{
			name:           "Slightly over-provisioned (1.25x)",
			candidateMemGB: 50,
			requiredMemGB:  ptr.To[int64](40),
			expectedScore:  0.8,
			description:    "1.25x over = 0.8 score",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create candidate with memory
			memBytes := tt.candidateMemGB * 1024 * 1024 * 1024
			memQuantity := resource.NewQuantity(memBytes, resource.BinarySI)

			candidate := v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-accelerator"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						MemoryGB: memQuantity,
					},
				},
			}

			constraints := &v1beta1.AcceleratorConstraints{
				MinMemory: tt.requiredMemGB,
			}

			score := calculateMemoryFitScore(candidate, constraints)

			// Allow small floating point tolerance
			tolerance := 0.01
			if score < tt.expectedScore-tolerance || score > tt.expectedScore+tolerance {
				t.Errorf("%s: calculateMemoryFitScore() = %f, want %f (tolerance ±%f)",
					tt.description, score, tt.expectedScore, tolerance)
			}
		})
	}
}

// TestCalculateComputePerformanceTFLOPSScore tests compute scoring with precision fallback
func TestCalculateComputePerformanceTFLOPSScore(t *testing.T) {
	tests := []struct {
		name                string
		performance         *v1beta1.AcceleratorPerformance
		requiredTFLOPS      *int64
		preferredPrecisions []string
		expectedScore       float64
		description         string
	}{
		{
			name: "No requirement - perfect score",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](100),
			},
			requiredTFLOPS:      nil,
			preferredPrecisions: []string{"fp16"},
			expectedScore:       1.0,
			description:         "No MinComputePerformanceTFLOPS requirement should return 1.0",
		},
		{
			name: "First precision exact match",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](100),
			},
			requiredTFLOPS:      ptr.To[int64](100),
			preferredPrecisions: []string{"fp16"},
			expectedScore:       1.0,
			description:         "First precision meeting requirement gets full score",
		},
		{
			name: "First precision exceeds requirement",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](200),
			},
			requiredTFLOPS:      ptr.To[int64](100),
			preferredPrecisions: []string{"fp16"},
			expectedScore:       1.0,
			description:         "Exceeding requirement caps at 1.0",
		},
		{
			name: "First precision half of requirement",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](50),
			},
			requiredTFLOPS:      ptr.To[int64](100),
			preferredPrecisions: []string{"fp16"},
			expectedScore:       0.5,
			description:         "50 TFLOPS / 100 required = 0.5 score",
		},
		{
			name: "Second precision fallback with penalty",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](100),
			},
			requiredTFLOPS:      ptr.To[int64](100),
			preferredPrecisions: []string{"fp8", "fp16"},
			expectedScore:       0.5,
			description:         "Second precision gets 0.5 penalty (1.0 * 0.5)",
		},
		{
			name: "Third precision fallback with penalty",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](100),
			},
			requiredTFLOPS:      ptr.To[int64](100),
			preferredPrecisions: []string{"int4", "int8", "fp16"},
			expectedScore:       0.25,
			description:         "Third precision gets 0.25 penalty (1.0 * 0.5 * 0.5)",
		},
		{
			name: "No preferred precisions - use max",
			performance: &v1beta1.AcceleratorPerformance{
				Fp32Tflops: ptr.To[int64](50),
				Fp16Tflops: ptr.To[int64](100),
				Int8Tops:   ptr.To[int64](200),
			},
			requiredTFLOPS:      ptr.To[int64](200),
			preferredPrecisions: []string{},
			expectedScore:       1.0,
			description:         "No preference uses max TFLOPS (INT8 = 200)",
		},
		{
			name:                "No precision has TFLOPS data",
			performance:         &v1beta1.AcceleratorPerformance{},
			requiredTFLOPS:      ptr.To[int64](100),
			preferredPrecisions: []string{"fp16"},
			expectedScore:       0.0,
			description:         "No TFLOPS data returns 0.0",
		},
		{
			name:                "Nil performance struct",
			performance:         nil,
			requiredTFLOPS:      ptr.To[int64](100),
			preferredPrecisions: []string{"fp16"},
			expectedScore:       0.0,
			description:         "Nil performance returns 0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-accelerator"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						Performance: tt.performance,
					},
				},
			}

			constraints := &v1beta1.AcceleratorConstraints{
				MinComputePerformanceTFLOPS: tt.requiredTFLOPS,
				PreferredPrecisions:         tt.preferredPrecisions,
			}

			score := calculateComputePerformanceTFLOPSScore(candidate, constraints)

			tolerance := 0.01
			if score < tt.expectedScore-tolerance || score > tt.expectedScore+tolerance {
				t.Errorf("%s: calculateComputePerformanceTFLOPSScore() = %f, want %f",
					tt.description, score, tt.expectedScore)
			}
		})
	}
}

// TestCalculateBestFitScore tests the overall BestFit scoring (70-30 weights: memory-precision)
func TestCalculateBestFitScore(t *testing.T) {
	tests := []struct {
		name                string
		candidateMemGB      int64
		candidateTFLOPS     int64
		requiredMemGB       *int64
		precision           string
		preferredPrecisions []string
		expectedScore       float64
		description         string
	}{
		{
			name:                "Perfect match on memory and precision",
			candidateMemGB:      40,
			candidateTFLOPS:     100,
			requiredMemGB:       ptr.To[int64](40),
			precision:           "fp16",
			preferredPrecisions: []string{"fp16"},
			expectedScore:       1.0,
			description:         "Perfect match should score 1.0",
		},
		{
			name:                "Memory 2x over, precision exact",
			candidateMemGB:      80,
			candidateTFLOPS:     100,
			requiredMemGB:       ptr.To[int64](40),
			precision:           "fp16",
			preferredPrecisions: []string{"fp16"},
			expectedScore:       0.65, // 0.7 * 0.5 (mem) + 0.3 * 1.0 (precision)
			description:         "2x memory over-provisioning",
		},
		{
			name:                "Memory exact, precision match",
			candidateMemGB:      40,
			candidateTFLOPS:     100,
			requiredMemGB:       ptr.To[int64](40),
			precision:           "fp16",
			preferredPrecisions: []string{"fp16"},
			expectedScore:       1.0, // 0.7 * 1.0 (mem) + 0.3 * 1.0 (precision)
			description:         "Exact memory and precision match",
		},
		{
			name:                "Memory exact, second precision match",
			candidateMemGB:      40,
			candidateTFLOPS:     100,
			requiredMemGB:       ptr.To[int64](40),
			precision:           "fp16",
			preferredPrecisions: []string{"fp8", "fp16"},
			expectedScore:       0.85, // 0.7 * 1.0 (mem) + 0.3 * 0.5 (compute: fp16 with 0.5 penalty)
			description:         "Memory exact, second precision in preference list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memBytes := tt.candidateMemGB * 1024 * 1024 * 1024
			memQuantity := resource.NewQuantity(memBytes, resource.BinarySI)

			var perf *v1beta1.AcceleratorPerformance
			switch tt.precision {
			case "fp16":
				perf = &v1beta1.AcceleratorPerformance{
					Fp16Tflops: &tt.candidateTFLOPS,
				}
			case "fp32":
				perf = &v1beta1.AcceleratorPerformance{
					Fp32Tflops: &tt.candidateTFLOPS,
				}
			case "fp8":
				perf = &v1beta1.AcceleratorPerformance{
					Int8Tops: &tt.candidateTFLOPS,
				}
			}

			candidate := v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-accelerator"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						MemoryGB:    memQuantity,
						Performance: perf,
					},
				},
			}

			constraints := &v1beta1.AcceleratorConstraints{
				MinMemory:           tt.requiredMemGB,
				PreferredPrecisions: tt.preferredPrecisions,
			}

			score := calculateBestFitScore(candidate, constraints)

			tolerance := 0.02
			if score < tt.expectedScore-tolerance || score > tt.expectedScore+tolerance {
				t.Errorf("%s: calculateBestFitScore() = %f, want %f",
					tt.description, score, tt.expectedScore)
			}
		})
	}
}

// TestCostGroupCheaper asserts the order inside each cost group: the lower figure is
// cheaper, equal figures are not, and the spot price stands for the hourly figure.
func TestCostGroupCheaper(t *testing.T) {
	quantity := func(v int64) *resource.Quantity { return resource.NewQuantity(v, resource.DecimalSI) }
	tests := []struct {
		name  string
		group string
		cheap v1beta1.AcceleratorCost
		dear  v1beta1.AcceleratorCost
	}{
		{
			name:  "hourly on-demand",
			group: "hourly",
			cheap: v1beta1.AcceleratorCost{PerHour: quantity(1)},
			dear:  v1beta1.AcceleratorCost{PerHour: quantity(2)},
		},
		{
			name:  "hourly spot price counts before on-demand",
			group: "hourly",
			cheap: v1beta1.AcceleratorCost{PerHour: quantity(5), SpotPerHour: quantity(1)},
			dear:  v1beta1.AcceleratorCost{PerHour: quantity(2)},
		},
		{
			name:  "per million tokens",
			group: "per-million-tokens",
			cheap: v1beta1.AcceleratorCost{PerMillionTokens: quantity(50)},
			dear:  v1beta1.AcceleratorCost{PerMillionTokens: quantity(100)},
		},
		{
			name:  "tier",
			group: "tier",
			cheap: v1beta1.AcceleratorCost{Tier: "low"},
			dear:  v1beta1.AcceleratorCost{Tier: "high"},
		},
		{
			name:  "unknown tier counts as medium",
			group: "tier",
			cheap: v1beta1.AcceleratorCost{Tier: "bronze"},
			dear:  v1beta1.AcceleratorCost{Tier: "high"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var group *costGroup
			for i := range costGroups {
				if costGroups[i].label == tt.group {
					group = &costGroups[i]
				}
			}
			if group == nil {
				t.Fatalf("no cost group %q", tt.group)
			}
			if !group.stated(tt.cheap) || !group.stated(tt.dear) {
				t.Fatalf("both costs should state a %s figure", tt.group)
			}
			if !group.cheaper(tt.cheap, tt.dear) {
				t.Errorf("cheaper(cheap, dear) = false, want true")
			}
			if group.cheaper(tt.dear, tt.cheap) {
				t.Errorf("cheaper(dear, cheap) = true, want false")
			}
			if group.cheaper(tt.cheap, tt.cheap) {
				t.Errorf("cheaper(cheap, cheap) = true, want false")
			}
		})
	}
}

// TestHighestScoring asserts the choice among scored candidates: the highest score wins,
// the first listed wins a tie, and nothing is scored when fewer than two remain.
func TestHighestScoring(t *testing.T) {
	named := func(names ...string) []v1beta1.AcceleratorClass {
		candidates := make([]v1beta1.AcceleratorClass, 0, len(names))
		for _, name := range names {
			candidates = append(candidates, v1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{Name: name}})
		}
		return candidates
	}
	byName := func(scores map[string]float64) func(v1beta1.AcceleratorClass) float64 {
		return func(c v1beta1.AcceleratorClass) float64 { return scores[c.Name] }
	}
	ctx := context.Background()

	t.Run("Highest score wins", func(t *testing.T) {
		score := byName(map[string]float64{"c1": 0.5, "c2": 0.9, "c3": 0.3, "c4": 0.7})
		selected := highestScoring(ctx, "BestFit", named("c1", "c2", "c3", "c4"), score)
		if selected == nil || *selected != "c2" {
			t.Errorf("got %v, want c2", selected)
		}
	})

	t.Run("First listed wins a tie", func(t *testing.T) {
		score := byName(map[string]float64{"c1": 0.7, "c2": 0.9, "c3": 0.9})
		selected := highestScoring(ctx, "BestFit", named("c1", "c2", "c3"), score)
		if selected == nil || *selected != "c2" {
			t.Errorf("got %v, want c2", selected)
		}
	})

	t.Run("Empty slice", func(t *testing.T) {
		selected := highestScoring(ctx, "BestFit", nil, byName(nil))
		if selected != nil {
			t.Errorf("got %s, want nil", *selected)
		}
	})

	t.Run("Single element is chosen without scoring", func(t *testing.T) {
		score := func(c v1beta1.AcceleratorClass) float64 {
			t.Fatalf("scored %s", c.Name)
			return 0
		}
		selected := highestScoring(ctx, "BestFit", named("only"), score)
		if selected == nil || *selected != "only" {
			t.Errorf("got %v, want only", selected)
		}
	})
}

// TestCalculateCapabilityScore tests MostCapable scoring
func TestCalculateCapabilityScore(t *testing.T) {
	tests := []struct {
		name                string
		performance         *v1beta1.AcceleratorPerformance
		memoryGB            int64
		bandwidthGBps       int64
		preferredPrecisions []string
		expectedNonZero     bool
		description         string
	}{
		{
			name: "High performance GPU",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](1000),
			},
			memoryGB:            80,
			bandwidthGBps:       2000,
			preferredPrecisions: []string{"fp16"},
			expectedNonZero:     true,
			description:         "Should score high for capable GPU",
		},
		{
			name: "Low performance GPU",
			performance: &v1beta1.AcceleratorPerformance{
				Fp16Tflops: ptr.To[int64](100),
			},
			memoryGB:            16,
			bandwidthGBps:       500,
			preferredPrecisions: []string{"fp16"},
			expectedNonZero:     true,
			description:         "Should score lower but non-zero",
		},
		{
			name:                "No performance data - memory fallback",
			performance:         nil,
			memoryGB:            40,
			bandwidthGBps:       1000,
			preferredPrecisions: []string{"fp16"},
			expectedNonZero:     true,
			description:         "Should use memory fallback when no performance data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memBytes := tt.memoryGB * 1024 * 1024 * 1024
			memQuantity := resource.NewQuantity(memBytes, resource.BinarySI)
			bandwidthQuantity := resource.NewQuantity(tt.bandwidthGBps, resource.DecimalSI)

			candidate := v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-accelerator"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						MemoryGB:            memQuantity,
						MemoryBandwidthGBps: bandwidthQuantity,
						Performance:         tt.performance,
					},
				},
			}

			maxVals := computeMaxValues([]v1beta1.AcceleratorClass{candidate}, tt.preferredPrecisions)
			score := calculateCapabilityScore(candidate, tt.preferredPrecisions, maxVals)

			if tt.expectedNonZero && score == 0.0 {
				t.Errorf("%s: expected non-zero score, got 0.0", tt.description)
			}
			if !tt.expectedNonZero && score != 0.0 {
				t.Errorf("%s: expected 0.0 score, got %f", tt.description, score)
			}
		})
	}
}

// TestMeetsRequirements tests the filtering logic for individual candidates
func TestMeetsRequirements(t *testing.T) {
	tests := []struct {
		name             string
		candidate        v1beta1.AcceleratorClass
		constraints      *v1beta1.AcceleratorConstraints
		expectedEligible bool
		expectedReason   string
	}{
		{
			name: "No constraints - always eligible",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "test-gpu"},
				Spec:       v1beta1.AcceleratorClassSpec{},
			},
			constraints:      nil,
			expectedEligible: true,
			expectedReason:   "",
		},
		{
			name: "Explicitly excluded",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "excluded-gpu"},
				Spec:       v1beta1.AcceleratorClassSpec{},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				ExcludedClasses: []string{"excluded-gpu", "other-gpu"},
			},
			expectedEligible: false,
			expectedReason:   "explicitly excluded",
		},
		{
			name: "Below MinMemory",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "small-gpu"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						MemoryGB: resource.NewQuantity(16*1024*1024*1024, resource.BinarySI),
					},
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](32),
			},
			expectedEligible: false,
			expectedReason:   "memory 16GB < required 32GB",
		},
		{
			name: "Meets MinMemory",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "medium-gpu"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						MemoryGB: resource.NewQuantity(40*1024*1024*1024, resource.BinarySI),
					},
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](32),
			},
			expectedEligible: true,
			expectedReason:   "",
		},
		{
			name: "Exceeds MaxMemory",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "large-gpu"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						MemoryGB: resource.NewQuantity(80*1024*1024*1024, resource.BinarySI),
					},
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				MaxMemory: ptr.To[int64](64),
			},
			expectedEligible: false,
			expectedReason:   "memory 80GB > max allowed 64GB",
		},
		{
			name: "Missing required feature",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "basic-gpu"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						Features: []string{"cuda", "tensor-cores"},
					},
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				RequiredFeatures: []string{"nvlink"},
			},
			expectedEligible: false,
			expectedReason:   "missing required feature: nvlink",
		},
		{
			name: "Has all required features",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "full-gpu"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{
						Features: []string{"cuda", "tensor-cores", "nvlink"},
					},
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				RequiredFeatures: []string{"tensor-cores", "nvlink"},
			},
			expectedEligible: true,
			expectedReason:   "",
		},
		{
			name: "Architecture family mismatch",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "amd-gpu"},
				Spec: v1beta1.AcceleratorClassSpec{
					Vendor: "AMD",
					Family: "RDNA",
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				ArchitectureFamilies: []string{"nvidia-ampere", "nvidia-hopper"},
			},
			expectedEligible: false,
			expectedReason:   "architecture family rdna not in allowed list",
		},
		{
			name: "Architecture family match",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "nvidia-gpu"},
				Spec: v1beta1.AcceleratorClassSpec{
					Vendor: "NVIDIA",
					Family: "Ampere",
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				ArchitectureFamilies: []string{"nvidia-ampere", "nvidia-hopper"},
			},
			expectedEligible: true,
			expectedReason:   "",
		},
		{
			name: "Missing memory specification when MinMemory required",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "no-mem-spec"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{},
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				MinMemory: ptr.To[int64](40),
			},
			expectedEligible: false,
			expectedReason:   "missing memory specification for memory check",
		},
		{
			name: "MinComputePerformanceTFLOPS not a hard filter (soft constraint)",
			candidate: v1beta1.AcceleratorClass{
				ObjectMeta: metav1.ObjectMeta{Name: "no-perf-spec"},
				Spec: v1beta1.AcceleratorClassSpec{
					Capabilities: v1beta1.AcceleratorCapabilities{},
				},
			},
			constraints: &v1beta1.AcceleratorConstraints{
				MinComputePerformanceTFLOPS: ptr.To[int64](100),
			},
			expectedEligible: true,
			expectedReason:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eligible, reason := meetsRequirements(tt.candidate, tt.constraints)

			if eligible != tt.expectedEligible {
				t.Errorf("meetsRequirements() eligible = %v, want %v (reason: %s)",
					eligible, tt.expectedEligible, reason)
			}

			if !eligible && tt.expectedReason != "" {
				if reason != tt.expectedReason {
					t.Errorf("meetsRequirements() reason = %q, want %q", reason, tt.expectedReason)
				}
			}
		})
	}
}

// A compute capability is a dotted number ("major.minor", as NVIDIA states
// it), so a two-digit major sorts above a one-digit one: a class at "10.0"
// satisfies a minimum of "9.0", and a class at "9.0" does not satisfy a
// minimum of "10.0". A value that is not a number form keeps the text order.
func TestMeetsRequirements_ComputeCapabilityIsComparedAsNumbers(t *testing.T) {
	class := func(capability string) v1beta1.AcceleratorClass {
		return v1beta1.AcceleratorClass{
			ObjectMeta: metav1.ObjectMeta{Name: "gpu-" + capability},
			Spec:       v1beta1.AcceleratorClassSpec{Capabilities: v1beta1.AcceleratorCapabilities{ComputeCapability: capability}},
		}
	}
	for _, tc := range []struct {
		name       string
		have, want string
		eligible   bool
		reason     string
	}{
		{"two-digit major above a one-digit minimum", "10.0", "9.0", true, ""},
		{"one-digit major below a two-digit minimum", "9.0", "10.0", false, "compute capability 9.0 < required 10.0"},
		{"minor below the minimum", "7.5", "8.0", false, "compute capability 7.5 < required 8.0"},
		{"equal to the minimum", "9.0", "9.0", true, ""},
		{"a text form keeps the text order", "sm_90", "sm_80", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eligible, reason := meetsRequirements(class(tc.have), &v1beta1.AcceleratorConstraints{MinArchitectureVersion: ptr.To(tc.want)})
			if eligible != tc.eligible || reason != tc.reason {
				t.Fatalf("capability %s against minimum %s: eligible=%v reason=%q, want eligible=%v reason=%q",
					tc.have, tc.want, eligible, reason, tc.eligible, tc.reason)
			}
		})
	}
}
