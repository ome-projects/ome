package acceleratorclassselector

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// scoredCandidate wraps a candidate with its calculated score
type scoredCandidate struct {
	v1beta1.AcceleratorClass
	Score  float64
	Reason string // Human-readable explanation for debugging
}

// candidatesFromNames fetches AcceleratorClass specs for given names
func candidatesFromNames(
	ctx context.Context,
	fetcher AcceleratorFetcher,
	names []string,
) ([]v1beta1.AcceleratorClass, error) {
	logger := log.FromContext(ctx)

	// Deduplicate names while preserving order
	seen := make(map[string]struct{})
	candidates := make([]v1beta1.AcceleratorClass, 0, len(names))

	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		ac, found, err := fetcher.GetAcceleratorClass(ctx, name)
		if err != nil {
			logger.Error(err, "Failed to fetch AcceleratorClass", "name", name)
			return nil, fmt.Errorf("failed to fetch AcceleratorClass %s: %w", name, err)
		}
		if !found {
			logger.V(1).Info("AcceleratorClass not found, skipping", "name", name)
			continue
		}
		if ac == nil {
			logger.Error(nil, "AcceleratorClass is nil", "name", name)
			continue
		}

		candidates = append(candidates, *ac)
	}

	return candidates, nil
}

// filterCandidates applies constraints to filter out ineligible candidates
func filterCandidates(
	ctx context.Context,
	candidates []v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) []v1beta1.AcceleratorClass {
	logger := log.FromContext(ctx)
	filtered := make([]v1beta1.AcceleratorClass, 0, len(candidates))

	for _, candidate := range candidates {
		eligible, reason := meetsRequirements(candidate, constraints)
		if eligible {
			filtered = append(filtered, candidate)
		} else {
			logger.V(1).Info("Filtered out candidate", "name", candidate.Name, "reason", reason)
		}
	}

	logger.Info("Filtered candidates", "total", len(candidates), "eligible", len(filtered))
	return filtered
}

// meetsRequirements checks if a candidate satisfies all requirements from InferenceService
func meetsRequirements(
	candidate v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) (bool, string) {
	spec := candidate.Spec

	// If no constraints, all candidates are eligible
	if constraints == nil {
		return true, ""
	}

	// Check if excluded
	for _, excluded := range constraints.ExcludedClasses {
		if candidate.Name == excluded {
			return false, "explicitly excluded"
		}
	}

	// Check architecture family
	if len(constraints.ArchitectureFamilies) > 0 {
		candidateVendorFamily := fmt.Sprintf("%s-%s",
			strings.ToLower(spec.Vendor),
			strings.ToLower(spec.Family))
		candidateFamily := strings.ToLower(spec.Family)
		found := false
		for _, family := range constraints.ArchitectureFamilies {
			if strings.Contains(family, "-") {
				if strings.EqualFold(family, candidateVendorFamily) {
					found = true
					break
				}
			} else {
				if strings.EqualFold(family, candidateFamily) {
					found = true
					break
				}
			}
		}
		if !found {
			return false, fmt.Sprintf("architecture family %s not in allowed list", candidateFamily)
		}
	}

	// Check MinMemory and MaxMemory (cost control)
	if constraints.MinMemory != nil || constraints.MaxMemory != nil {
		if spec.Capabilities.MemoryGB == nil {
			return false, "missing memory specification for memory check"
		}
		memGB := spec.Capabilities.MemoryGB.Value() / (1024 * 1024 * 1024)

		if constraints.MinMemory != nil && memGB < *constraints.MinMemory {
			return false, fmt.Sprintf("memory %dGB < required %dGB", memGB, *constraints.MinMemory)
		}

		if constraints.MaxMemory != nil && memGB > *constraints.MaxMemory {
			return false, fmt.Sprintf("memory %dGB > max allowed %dGB", memGB, *constraints.MaxMemory)
		}
	}

	// MinComputePerformanceTFLOPS is a scoring factor (calculateComputePerformanceTFLOPSScore), not a hard filter: a low value lowers a candidate's score but does not disqualify it.

	// Check RequiredFeatures (ALL must be present - hard requirement)
	if len(constraints.RequiredFeatures) > 0 {
		candidateFeatures := make(map[string]struct{})
		for _, f := range spec.Capabilities.Features {
			candidateFeatures[strings.ToLower(f)] = struct{}{}
		}
		for _, required := range constraints.RequiredFeatures {
			if _, found := candidateFeatures[strings.ToLower(required)]; !found {
				return false, fmt.Sprintf("missing required feature: %s", required)
			}
		}
	}

	// Check MinArchitectureVersion (e.g., "8.0" for Ampere, "9.0" for Hopper)
	if constraints.MinArchitectureVersion != nil {
		if spec.Capabilities.ComputeCapability == "" {
			return false, "missing compute capability for architecture version check"
		}

		// Compare version strings lexicographically (works for NVIDIA format like "7.5", "8.0", "8.6", "9.0")
		candidateVersion := spec.Capabilities.ComputeCapability
		requiredVersion := *constraints.MinArchitectureVersion

		if candidateVersion < requiredVersion {
			return false, fmt.Sprintf("compute capability %s < required %s",
				candidateVersion, requiredVersion)
		}
	}

	return true, ""
}

// calculateBestFitScore computes multi-criteria score for best fit
// Note: RequiredFeatures are NOT scored - they are hard filters applied before scoring
// Note: Precision matching is folded into compute scoring via iterative precision fallback
func calculateBestFitScore(
	candidate v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) float64 {
	weights := struct {
		memory  float64
		compute float64
	}{0.70, 0.30}

	memScore := calculateMemoryFitScore(candidate, constraints)
	computeScore := calculateComputePerformanceTFLOPSScore(candidate, constraints)

	totalScore := weights.memory*memScore + weights.compute*computeScore

	return totalScore
}

// calculateMemoryFitScore penalizes over-provisioning
func calculateMemoryFitScore(
	candidate v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) float64 {
	if constraints == nil || constraints.MinMemory == nil {
		return 1.0 // No requirement, perfect score
	}

	// Should already be filtered, but safety check
	spec := candidate.Spec
	if spec.Capabilities.MemoryGB == nil {
		return 0.0
	}

	candidateMemGB := float64(spec.Capabilities.MemoryGB.Value()) / (1024 * 1024 * 1024)
	requiredMemGB := float64(*constraints.MinMemory)

	if candidateMemGB < requiredMemGB {
		return 0.0 // Should already be filtered, but safety check
	}

	if candidateMemGB == requiredMemGB {
		return 1.0 // Perfect match
	}

	// Penalize over-provisioning: score = 1 / ratio (NO CAP)
	// 2x over = 0.5, 10x over = 0.1 (allows full penalty)
	ratio := candidateMemGB / requiredMemGB
	score := 1.0 / ratio

	return score
}

// scoreFromTFLOPS calculates score based on TFLOPS ratio to requirement
// - tflops = 0: returns 0.0
// - requiredTFLOPS = 0: returns 1.0 (no requirement, any non-zero TFLOPS is perfect)
// - tflops >= requiredTFLOPS: returns 1.0 (meeting or exceeding requirement)
// - tflops < requiredTFLOPS: returns proportional score (e.g., 50/100 = 0.5)
func scoreFromTFLOPS(tflops int64, requiredTFLOPS int64) float64 {
	if tflops == 0 {
		return 0.0
	}
	if requiredTFLOPS == 0 {
		// No requirement specified, any non-zero TFLOPS is perfect
		return 1.0
	}
	ratio := float64(tflops) / float64(requiredTFLOPS)
	// Cap at 1.0 - meeting requirement is best score, exceeding doesn't give bonus
	if ratio >= 1.0 {
		return 1.0
	}
	// Proportional score for partial matches (e.g., 50 TFLOPS / 100 required = 0.5 score)
	return ratio
}

// calculateComputePerformanceTFLOPSScore scores compute performance with iterative precision fallback
// Iterative precision logic:
// - If no PreferredPrecisions: use max TFLOPS across all precisions
// - If PreferredPrecisions exist: iterate through them
//   - First precision with TFLOPS > 0: baseScore * 1.0
//   - Second precision: baseScore * 0.5 (penalty *= 0.5)
//   - Third precision: baseScore * 0.25 (penalty *= 0.5)
//
// - Fallback to fp16 if not in list
// - Return 0.0 if no precision has any TFLOPS data
func calculateComputePerformanceTFLOPSScore(
	candidate v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) float64 {
	spec := candidate.Spec
	if spec.Capabilities.Performance == nil {
		return 0.0
	}

	perf := spec.Capabilities.Performance
	if constraints == nil {
		constraints = &v1beta1.AcceleratorConstraints{}
	}

	// If no MinComputePerformanceTFLOPS requirement, return 1.0
	requiredTFLOPS := int64(0)
	if constraints.MinComputePerformanceTFLOPS != nil {
		requiredTFLOPS = *constraints.MinComputePerformanceTFLOPS
	}

	// Case 1: No preferred precisions specified - use max TFLOPS across all precisions
	if len(constraints.PreferredPrecisions) == 0 {
		tflops := getMaxTFLOPS(perf)
		return scoreFromTFLOPS(tflops, requiredTFLOPS)
	}

	// Case 2: Try each preferred precision in order with penalty degradation
	penalty := 1.0
	for _, precision := range constraints.PreferredPrecisions {
		tflops := getTFLOPSForPrecision(perf, strings.ToLower(precision))
		if tflops > 0 {
			baseScore := scoreFromTFLOPS(tflops, requiredTFLOPS)
			// Apply precision penalty (1.0 for first, 0.5 for second, 0.25 for third, etc.)
			return baseScore * penalty
		}
		// Degrade penalty for next precision
		penalty *= 0.5
	}

	// Case 3: Fallback to fp16 if not already checked
	fp16InList := false
	for _, p := range constraints.PreferredPrecisions {
		if strings.ToLower(p) == "fp16" {
			fp16InList = true
			break
		}
	}

	if !fp16InList {
		tflops := getTFLOPSForPrecision(perf, "fp16")
		if tflops > 0 {
			baseScore := scoreFromTFLOPS(tflops, requiredTFLOPS)
			return baseScore * penalty // Heavily penalized fallback
		}
	}

	// Case 4: No precision has any TFLOPS data
	return 0.0
}

// getHourlyCost returns SpotPerHour if available, otherwise PerHour.
func getHourlyCost(candidate v1beta1.AcceleratorClass) *resource.Quantity {
	if candidate.Spec.Cost.SpotPerHour != nil {
		return candidate.Spec.Cost.SpotPerHour
	}
	return candidate.Spec.Cost.PerHour
}

// compareCosts returns -1 if cost1 < cost2, 0 if equal, 1 if cost1 > cost2
func compareCosts(cost1, cost2 *resource.Quantity) int {
	return cost1.Cmp(*cost2)
}

// findCheapestByCost returns the candidate with the lowest cost extracted by costFn.
func findCheapestByCost(candidates []v1beta1.AcceleratorClass, costFn func(v1beta1.AcceleratorClass) *resource.Quantity) v1beta1.AcceleratorClass {
	cheapest := candidates[0]
	cheapestCost := costFn(cheapest)
	for _, c := range candidates[1:] {
		cost := costFn(c)
		if compareCosts(cost, cheapestCost) < 0 {
			cheapest = c
			cheapestCost = cost
		}
	}
	return cheapest
}

// tierToNumeric maps a cost tier string to a numeric value for comparison.
func tierToNumeric(tier string) int64 {
	switch strings.ToLower(tier) {
	case "low":
		return 1
	case "medium":
		return 2
	case "high":
		return 3
	default:
		return 2 // default medium
	}
}

// candidateMaxValues holds the maximum values across all candidates for normalization
type candidateMaxValues struct {
	maxTFLOPS    float64
	maxMemGB     float64
	maxBandwidth float64
}

// getCandidateRawValues extracts raw TFLOPS, memory, and bandwidth from a candidate
func getCandidateRawValues(candidate v1beta1.AcceleratorClass, preferredPrecisions []string) (tflops, memGB, bandwidthGBps float64) {
	spec := candidate.Spec

	primaryPrecision := "fp16"
	if len(preferredPrecisions) > 0 {
		primaryPrecision = strings.ToLower(preferredPrecisions[0])
	}

	tflops = float64(getTFLOPSForPrecision(spec.Capabilities.Performance, primaryPrecision))

	if tflops == 0.0 && len(preferredPrecisions) > 1 {
		for i := 1; i < len(preferredPrecisions); i++ {
			fallbackTFLOPS := getTFLOPSForPrecision(spec.Capabilities.Performance, strings.ToLower(preferredPrecisions[i]))
			if fallbackTFLOPS > 0 {
				tflops = float64(fallbackTFLOPS)
				break
			}
		}
	}

	if spec.Capabilities.MemoryGB != nil {
		memGB = float64(spec.Capabilities.MemoryGB.Value()) / (1024 * 1024 * 1024)
	}

	if spec.Capabilities.MemoryBandwidthGBps != nil {
		bandwidthGBps = float64(spec.Capabilities.MemoryBandwidthGBps.Value())
	}

	return tflops, memGB, bandwidthGBps
}

// computeMaxValues finds the maximum TFLOPS, memory, and bandwidth across all candidates
func computeMaxValues(candidates []v1beta1.AcceleratorClass, preferredPrecisions []string) candidateMaxValues {
	maxVals := candidateMaxValues{}
	for _, c := range candidates {
		tflops, memGB, bw := getCandidateRawValues(c, preferredPrecisions)
		if tflops > maxVals.maxTFLOPS {
			maxVals.maxTFLOPS = tflops
		}
		if memGB > maxVals.maxMemGB {
			maxVals.maxMemGB = memGB
		}
		if bw > maxVals.maxBandwidth {
			maxVals.maxBandwidth = bw
		}
	}
	return maxVals
}

// calculateCapabilityScore computes performance score based on precision,
// normalized against the maximum values among all candidates
func calculateCapabilityScore(
	candidate v1beta1.AcceleratorClass,
	preferredPrecisions []string,
	maxVals candidateMaxValues,
) float64 {
	tflops, memGB, bandwidthGBps := getCandidateRawValues(candidate, preferredPrecisions)

	// Normalize against actual candidate maximums
	normTFLOPS := 0.0
	if maxVals.maxTFLOPS > 0 {
		normTFLOPS = tflops / maxVals.maxTFLOPS
	}
	normMemGB := 0.0
	if maxVals.maxMemGB > 0 {
		normMemGB = memGB / maxVals.maxMemGB
	}
	normBandwidth := 0.0
	if maxVals.maxBandwidth > 0 {
		normBandwidth = bandwidthGBps / maxVals.maxBandwidth
	}

	// Composite score: 50% memory, 30% bandwidth, 20% performance
	score := 0.5*normMemGB + 0.3*normBandwidth + 0.2*normTFLOPS

	return score
}

// getTFLOPSForPrecision extracts TFLOPS value for given precision
func getTFLOPSForPrecision(
	perf *v1beta1.AcceleratorPerformance,
	precision string,
) int64 {
	if perf == nil {
		return 0
	}

	switch strings.ToLower(precision) {
	case "fp32":
		if perf.Fp32Tflops != nil {
			return *perf.Fp32Tflops
		}
	case "fp16":
		if perf.Fp16Tflops != nil {
			return *perf.Fp16Tflops
		}
	case "int8", "fp8":
		if perf.Int8Tops != nil {
			return *perf.Int8Tops
		}
	case "int4":
		if perf.Int4Tops != nil {
			return *perf.Int4Tops
		}
	}

	return 0
}

// getMaxTFLOPS returns the maximum TFLOPS across all precisions
// Used when no precision preference is specified
func getMaxTFLOPS(perf *v1beta1.AcceleratorPerformance) int64 {
	if perf == nil {
		return 0
	}

	maxTFLOPS := int64(0)

	if perf.Fp32Tflops != nil && *perf.Fp32Tflops > maxTFLOPS {
		maxTFLOPS = *perf.Fp32Tflops
	}
	if perf.Fp16Tflops != nil && *perf.Fp16Tflops > maxTFLOPS {
		maxTFLOPS = *perf.Fp16Tflops
	}
	if perf.Int8Tops != nil && *perf.Int8Tops > maxTFLOPS {
		maxTFLOPS = *perf.Int8Tops
	}
	if perf.Int4Tops != nil && *perf.Int4Tops > maxTFLOPS {
		maxTFLOPS = *perf.Int4Tops
	}

	return maxTFLOPS
}

// sortScoredCandidates sorts candidates by score
// ascending=true sorts lowest to highest (for cost minimization)
// ascending=false sorts highest to lowest (for capability/fit maximization)
func sortScoredCandidates(scored []scoredCandidate, ascending bool) {
	sort.Slice(scored, func(i, j int) bool {
		if ascending {
			return scored[i].Score < scored[j].Score
		}
		return scored[i].Score > scored[j].Score
	})
}
