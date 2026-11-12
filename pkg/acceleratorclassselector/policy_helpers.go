package acceleratorclassselector

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/version"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// gibibyte converts a memory quantity in bytes to gigabytes.
const gibibyte = 1024 * 1024 * 1024

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

// constraintCheck returns why a candidate breaks one constraint, or "" when it satisfies it.
type constraintCheck func(candidate v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) string

// constraintChecks are the hard constraints, in the order their refusals are reported.
// MinComputePerformanceTFLOPS is not one of them: it is a scoring factor
// (calculateComputePerformanceTFLOPSScore), so a low figure lowers a candidate's
// score but does not disqualify it.
var constraintChecks = []constraintCheck{
	notExcluded,
	inArchitectureFamilies,
	memoryWithinBounds,
	hasRequiredFeatures,
	computeCapabilityAtLeast,
}

// meetsRequirements reports whether a candidate satisfies every constraint of the
// InferenceService, and the first refusal when it does not.
func meetsRequirements(
	candidate v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) (bool, string) {
	if constraints == nil {
		return true, ""
	}
	for _, check := range constraintChecks {
		if refusal := check(candidate, constraints); refusal != "" {
			return false, refusal
		}
	}
	return true, ""
}

func notExcluded(candidate v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) string {
	if slices.Contains(constraints.ExcludedClasses, candidate.Name) {
		return "explicitly excluded"
	}
	return ""
}

// inArchitectureFamilies accepts a family named alone or as vendor-family, ignoring case.
func inArchitectureFamilies(candidate v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) string {
	if len(constraints.ArchitectureFamilies) == 0 {
		return ""
	}
	family := strings.ToLower(candidate.Spec.Family)
	vendorFamily := strings.ToLower(candidate.Spec.Vendor) + "-" + family
	allowed := slices.ContainsFunc(constraints.ArchitectureFamilies, func(wanted string) bool {
		if strings.Contains(wanted, "-") {
			return strings.EqualFold(wanted, vendorFamily)
		}
		return strings.EqualFold(wanted, family)
	})
	if !allowed {
		return fmt.Sprintf("architecture family %s not in allowed list", family)
	}
	return ""
}

// memoryWithinBounds compares whole gigabytes against the minimum and maximum the service allows.
func memoryWithinBounds(candidate v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) string {
	if constraints.MinMemory == nil && constraints.MaxMemory == nil {
		return ""
	}
	if candidate.Spec.Capabilities.MemoryGB == nil {
		return "missing memory specification for memory check"
	}
	memGB := candidate.Spec.Capabilities.MemoryGB.Value() / gibibyte
	if constraints.MinMemory != nil && memGB < *constraints.MinMemory {
		return fmt.Sprintf("memory %dGB < required %dGB", memGB, *constraints.MinMemory)
	}
	if constraints.MaxMemory != nil && memGB > *constraints.MaxMemory {
		return fmt.Sprintf("memory %dGB > max allowed %dGB", memGB, *constraints.MaxMemory)
	}
	return ""
}

// hasRequiredFeatures demands every required feature, compared ignoring case.
func hasRequiredFeatures(candidate v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) string {
	have := make(map[string]struct{}, len(candidate.Spec.Capabilities.Features))
	for _, feature := range candidate.Spec.Capabilities.Features {
		have[strings.ToLower(feature)] = struct{}{}
	}
	for _, required := range constraints.RequiredFeatures {
		if _, ok := have[strings.ToLower(required)]; !ok {
			return fmt.Sprintf("missing required feature: %s", required)
		}
	}
	return ""
}

// computeCapabilityAtLeast demands a compute capability at or above the minimum.
// Both read as dotted numbers ("major.minor", as NVIDIA states them), so
// "10.0" is above "9.0"; a value that is not a number form keeps the text order.
func computeCapabilityAtLeast(candidate v1beta1.AcceleratorClass, constraints *v1beta1.AcceleratorConstraints) string {
	if constraints.MinArchitectureVersion == nil {
		return ""
	}
	have := candidate.Spec.Capabilities.ComputeCapability
	if have == "" {
		return "missing compute capability for architecture version check"
	}
	if capabilityBelow(have, *constraints.MinArchitectureVersion) {
		return fmt.Sprintf("compute capability %s < required %s", have, *constraints.MinArchitectureVersion)
	}
	return ""
}

// capabilityBelow reports whether have sorts below minimum: by number when
// both parse as dotted numbers, by text otherwise.
func capabilityBelow(have, minimum string) bool {
	h, errHave := version.ParseGeneric(have)
	m, errMinimum := version.ParseGeneric(minimum)
	if errHave != nil || errMinimum != nil {
		return have < minimum
	}
	return h.LessThan(m)
}

// calculateBestFitScore computes multi-criteria score for best fit
// Note: RequiredFeatures are NOT scored - they are hard filters applied before scoring
// Note: Precision matching is folded into compute scoring via iterative precision fallback
func calculateBestFitScore(
	candidate v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) float64 {
	// A service that states no constraints requires nothing: score it as an empty block.
	if constraints == nil {
		constraints = &v1beta1.AcceleratorConstraints{}
	}
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
	if constraints.MinMemory == nil {
		return 1.0 // No requirement, perfect score
	}

	// Should already be filtered, but safety check
	candidateMemGB, stated := memoryGB(candidate.Spec.Capabilities)
	if !stated {
		return 0.0
	}
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

// memoryGB reads the class's memory in gigabytes; stated is false when the class gives none.
func memoryGB(capabilities v1beta1.AcceleratorCapabilities) (gb float64, stated bool) {
	if capabilities.MemoryGB == nil {
		return 0, false
	}
	return float64(capabilities.MemoryGB.Value()) / gibibyte, true
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

// calculateComputePerformanceTFLOPSScore scores compute against MinComputePerformanceTFLOPS
// at the first preferred precision that states a figure; every precision tried before it
// halves the score. Without preferred precisions the best figure of any precision counts.
// When no preferred precision states a figure and fp16 is not among them, fp16 serves
// with the full penalty.
func calculateComputePerformanceTFLOPSScore(
	candidate v1beta1.AcceleratorClass,
	constraints *v1beta1.AcceleratorConstraints,
) float64 {
	perf := candidate.Spec.Capabilities.Performance
	if perf == nil {
		return 0.0
	}

	requiredTFLOPS := int64(0)
	if constraints.MinComputePerformanceTFLOPS != nil {
		requiredTFLOPS = *constraints.MinComputePerformanceTFLOPS
	}

	precisions := constraints.PreferredPrecisions
	if len(precisions) == 0 {
		return scoreFromTFLOPS(getMaxTFLOPS(perf), requiredTFLOPS)
	}

	tflops, missed := firstPrecisionWithTFLOPS(perf, precisions)
	fp16Listed := slices.ContainsFunc(precisions, func(precision string) bool {
		return strings.ToLower(precision) == "fp16"
	})
	if tflops == 0 && !fp16Listed {
		tflops = getTFLOPSForPrecision(perf, "fp16")
	}
	if tflops <= 0 {
		return 0.0
	}
	return scoreFromTFLOPS(tflops, requiredTFLOPS) * math.Pow(0.5, float64(missed))
}

// firstPrecisionWithTFLOPS returns the figure of the first listed precision that states
// one, and how many precisions before it stated none (the list's length when none does).
func firstPrecisionWithTFLOPS(perf *v1beta1.AcceleratorPerformance, precisions []string) (tflops int64, missed int) {
	for i, precision := range precisions {
		if tflops := getTFLOPSForPrecision(perf, precision); tflops > 0 {
			return tflops, i
		}
	}
	return 0, len(precisions)
}

// costGroup is one way a class states its price. The groups are tried in order; the
// first one that any candidate states decides, and within it the lowest figure wins
// (the first listed on a tie).
type costGroup struct {
	label   string
	stated  func(v1beta1.AcceleratorCost) bool
	cheaper func(a, b v1beta1.AcceleratorCost) bool
}

var costGroups = []costGroup{
	{
		label:   "hourly",
		stated:  func(cost v1beta1.AcceleratorCost) bool { return cost.SpotPerHour != nil || cost.PerHour != nil },
		cheaper: func(a, b v1beta1.AcceleratorCost) bool { return hourlyCost(a).Cmp(*hourlyCost(b)) < 0 },
	},
	{
		label:   "per-million-tokens",
		stated:  func(cost v1beta1.AcceleratorCost) bool { return cost.PerMillionTokens != nil },
		cheaper: func(a, b v1beta1.AcceleratorCost) bool { return a.PerMillionTokens.Cmp(*b.PerMillionTokens) < 0 },
	},
	{
		label:   "tier",
		stated:  func(cost v1beta1.AcceleratorCost) bool { return cost.Tier != "" },
		cheaper: func(a, b v1beta1.AcceleratorCost) bool { return tierToNumeric(a.Tier) < tierToNumeric(b.Tier) },
	},
}

// hourlyCost is the spot price when the class states one, otherwise the on-demand price.
func hourlyCost(cost v1beta1.AcceleratorCost) *resource.Quantity {
	if cost.SpotPerHour != nil {
		return cost.SpotPerHour
	}
	return cost.PerHour
}

// cheapestIn returns the candidate with the group's lowest figure, nil when none states one.
func cheapestIn(group costGroup, priced []v1beta1.AcceleratorClass) *v1beta1.AcceleratorClass {
	var cheapest *v1beta1.AcceleratorClass
	for i := range priced {
		candidate := &priced[i]
		cost := *candidate.Spec.Cost
		if !group.stated(cost) {
			continue
		}
		if cheapest == nil || group.cheaper(cost, *cheapest.Spec.Cost) {
			cheapest = candidate
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

// getCandidateRawValues extracts raw TFLOPS, memory, and bandwidth from a candidate.
// The TFLOPS figure is the first preferred precision's (fp16's when the service prefers
// none); when that is zero, the next preferred precision that states one.
func getCandidateRawValues(candidate v1beta1.AcceleratorClass, preferredPrecisions []string) (tflops, memGB, bandwidthGBps float64) {
	spec := candidate.Spec

	precisions := []string{"fp16"}
	if len(preferredPrecisions) > 0 {
		precisions = preferredPrecisions
	}
	tflops = float64(getTFLOPSForPrecision(spec.Capabilities.Performance, precisions[0]))
	if tflops == 0 {
		fallback, _ := firstPrecisionWithTFLOPS(spec.Capabilities.Performance, precisions[1:])
		tflops = float64(fallback)
	}

	memGB, _ = memoryGB(spec.Capabilities)

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

// computeFigure is one compute figure an AcceleratorClass states, with the
// precision names it answers for.
type computeFigure struct {
	precision string
	alias     string
	value     *int64
}

// computeFigures lists the class's compute figures in the order precision
// names are matched: fp32, fp16, int8 (which also answers fp8), int4.
func computeFigures(perf *v1beta1.AcceleratorPerformance) [4]computeFigure {
	return [4]computeFigure{
		{precision: "fp32", value: perf.Fp32Tflops},
		{precision: "fp16", value: perf.Fp16Tflops},
		{precision: "int8", alias: "fp8", value: perf.Int8Tops},
		{precision: "int4", value: perf.Int4Tops},
	}
}

// answers reports whether the figure is the one a precision name asks for.
func (f computeFigure) answers(precision string) bool {
	return precision == f.precision || (f.alias != "" && precision == f.alias)
}

// getTFLOPSForPrecision is the figure the class states for a precision, or 0
// when it states none or the precision is unknown.
func getTFLOPSForPrecision(
	perf *v1beta1.AcceleratorPerformance,
	precision string,
) int64 {
	if perf == nil {
		return 0
	}
	wanted := strings.ToLower(precision)
	for _, figure := range computeFigures(perf) {
		if figure.answers(wanted) && figure.value != nil {
			return *figure.value
		}
	}
	return 0
}

// getMaxTFLOPS is the largest figure the class states at any precision, or 0.
func getMaxTFLOPS(perf *v1beta1.AcceleratorPerformance) int64 {
	if perf == nil {
		return 0
	}
	maxTFLOPS := int64(0)
	for _, figure := range computeFigures(perf) {
		if figure.value != nil && *figure.value > maxTFLOPS {
			maxTFLOPS = *figure.value
		}
	}
	return maxTFLOPS
}
