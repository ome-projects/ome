package types

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ScaleDownPodFootprint counts authoritative Pods and one unit for each
// Podless status, so cleanup work contributes to a percentage budget.
func ScaleDownPodFootprint(statuses []InstanceStatus, podsByInstance map[int32][]*corev1.Pod) int32 {
	seen := make(map[int32]struct{}, len(statuses))
	var total int64
	for _, status := range statuses {
		seen[status.Index] = struct{}{}
		total += int64(max(1, len(podsByInstance[status.Index])))
	}
	for index, pods := range podsByInstance {
		if _, ok := seen[index]; !ok {
			total += int64(len(pods))
		}
	}
	return int32(min(total, math.MaxInt32))
}

// ResolveScaleDownPodBatchSize validates the policy and resolves percentages
// against the current Component footprint, rounding up so work can progress.
// Nil preserves unbounded selection. No API reads are needed.
func ResolveScaleDownPodBatchSize(configured *intstr.IntOrString, footprint int32) (*int32, error) {
	if configured == nil {
		return nil, nil
	}
	var resolved int32
	switch configured.Type {
	case intstr.Int:
		resolved = configured.IntVal
	case intstr.String:
		raw := configured.StrVal
		digits, found := strings.CutSuffix(raw, "%")
		if !found || digits == "" || strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return nil, fmt.Errorf("scaleDownPodBatchSize %q must be a positive integer or percentage from 1%% to 100%%", raw)
		}
		percent, err := strconv.Atoi(digits)
		if err != nil || percent < 1 || percent > 100 {
			return nil, fmt.Errorf("scaleDownPodBatchSize %q must be between 1%% and 100%%", raw)
		}
		// Widen before multiplying; the cost-1 floor permits statusless cleanup.
		resolved = int32(max(1, (int64(footprint)*int64(percent)+99)/100))
	default:
		return nil, fmt.Errorf("scaleDownPodBatchSize has unsupported IntOrString type %d", configured.Type)
	}
	if resolved <= 0 {
		return nil, fmt.Errorf("scaleDownPodBatchSize: must be > 0, got %d", resolved)
	}
	return &resolved, nil
}
