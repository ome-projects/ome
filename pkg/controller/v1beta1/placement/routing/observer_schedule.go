package routing

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// normalizeTargets checks one observation request's targets against the
// request's owner and TrafficMap, canonicalises every endpoint and returns the
// targets sorted by cluster name. kind names the observer in error texts.
// policyRule, when set, runs on each target after its identity is checked and
// before its cluster name is.
func normalizeTargets(kind string, ownerUID types.UID, mapKey types.NamespacedName, targets []Target, policyRule func(Target) error) ([]Target, error) {
	normalized := make([]Target, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for i, target := range targets {
		if target.OwnerUID == "" {
			return nil, fmt.Errorf("%s target cluster %q has an empty owner UID", kind, target.Cluster)
		}
		if target.OwnerUID != ownerUID {
			return nil, fmt.Errorf("%s target cluster %q has owner UID %q, want %q",
				kind, target.Cluster, target.OwnerUID, ownerUID)
		}
		if target.Map.Namespace == "" || target.Map.Name == "" {
			return nil, fmt.Errorf("%s target cluster %q has an empty TrafficMap identity", kind, target.Cluster)
		}
		if target.Map != mapKey {
			return nil, fmt.Errorf("%s target cluster %q belongs to TrafficMap %q, want %q",
				kind, target.Cluster, target.Map.String(), mapKey.String())
		}
		if policyRule != nil {
			if err := policyRule(target); err != nil {
				return nil, err
			}
		}
		if target.Cluster == "" {
			return nil, fmt.Errorf("%s target cluster is empty", kind)
		}
		if _, found := seen[target.Cluster]; found {
			return nil, fmt.Errorf("%s target cluster %q is duplicated", kind, target.Cluster)
		}
		seen[target.Cluster] = struct{}{}
		endpoint, err := CanonicalEndpoint(target.URL)
		if err != nil {
			return nil, fmt.Errorf("%s target cluster %q: %w", kind, target.Cluster, err)
		}
		target.OwnerUID = ownerUID
		target.Map = mapKey
		target.URL = endpoint
		normalized[i] = target
	}
	slices.SortFunc(normalized, func(a, b Target) int { return strings.Compare(a.Cluster, b.Cluster) })
	return normalized, nil
}

// advanceDeadline returns the next deadline of a periodic observation: one
// period from now when none was set, the deadline itself while it is still
// ahead, and otherwise the first multiple of the period past now, so a missed
// deadline keeps its phase.
func advanceDeadline(deadline, now time.Time, period time.Duration) time.Time {
	if deadline.IsZero() {
		return now.Add(period)
	}
	if deadline.After(now) {
		return deadline
	}
	return deadline.Add((now.Sub(deadline)/period + 1) * period)
}
