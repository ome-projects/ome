package engine

import (
	"math"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// dispatchBudget unions UUID identities, not counts from disjoint caches.
// An unresolved external migration also blocks this conservative serial path.
func dispatchBudget(s *input.Snapshot, j *dispatchJournal, c policy.Candidate, cfg *config.Config, now time.Time, existing *dispatchEntry) string {
	if !dispatchConfigCooldownsValid(cfg) {
		return "InvalidCooldown"
	}
	active, hourly := map[string]bool{}, map[string]bool{}
	workloadWindow := cfg.PerWorkloadCooldown()
	key := func(owner, id string) string { return owner + "/" + id }
	own := ""
	if existing != nil {
		own = key(string(existing.WorkloadUID), existing.UUID)
	}
	for _, owner := range s.InferenceServices {
		window, valid := dispatchWorkloadCooldown(&owner, cfg)
		if !valid {
			return "InvalidCooldown"
		}
		if owner.Namespace == c.Workload.Namespace && owner.Name == c.Workload.Name {
			workloadWindow = window
		}
		for annotation := range owner.Annotations {
			if strings.HasPrefix(annotation, migrationRequestPrefix) {
				k := key(string(owner.UID), strings.TrimPrefix(annotation, migrationRequestPrefix))
				active[k] = true
				hourly[k] = true
			}
		}
	}
	if c.Reason == policy.ReasonNodeUnhealthy {
		workloadWindow = cfg.HealthCooldownFloor()
	}
	for _, ir := range s.InferenceReplicas {
		owner := ""
		for _, ref := range ir.OwnerReferences {
			if ref.Kind == "InferenceService" {
				owner = string(ref.UID)
				break
			}
		}
		if owner == "" && len(ir.Status.Migrations) > 0 {
			return "MigrationStateInvalid"
		}
		for _, m := range ir.Status.Migrations {
			if m.RequestUUID == "" || m.StartedAt.IsZero() || m.StartedAt.After(now) {
				return "MigrationStateInvalid"
			}
			k := key(owner, m.RequestUUID)
			switch m.Phase {
			case v1beta1.MigrationPhaseCompleted, v1beta1.MigrationPhaseFailed, v1beta1.MigrationPhaseRelocated:
				// A terminal row and a still-present annotation are not cancellation;
				// pending annotations conservatively continue to occupy the serial gate.
			case v1beta1.MigrationPhaseAccepted, v1beta1.MigrationPhaseSurgePending, v1beta1.MigrationPhaseSurgeReady, v1beta1.MigrationPhaseDraining:
				active[k] = true
			default:
				return "MigrationStateInvalid"
			}
			if now.Sub(m.StartedAt.Time) <= time.Hour {
				hourly[k] = true
			}
		}
	}
	for _, e := range j.Entries {
		k := key(string(e.WorkloadUID), e.UUID)
		if !e.terminal() {
			active[k] = true
		}
		if now.Sub(e.CreatedAt) <= time.Hour {
			hourly[k] = true
		}
		if e.UUID == existingUUID(existing) {
			continue
		}
		if e.Workload == c.Workload && e.CompletedAt != nil && now.Sub(*e.CompletedAt) < workloadWindow {
			return "Cooldown"
		}
	}
	delete(active, own)
	delete(hourly, own)
	if len(active) > 0 {
		return "InFlightCap"
	}
	if len(hourly) >= cfg.MaxMigrationsPerHour {
		return "HourlyCap"
	}
	if j.BackoffUntil != nil && now.Before(*j.BackoffUntil) {
		return "FailureBackoff"
	}
	if c.Reason != policy.ReasonNodeUnhealthy && journalNodeCooling(j, c.FromNode, cfg.PerNodeCooldown(), now, existing) {
		return "NodeCooldown"
	}
	return ""
}

// Validate raw minutes before conversion: a wrapped duration would erase
// required history and turn a cooldown into permission to move immediately.
func dispatchConfigCooldownsValid(cfg *config.Config) bool {
	for _, minutes := range []int{cfg.PerNodeCooldownMinutes, cfg.PerWorkloadCooldownMinutes, cfg.Policies.NodeHealth.HealthCooldownFloorMinutes, cfg.RecentPlacementCooldownMinutes} {
		if _, valid := dispatchCooldownDuration(int64(minutes)); !valid {
			return false
		}
	}
	return true
}

func dispatchCooldownDuration(minutes int64) (time.Duration, bool) {
	if minutes < 0 || minutes > math.MaxInt64/int64(time.Minute) {
		return 0, false
	}
	return time.Duration(minutes) * time.Minute, true
}

// Use admission's public override, including zero. Unlike advisory projection,
// execution fails closed on malformed or unrepresentable explicit values.
func dispatchWorkloadCooldown(owner *v1beta1.InferenceService, cfg *config.Config) (time.Duration, bool) {
	if raw, present := owner.Annotations[constants.AlfredCooldownMinutesAnnotationKey]; present {
		minutes, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, false
		}
		return dispatchCooldownDuration(minutes)
	}
	return dispatchCooldownDuration(int64(cfg.PerWorkloadCooldownMinutes))
}

// Retain enough terminal history for both the hourly budget and every current
// cooldown, including another workload's override. A later unrelated dispatch
// must not erase a node/workload cooldown still needed after leader failover.
func dispatchHistoryWindow(s *input.Snapshot, cfg *config.Config) (time.Duration, bool) {
	if !dispatchConfigCooldownsValid(cfg) {
		return 0, false
	}
	window := max(dispatchHistoryTTL, cfg.PerNodeCooldown(), cfg.PerWorkloadCooldown(), cfg.HealthCooldownFloor())
	for i := range s.InferenceServices {
		override, valid := dispatchWorkloadCooldown(&s.InferenceServices[i], cfg)
		if !valid {
			return 0, false
		}
		window = max(window, override)
	}
	return window, true
}

func pruneDispatchHistory(j *dispatchJournal, window time.Duration, now time.Time) {
	// Keep the journal's fixed one-hour pruning contract unchanged. Moving
	// its effective clock backwards extends retention; the existing size and
	// entry bounds fail closed if all remaining history is still needed.
	j.prune(now.Add(dispatchHistoryTTL - max(dispatchHistoryTTL, window)))
}

func existingUUID(e *dispatchEntry) string {
	if e == nil {
		return ""
	}
	return e.UUID
}

func journalNodeCooling(j *dispatchJournal, node string, window time.Duration, now time.Time, existing *dispatchEntry) bool {
	for _, e := range j.Entries {
		if e.UUID == existingUUID(existing) {
			continue
		}
		at := e.CreatedAt
		if e.CompletedAt != nil {
			at = *e.CompletedAt
		}
		if now.Sub(at) >= window {
			continue
		}
		if e.FromNode == node {
			return true
		}
		for _, target := range e.Targets {
			if target == node {
				return true
			}
		}
	}
	return false
}
