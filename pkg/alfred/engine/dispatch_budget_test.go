package engine

import (
	"math"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/scheduling/input"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDispatchBudgetCountsAllRetainedUUIDsAndDeduplicates(t *testing.T) {
	c := cand("prod/a", "source", "target")
	first, _ := newDispatchEntry(c, "owner", "a-engine", "ir", "fingerprint", testNow.Add(-10*time.Minute))
	second, _ := newDispatchEntry(c, "owner", "a-engine", "ir", "fingerprint", testNow.Add(-5*time.Minute))
	complete := testNow.Add(-4 * time.Minute)
	first.Phase = dispatchCompleted
	first.CompletedAt = &complete
	second.Phase = dispatchCompleted
	second.CompletedAt = &complete
	captured := &input.Snapshot{InferenceServices: []v1beta1.InferenceService{{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "prod", UID: "owner"}}}, InferenceReplicas: []v1beta1.InferenceReplica{{ObjectMeta: metav1.ObjectMeta{Name: "a-engine", Namespace: "prod", UID: "ir", OwnerReferences: []metav1.OwnerReference{{Kind: "InferenceService", Name: "a", UID: "owner"}}}, Status: v1beta1.InferenceReplicaStatus{Migrations: []v1beta1.MigrationStatus{
		{RequestUUID: first.UUID, Phase: v1beta1.MigrationPhaseCompleted, StartedAt: metav1.NewTime(first.CreatedAt), CompletedAt: &metav1.Time{Time: complete}},
		{RequestUUID: second.UUID, Phase: v1beta1.MigrationPhaseCompleted, StartedAt: metav1.NewTime(second.CreatedAt), CompletedAt: &metav1.Time{Time: complete}},
	}}}}}
	j := &dispatchJournal{Version: "v1", Entries: []dispatchEntry{first, second}}
	cfg := config.Default()
	cfg.MaxMigrationsPerHour = 2
	unrelated := cand("prod/other", "elsewhere", "other-target")
	if reason := dispatchBudget(captured, j, unrelated, cfg, testNow, nil); reason != "HourlyCap" {
		t.Fatalf("lost earlier terminal history: %q", reason)
	}
	cfg.MaxMigrationsPerHour = 3
	if reason := dispatchBudget(captured, j, unrelated, cfg, testNow, nil); reason != "" {
		t.Fatalf("counted same UUID twice: %q", reason)
	}
	captured.InferenceServices[0].Annotations = map[string]string{migrationRequestPrefix + "b28a4230-f208-4c0f-afbc-dd864231c957": `{"schemaVersion":"v1"}`}
	if reason := dispatchBudget(captured, j, unrelated, cfg, testNow, nil); reason != "InFlightCap" {
		t.Fatalf("pending unobserved external request ignored: %q", reason)
	}
}

func TestDispatchBudgetUsesEffectiveWorkloadCooldown(t *testing.T) {
	for _, tc := range []struct {
		name     string
		override string
		health   bool
		age      time.Duration
		want     string
	}{
		{name: "longer override", override: "60", age: 45 * time.Minute, want: "Cooldown"},
		{name: "shorter override", override: "10", age: 20 * time.Minute},
		{name: "health floor", override: "60", health: true, age: 6 * time.Minute},
		{name: "inside health floor", override: "1", health: true, age: 4 * time.Minute, want: "Cooldown"},
		{name: "invalid override fails closed", override: "invalid", age: 20 * time.Minute, want: "InvalidCooldown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cand("prod/a", "new-source", "new-target")
			if tc.health {
				c.Reason = policy.ReasonNodeUnhealthy
			}
			earlier := cand("prod/a", "old-source", "old-target")
			entry, err := newDispatchEntry(earlier, "owner", "a-engine", "ir", "fingerprint", testNow.Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			completed := testNow.Add(-tc.age)
			entry.Phase, entry.CompletedAt = dispatchCompleted, &completed
			j := &dispatchJournal{Version: "v1", Entries: []dispatchEntry{entry}}
			// Terminal status has been compacted; the durable Alfred entry is the fallback.
			captured := &input.Snapshot{InferenceServices: []v1beta1.InferenceService{{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: "a", UID: "owner", Annotations: map[string]string{constants.AlfredCooldownMinutesAnnotationKey: tc.override},
			}}}}
			if reason := dispatchBudget(captured, j, c, config.Default(), testNow, nil); reason != tc.want {
				t.Fatalf("effective cooldown: got %q want %q", reason, tc.want)
			}
		})
	}
}

func TestDispatchBudgetRejectsUnrepresentableCooldowns(t *testing.T) {
	for _, raw := range []string{"-1", strconv.FormatInt(math.MaxInt64/int64(time.Minute)+1, 10), "9223372036854775808"} {
		t.Run(raw, func(t *testing.T) {
			captured := &input.Snapshot{InferenceServices: []v1beta1.InferenceService{{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: "other", Annotations: map[string]string{constants.AlfredCooldownMinutesAnnotationKey: raw},
			}}}}
			if reason := dispatchBudget(captured, &dispatchJournal{Version: "v1", Entries: []dispatchEntry{}}, cand("prod/a", "source", "target"), config.Default(), testNow, nil); reason != "InvalidCooldown" {
				t.Fatalf("invalid raw cooldown %q did not block retention/dispatch: %q", raw, reason)
			}
		})
	}
}

func TestPruneDispatchHistoryUsesInclusiveEffectiveHorizon(t *testing.T) {
	for _, window := range []time.Duration{time.Hour, 2 * time.Hour} {
		t.Run(window.String(), func(t *testing.T) {
			entries := []dispatchEntry{{UUID: "unresolved", Phase: dispatchStalled, CreatedAt: testNow.Add(-24 * time.Hour)}}
			for _, age := range []time.Duration{window + time.Second, window, window - time.Second} {
				completed := testNow.Add(-age)
				entries = append(entries, dispatchEntry{UUID: age.String(), Phase: dispatchCompleted, CreatedAt: completed.Add(-time.Minute), CompletedAt: &completed})
			}
			j := &dispatchJournal{Version: "v1", Entries: entries}
			pruneDispatchHistory(j, window, testNow)
			if len(j.Entries) != 3 || j.Entries[0].UUID != "unresolved" || j.Entries[1].UUID != window.String() || j.Entries[2].UUID != (window-time.Second).String() {
				t.Fatalf("wrong inclusive retention or unresolved entry lost: %+v", j.Entries)
			}
		})
	}
}
