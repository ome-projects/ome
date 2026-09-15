package clusterstatus

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

const (
	// ConditionScanLimit admits a complete group or no condition evidence.
	ConditionScanLimit = 128
	// ConditionOutputLimit caps details only after full admitted validation.
	ConditionOutputLimit = 32
)

// Project samples the caller clock exactly once; declarations are never
// resolved, and controller reports are never promoted to observed probes.
func Project(s Snapshot, clock r.Clock) r.ClusterStatusReport {
	if clock == nil {
		clock = r.SystemClock{}
	}
	now := clock.Now().UTC()
	report := r.ClusterStatusReport{CollectedAt: now, Observation: r.ClusterComplete, UnavailableReason: s.Unavailable, ObservedPages: s.Pages, ReturnedSources: s.Returned, AdmittedSources: len(s.Items), SourceLimit: s.Limits.MaxItems, PageLimit: s.Limits.MaxPages, PageSize: s.Limits.PageSize, ConditionScanLimit: ConditionScanLimit, ConditionOutputLimit: ConditionOutputLimit, SourcesTruncated: s.Truncated, Clusters: []r.ClusterStatusRow{}}
	switch {
	case s.Truncated || s.Unavailable != "" && len(s.Items) > 0:
		report.Observation = r.ClusterPartial
	case s.Unavailable != "":
		report.Observation = r.ClusterUnavailable
	case len(s.Items) == 0:
		report.Observation = r.ClusterEmpty
	}
	names := make(map[string]int, len(s.Items))
	for i := range s.Items {
		names[s.Items[i].Name]++
	}
	for i := range s.Items {
		w := &s.Items[i]
		row := projectRow(w, now)
		if w.Namespace != "" || len(validation.IsDNS1123Subdomain(w.Name)) != 0 || names[w.Name] > 1 {
			row.Name = ""
			row.ConditionState = r.ClusterConditionMalformed
			row.Freshness = r.StatusFreshnessInvalid
			row.ReportedReady = r.ClusterReadyUnknown
			row.ConnectionState = "Unknown"
		}
		report.Clusters = append(report.Clusters, row)
	}
	return report.Canonical()
}

func projectRow(w *ome.WorkloadCluster, now time.Time) r.ClusterStatusRow {
	row := r.ClusterStatusRow{Name: w.Name, Generation: w.Generation, SourceKind: r.ClusterInvalidSource, SourceState: r.ClusterConditionMalformed, ProfileResolution: "NotAttempted", Evidence: r.EvidenceReported, ReportedReady: r.ClusterReadyUnknown, ConditionState: r.ClusterConditionMissing, Freshness: r.StatusFreshnessUnobserved, ConnectionState: "Unknown", TotalConditions: len(w.Status.Conditions), Conditions: []r.ClusterCondition{}}
	source := w.Spec.ClusterSource
	switch {
	case source.KubeConfig != nil && source.ClusterProfileRef == nil:
		k := source.KubeConfig
		if len(k.Key) > 253 || len(k.SecretRef.Name) > 253 || len(k.SecretRef.Namespace) > 63 {
			row.SourceState = r.ClusterConditionTruncated
			break
		}
		if len(validation.IsDNS1123Subdomain(k.SecretRef.Name)) == 0 && len(validation.IsDNS1123Label(k.SecretRef.Namespace)) == 0 && (k.Key == "" || len(validation.IsConfigMapKey(k.Key)) == 0) {
			row.SourceKind = r.ClusterKubeConfigSecret
			row.SourceState = r.ClusterConditionReported
		}
	case source.ClusterProfileRef != nil && source.KubeConfig == nil:
		name := source.ClusterProfileRef.Name
		if len(name) > 253 {
			row.SourceState = r.ClusterConditionTruncated
			break
		}
		if len(validation.IsDNS1123Subdomain(name)) == 0 {
			row.SourceKind = r.ClusterProfile
			row.SourceState = r.ClusterConditionReported
			row.DeclaredProfile = name
		}
	}
	if row.TotalConditions > ConditionScanLimit {
		row.ConditionState = r.ClusterConditionTruncated
		row.ConditionsTruncated = true
		return row
	}
	row.ScannedConditions = row.TotalConditions
	readyCount := 0
	malformed := false
	var ready r.ClusterCondition
	types := make(map[string]bool, row.TotalConditions)
	for _, condition := range w.Status.Conditions {
		c := projectCondition(condition, w.Generation, now)
		row.Conditions = append(row.Conditions, c)
		if len(condition.Type) > 316 || len(validation.IsQualifiedName(condition.Type)) != 0 || types[condition.Type] ||
			condition.Status != "True" && condition.Status != "False" && condition.Status != "Unknown" {
			malformed = true
		}
		types[condition.Type] = true
		if condition.Type != "Ready" {
			continue
		}
		readyCount++
		ready = c
	}
	if readyCount > 1 || malformed {
		row.ConditionState = r.ClusterConditionMalformed
		row.Freshness = r.StatusFreshnessInvalid
	} else if readyCount == 1 {
		row.ConditionState = r.ClusterConditionReported
		row.ReportedReady = ready.Status
		row.Freshness = ready.Freshness
		if row.SourceKind != r.ClusterInvalidSource && ready.Freshness == r.StatusFreshnessCurrent && ready.TransitionState == r.ClusterConditionReported {
			switch ready.Status {
			case r.ClusterReadyTrue:
				row.ConnectionState = "ReportedReady"
			case r.ClusterReadyFalse:
				row.ConnectionState = "ReportedNotReady"
			}
		}
	}
	// Canonicalize before clipping so the output subset is independent of
	// API/caller order. Complete group validation above is never clipped.
	row = (r.ClusterStatusReport{Clusters: []r.ClusterStatusRow{row}}).Canonical().Clusters[0]
	if len(row.Conditions) > ConditionOutputLimit {
		row.Conditions = row.Conditions[:ConditionOutputLimit]
		row.ConditionsTruncated = true
	}
	row.RetainedConditions = len(row.Conditions)
	return row
}

func projectCondition(c metav1.Condition, generation int64, now time.Time) r.ClusterCondition {
	result := r.ClusterCondition{Type: "Other", Status: r.ClusterReadyUnknown, ObservedGeneration: c.ObservedGeneration, Freshness: freshness(generation, c.ObservedGeneration), Reason: r.ClusterReasonUnknown, TransitionState: r.ClusterConditionMissing}
	if c.Type == "Ready" {
		result.Type = "Ready"
	}
	switch c.Status {
	case "True":
		result.Status = r.ClusterReadyTrue
	case "False":
		result.Status = r.ClusterReadyFalse
	case "Unknown":
	default:
		result.Freshness = r.StatusFreshnessInvalid
	}
	switch c.Reason {
	case "Connected":
		result.Reason = r.ClusterReasonConnected
	case "Disconnected":
		result.Reason = r.ClusterReasonDisconnected
	case "ConnectionFailed":
		result.Reason = r.ClusterReasonConnectionFailed
	case "":
		result.Reason = r.ClusterReasonMissing
	}
	if !c.LastTransitionTime.IsZero() {
		t := c.LastTransitionTime.UTC()
		if t.Year() < 1 || t.Year() > 9999 || t.After(now) {
			result.TransitionState = r.ClusterConditionMalformed
		} else {
			result.TransitionTime = &t
			result.TransitionState = r.ClusterConditionReported
		}
	}
	return result
}

func freshness(generation, observed int64) r.StatusFreshness {
	switch {
	case generation <= 0 || observed < 0 || observed > generation:
		return r.StatusFreshnessInvalid
	case observed == 0:
		return r.StatusFreshnessUnobserved
	case observed < generation:
		return r.StatusFreshnessStale
	default:
		return r.StatusFreshnessCurrent
	}
}
