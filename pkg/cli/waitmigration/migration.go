// Package waitmigration evaluates one exact, authoritative migration record.
package waitmigration

import (
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

// Evaluator pins the exact IR source identity when a request first appears.
// A replacement IR cannot inherit attribution for a previously observed UUID.
type Evaluator struct {
	sourceName string
	sourceUID  string
}

func (e *Evaluator) Evaluate(r reportv1alpha1.MigrationStatusReport, id string) (waitengine.Decision, reportv1alpha1.WaitMigrationObservation) {
	decision, observed := Evaluate(r, id)
	if observed.Validity != "Valid" {
		return decision, observed
	}
	sourceName := ""
	for _, record := range r.Content.Migrations {
		if record.RequestID == id {
			sourceName = record.SourceName
			break
		}
	}
	uid := ""
	matches := 0
	for _, source := range r.Sources {
		if source.Kind == reportv1alpha1.MigrationSourceInferenceReplica && source.Name == sourceName && sourceName != "" {
			uid = source.UID
			matches++
		}
	}
	if matches != 1 || uid == "" || e.sourceName != "" && (e.sourceName != sourceName || e.sourceUID != uid) {
		observed.Validity = "Invalid"
		return waitengine.Decision{Reason: waitengine.ReasonInvalidMigration}, observed
	}
	e.sourceName, e.sourceUID = sourceName, uid
	return decision, observed
}

// Evaluate never infers an action result from another request's terminal
// record, a parent history entry, or an incomplete sibling-IR snapshot.
func Evaluate(r reportv1alpha1.MigrationStatusReport, id string) (waitengine.Decision, reportv1alpha1.WaitMigrationObservation) {
	o := reportv1alpha1.WaitMigrationObservation{
		RequestID: id, Phase: reportv1alpha1.MigrationPhaseUnknown,
		Outcome: reportv1alpha1.MigrationOutcomeUnknown, Validity: "Unavailable",
		InspectedSources: len(r.Sources), InspectedRecords: len(r.Content.Migrations),
	}
	if len(r.Sources) == 0 || len(r.Sources) > 65 || len(r.Content.Migrations) > 200 ||
		len(r.Warnings) != 0 || len(r.Content.Issues) != 0 ||
		(r.Content.Summary.State != reportv1alpha1.MigrationReportStateReported && r.Content.Summary.State != reportv1alpha1.MigrationReportStateEmpty) {
		o.Validity = "Invalid"
		return waitengine.Decision{Reason: waitengine.ReasonInvalidMigration}, o
	}
	for _, source := range r.Sources {
		if source.Freshness != reportv1alpha1.StatusFreshnessCurrent {
			o.Validity = "Invalid"
			return waitengine.Decision{Reason: waitengine.ReasonInvalidMigration}, o
		}
	}
	var found *reportv1alpha1.MigrationRecord
	for i := range r.Content.Migrations {
		record := &r.Content.Migrations[i]
		if record.RequestID != id {
			continue
		}
		if found != nil || record.Classification == reportv1alpha1.MigrationClassificationInvalid ||
			record.Freshness != reportv1alpha1.StatusFreshnessCurrent || len(record.Issues) != 0 {
			o.Validity = "Invalid"
			return waitengine.Decision{Reason: waitengine.ReasonInvalidMigration}, o
		}
		found = record
	}
	if found == nil {
		return waitengine.Decision{Reason: waitengine.ReasonMigrationNotRecorded}, o
	}
	o.Component, o.Phase, o.Outcome = found.Component, found.Phase, found.Outcome
	if found.Classification == reportv1alpha1.MigrationClassificationActive && active(found.Phase) && found.Outcome == reportv1alpha1.MigrationOutcomeInProgress {
		o.Validity = "Valid"
		return waitengine.Decision{Reason: waitengine.ReasonMigrationInProgress}, o
	}
	if found.Classification == reportv1alpha1.MigrationClassificationTerminal && terminal(found.Phase, found.Outcome) {
		o.Validity = "Valid"
		return waitengine.Decision{Matched: true, Reason: waitengine.ReasonMigrationMatched}, o
	}
	o.Validity = "Invalid"
	return waitengine.Decision{Reason: waitengine.ReasonInvalidMigration}, o
}

func active(phase reportv1alpha1.MigrationPhase) bool {
	switch phase {
	case reportv1alpha1.MigrationPhaseAccepted, reportv1alpha1.MigrationPhaseSurgePending,
		reportv1alpha1.MigrationPhaseSurgeReady, reportv1alpha1.MigrationPhaseDraining:
		return true
	}
	return false
}

func terminal(phase reportv1alpha1.MigrationPhase, outcome reportv1alpha1.MigrationOutcome) bool {
	switch phase {
	case reportv1alpha1.MigrationPhaseCompleted:
		return outcome == reportv1alpha1.MigrationOutcomeCompleted
	case reportv1alpha1.MigrationPhaseFailed:
		return outcome == reportv1alpha1.MigrationOutcomeFailed
	case reportv1alpha1.MigrationPhaseRelocated:
		return outcome == reportv1alpha1.MigrationOutcomeRelocated || outcome == reportv1alpha1.MigrationOutcomeRelocationConfirmed
	}
	return false
}
