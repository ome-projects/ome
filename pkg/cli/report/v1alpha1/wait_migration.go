package v1alpha1

import "github.com/google/uuid"

// WaitMigrationObservation contains only allowlisted evidence for one exact
// migration request. Arbitrary controller messages and annotations stay out.
type WaitMigrationObservation struct {
	RequestID        string               `json:"requestID"`
	Component        RuntimeComponentType `json:"component,omitempty"`
	Phase            MigrationPhase       `json:"phase"`
	Outcome          MigrationOutcome     `json:"migrationOutcome"`
	Validity         string               `json:"validity"`
	InspectedSources int                  `json:"inspectedSources"`
	InspectedRecords int                  `json:"inspectedRecords"`
}

func (r WaitRequested) IsMigration() bool { return r == WaitRequestedMigrationTerminal }

func (o WaitMigrationObservation) Canonical() WaitMigrationObservation {
	parsed, err := uuid.Parse(o.RequestID)
	if err != nil || parsed.String() != o.RequestID || parsed.Variant() != uuid.RFC4122 || parsed.Version() < 1 || parsed.Version() > 8 {
		o.RequestID = ""
	}
	switch o.Component {
	case RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter, "":
	default:
		o.Component = ""
	}
	switch o.Phase {
	case MigrationPhaseAccepted, MigrationPhaseSurgePending, MigrationPhaseSurgeReady,
		MigrationPhaseDraining, MigrationPhaseCompleted, MigrationPhaseFailed, MigrationPhaseRelocated:
	default:
		o.Phase = MigrationPhaseUnknown
	}
	switch o.Outcome {
	case MigrationOutcomeInProgress, MigrationOutcomeCompleted, MigrationOutcomeFailed,
		MigrationOutcomeRelocated, MigrationOutcomeRelocationConfirmed:
	default:
		o.Outcome = MigrationOutcomeUnknown
	}
	if o.Validity != "Valid" && o.Validity != "Unavailable" {
		o.Validity = "Invalid"
	}
	o.InspectedSources = max(0, min(o.InspectedSources, 65))
	o.InspectedRecords = max(0, min(o.InspectedRecords, 200))
	if o.Validity == "Valid" && (o.RequestID == "" || o.Component == "" ||
		o.InspectedSources < 2 || o.InspectedRecords < 1 || !validWaitMigrationPair(o.Phase, o.Outcome)) {
		o.Validity = "Invalid"
	}
	return o
}

func validWaitMigrationPair(phase MigrationPhase, outcome MigrationOutcome) bool {
	switch phase {
	case MigrationPhaseAccepted, MigrationPhaseSurgePending, MigrationPhaseSurgeReady, MigrationPhaseDraining:
		return outcome == MigrationOutcomeInProgress
	case MigrationPhaseCompleted:
		return outcome == MigrationOutcomeCompleted
	case MigrationPhaseFailed:
		return outcome == MigrationOutcomeFailed
	case MigrationPhaseRelocated:
		return outcome == MigrationOutcomeRelocated || outcome == MigrationOutcomeRelocationConfirmed
	default:
		return false
	}
}
