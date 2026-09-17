// Package waitir evaluates exact, current InferenceReplica count evidence.
package waitir

import (
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

// ReadyObservation intentionally omits IR identity and controller-provided
// free text. A nil ReadyReplicas is distinct from a verified zero count.
type ReadyObservation struct {
	Component     reportv1alpha1.RuntimeComponentType
	Requested     int32
	ReadyReplicas *int32
	Validity      string
}

// Evaluator pins the selected IR across polls. A replacement IR cannot
// inherit a prior observation, even if its ready count matches the target.
type Evaluator struct {
	sourceName string
	sourceUID  string
}

func (e *Evaluator) Evaluate(r reportv1alpha1.InstanceListReport, component reportv1alpha1.RuntimeComponentType, requested int32) (waitengine.Decision, ReadyObservation) {
	observed := ReadyObservation{Component: component, Requested: requested, Validity: "Unavailable"}
	invalid := func() (waitengine.Decision, ReadyObservation) {
		observed.Validity = "Invalid"
		return waitengine.Decision{Reason: waitengine.ReasonInvalidReplicaReady}, observed
	}
	if !validComponent(component) || requested < 0 || e == nil {
		return invalid()
	}
	if len(r.Content.Components) > 3 || len(r.Sources) > 4 || len(r.Sources) == 0 ||
		len(r.Content.Issues) != 0 || len(r.Warnings) != 0 ||
		r.Content.Summary.State != reportv1alpha1.InstanceListStateReported || r.Content.Summary.Truncated ||
		r.Content.Summary.Components != len(r.Content.Components) {
		return invalid()
	}
	parentSources := 0
	replicaSources := make(map[string]string, len(r.Content.Components))
	seenUIDs := make(map[string]bool, len(r.Sources))
	for _, source := range r.Sources {
		if source.UID == "" || seenUIDs[source.UID] {
			return invalid()
		}
		seenUIDs[source.UID] = true
		switch source.Kind {
		case "InferenceService":
			if source.Name != r.Metadata.Name || source.Namespace != r.Metadata.Namespace ||
				source.UID == "" || source.Evidence != reportv1alpha1.EvidenceReported {
				return invalid()
			}
			parentSources++
		case "InferenceReplica":
			if source.Name == "" || source.Namespace != r.Metadata.Namespace || source.UID == "" ||
				source.Evidence != reportv1alpha1.EvidenceReported || replicaSources[source.Name] != "" {
				return invalid()
			}
			replicaSources[source.Name] = source.UID
		default:
			return invalid()
		}
	}
	if parentSources != 1 || len(replicaSources) != len(r.Content.Components) {
		return invalid()
	}
	var selected *reportv1alpha1.InstanceListComponent
	seenComponents := make(map[reportv1alpha1.RuntimeComponentType]bool, len(r.Content.Components))
	for i := range r.Content.Components {
		candidate := &r.Content.Components[i]
		if !validComponent(candidate.Type) || seenComponents[candidate.Type] || candidate.State != reportv1alpha1.InstanceEvidenceReported ||
			candidate.Generation <= 0 || candidate.ObservedGeneration <= 0 || candidate.ObservedGeneration != candidate.Generation ||
			candidate.Replicas < 0 || candidate.ReadyReplicas < 0 || candidate.ReadyReplicas > candidate.Replicas ||
			replicaSources[candidate.InferenceReplica] == "" {
			return invalid()
		}
		seenComponents[candidate.Type] = true
		if candidate.Type != component {
			continue
		}
		if selected != nil {
			return invalid()
		}
		selected = candidate
	}
	if selected == nil {
		return waitengine.Decision{Reason: waitengine.ReasonReplicaReadyNotRecorded}, observed
	}
	selectedUID := replicaSources[selected.InferenceReplica]
	if e.sourceName != "" && (e.sourceName != selected.InferenceReplica || e.sourceUID != selectedUID) {
		return invalid()
	}
	e.sourceName, e.sourceUID = selected.InferenceReplica, selectedUID
	count := selected.ReadyReplicas
	observed.ReadyReplicas = &count
	observed.Validity = "Valid"
	if count == requested {
		return waitengine.Decision{Matched: true, Reason: waitengine.ReasonReplicaReadyMatched}, observed
	}
	return waitengine.Decision{Reason: waitengine.ReasonReplicaReadyNotMatched}, observed
}

func validComponent(component reportv1alpha1.RuntimeComponentType) bool {
	switch component {
	case reportv1alpha1.RuntimeComponentEngine, reportv1alpha1.RuntimeComponentDecoder, reportv1alpha1.RuntimeComponentRouter:
		return true
	default:
		return false
	}
}
