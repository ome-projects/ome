package validation

import "errors"

// ValidateInferenceReplicaTemplateSource enforces that a replica renders from
// exactly one source: stored runners, or a model reference (with or without a
// runtime reference), or a runtime reference alone. Runtime pins are not
// honored on a replica.
func ValidateInferenceReplicaTemplateSource(runners int, modelRef, runtimeRef, pinnedRuntime bool) error {
	refs := modelRef || runtimeRef
	switch {
	case runners > 0 && refs:
		return errors.New("spec.runners and spec.modelRef/spec.runtimeRef are exclusive; a replica renders from one source")
	case runners == 0 && !refs:
		return errors.New("spec.runners is required unless spec.modelRef or spec.runtimeRef is set")
	case pinnedRuntime:
		return errors.New("spec.runtimeRef.autoSync=false and spec.runtimeRef.revision are not honored on an InferenceReplica; the live runtime renders")
	}
	return nil
}
