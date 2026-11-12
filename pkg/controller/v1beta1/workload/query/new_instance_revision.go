package query

import workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"

// CanaryStepStableRevision names the revision a canary step keeps the
// Component's stable side on while its projected partition holds
// Instances: the revision the status reports as current, when the roll
// target is another one. Empty when no step holds, when the Component has
// promoted no revision yet, or when the roll returns to the current
// revision. The user's own rollingUpdate partition is no step and names
// nothing.
func CanaryStepStableRevision(desired workload.WorkloadDesiredSpec, observed *workload.WorkloadObservedState, target string) string {
	pacing := desired.Pacing
	if target == "" || observed == nil || pacing == nil || pacing.Partition == nil || *pacing.Partition <= 0 {
		return ""
	}
	current := observed.CurrentRevision
	if current == "" || current == target {
		return ""
	}
	return current
}

// NewInstanceRevision names the revision an Instance that has run no
// revision and has no live pod is built on. The plan and the Create pass
// both read it here, so the PodGroup a new gang is admitted under is sized
// for the pods the Create pass renders. While a canary step holds the
// Component the Instance joins the step's stable side, so the share of the
// fleet on the canary revision stays what the step staged; otherwise the
// answer is empty and the roll target decides.
func NewInstanceRevision(desired workload.WorkloadDesiredSpec, observed *workload.WorkloadObservedState, target string) string {
	return CanaryStepStableRevision(desired, observed, target)
}
