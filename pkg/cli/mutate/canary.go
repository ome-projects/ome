package mutate

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/canaryevidence"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

var (
	ErrAnalysisOverrideConfirmation = errors.New("analysis override requires --override-analysis --yes")
	ErrCanaryPaused                 = errors.New("action refused: globally paused canary cannot consume a request")
	ErrCanaryGate                   = errors.New("action refused: promote requires an active indefinite manual gate; analysis requires explicit override")
)

// canaryPreview holds only allowlisted, normalized scalars. It is not a public
// report and never retains metric names, queries, messages or provider data.
type canaryPreview struct {
	rows     [][]string
	override bool
	final    bool
}

// PrepareCanaryRollout proves applicability of the existing hash-only mailbox.
// It neither evaluates controller readiness nor chooses a rollback revision.
func PrepareCanaryRollout(v *v1beta1.InferenceService, state *effective.RuntimeState, work ReplicaEvidence, action string, overrideAnalysis, yes bool, clock reportv1alpha1.Clock) (RolloutPlan, error) {
	if action != "promote" && action != "rollback" {
		return RolloutPlan{}, errors.New("unsupported canary action")
	}
	if overrideAnalysis && (action != "promote" || !yes) {
		return RolloutPlan{}, ErrAnalysisOverrideConfirmation
	}
	if err := ValidateTarget(v); err != nil {
		return RolloutPlan{}, err
	}
	native, err := RequireNativeRuntime(v, state)
	if err != nil {
		return RolloutPlan{}, err
	}
	if paused, _ := constants.RolloutPauseState(v.Annotations); paused {
		return RolloutPlan{}, ErrCanaryPaused
	}
	for _, key := range []string{constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation} {
		if _, exists := v.Annotations[key]; exists {
			return RolloutPlan{}, ErrPending
		}
	}
	if !work.complete {
		return RolloutPlan{}, ErrBounds
	}
	if work.uid != string(v.UID) || work.resourceVersion != v.ResourceVersion {
		return RolloutPlan{}, ErrStale
	}
	if v.Status.Rollout == nil || v.Status.Rollout.ActiveRun == nil || v.Status.Canary == nil {
		return RolloutPlan{}, ErrIdle
	}
	if !pinnedevidence.ValidActiveRun(v) {
		return RolloutPlan{}, ErrStale
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	now := clock.Now()
	run := v.Status.Rollout.ActiveRun
	if run.PinnedAt.Time.After(now) {
		return RolloutPlan{}, ErrStale
	}
	groupIndex := -1
	for i := range run.Plan.Groups {
		if run.Plan.Groups[i].Group.Canary != nil {
			if groupIndex != -1 {
				return RolloutPlan{}, ErrStale
			}
			groupIndex = i
		}
	}
	if groupIndex < 0 {
		return RolloutPlan{}, ErrIdle
	}
	group := &run.Plan.Groups[groupIndex]
	primary, valid := canaryevidence.Primary(group.Group.Components)
	if !valid {
		return RolloutPlan{}, ErrStale
	}
	nativeSet := map[v1beta1.ComponentType]bool{}
	for _, component := range native {
		nativeSet[v1beta1.ComponentType(component)] = true
	}
	targets := map[v1beta1.ComponentType]v1beta1.RolloutRunTarget{}
	for _, target := range run.TargetRevisions {
		if work.sources[target.Component] != target.Revision {
			return RolloutPlan{}, ErrStale
		}
		targets[target.Component] = target
	}
	parts := make([]string, 0, len(group.Group.Components))
	components := make([]string, 0, len(group.Group.Components))
	changedSecondary := false
	for _, component := range group.Group.Components {
		if !nativeSet[component] {
			return RolloutPlan{}, ErrRuntime
		}
		target := targets[component]
		if !canaryevidence.SafeRevisionHash(target.StableRevision) {
			return RolloutPlan{}, ErrStale
		}
		parts = append(parts, string(component)+"="+target.Revision)
		components = append(components, string(component))
		if component != primary && target.StableRevision != target.Revision {
			changedSecondary = true
		}
	}
	sort.Strings(parts)
	cs := v.Status.Canary
	// Pending and Failed do not bind serving traffic. They still need the
	// secondary-only identity proof when primary stable and target are equal.
	if cs.StableRevisionHash == cs.CanaryRevisionHash && !changedSecondary {
		return RolloutPlan{}, ErrStale
	}
	if cs.TargetID != "ct1:"+rolloutpolicy.ShortHash([]byte(strings.Join(parts, ";"))) ||
		cs.CanaryRevisionHash != targets[primary].Revision || cs.StableRevisionHash != targets[primary].StableRevision {
		return RolloutPlan{}, ErrStale
	}
	for _, stamp := range []*metav1.Time{cs.StepEnteredTime, cs.LastEvaluationTime, cs.LastConclusiveEvaluationTime} {
		if stamp != nil && (stamp.IsZero() || stamp.Time.After(now)) {
			return RolloutPlan{}, ErrStale
		}
	}
	if cs.LastConclusiveEvaluationTime != nil && (cs.LastEvaluationTime == nil || cs.LastConclusiveEvaluationTime.After(cs.LastEvaluationTime.Time)) {
		return RolloutPlan{}, ErrStale
	}
	if cs.CurrentStep < 0 || int(cs.CurrentStep) >= len(group.Group.Canary.Steps) {
		return RolloutPlan{}, ErrStale
	}
	step := &group.Group.Canary.Steps[cs.CurrentStep]
	// Analysis samples survive capacity dips and recovery, both of which
	// restamp entry. Entry is not immutable sample/run identity evidence.
	if step.Analysis == nil && cs.LastEvaluationTime != nil && cs.StepEnteredTime != nil && cs.LastEvaluationTime.Before(cs.StepEnteredTime) {
		return RolloutPlan{}, ErrStale
	}
	projection, err := rolloutprojection.Project(v, clock)
	if err != nil {
		return RolloutPlan{}, ErrStale
	}
	for _, issue := range projection.Content.Issues {
		if issue.Code != reportv1alpha1.RolloutIssueEpochUnverifiable && issue.Code != reportv1alpha1.RolloutIssueAnalysisInconclusive {
			return RolloutPlan{}, ErrStale
		}
	}
	var observed *reportv1alpha1.RolloutGroupStatus
	for i := range projection.Content.Groups {
		if projection.Content.Groups[i].Index == groupIndex {
			observed = &projection.Content.Groups[i]
		}
	}
	if observed == nil || observed.Step == nil {
		return RolloutPlan{}, ErrStale
	}
	if cs.RolledBackRevisionHash != "" {
		return RolloutPlan{}, ErrIdle
	}
	final := int(cs.CurrentStep) == len(group.Group.Canary.Steps)-1
	if action == "promote" {
		if cs.PreStepHold || cs.PromotedThrough != "" || cs.StepEnteredTime == nil || cs.ObservedTrafficWeight != step.Traffic ||
			(!final && observed.Phase != reportv1alpha1.RolloutPhasePaused) ||
			(final && (observed.Phase != reportv1alpha1.RolloutPhasePromoting || step.Traffic != 100)) {
			return RolloutPlan{}, ErrCanaryGate
		}
		if overrideAnalysis {
			if observed.Step.Gate != reportv1alpha1.RolloutGateAnalysis {
				return RolloutPlan{}, ErrCanaryGate
			}
		} else if observed.Step.Gate != reportv1alpha1.RolloutGateManual {
			return RolloutPlan{}, ErrCanaryGate
		}
	} else {
		switch observed.Phase {
		case reportv1alpha1.RolloutPhasePending, reportv1alpha1.RolloutPhaseCanarying, reportv1alpha1.RolloutPhasePaused, reportv1alpha1.RolloutPhasePromoting, reportv1alpha1.RolloutPhaseFailed:
		default:
			return RolloutPlan{}, ErrIdle
		}
	}
	preview, err := prepareCanaryPreview(run, groupIndex, primary, targets, cs, observed, step, overrideAnalysis, final)
	if err != nil {
		return RolloutPlan{}, err
	}
	p := RolloutPlan{target: reportv1alpha1.ActionTarget{Kind: "InferenceService", Namespace: v.Namespace, Name: v.Name, UID: string(v.UID), ResourceVersion: v.ResourceVersion}, action: action, components: components, revisionHash: cs.CanaryRevisionHash, canary: preview, work: work}
	key, value := constants.RolloutPromoteAnnotation, cs.CanaryRevisionHash
	if action == "rollback" {
		key, value = constants.RolloutRollbackAnnotation, "true"
	}
	patch := []patchOperation{{Op: "test", Path: "/metadata/uid", Value: string(v.UID)}, {Op: "test", Path: "/metadata/resourceVersion", Value: v.ResourceVersion}}
	if v.Annotations == nil {
		patch = append(patch, patchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]string{}})
	}
	path := "/metadata/annotations/" + strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
	patch = append(patch, patchOperation{Op: "add", Path: path, Value: value})
	p.patch, err = json.Marshal(patch)
	if err != nil {
		return RolloutPlan{}, errors.New("encode guarded patch")
	}
	return p, nil
}

func prepareCanaryPreview(run *v1beta1.RolloutRun, index int, primary v1beta1.ComponentType, targets map[v1beta1.ComponentType]v1beta1.RolloutRunTarget, cs *v1beta1.CanaryStatus, observed *reportv1alpha1.RolloutGroupStatus, step *v1beta1.RolloutGroupStep, override, final bool) (*canaryPreview, error) {
	p := &canaryPreview{override: override, final: final}
	add := func(field, value string) { p.rows = append(p.rows, []string{field, value}) }
	add("Run", run.RunID)
	add("Pinned digest", run.Plan.Groups[index].PortableDigest)
	add("Target ID", cs.TargetID)
	add("Group index", strconv.Itoa(index))
	add("Primary", string(primary))
	add("Phase", string(observed.Phase))
	add("Step index", fmt.Sprintf("%d of %d (zero-based)", cs.CurrentStep, len(run.Plan.Groups[index].Group.Canary.Steps)))
	add("Capacity", step.Capacity.String())
	add("Traffic", fmt.Sprintf("target=%d%% observed=%d%%", step.Traffic, cs.ObservedTrafficWeight))
	add("Gate", string(observed.Step.Gate))
	add("Entered", previewTime(cs.StepEnteredTime))
	add("Pre-step hold", strconv.FormatBool(cs.PreStepHold))
	for _, component := range run.Plan.Groups[index].Group.Components {
		add(string(component)+" target", targets[component].Revision)
		add(string(component)+" stable", targets[component].StableRevision+" (Reported)")
	}
	if step.Analysis == nil {
		return p, nil
	}
	add("Analysis", string(observed.Step.Analysis))
	add("Evaluated", previewTime(cs.LastEvaluationTime))
	if cs.LastEvaluationTime != nil && cs.StepEnteredTime != nil && cs.LastEvaluationTime.Before(cs.StepEnteredTime) {
		add("Sample provenance", "Retained (before current entry)")
	}
	add("Conclusive", previewTime(cs.LastConclusiveEvaluationTime))
	add("Failed checks", fmt.Sprintf("%d / %d", cs.AnalysisFailedChecks, step.Analysis.FailureLimit))
	add("Interval", step.Analysis.Interval.Duration.String())
	if step.Analysis.InitialDelay != nil {
		add("Warm-up", step.Analysis.InitialDelay.Duration.String())
	} else {
		add("Warm-up", "0s")
	}
	if step.Pause != nil && step.Pause.Duration != nil {
		add("Bake", step.Pause.Duration.Duration.String())
	} else {
		add("Bake", "0s")
	}
	metricIndex := map[string]int{}
	counts := map[string]int{}
	for _, r := range cs.MetricResults {
		counts[r.Name]++
	}
	for i, metric := range step.Analysis.Metrics {
		metricIndex[metric.Name] = i + 1
		threshold, err := previewNumber(metric.Threshold)
		if err != nil {
			return nil, err
		}
		add(fmt.Sprintf("Metric %d", i+1), string(metric.Operator)+" "+threshold)
		if counts[metric.Name] == 0 {
			add(fmt.Sprintf("Metric %d state", i+1), "Unobserved / Unavailable")
		}
	}
	for i, result := range cs.MetricResults {
		value, err := previewNumber(result.Value)
		if err != nil {
			return nil, err
		}
		threshold, err := previewNumber(result.Threshold)
		if err != nil {
			return nil, err
		}
		ordinal, found := metricIndex[result.Name]
		binding := "Unmatched"
		outcome := "Inconclusive"
		operator := "Unavailable"
		switch result.Operator {
		case v1beta1.ComparisonLT, v1beta1.ComparisonLTE, v1beta1.ComparisonGT, v1beta1.ComparisonGTE:
			operator = string(result.Operator)
		}
		if found {
			binding = fmt.Sprintf("Metric %d", ordinal)
			metric := step.Analysis.Metrics[ordinal-1]
			if counts[result.Name] == 1 && result.Value != "" && result.Operator == metric.Operator && result.Threshold == metric.Threshold {
				outcome = "Failing"
				if result.Passed {
					outcome = "Passing"
				}
			}
		}
		add(fmt.Sprintf("Result %d", i+1), binding+" / "+outcome)
		add("Value / bound", value+" / "+operator+" "+threshold)
		add("Sampled", previewTime(result.Time))
	}
	return p, nil
}

func previewTime(stamp *metav1.Time) string {
	if stamp == nil {
		return "Unobserved"
	}
	return stamp.Time.UTC().Format(time.RFC3339)
}

func previewNumber(raw string) (string, error) {
	if raw == "" {
		return "Unavailable", nil
	}
	number, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
		return "", ErrStale
	}
	return strconv.FormatFloat(number, 'g', -1, 64), nil
}
