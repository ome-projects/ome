package waitrollout

import (
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	report "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitpredicate"
)

// Admission never copies a matching prefix or invokes a projection/helper
// before bounding every input it may inspect. Unrelated private payload is
// also traversed without allocation before helpers can DeepCopy the parent.
func inspect(v *ome.InferenceService, now time.Time) (report.WaitRolloutObservation, error) {
	o := report.WaitRolloutObservation{Validity: "Valid", Inspection: report.WaitRolloutInspection{State: "Complete"}, Warnings: []report.WaitRolloutWarning{}}
	identity := func() (report.WaitRolloutObservation, error) {
		return o, &waitengine.Error{Reason: waitengine.ReasonInvalidIdentity}
	}
	limit := func() (report.WaitRolloutObservation, error) {
		return o, &waitengine.Error{Reason: waitengine.ReasonRolloutInspectionLimit}
	}
	if v == nil || v.UID == "" || v.ResourceVersion == "" || len(validation.IsDNS1123Subdomain(v.Name)) > 0 || len(validation.IsDNS1123Label(v.Namespace)) > 0 || (v.Kind != "" && v.Kind != "InferenceService") || (v.APIVersion != "" && v.APIVersion != "ome.io/v1beta1") {
		return identity()
	}
	o.Inspection.Conditions = len(v.Status.Conditions)
	o.Inspection.Components = len(v.Status.Components)
	if len(v.UID) > 256 || len(v.ResourceVersion) > 256 || len(v.Status.Conditions) > 64 || len(v.Status.Components) > 3 {
		return limit()
	}
	if v.Spec.Rollout != nil {
		o.Inspection.Groups = len(v.Spec.Rollout.Groups)
		if !groupsBounded(v.Spec.Rollout.Groups) {
			return limit()
		}
	}
	if r := v.Status.Rollout; r != nil {
		if len(r.Groups) > 3 {
			return limit()
		}
		if last := r.LastRun; last != nil && (len(last.Groups) > 3 || len(last.TargetRevisions) > 3) {
			return limit()
		}
		if run := r.ActiveRun; run != nil {
			o.Inspection.PinnedGroups = len(run.Plan.Groups)
			o.Inspection.Targets = len(run.TargetRevisions)
			if len(run.Plan.Groups) > 3 || len(run.TargetRevisions) > 3 {
				return limit()
			}
			for i := range run.Plan.Groups {
				if !groupBounded(&run.Plan.Groups[i].Group) {
					return limit()
				}
			}
		}
	}
	if c := v.Status.Canary; c != nil && len(c.MetricResults) > 10 {
		return limit()
	}
	if c := v.Status.RolloutCoordination; c != nil {
		if len(c.Groups) > 3 {
			return limit()
		}
		for i := range c.Groups {
			g := &c.Groups[i]
			if len(g.Components) > 3 || len(g.Order) > 3 {
				return limit()
			}
			if r := g.ObservedRatio; r != nil && (len(r.Original) > 3 || len(r.Current) > 3 || len(r.NewPods) > 3) {
				return limit()
			}
		}
	}
	for _, s := range v.Status.Components {
		if len(s.Traffic) > 16 {
			return limit()
		}
		if s.Lifecycle != nil && len(s.Lifecycle.Conditions) > 64 {
			return limit()
		}
		if s.Canary != nil && len(s.Canary.MetricResults) > 10 {
			return limit()
		}
	}
	b := payloadBudget{}
	if !b.inspect(reflect.ValueOf(v), 0, "") {
		return limit()
	}
	invalid := func(w report.WaitRolloutWarning) {
		o.Validity = "Invalid"
		if !slices.Contains(o.Warnings, w) {
			o.Warnings = append(o.Warnings, w)
		}
	}
	if v.Generation < 0 || v.Status.ObservedGeneration < 0 {
		invalid("InvalidRolloutRecord")
	}
	_, conditions, err := waitpredicate.EvaluateReady(v, corev1.ConditionTrue, now)
	if err != nil {
		return o, err
	}
	for _, w := range conditions.Inspection.Warnings {
		switch w {
		case waitpredicate.WarningInvalidRecord, waitpredicate.WarningOversizedRecord, waitpredicate.WarningFutureTimestamp:
			invalid(report.WaitRolloutWarning(w))
		case waitpredicate.WarningConflictingReady:
			invalid(report.WaitRolloutWarning(w))
		}
	}
	seen := map[string]bool{}
	for _, c := range v.Status.Conditions {
		switch string(c.Type) {
		case ome.RolloutPlanReadyCondition, ome.RolloutPlanDriftCondition, ome.RolloutCoordinationReady:
			if seen[string(c.Type)] {
				invalid("InvalidConditionRecord")
			}
			seen[string(c.Type)] = true
		}
	}
	for component, s := range v.Status.Components {
		if !supported(component) {
			invalid("InvalidRolloutRecord")
		}
		if l := s.Lifecycle; l != nil {
			if l.ObservedGeneration < 0 {
				invalid("InvalidRolloutRecord")
			}
			for _, count := range []int32{l.Replicas, l.ReadyReplicas, l.ServingReplicas, l.AvailableReplicas, l.UpdatedReplicas, l.UpdatedReadyReplicas} {
				if count < 0 {
					invalid("InvalidRolloutRecord")
				}
			}
			seenTypes := map[string]bool{}
			for _, c := range l.Conditions {
				// These conditions and their stamp are copied from IR.Status;
				// the parent ISVC generation is a different resource domain.
				if c.Type == "" || len(c.Type) > 253 || len(c.Reason) > 1024 || len(c.Message) > 4096 || !utf8.ValidString(c.Type) || !utf8.ValidString(c.Reason) || !utf8.ValidString(c.Message) || !validCondition(c.Status) || seenTypes[c.Type] || c.ObservedGeneration < 0 || c.ObservedGeneration > l.ObservedGeneration || c.LastTransitionTime.IsZero() || c.LastTransitionTime.Time.After(now) {
					invalid("InvalidConditionRecord")
				}
				seenTypes[c.Type] = true
				if c.ObservedGeneration >= 0 && c.ObservedGeneration < l.ObservedGeneration && !slices.Contains(o.Warnings, report.WaitRolloutWarning("StaleReportedCondition")) {
					o.Warnings = append(o.Warnings, "StaleReportedCondition")
				}
			}
		}
	}
	validTime := func(t *metav1.Time) {
		if t != nil && (t.IsZero() || t.Time.After(now)) {
			invalid("InvalidRolloutTimestamp")
		}
	}
	if c := v.Status.RolloutCoordination; c != nil {
		for _, g := range c.Groups {
			validTime(g.LastTransitionTime)
			if r := g.ObservedRatio; r != nil {
				for _, m := range []map[ome.ComponentType]int32{r.Original, r.Current, r.NewPods} {
					for key, count := range m {
						if !supported(key) || count < 0 {
							invalid("InvalidRolloutRecord")
						}
					}
				}
			}
		}
	}
	inspectCanaryTimes := func(c *ome.CanaryStatus) {
		if c == nil {
			return
		}
		validTime(c.StepEnteredTime)
		validTime(c.LastEvaluationTime)
		validTime(c.LastConclusiveEvaluationTime)
		if c.LastEvaluationTime != nil && c.LastConclusiveEvaluationTime != nil && c.LastConclusiveEvaluationTime.Time.After(c.LastEvaluationTime.Time) {
			invalid("InvalidRolloutTimestamp")
		}
		for _, m := range c.MetricResults {
			validTime(m.Time)
		}
	}
	inspectCanaryTimes(v.Status.Canary)
	for _, s := range v.Status.Components {
		inspectCanaryTimes(s.Canary)
	}
	if r := v.Status.Rollout; r != nil && r.ActiveRun != nil {
		run := r.ActiveRun
		if !pinnedevidence.ValidActiveRun(v) || run.OpenedAt.Time.After(now) || run.PinnedAt.Time.After(now) {
			invalid("InvalidPinnedRun")
		}
	}
	return o, nil
}

func supported(c ome.ComponentType) bool {
	return c == ome.EngineComponent || c == ome.DecoderComponent || c == ome.RouterComponent
}
func validCondition(c metav1.ConditionStatus) bool {
	return c == metav1.ConditionTrue || c == metav1.ConditionFalse || c == metav1.ConditionUnknown
}
func groupsBounded(groups []ome.RolloutGroup) bool {
	if len(groups) > 3 {
		return false
	}
	for i := range groups {
		if !groupBounded(&groups[i]) {
			return false
		}
	}
	return true
}
func groupBounded(g *ome.RolloutGroup) bool {
	if len(g.Components) > 3 || len(g.Order) > 3 {
		return false
	}
	if g.Canary != nil {
		if len(g.Canary.Steps) > 20 {
			return false
		}
		for _, s := range g.Canary.Steps {
			if s.Analysis != nil && len(s.Analysis.Metrics) > 10 {
				return false
			}
		}
	}
	return true
}

type payloadBudget struct{ nodes, bytes int }

func (b *payloadBudget) inspect(v reflect.Value, depth int, field string) bool {
	b.nodes++
	if b.nodes > 65536 || depth > 64 {
		return false
	}
	if !v.IsValid() {
		return true
	}
	if v.Type() == reflect.TypeOf(intstr.IntOrString{}) && len(v.FieldByName("StrVal").String()) > 64 {
		return false
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			return b.inspect(v.Elem(), depth+1, field)
		}
	case reflect.String:
		s := v.String()
		maxBytes := 4096
		if field == "Name" || field == "Namespace" || strings.Contains(field, "Revision") || strings.Contains(field, "Digest") {
			maxBytes = 253
		}
		b.bytes += len(s)
		if len(s) > maxBytes || b.bytes > 2*1024*1024 {
			return false
		}
	case reflect.Map:
		if v.Len() > 256 {
			return false
		}
		it := v.MapRange()
		for it.Next() {
			if !b.inspect(it.Key(), depth+1, "") || !b.inspect(it.Value(), depth+1, "") {
				return false
			}
		}
	case reflect.Slice, reflect.Array:
		if v.Len() > 2048 {
			return false
		}
		for i := 0; i < v.Len(); i++ {
			if !b.inspect(v.Index(i), depth+1, field) {
				return false
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if f.PkgPath == "" && !b.inspect(v.Field(i), depth+1, f.Name) {
				return false
			}
		}
	}
	return true
}
