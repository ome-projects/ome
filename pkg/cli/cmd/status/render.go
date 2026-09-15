package status

import (
	"cmp"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

// componentOrder fixes section ordering; conditions sort component-first,
// aggregate Ready last.
var componentOrder = []v1beta1.ComponentType{
	v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent,
}

const (
	maxCompactSummaryRunes      = 64
	maxCompactComponentRunes    = 12
	maxCompactExtraComponents   = 5
	maxCompactRestartExact      = 9999999
	maxCompactWarnings          = 5
	maxCompactWarningRunes      = 72
	maxCompactEvents            = 5
	maxCompactEventObjectRunes  = 24
	maxCompactEventReasonRunes  = 20
	maxCompactEventMessageRunes = 28
)

func renderCompact(r *report, w io.Writer) error {
	isvc := r.ISVC
	readyCondition := isvc.Status.GetCondition(apis.ConditionReady)
	runtime := "(auto-selected)"
	if isvc.Spec.Runtime != nil {
		runtime = isvc.Spec.Runtime.Name
	}
	summary := printers.Table{
		Headers: []string{"FIELD", "VALUE"},
		Rows: [][]string{
			{"NAME", compactMiddle(isvc.Name, maxCompactSummaryRunes)},
			{"NAMESPACE", compactMiddle(isvc.Namespace, maxCompactSummaryRunes)},
			{"READY", serviceReadyStatus(isvc)},
		},
	}
	if readyCondition != nil && readyCondition.Reason != "" {
		summary.Rows = append(summary.Rows, []string{
			"READY-REASON", compactText(readyCondition.Reason, maxCompactSummaryRunes),
		})
	}
	if isvc.Status.URL != nil {
		summary.Rows = append(summary.Rows, []string{
			"URL", compactMiddle(isvc.Status.URL.String(), maxCompactSummaryRunes),
		})
	}
	if isvc.Spec.Model != nil {
		summary.Rows = append(summary.Rows, []string{
			"MODEL", compactMiddle(isvc.Spec.Model.Name, maxCompactSummaryRunes),
		})
	}
	if isvc.Status.ModelStatus.TransitionStatus != "" {
		summary.Rows = append(summary.Rows, []string{
			"MODEL-STATE", compactText(string(isvc.Status.ModelStatus.TransitionStatus), maxCompactSummaryRunes),
		})
	}
	summary.Rows = append(summary.Rows, []string{
		"RUNTIME", compactMiddle(runtime, maxCompactSummaryRunes),
	})
	if err := summary.WriteSanitized(w); err != nil {
		return err
	}

	components, omittedComponents := compactComponentTable(r)
	if len(components.Rows) > 0 {
		if err := writeStatusf(w, "\n"); err != nil {
			return err
		}
		if err := components.WriteSanitized(w); err != nil {
			return err
		}
		if omittedComponents > 0 {
			if err := writeStatusf(
				w, "... %d more %s; use -o wide\n",
				omittedComponents, plural("component", omittedComponents),
			); err != nil {
				return err
			}
		}
		if err := writeStatusf(
			w,
			"Phase key: R=Running P=Pending F=Failed S=Succeeded U=Unknown T=Terminating\n",
		); err != nil {
			return err
		}
	}

	if err := writeStatusf(w, "\n"); err != nil {
		return err
	}
	features := printers.Table{
		Headers: []string{"FEATURE", "STATUS"},
		Rows: [][]string{
			{"TRAFFIC", compactPresence(isvc.Status.Traffic != nil)},
			{"ROLLOUT", compactRolloutPresence(isvc)},
		},
	}
	if err := features.WriteSanitized(w); err != nil {
		return err
	}

	if len(r.Warnings) > 0 {
		if err := writeStatusf(w, "\nObservation Warnings:\n"); err != nil {
			return err
		}
		warnings := printers.Table{Headers: []string{"WARNING"}}
		limit := min(len(r.Warnings), maxCompactWarnings)
		for _, warning := range r.Warnings[:limit] {
			warnings.Rows = append(warnings.Rows, []string{
				compactText(warning, maxCompactWarningRunes),
			})
		}
		if omitted := len(r.Warnings) - limit; omitted > 0 {
			warnings.Rows = append(warnings.Rows, []string{
				fmt.Sprintf("... %d more %s; use -o wide", omitted, plural("warning", omitted)),
			})
		}
		if err := warnings.WriteSanitized(w); err != nil {
			return err
		}
	}

	if len(r.Events) > 0 {
		if err := writeStatusf(w, "\nRecent Warning Events:\n"); err != nil {
			return err
		}
		events := printers.Table{Headers: []string{"OBJECT", "REASON", "MESSAGE"}}
		recent := compactRecentEvents(r.Events)
		limit := min(len(recent), maxCompactEvents)
		for _, event := range recent[:limit] {
			events.Rows = append(events.Rows, []string{
				compactMiddle(
					printers.OrDash(event.InvolvedObject.Kind)+"/"+printers.OrDash(event.InvolvedObject.Name),
					maxCompactEventObjectRunes,
				),
				compactText(event.Reason, maxCompactEventReasonRunes),
				compactText(event.Message, maxCompactEventMessageRunes),
			})
		}
		if err := events.WriteSanitized(w); err != nil {
			return err
		}
		if omitted := len(recent) - limit; omitted > 0 {
			return writeStatusf(
				w, "... %d more %s; use -o wide\n", omitted, plural("event", omitted),
			)
		}
	}
	return nil
}

func compactRecentEvents(events []corev1.Event) []corev1.Event {
	recent := append([]corev1.Event(nil), events...)
	sort.SliceStable(recent, func(i, j int) bool {
		leftTime := compactEventTime(recent[i])
		rightTime := compactEventTime(recent[j])
		if !leftTime.Equal(rightTime) {
			return leftTime.After(rightTime)
		}
		return cmp.Or(
			cmp.Compare(recent[i].Namespace, recent[j].Namespace),
			cmp.Compare(recent[i].Name, recent[j].Name),
			cmp.Compare(recent[i].InvolvedObject.Kind, recent[j].InvolvedObject.Kind),
			cmp.Compare(recent[i].InvolvedObject.Namespace, recent[j].InvolvedObject.Namespace),
			cmp.Compare(recent[i].InvolvedObject.Name, recent[j].InvolvedObject.Name),
			cmp.Compare(string(recent[i].InvolvedObject.UID), string(recent[j].InvolvedObject.UID)),
			cmp.Compare(recent[i].Reason, recent[j].Reason),
			cmp.Compare(recent[i].Message, recent[j].Message),
		) < 0
	})
	return recent
}

func compactEventTime(event corev1.Event) time.Time {
	if event.Series != nil && !event.Series.LastObservedTime.IsZero() {
		return event.Series.LastObservedTime.Time
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	return event.CreationTimestamp.Time
}

func compactPresence(present bool) string {
	if present {
		return "present"
	}
	return "-"
}

func compactRolloutPresence(isvc *v1beta1.InferenceService) string {
	var sources []string
	if isvc.Status.Rollout != nil {
		sources = append(sources, "run")
	}
	if isvc.Status.Canary != nil {
		sources = append(sources, "canary")
	}
	if isvc.Status.RolloutCoordination != nil {
		sources = append(sources, "coordination")
	}
	if len(sources) == 0 {
		return "-"
	}
	return strings.Join(sources, ",")
}

func compactComponentTable(r *report) (printers.Table, int) {
	table := printers.Table{
		Headers: []string{"COMPONENT", "STATUS", "READY-PODS", "RESTARTS", "PHASES"},
	}
	seen := make(map[v1beta1.ComponentType]bool, len(componentOrder))
	for _, component := range componentOrder {
		seen[component] = true
		if _, inStatus := r.ISVC.Status.Components[component]; !inStatus &&
			len(r.Pods[component]) == 0 && compactComponentCondition(r.ISVC, component) == nil {
			continue
		}
		table.Rows = append(table.Rows, compactComponentRow(r, component))
	}

	remainingSet := make(map[v1beta1.ComponentType]struct{})
	for component := range r.ISVC.Status.Components {
		if !seen[component] {
			remainingSet[component] = struct{}{}
		}
	}
	for component := range r.Pods {
		if !seen[component] {
			remainingSet[component] = struct{}{}
		}
	}
	remaining := make([]v1beta1.ComponentType, 0, len(remainingSet))
	for component := range remainingSet {
		remaining = append(remaining, component)
	}
	sort.Slice(remaining, func(i, j int) bool { return remaining[i] < remaining[j] })
	visible := min(len(remaining), maxCompactExtraComponents)
	for _, component := range remaining[:visible] {
		table.Rows = append(table.Rows, compactComponentRow(r, component))
	}
	omitted := len(remaining) - visible
	if omitted > 0 {
		var pods []corev1.Pod
		for _, component := range remaining[visible:] {
			pods = append(pods, r.Pods[component]...)
		}
		table.Rows = append(table.Rows, compactPodsRow(
			fmt.Sprintf("(%d more)", omitted), "-", pods,
		))
	}
	return table, omitted
}

func compactComponentRow(r *report, component v1beta1.ComponentType) []string {
	condition := "-"
	if observed := compactComponentCondition(r.ISVC, component); observed != nil {
		condition = compactConditionStatus(observed.Status)
	}
	return compactPodsRow(componentLabel(component), condition, r.Pods[component])
}

func compactPodsRow(label, condition string, pods []corev1.Pod) []string {
	ready := 0
	var restarts int64
	phaseCounts := make(map[string]int)
	for _, pod := range pods {
		if compactPodReady(pod) {
			ready++
		}
		for _, status := range pod.Status.ContainerStatuses {
			restarts += int64(status.RestartCount)
		}
		phaseCounts[compactPodPhase(pod)]++
	}
	return []string{
		compactText(label, maxCompactComponentRunes),
		condition,
		fmt.Sprintf("%d/%d", ready, len(pods)),
		compactRestartCount(restarts),
		compactPhaseSummary(phaseCounts),
	}
}

func compactConditionStatus(status corev1.ConditionStatus) string {
	switch status {
	case corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown:
		return string(status)
	default:
		return "Unknown"
	}
}

func compactRestartCount(restarts int64) string {
	if restarts > maxCompactRestartExact {
		return fmt.Sprintf(">%d", maxCompactRestartExact)
	}
	return fmt.Sprintf("%d", restarts)
}

func compactComponentCondition(
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) *apis.Condition {
	var conditionType apis.ConditionType
	switch component {
	case v1beta1.EngineComponent:
		conditionType = v1beta1.EngineReady
	case v1beta1.DecoderComponent:
		conditionType = v1beta1.DecoderReady
	case v1beta1.RouterComponent:
		conditionType = v1beta1.RouterReady
	default:
		return nil
	}
	return isvc.Status.GetCondition(conditionType)
}

func compactPodReady(pod corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func compactPodPhase(pod corev1.Pod) string {
	if pod.DeletionTimestamp != nil {
		return "Terminating"
	}
	switch pod.Status.Phase {
	case corev1.PodPending:
		return "Pending"
	case corev1.PodRunning:
		return "Running"
	case corev1.PodSucceeded:
		return "Succeeded"
	case corev1.PodFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

func compactPhaseSummary(counts map[string]int) string {
	if len(counts) == 0 {
		return "-"
	}
	order := []struct {
		name string
		code string
	}{
		{name: "Running", code: "R"},
		{name: "Pending", code: "P"},
		{name: "Failed", code: "F"},
		{name: "Succeeded", code: "S"},
		{name: "Unknown", code: "U"},
		{name: "Terminating", code: "T"},
	}
	parts := make([]string, 0, len(counts))
	for _, phase := range order {
		if count := counts[phase.name]; count > 0 {
			parts = append(parts, fmt.Sprintf("%s%d", phase.code, count))
		}
	}
	return strings.Join(parts, "/")
}

func compactText(value string, limit int) string {
	if value == "" {
		return "-"
	}
	return printers.BoundedCell(value, limit)
}

func compactMiddle(value string, limit int) string {
	if value == "" {
		return "-"
	}
	return printers.BoundedMiddleCell(value, limit)
}

func plural(word string, count int) string {
	if count == 1 {
		return word
	}
	return word + "s"
}

func serviceReadyStatus(isvc *v1beta1.InferenceService) string {
	condition := isvc.Status.GetCondition(apis.ConditionReady)
	if condition == nil {
		return "Unknown"
	}
	return compactConditionStatus(condition.Status)
}

func render(r *report, w io.Writer) error {
	isvc := r.ISVC
	if err := writeStatusf(w, "Name:       %s\n", isvc.Name); err != nil {
		return err
	}
	if err := writeStatusf(w, "Namespace:  %s\n", isvc.Namespace); err != nil {
		return err
	}
	if err := writeStatusf(w, "Ready:      %s\n", serviceReadyStatus(isvc)); err != nil {
		return err
	}
	if isvc.Status.URL != nil {
		if err := writeStatusf(w, "URL:        %s\n", isvc.Status.URL.String()); err != nil {
			return err
		}
	}
	if isvc.Spec.Model != nil {
		if err := writeStatusf(w, "Model:      %s\n", isvc.Spec.Model.Name); err != nil {
			return err
		}
	}
	if isvc.Spec.Runtime != nil {
		if err := writeStatusf(w, "Runtime:    %s\n", isvc.Spec.Runtime.Name); err != nil {
			return err
		}
	} else {
		if err := writeStatusf(w, "Runtime:    (auto-selected)\n"); err != nil {
			return err
		}
	}
	if len(r.Warnings) > 0 {
		if err := writeStatusf(w, "\nObservation Warnings:\n"); err != nil {
			return err
		}
		for _, warning := range r.Warnings {
			if err := writeStatusf(w, "  - %s\n", warning); err != nil {
				return err
			}
		}
	}

	if err := writeStatusf(w, "\nConditions:\n"); err != nil {
		return err
	}
	conds := append([]apis.Condition{}, isvc.Status.Conditions...)
	sort.SliceStable(conds, func(i, j int) bool { return condRank(conds[i].Type) < condRank(conds[j].Type) })
	condTable := printers.Table{Headers: []string{"  TYPE", "STATUS", "REASON", "MESSAGE"}}
	for _, c := range conds {
		condTable.Rows = append(condTable.Rows, []string{
			"  " + string(c.Type), string(c.Status), printers.OrDash(c.Reason), printers.OrDash(c.Message),
		})
	}
	if err := condTable.Write(w); err != nil {
		return err
	}

	if err := writeStatusf(w, "\nComponents:\n"); err != nil {
		return err
	}
	seen := make(map[v1beta1.ComponentType]bool, len(componentOrder))
	for _, ct := range componentOrder {
		seen[ct] = true
		spec, inStatus := isvc.Status.Components[ct]
		pods := r.Pods[ct]
		if !inStatus && len(pods) == 0 {
			continue
		}
		if err := writeComponent(w, componentLabel(ct), spec, pods); err != nil {
			return err
		}
	}
	// gather() buckets pods by their component label verbatim, so a pod
	// whose label is missing (ComponentType("")) or names something other
	// than engine/decoder/router lands here instead of in componentOrder.
	// Surface it rather than silently dropping it from the report; sort
	// for deterministic output.
	var remaining []v1beta1.ComponentType
	for ct := range r.Pods {
		if !seen[ct] {
			remaining = append(remaining, ct)
		}
	}
	sort.Slice(remaining, func(i, j int) bool { return remaining[i] < remaining[j] })
	for _, ct := range remaining {
		if err := writeComponent(w, componentLabel(ct), isvc.Status.Components[ct], r.Pods[ct]); err != nil {
			return err
		}
	}

	if err := writeStatusf(
		w, "\nModel Status:\n  Transition: %s\n",
		printers.OrDash(string(isvc.Status.ModelStatus.TransitionStatus)),
	); err != nil {
		return err
	}
	for _, section := range []struct {
		name    string
		present bool
	}{
		{name: "Traffic", present: isvc.Status.Traffic != nil},
		{name: "Canary", present: isvc.Status.Canary != nil},
		{name: "Placement", present: isvc.Status.Placement != nil},
		{name: "RolloutCoordination", present: isvc.Status.RolloutCoordination != nil},
	} {
		if err := writeOptionalSection(w, section.name, section.present); err != nil {
			return err
		}
	}

	if len(r.Events) > 0 {
		if err := writeStatusf(w, "\nRecent Warning Events:\n"); err != nil {
			return err
		}
		evTable := printers.Table{Headers: []string{"  OBJECT", "REASON", "MESSAGE"}}
		for _, e := range r.Events {
			evTable.Rows = append(evTable.Rows, []string{
				"  " + e.InvolvedObject.Kind + "/" + e.InvolvedObject.Name, e.Reason, e.Message,
			})
		}
		if err := evTable.Write(w); err != nil {
			return err
		}
	}
	return nil
}

// componentLabel names a Components section heading. Pods whose component
// label was missing land under ComponentType("") (see gather.go); render
// that as an explicit placeholder rather than a blank heading.
func componentLabel(ct v1beta1.ComponentType) string {
	if ct == "" {
		return "(unlabeled)"
	}
	return string(ct)
}

// writeComponent renders one Components section: the revision header line
// plus a pod table (or a "(no pods)" placeholder).
func writeComponent(w io.Writer, label string, spec v1beta1.ComponentStatusSpec, pods []corev1.Pod) error {
	if err := writeStatusf(w, "  %s   revision %s\n", label, printers.OrDash(spec.LatestReadyRevision)); err != nil {
		return err
	}
	if len(pods) == 0 {
		return writeStatusf(w, "    (no pods)\n")
	}
	podTable := printers.Table{Headers: []string{"    POD", "PHASE", "READY", "RESTARTS", "NODE"}}
	for _, p := range pods {
		ready, restarts := 0, int32(0)
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Ready {
				ready++
			}
			restarts += cs.RestartCount
		}
		podTable.Rows = append(podTable.Rows, []string{
			"    " + p.Name, string(p.Status.Phase),
			fmt.Sprintf("%d/%d", ready, len(p.Spec.Containers)),
			fmt.Sprintf("%d", restarts), printers.OrDash(p.Spec.NodeName),
		})
	}
	return podTable.Write(w)
}

// writeOptionalSection prints a one-line presence indicator for a
// v1-deferred status section (Traffic/Canary/Placement/RolloutCoordination):
// full rendering is follow-up work, but presence must never be silently
// dropped from the report.
func writeOptionalSection(w io.Writer, name string, present bool) error {
	if !present {
		return nil
	}
	return writeStatusf(w, "  %s: present (inspect with kubectl get inferenceservice -o yaml)\n", name)
}

func writeStatusf(w io.Writer, format string, args ...any) error {
	_, err := fmt.Fprintf(w, format, args...)
	return err
}

func condRank(t apis.ConditionType) int {
	switch t {
	case "EngineReady":
		return 0
	case "DecoderReady":
		return 1
	case "RouterReady":
		return 2
	case "IngressReady":
		return 3
	case apis.ConditionReady:
		return 5
	default:
		return 4
	}
}
