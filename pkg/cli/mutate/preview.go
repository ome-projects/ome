package mutate

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

func (p RolloutPlan) WritePreview(out io.Writer, contextName, omeNamespace string, dryRun reportv1alpha1.DryRunMode) error {
	if !SafeScalar(contextName) || !SafeScalar(omeNamespace) || p.target.UID == "" {
		return errors.New("action preview identity is unavailable or unsafe")
	}
	rows := [][]string{}
	add := func(field, value string) {
		if value == "" {
			value = "<absent>"
		}
		for len(value) > 56 {
			rows = append(rows, []string{field, value[:56]})
			value = value[56:]
			field = "(continued)"
		}
		rows = append(rows, []string{field, value})
	}
	add("Action", "rollout "+p.action)
	add("Context", contextName)
	add("Workload NS", p.target.Namespace)
	add("OME NS", omeNamespace)
	add("Target", p.target.Kind+"/"+p.target.Name)
	add("UID", p.target.UID)
	add("ResourceVersion", p.target.ResourceVersion)
	add("Dry-run", string(dryRun))
	add("Affected", strings.Join(p.components, ", "))
	add("Pause depth", p.previousPause)
	if p.action == "pause" {
		add("Set annotation", constants.PausedRolloutAnnotation)
		add("Value", "true")
	} else {
		add("Remove annotation", constants.PausedRolloutAnnotation)
		add("Value", strconv.Quote(p.previousPause))
		for _, value := range p.removed {
			add("Remove annotation", value.key)
			add("Value", strconv.Quote(value.value))
		}
	}
	add("Active records", fmt.Sprintf("operations=%d migrations=%d", p.work.operations, p.work.migrations))
	if _, err := fmt.Fprintln(out, "ALPHA guarded action preview (not controller convergence)"); err != nil {
		return errors.New("write action preview failed")
	}
	if err := (report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}).Write(out); err != nil {
		return errors.New("write action preview failed")
	}
	for _, warning := range []string{
		"Scope: service-wide OMENative lifecycle, not a full workload freeze.",
		"true holds Update/Create/Migration; RestartPolicy repair continues.",
		"freeze also holds existing-instance repair; resume clears either depth.",
		"Canary timed gates continue aging and may advance immediately on resume.",
		"Deliberate scale-down and deletion teardown can still proceed.",
		"Parent UID/resourceVersion CAS is not a multi-object transaction.",
	} {
		if _, err := fmt.Fprintln(out, warning); err != nil {
			return errors.New("write action preview failed")
		}
	}
	return nil
}
