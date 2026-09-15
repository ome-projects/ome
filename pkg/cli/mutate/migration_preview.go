package mutate

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// WritePreview emits reviewed logical request values, never operational evidence.
func (p MigrationPlan) WritePreview(out io.Writer, contextName, omeNamespace string, dryRun reportv1alpha1.DryRunMode) error {
	if !SafeScalar(contextName) || !SafeScalar(omeNamespace) || p.target.UID == "" {
		return errors.New("action preview identity is unavailable or unsafe")
	}
	rows := [][]string{}
	add := func(field, value string) {
		if value == "" {
			value = "<absent>"
		}
		chunk := ""
		for _, r := range value {
			if chunk != "" && printers.CellDisplayWidth(chunk+string(r)) > 56 {
				rows = append(rows, []string{field, chunk})
				chunk = ""
				field = "(continued)"
			}
			chunk += string(r)
		}
		rows = append(rows, []string{field, chunk})
	}
	for _, row := range [][2]string{{"Action", "migration start"}, {"Context", contextName}, {"Workload NS", p.target.Namespace}, {"OME NS", omeNamespace}, {"Target", p.target.Kind + "/" + p.target.Name}, {"UID", p.target.UID}, {"ResourceVersion", p.target.ResourceVersion}, {"Dry-run", string(dryRun)}, {"Request UUID", p.id}, {"Schema", p.request.SchemaVersion}, {"Component", p.request.Component}, {"Instance", strconv.FormatInt(int64(p.request.Instance), 10)}, {"From node", p.request.FromNode}, {"Hint nodes", strings.Join(p.request.HintNodes, ", ")}, {"Reason", p.request.Reason}, {"Requested at", p.request.RequestedAt}, {"Requested by", p.request.RequestedBy}, {"Lookup only", strconv.FormatBool(p.existing)}} {
		value := row[1]
		if row[0] == "Reason" && value != "" {
			value = strconv.Quote(value)
		}
		add(row[0], value)
	}
	if p.evidence.ir != nil {
		add("Source IR", p.evidence.ir.Name)
		add("IR UID", string(p.evidence.ir.UID))
		add("IR version", p.evidence.ir.ResourceVersion)
	}
	if p.evidence.source != nil {
		add("Migration mode", p.evidence.mode)
		add("Running revision", p.evidence.source.RunningRevision)
		add("Incarnation", strconv.FormatInt(p.evidence.source.Incarnation, 10))
		add("Current nodes", strings.Join(p.evidence.nodes, ", "))
	}
	if _, err := fmt.Fprintln(out, "ALPHA guarded migration preview (not controller convergence)"); err != nil {
		return errors.New("write action preview failed")
	}
	if err := (report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}).Write(out); err != nil {
		return errors.New("write action preview failed")
	}
	for _, warning := range p.evidence.warnings {
		line := ""
		for _, word := range strings.Fields(warning) {
			candidate := word
			if line != "" {
				candidate = line + " " + word
			}
			if printers.CellDisplayWidth(candidate) > 80 && line != "" {
				if _, err := fmt.Fprintln(out, line); err != nil {
					return errors.New("write action preview failed")
				}
				line = word
			} else {
				line = candidate
			}
		}
		if _, err := fmt.Fprintln(out, line); err != nil {
			return errors.New("write action preview failed")
		}
	}
	return nil
}
