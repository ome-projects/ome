package mutate

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var errorsPreviewWrite = errors.New("write scale preview failed; no request submitted")

func (p ScalePlan) WritePreview(out io.Writer, contextName, omeNamespace string, dryRun reportv1alpha1.DryRunMode) error {
	if len(p.patch) == 0 || !SafeScalar(contextName) || !SafeScalar(omeNamespace) {
		return ErrUnsafeValue
	}
	if err := writeScaleWarning(out, "ALPHA guarded scale preview (not controller convergence)"); err != nil {
		return err
	}
	d := p.details
	rows := [][]string{{"context", contextName}, {"workload namespace", p.target.Namespace}, {"OME namespace", omeNamespace},
		{"parent", d.Parent.Name}, {"parent UID", d.Parent.UID}, {"parent generation", strconv.FormatInt(d.Parent.Generation, 10)},
		{"IR", p.target.Name}, {"IR UID", p.target.UID}, {"IR RV", p.target.ResourceVersion}, {"IR generation", strconv.FormatInt(d.ReplicaGeneration, 10)},
		{"parent stamp", strconv.FormatInt(d.ParentGenerationStamp, 10)}, {"component", d.Component}, {"/scale spec.replicas", fmt.Sprintf("%d -> %d", d.PriorReplicas, d.RequestedReplicas)},
		{"bounds", fmt.Sprintf("%d..%d", d.MinReplicas, d.MaxReplicas)}, {"verified ownership", d.Class + " / " + d.ManagedBy}, {"verified source", string(d.SpecSource)},
		{"override / transient", "true / true"}, {"dry-run", string(dryRun)}, {"reported Instances", fmt.Sprintf("%d total; %d ready; %d serving; %d available", d.Instances.Replicas, d.Instances.Ready, d.Instances.Serving, d.Instances.Available)}, {"lifecycle / canary", "No observed selected active work"}, {"parent freshness", "Unverifiable (advisory)"}}
	proofs := 1
	for _, source := range d.Sources {
		if source.Kind == "InferenceReplica" {
			proofs++
			rows = append(rows, []string{"pinned sibling proof", source.Namespace + "/" + source.Name})
		}
	}
	rows = append(rows, []string{"IR proofs revalidated", strconv.Itoa(proofs)})
	bounded := make([][]string, 0, len(rows))
	for _, row := range rows {
		for len(row[1]) > 54 {
			bounded = append(bounded, []string{row[0], row[1][:54]})
			row[0] = ""
			row[1] = row[1][54:]
		}
		bounded = append(bounded, row)
	}
	if err := (report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: bounded}).Write(out); err != nil {
		return errorsPreviewWrite
	}
	warnings := []string{"This is a transient /scale request, not a change to parent replica intent.", scaleOwnerWarning(d.Class), "Scale-down may drain/delete logical Instances; pause does not freeze teardown.", "IR UID/resourceVersion CAS is not a multi-object transaction."}
	if proofs > 1 {
		warnings = append(warnings, "Pinned target proofs require conditional exact sibling IR reads and best-effort revalidation.")
	}
	for _, warning := range warnings {
		if err := writeScaleWarning(out, warning); err != nil {
			return err
		}
	}
	return nil
}

func scaleOwnerWarning(class string) string {
	switch class {
	case "HPA":
		return "OME's HPA can overwrite the request immediately."
	case "KEDA":
		return "KEDA or its generated HPA can overwrite the request immediately."
	case "External":
		return "The operator-owned scaler is not observed and can overwrite the request immediately."
	case "None":
		return "The ISVC projector restores ISVC/runtime-derived desired replicas on reconciliation."
	default:
		return "Verified ownership is required."
	}
}

func writeScaleWarning(out io.Writer, text string) error {
	line := ""
	for _, word := range strings.Fields(text) {
		if len(line)+len(word)+1 > 80 {
			if _, err := fmt.Fprintln(out, line); err != nil {
				return errorsPreviewWrite
			}
			line = ""
		}
		if line != "" {
			line += " "
		}
		line += word
	}
	if _, err := fmt.Fprintln(out, line); err != nil {
		return errorsPreviewWrite
	}
	return nil
}
