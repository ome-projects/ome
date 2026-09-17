package mutate

import (
	"errors"
	"fmt"
	"io"

	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func (p RuntimeSyncPlan) WritePreview(out io.Writer, contextName, namespace string, mode reportv1alpha1.DryRunMode) error {
	if !p.valid || !SafeScalar(contextName) || !SafeScalar(namespace) {
		return ErrUnsafeTarget
	}
	rows := [][]string{}
	add := func(key, value string) {
		for len(value) > 56 {
			rows = append(rows, []string{key, value[:56]})
			value = value[56:]
			key = "(continued)"
		}
		rows = append(rows, []string{key, value})
	}
	for _, row := range [][2]string{{"Action", "runtime sync"}, {"Context", contextName}, {"Workload NS", p.target.Namespace}, {"OME NS", namespace}, {"Target", p.target.Kind + "/" + p.target.Name}, {"UID", p.target.UID}, {"ResourceVersion", p.target.ResourceVersion}, {"Dry-run", string(mode)}} {
		add(row[0], row[1])
	}
	for _, row := range p.rows {
		add(row[0], row[1])
	}
	add("Request UUID", p.requestID)
	add("Set annotation", "ome.io/runtime-sync")
	add("New token", p.token)
	if _, err := fmt.Fprintln(out, "ALPHA runtime sync preview (acceptance is not convergence)"); err != nil {
		return errors.New("write runtime sync preview failed")
	}
	if err := (report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}).Write(out); err != nil {
		return errors.New("write runtime sync preview failed")
	}
	for _, warning := range []string{"API acceptance is not convergence or serving-revision readiness.", "Requests latest merged live runtime when controller consumes the token.", "Previewed content is not locked by this annotation.", "Parent CAS is not an atomic transaction across runtimes, revisions and IRs.", "Global observed generation is advisory; freshness is Unverifiable.", "Existing pause/freeze and lifecycle/rollout policies remain in effect."} {
		if _, err := fmt.Fprintln(out, warning); err != nil {
			return errors.New("write runtime sync preview failed")
		}
	}
	return nil
}
