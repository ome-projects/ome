// Package admin implements read-only control-plane diagnostics.
package admin

import (
	"errors"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/alfredrecommendations"
	"sigs.k8s.io/ome/pkg/cli/doctorcollection"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// NewCmd builds the admin family with control-plane flags scoped to it.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{Use: "admin", Short: "Inspect OME control-plane evidence"}
	ns := namespace.NewOptions()
	ns.AddFlags(cmd.PersistentFlags())
	cmd.AddCommand(newRecommendationsCmd(f, streams, ns))
	cmd.AddCommand(newDoctorCmd(f, streams, ns, doctorDependencies{clock: reportv1.SystemClock{}, collect: doctorcollection.Collect}))
	return cmd
}

func newRecommendationsCmd(f factory.Factory, streams genericiooptions.IOStreams, ns *namespace.Options) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use: "recommendations", Short: "Inspect Alfred's advisory recommendation record",
		Long: `Read Alfred's selected configuration ConfigMap and, when enabled, its
configured recommendations ConfigMap with at most two read-only named GETs.
Both sources use --alfred-namespace (default: --ome-namespace, then ome),
independently of the workload namespace. No Secrets or namespace scans.

Output is advisory evidence, never permission to execute. A reported dispatch
or admission does not prove convergence. Executability is not persisted and
is unverifiable. ConfigMap settings may differ from Alfred's last-known-good
running configuration; record mode is shown independently.

Only last-cycle.json is inspected: no complete history or node remediation
inventory is claimed. Configuration is capped at 64 KiB; the cycle at 256 KiB.
At most 800 rows are validated before the 200-row output cap; larger cycles
are marked unavailable/truncated. Unknown and conflicting rows are omitted.
Freshness is evaluated against twice the declared decision-loop interval
(default 10 minutes); it does not verify policy-loop health. Each GET has a
10-second timeout. Arbitrary configuration, error text and scheduling details
are omitted. Compact tables fit 80 columns; wide expands safe details.`,
		Example: "  kubectl ome admin recommendations\n  kubectl ome admin recommendations --alfred-namespace caretaker -o wide",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			format := report.FormatTable
			var err error
			wide := output == "wide"
			if !wide {
				format, err = report.ParseFormat(output)
				if err != nil {
					return errors.New("output format must be table, wide, json, or yaml")
				}
			}
			// This command has no workload scope. Resolve against the helper's
			// valid placeholder to avoid loading kubeconfig before local checks.
			resolved, err := ns.Resolve("default")
			if err != nil {
				return errors.New("invalid Alfred namespace or config selection")
			}
			if cmd.Context().Err() != nil {
				return cmd.Context().Err()
			}
			client, err := f.KubeClient()
			if err != nil {
				return errors.New("Kubernetes client unavailable")
			}
			snapshot, err := alfredrecommendations.Collect(cmd.Context(), client.CoreV1(), resolved, alfredrecommendations.RequestTimeout)
			if err != nil {
				return err
			}
			value := alfredrecommendations.Project(snapshot, reportv1.SystemClock{})
			if wide {
				return value.Content.WideTable().Write(streams.Out)
			}
			return report.Write(streams.Out, format, value)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table, wide, json, or yaml")
	return cmd
}
