// Package cluster exposes alpha, observational current-context diagnostics.
package cluster

import (
	"errors"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/clusterstatus"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/report"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// NewCmd registers the observational cluster command family.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	return newCmd(f, streams, r.SystemClock{})
}

func newCmd(f factory.Factory, streams genericiooptions.IOStreams, clock r.Clock) *cobra.Command {
	cmd := &cobra.Command{Use: "cluster", Short: "Inspect declared and reported workload clusters"}
	output := ""
	status := &cobra.Command{
		Use: "status [WORKLOADCLUSTER]", Short: "Show alpha current-context WorkloadCluster status",
		Long: `Show observational alpha WorkloadCluster evidence from the current context.
Read one cluster-scoped object or a bounded list (32/page, 64 objects, 2 pages).
Ready and generation freshness are controller-reported, not a reachability probe.
Declared profiles are not resolved and never imply a working connection.
No Secret, kubeconfig content, profile, remote cluster, or capacity API is read.
Condition groups above 128 entries are unavailable; full admitted groups are
validated before retaining at most 32 canonical condition details.
Compact output fits 80 columns; wide output wraps full safe evidence.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := ""
			if len(args) > 0 {
				name = args[0]
				if len(validation.IsDNS1123Subdomain(name)) != 0 {
					return errors.New("invalid WorkloadCluster name")
				}
			}
			wide := output == "wide"
			formatValue := output
			if wide {
				formatValue = "table"
			}
			if formatValue != "" && formatValue != "table" && formatValue != "json" && formatValue != "yaml" {
				return errors.New("output must be table, wide, json or yaml")
			}
			format, err := report.ParseFormat(formatValue)
			if err != nil {
				return err
			}
			if err := cmd.Context().Err(); err != nil {
				return err
			}
			client, err := f.OMEClient()
			if err != nil || client == nil {
				return errors.New("cannot create current-context OME client")
			}
			snapshot, err := clusterstatus.Collect(cmd.Context(), client.OmeV1beta1(), name, paging.Limits{PageSize: 32, MaxItems: 64, MaxPages: 2, RequestTimeout: 10 * time.Second})
			if err != nil {
				return err
			}
			value := clusterstatus.Project(snapshot, clock)
			if wide {
				return value.WideTable().Write(streams.Out)
			}
			return report.Write(streams.Out, format, value)
		},
	}
	status.Flags().StringVarP(&output, "output", "o", "table", "Output format: table, wide, json or yaml")
	cmd.AddCommand(status)
	return cmd
}
