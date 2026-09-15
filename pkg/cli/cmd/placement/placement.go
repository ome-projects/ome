// Package placement implements bounded observational placement diagnostics.
package placement

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
	validation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
	c "sigs.k8s.io/ome/pkg/cli/placementcollection"
	"sigs.k8s.io/ome/pkg/cli/placementprojection"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// NewCmd builds read-only status, explain, and endpoint commands.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{Use: "placement", Short: "Inspect reported multi-cluster placement evidence"}
	for _, view := range []c.View{c.Status, c.Explain, c.Endpoint} {
		cmd.AddCommand(newReadCmd(f, streams, view, v.SystemClock{}))
	}
	return cmd
}

func newReadCmd(f factory.Factory, streams genericiooptions.IOStreams, view c.View, clock v.Clock) *cobra.Command {
	output := "table"
	short := map[c.View]string{c.Status: "Show controller-reported placement status", c.Explain: "Explain placement selectors and registry observations", c.Endpoint: "Show reported placement origins and routing evidence"}[view]
	cmd := &cobra.Command{
		Use: string(view) + " INFERENCESERVICE", Short: short,
		Long: `Inspect bounded current-context observations for one InferenceService.
Placement freshness is unverifiable; reported addresses do not prove success.
Explain computes only standard label-selector compatibility. WLC Ready is
reported control-plane reachability, not capacity, quota, or eligibility.
Endpoint shows origins only, not full URLs. Routing weights are reported
verbatim; publisher acknowledgement and recorded probes are separate facts.
Optional missing or unreadable sources remain diagnostics. No remote clients,
credential/profile resolution, endpoint probes, mutations, or watches are used.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd.Context(), f, streams, view, args[0], output, clock)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table, wide, json or yaml")
	return cmd
}

func run(ctx context.Context, f factory.Factory, streams genericiooptions.IOStreams, view c.View, name, output string, clock v.Clock) error {
	format, wide, err := parseOutput(output)
	if err != nil {
		return err
	}
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return errors.New("invalid InferenceService name")
	}
	namespace, _, err := f.Namespace()
	if err != nil {
		return errors.New("resolve placement namespace failed")
	}
	if len(validation.IsDNS1123Label(namespace)) != 0 {
		return errors.New("invalid placement namespace")
	}
	client, err := f.OMEClient()
	if err != nil || client == nil {
		return errors.New("create placement OME client failed")
	}
	snapshot, err := c.Collect(ctx, client.OmeV1beta1(), namespace, name, view)
	if err != nil {
		return err
	}
	switch view {
	case c.Status:
		r, projectErr := placementprojection.ProjectStatus(snapshot, clock)
		if projectErr != nil {
			return projectErr
		}
		if wide {
			return r.Content.WideTable().Write(streams.Out)
		}
		return report.Write(streams.Out, format, r)
	case c.Explain:
		r, projectErr := placementprojection.ProjectExplain(snapshot, clock)
		if projectErr != nil {
			return projectErr
		}
		if wide {
			return r.Content.WideTable().Write(streams.Out)
		}
		return report.Write(streams.Out, format, r)
	case c.Endpoint:
		r, projectErr := placementprojection.ProjectEndpoint(snapshot, clock)
		if projectErr != nil {
			return projectErr
		}
		if wide {
			return r.Content.WideTable().Write(streams.Out)
		}
		return report.Write(streams.Out, format, r)
	default:
		return errors.New("invalid placement view")
	}
}

func parseOutput(output string) (report.Format, bool, error) {
	if output == "wide" {
		return report.FormatTable, true, nil
	}
	switch output {
	case "", "table", "json", "yaml":
		format, err := report.ParseFormat(output)
		return format, false, err
	default:
		return "", false, errors.New("unsupported placement output (supported: table, wide, json, yaml)")
	}
}
