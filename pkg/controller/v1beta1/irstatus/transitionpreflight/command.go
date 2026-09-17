package transitionpreflight

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"sigs.k8s.io/ome/pkg/version"
)

// ErrNoGo is returned by the command when at least one cluster failed, after
// the report has been written, so callers can map it to a distinct exit code.
var ErrNoGo = errors.New("transition preflight: no-go")

// NewCommand builds the ome-status-preflight command. It loads the inventory,
// runs the read-only checks against every cluster, writes the report to out,
// and returns ErrNoGo when any cluster failed.
func NewCommand(out, errOut io.Writer) *cobra.Command {
	var (
		inventoryPath string
		asJSON        bool
	)
	cmd := &cobra.Command{
		Use:   "ome-status-preflight --inventory FILE",
		Short: "Read-only go/no-go check for an InferenceReplica status representation transition",
		Long: `Check every cluster of an operator-supplied inventory before and after an
InferenceReplica status representation transition.

For each cluster the command reads, directly and without a cache, the manager
Deployment image, the omenativeStatus block of the manager configuration, the
served InferenceReplica schema through OpenAPI discovery, and every
InferenceReplica in pages, classifying each stored representation with the same
codec the manager uses. It reports go or no-go per cluster with the reason for
every failure: an unreachable cluster, a failed list page, an unexpected manager
image, a configuration mismatch, a stale schema, ColumnarV2 objects present under
a DenseV1 target, or ColumnarV2 objects above the expected row bound.

The command never writes to a cluster.`,
		Example: `  # Human-readable report; exit status 1 on no-go, 2 on a usage or inventory error.
  ome-status-preflight --inventory fleet.yaml

  # Machine-readable report for an audit trail.
  ome-status-preflight --inventory fleet.yaml --json > preflight.json`,
		Version:       fmt.Sprintf("%s (%s)", version.GitVersion, version.GitCommit),
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			inventory, digest, err := LoadInventory(inventoryPath)
			if err != nil {
				return err
			}
			report := Run(cmd.Context(), inventory, digest, ConnectWithKubeconfig)
			if asJSON {
				err = report.WriteJSON(out)
			} else {
				err = report.WriteText(out)
			}
			if err != nil {
				return fmt.Errorf("write report: %w", err)
			}
			if !report.Go {
				return ErrNoGo
			}
			return nil
		},
	}
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.Flags().StringVar(&inventoryPath, "inventory", "", "Path to the YAML inventory of clusters, expected manager image, and expected omenativeStatus values (required)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Write the report as JSON instead of text")
	_ = cmd.MarkFlagRequired("inventory")
	return cmd
}
