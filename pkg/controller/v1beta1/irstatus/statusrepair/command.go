package statusrepair

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// NewCommand builds the repair subcommand of ome-status-preflight. It is
// dry-run by default and writes only with --apply.
func NewCommand(out, errOut io.Writer) *cobra.Command {
	opts := Options{}
	cmd := &cobra.Command{
		Use:   "repair --inventory FILE --cluster NAME --namespace NS --name IR --replacement FILE [--apply]",
		Short: "Break-glass replacement of one InferenceReplica's stored per-Instance status",
		Long: `Replace the stored per-Instance representation of one InferenceReplica with a
complete, independently validated DenseV1 row set.

The command reads the object directly and without decoding the payload it is
about to replace, validates the replacement with the manager's codec (a
nonempty DenseV1 instanceStatuses list, no marker, no columns, no other status
field, within the configured row bound), selects the representation the
cluster's configured target selects, and reports the size before and after.
Every other status field is preserved from the live object. With --apply it
performs exactly one status write with the live resourceVersion as
precondition; a concurrent change refuses the write and nothing changes.

The replacement file holds only the rows, for example:

  instanceStatuses:
  - index: 0
    incarnation: 1
    phase: Ready
    runningRevision: example-engine-2f32f6fe
    podCount: 1
    servingPodCount: 1
    availablePodCount: 1
    admitted: true

The cluster's omenativeStatus block must match the inventory; the repair never
guesses the target or the bound.`,
		Example: `  # Report what would change; nothing is written.
  ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
    --namespace serving --name example-engine --replacement rows.yaml

  # Perform the single precondition-guarded write.
  ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
    --namespace serving --name example-engine --replacement rows.yaml --apply`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			result, err := Run(cmd.Context(), opts, ConnectWithKubeconfig)
			if err != nil {
				return err
			}
			if err := result.WriteText(out); err != nil {
				return fmt.Errorf("write report: %w", err)
			}
			return nil
		},
	}
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.Flags().StringVar(&opts.InventoryPath, "inventory", "", "Path to the YAML fleet inventory (required)")
	cmd.Flags().StringVar(&opts.Cluster, "cluster", "", "Inventory name of the cluster holding the object (required)")
	cmd.Flags().StringVar(&opts.Namespace, "namespace", "", "Namespace of the InferenceReplica (required)")
	cmd.Flags().StringVar(&opts.Name, "name", "", "Name of the InferenceReplica (required)")
	cmd.Flags().StringVar(&opts.ReplacementPath, "replacement", "", "Path to the YAML or JSON document holding the replacement instanceStatuses (required)")
	cmd.Flags().BoolVar(&opts.Apply, "apply", false, "Perform the write; without it the command only reports what would change")
	for _, flag := range []string{"inventory", "cluster", "namespace", "name", "replacement"} {
		_ = cmd.MarkFlagRequired(flag)
	}
	return cmd
}
