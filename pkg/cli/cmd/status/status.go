package status

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	output := "table"
	cmd := &cobra.Command{
		Use:   "status INFERENCESERVICE",
		Short: "Show the full readiness story of an InferenceService",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wide, err := parseOutput(output)
			if err != nil {
				return err
			}
			ns, _, err := f.Namespace()
			if err != nil {
				return err
			}
			r, err := gather(cmd.Context(), f, ns, args[0])
			if err != nil {
				return err
			}
			if !wide {
				return renderCompact(r, streams.Out)
			}
			return render(r, streams.Out)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "Output format: table or wide")
	return cmd
}

func parseOutput(value string) (bool, error) {
	switch value {
	case "table":
		return false, nil
	case "wide":
		return true, nil
	default:
		return false, fmt.Errorf(
			"unsupported output format %q (supported: table, wide)", value,
		)
	}
}
