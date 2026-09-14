// Package accelerator implements `kubectl ome accelerator` diagnostics.
package accelerator

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

// NewCmd builds the accelerator command family.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "accelerator",
		Short: "Inspect accelerator selection evidence",
	}
	cmd.AddCommand(newExplainCmd(f, streams))
	return cmd
}
