// Package runtime implements `kubectl ome runtime` subcommands.
package runtime

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runtime",
		Short: "Inspect OME runtime selection and configuration",
	}
	cmd.AddCommand(newExplainCmd(f, streams))
	cmd.AddCommand(newEffectiveCmd(f, streams))
	cmd.AddCommand(newHistoryCmd(f, streams))
	cmd.AddCommand(newTreeCmd(f, streams))
	return cmd
}
