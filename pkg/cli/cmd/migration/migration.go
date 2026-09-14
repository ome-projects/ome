// Package migration implements read-only OMENative migration inspection.
package migration

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

// NewCmd builds the migration command family.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migration",
		Short: "Inspect OMENative migrations",
	}
	cmd.AddCommand(newStatusCmd(f, streams))
	return cmd
}
