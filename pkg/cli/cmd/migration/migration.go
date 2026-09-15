// Package migration implements OMENative migration inspection and requests.
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
		Short: "Inspect and request OMENative migrations",
	}
	cmd.AddCommand(newHistoryCmd(f, streams), newStatusCmd(f, streams), newStartCmd(f, streams))
	return cmd
}
