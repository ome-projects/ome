// Package autoscale implements `kubectl ome autoscale` subcommands.
package autoscale

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/autoscaleprojection"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// NewCmd builds the autoscale command family.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "autoscale",
		Short: "Inspect autoscaling configuration and evidence",
	}
	cmd.AddCommand(newExplainCmd(f, streams))
	cmd.AddCommand(newStatusCmd(f, streams, statusDependencies{
		clock:   reportv1alpha1.SystemClock{},
		project: autoscaleprojection.Project,
	}))
	return cmd
}
