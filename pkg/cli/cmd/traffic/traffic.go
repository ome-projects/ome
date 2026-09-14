// Package traffic implements read-only controller-reported traffic inspection.
package traffic

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/trafficprojection"
)

// NewCmd builds the traffic command family.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "traffic",
		Short: "Inspect controller-reported traffic evidence",
	}
	cmd.AddCommand(newStatusCmd(f, streams, statusDependencies{
		clock:   reportv1alpha1.SystemClock{},
		project: trafficprojection.Project,
	}))
	return cmd
}
