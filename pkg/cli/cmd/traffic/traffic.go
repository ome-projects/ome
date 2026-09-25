// Package traffic implements controller-reported traffic inspection and
// guarded manual drain actions.
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
		Short: "Inspect traffic evidence and manage guarded drains",
	}
	cmd.AddCommand(newStatusCmd(f, streams, statusDependencies{
		clock:   reportv1alpha1.SystemClock{},
		project: trafficprojection.Project,
	}))
	cmd.AddCommand(newExplainCmd(f, streams, explainDependencies{
		clock:   reportv1alpha1.SystemClock{},
		project: trafficprojection.ProjectExplain,
	}))
	cmd.AddCommand(newActionCmd(f, streams, reportv1alpha1.SystemClock{}, "drain"))
	cmd.AddCommand(newActionCmd(f, streams, reportv1alpha1.SystemClock{}, "undrain"))
	return cmd
}
