// Package instance implements logical instance inspection and guarded alpha actions.
package instance

import (
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/instanceprojection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

const defaultMaxInstances = 1000

const defaultMaxRetryBlocks = 1000

// NewCmd builds the instance command family.
func NewCmd(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Inspect controller-reported logical instances",
	}
	cmd.AddCommand(newListCmd(f, streams, listDependencies{
		clock: reportv1alpha1.SystemClock{},
		limits: paging.Limits{
			PageSize: 50, MaxItems: 100, MaxPages: 10, RequestTimeout: 10 * time.Second,
		},
		maxInstances: defaultMaxInstances,
		project:      instanceprojection.Project,
	}))
	cmd.AddCommand(newRetryBlocksCmd(f, streams, retryBlocksDependencies{
		clock: reportv1alpha1.SystemClock{},
		limits: paging.Limits{
			PageSize: 50, MaxItems: 100, MaxPages: 10, RequestTimeout: 10 * time.Second,
		},
		maxRetryBlocks: defaultMaxRetryBlocks,
	}))
	cmd.AddCommand(newStatusCmd(f, streams))
	cmd.AddCommand(newReleaseHeldCmd(f, streams, reportv1alpha1.SystemClock{}))
	return cmd
}
