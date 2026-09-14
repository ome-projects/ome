package cli

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/cmd/autoscale"
	"sigs.k8s.io/ome/pkg/cli/cmd/get"
	"sigs.k8s.io/ome/pkg/cli/cmd/instance"
	"sigs.k8s.io/ome/pkg/cli/cmd/logs"
	"sigs.k8s.io/ome/pkg/cli/cmd/migration"
	"sigs.k8s.io/ome/pkg/cli/cmd/rollout"
	runtimecmd "sigs.k8s.io/ome/pkg/cli/cmd/runtime"
	"sigs.k8s.io/ome/pkg/cli/cmd/status"
	"sigs.k8s.io/ome/pkg/cli/cmd/traffic"
	"sigs.k8s.io/ome/pkg/cli/cmd/version"
	"sigs.k8s.io/ome/pkg/cli/factory"
)

// NewRootCmd builds the kubectl-ome root command. Subcommands are registered
// here and nowhere else; adding a future command family means adding one
// AddCommand line.
func NewRootCmd(streams genericiooptions.IOStreams) *cobra.Command {
	configFlags := genericclioptions.NewConfigFlags(true)
	return newRootCmd(factory.New(configFlags), configFlags, streams)
}

// NewRootCmdWithFactory builds the root command with an injected client
// factory while preserving the production kubectl configuration flags.
func NewRootCmdWithFactory(f factory.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	configFlags := genericclioptions.NewConfigFlags(true)
	return newRootCmd(f, configFlags, streams)
}

func newRootCmd(f factory.Factory, configFlags *genericclioptions.ConfigFlags, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ome",
		Short: "Inspect OME models, runtimes and inference services",
		Long: `kubectl-ome is the official OME CLI, invoked as a kubectl plugin:

  kubectl ome <command>

It provides model-centric visibility into OME resources: rich listings,
controller-reported autoscaling, logical-instance, migration, and traffic
evidence, InferenceService readiness and rollout diagnosis, runtime-selection
explanations and component-aware log streaming.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.SetIn(streams.In)
	cmd.SetOut(streams.Out)
	cmd.SetErr(streams.ErrOut)
	configFlags.AddFlags(cmd.PersistentFlags())

	// Command families. Keep alphabetical.
	cmd.AddCommand(autoscale.NewCmd(f, streams))
	cmd.AddCommand(get.NewCmd(f, streams))
	cmd.AddCommand(instance.NewCmd(f, streams))
	cmd.AddCommand(logs.NewCmd(f, streams))
	cmd.AddCommand(migration.NewCmd(f, streams))
	cmd.AddCommand(rollout.NewCmd(f, streams))
	cmd.AddCommand(runtimecmd.NewCmd(f, streams))
	cmd.AddCommand(status.NewCmd(f, streams))
	cmd.AddCommand(traffic.NewCmd(f, streams))
	cmd.AddCommand(version.NewCmd(f, streams))

	return cmd
}
