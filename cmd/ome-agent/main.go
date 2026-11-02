package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"

	finetunedadapter "sigs.k8s.io/ome/internal/ome-agent/fine-tuned-adapter"
	modelmetadata "sigs.k8s.io/ome/internal/ome-agent/model-metadata"
	"sigs.k8s.io/ome/internal/ome-agent/replica"
	servingagent "sigs.k8s.io/ome/internal/ome-agent/serving-agent"
	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/version"
	"sigs.k8s.io/ome/pkg/xet"
)

var rootCmd = &cobra.Command{
	Use:     "ome-agent",
	Short:   "Run OME Agent",
	Long:    "OME Agent is a swiss army knife for OME inference service, training job, model management, etc.",
	Version: fmt.Sprintf("gitVersion=%s, gitCommit=%s", version.GitVersion, version.GitCommit),
}

// commands are the agent's subcommands, one row per process the binary can be.
var commands = []command{
	{
		name:    "hf-download",
		short:   "Run OME HuggingFace Download Agent",
		long:    "OME Agent HuggingFace Download Agent downloads models from HuggingFace Hub using the xet client (Rust-backed libxet) with chunk-level deduplication, resume capability, and concurrent downloads.",
		modules: []fx.Option{afero.Module, logging.Module, logging.ModuleNamed("another_log"), xet.Module},
		runs:    fxBuilt[hfDownload](),
	},
	{
		name:  "replica",
		short: "Run OME Object Storage Replica Agent",
		long:  "OME Agent Object Storage Replica Agent is dedicated for replicate model weight across regions and/or tenancies.",
		modules: []fx.Option{
			afero.Module, logging.Module, logging.ModuleNamed("another_log"),
			OCIOSDataStoreListProvider(), PVCFileSystemProviders(), xet.Module, replica.Module,
		},
		runs: fxBuilt[*replica.ReplicaAgent](),
	},
	{
		name:  "serving-agent",
		short: "Run OME serving sidecar",
		long:  "OME Serving sidecar is for assisting some of the ome serving containers",
		modules: []fx.Option{
			afero.Module, logging.Module, logging.ModuleNamed("another_log"),
			ociobjectstore.OCIOSDataStoreModule, servingagent.Module,
		},
		runs: fxBuilt[*servingagent.ServingSidecar](),
	},
	{
		name:  "fine-tuned-adapter",
		short: "Run OME fine-tuned adapter",
		long:  "OME fine-tuned adapter is to download the fine-tuned weight and prepared for the serving container to consume",
		modules: []fx.Option{
			afero.Module, logging.Module, logging.ModuleNamed("another_log"),
			ociobjectstore.OCIOSDataStoreModule, finetunedadapter.Module,
		},
		runs: fxBuilt[*finetunedadapter.FineTunedAdapter](),
	},
	{
		name:  "model-metadata",
		short: "Extract model metadata from PVC-mounted models",
		long: "Model metadata agent parses a PVC-mounted model directory and writes a per-model status ConfigMap " +
			"that the BaseModel/ClusterBaseModel controller consumes to update the CR spec and lifecycle state.",
		flags:   modelMetadataFlags,
		modules: []fx.Option{logging.Module, fx.Provide(NewK8sClient), modelmetadata.Module},
		runs:    fxBuilt[*modelmetadata.MetadataExtractor](),
	},
}

// modelMetadataFlags are the flags the BaseModel controller passes when it runs
// the model-metadata agent as a Job. They bind to viper under underscore keys,
// the names the agent's config struct reads.
func modelMetadataFlags(cmd *cobra.Command) {
	cmd.Flags().String("model-path", "", "Path to the model directory")
	cmd.Flags().String("basemodel-name", "", "Name of the BaseModel CR")
	cmd.Flags().String("basemodel-namespace", "", "Namespace of the BaseModel CR")
	cmd.Flags().Bool("cluster-scoped", false, "Whether this is a ClusterBaseModel")

	_ = cmd.MarkFlagRequired("model-path")
	_ = cmd.MarkFlagRequired("basemodel-name")

	_ = viper.BindPFlag("model_path", cmd.Flags().Lookup("model-path"))
	_ = viper.BindPFlag("basemodel_name", cmd.Flags().Lookup("basemodel-name"))
	_ = viper.BindPFlag("basemodel_namespace", cmd.Flags().Lookup("basemodel-namespace"))
	_ = viper.BindPFlag("cluster_scoped", cmd.Flags().Lookup("cluster-scoped"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		exitWithError(err)
	}
}

// exitWithError reports err on stderr and ends the process with exit code 1.
func exitWithError(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
	os.Exit(1)
}

func init() {
	for _, c := range commands {
		rootCmd.AddCommand(c.cobraCommand())
	}
	rootCmd.AddCommand(enigmaCommand.cobraCommand())
}
