// Command ome-scheduler is the OME accelerator scheduler: the upstream
// kube-scheduler, built as a library, with the OME placement plugin registered
// alongside the built-in plugins.
package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"sigs.k8s.io/ome/scheduler/pkg/plugins/gangpack"
)

func main() {
	// NewSchedulerCommand returns the real kube-scheduler cobra command with our
	// plugin added to the registry; the scheduler config (ConfigMap) decides at
	// which hooks it runs. Nothing else about upstream is forked.
	cmd := app.NewSchedulerCommand(
		app.WithPlugin(gangpack.Name, gangpack.New),
	)
	os.Exit(cli.Run(cmd))
}
