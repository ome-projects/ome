// Command ome-status-preflight is the read-only operator check that runs
// before and after an InferenceReplica status representation transition. It
// reads every cluster named in an inventory file and reports go or no-go; it
// never writes to a cluster.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	// Registers the oidc, gcp, and azure auth providers. The command is run
	// against whatever kubeconfigs the operator already has, and without this
	// a context using one of them fails to authenticate at all.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/transitionpreflight"
)

func main() {
	// A paginated read of a large fleet can block on an unreachable
	// apiserver, so an interrupt has to reach the in-flight request rather
	// than only the shell that started it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := transitionpreflight.NewCommand(os.Stdout, os.Stderr)
	if err := cmd.ExecuteContext(ctx); err != nil {
		if errors.Is(err, transitionpreflight.ErrNoGo) {
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
}
