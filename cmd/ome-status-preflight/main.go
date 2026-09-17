// Command ome-status-preflight is the operator tooling around an
// InferenceReplica status representation transition. Its root command is the
// read-only go/no-go check that runs before and after a transition: it reads
// every cluster named in an inventory file and never writes. Its repair
// subcommand is the explicit break-glass writer for one object's stored
// per-Instance representation, dry-run unless --apply is given.
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

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/statusrepair"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/transitionpreflight"
)

func main() {
	// A paginated read of a large fleet can block on an unreachable
	// apiserver, so an interrupt has to reach the in-flight request rather
	// than only the shell that started it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := transitionpreflight.NewCommand(os.Stdout, os.Stderr)
	cmd.AddCommand(statusrepair.NewCommand(os.Stdout, os.Stderr))
	if err := cmd.ExecuteContext(ctx); err != nil {
		switch {
		case errors.Is(err, transitionpreflight.ErrNoGo):
			os.Exit(1)
		case statusrepair.IsRefusal(err):
			fmt.Fprintf(os.Stderr, "refused: %v\n", err)
			os.Exit(1)
		default:
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	}
}
