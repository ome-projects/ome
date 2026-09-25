package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli"
)

func main() {
	streams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	os.Exit(runWithSignalContext(os.Args[1:], streams, signal.NotifyContext, runContext))
}

func runWithSignalContext(
	args []string,
	streams genericiooptions.IOStreams,
	notify func(context.Context, ...os.Signal) (context.Context, context.CancelFunc),
	runner func(context.Context, []string, genericiooptions.IOStreams) int,
) int {
	ctx, stop := notify(context.Background(), os.Interrupt, syscall.SIGTERM)
	stop = sync.OnceFunc(stop)
	finished, watcherDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			// Restore default signal handling even if the runner is still blocked.
			stop()
		case <-finished:
		}
	}()
	defer func() {
		close(finished)
		stop()
		<-watcherDone
	}()
	return runner(ctx, args, streams)
}

func run(args []string, streams genericiooptions.IOStreams) int {
	return runContext(context.Background(), args, streams)
}

func runContext(ctx context.Context, args []string, streams genericiooptions.IOStreams) int {
	return cli.RunContext(ctx, args, streams)
}
