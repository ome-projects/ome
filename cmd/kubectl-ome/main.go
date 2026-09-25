package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli"
)

func main() {
	streams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	os.Exit(runWithSignalContext(os.Args[1:], streams, signal.NotifyContext))
}

func runWithSignalContext(args []string, streams genericiooptions.IOStreams, notify func(context.Context, ...os.Signal) (context.Context, context.CancelFunc)) int {
	ctx, stop := notify(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runContext(ctx, args, streams)
}

func run(args []string, streams genericiooptions.IOStreams) int {
	return runContext(context.Background(), args, streams)
}

func runContext(ctx context.Context, args []string, streams genericiooptions.IOStreams) int {
	return cli.RunContext(ctx, args, streams)
}
