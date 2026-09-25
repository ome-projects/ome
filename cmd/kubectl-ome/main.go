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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	streams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	os.Exit(runContext(ctx, os.Args[1:], streams))
}

func run(args []string, streams genericiooptions.IOStreams) int {
	return runContext(context.Background(), args, streams)
}

func runContext(ctx context.Context, args []string, streams genericiooptions.IOStreams) int {
	return cli.RunContext(ctx, args, streams)
}
