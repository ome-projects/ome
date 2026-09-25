package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
)

// ExecuteCommand executes cmd, writes one user-facing error, and returns the
// stable process exit code without terminating the process.
func ExecuteCommand(cmd *cobra.Command, stderr io.Writer) int {
	return ExecuteCommandContext(context.Background(), cmd, stderr)
}

// ExecuteCommandContext executes cmd with ctx, writes one user-facing error,
// and returns the stable process exit code without terminating the process.
func ExecuteCommandContext(ctx context.Context, cmd *cobra.Command, stderr io.Writer) int {
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetErr(stderr)

	err := cmd.ExecuteContext(ctx)
	if err == nil {
		return exitcode.Success
	}
	if stderr != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
	}
	return exitcode.FromError(err)
}

// Run constructs and executes the production command tree for args.
func Run(args []string, streams genericiooptions.IOStreams) int {
	return RunContext(context.Background(), args, streams)
}

// RunContext constructs and executes the production command tree with ctx.
func RunContext(ctx context.Context, args []string, streams genericiooptions.IOStreams) int {
	cmd := NewRootCmd(streams)
	cmd.SetArgs(args)
	return ExecuteCommandContext(ctx, cmd, streams.ErrOut)
}
