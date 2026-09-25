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
// stable process exit code without terminating the process. Contexts configured
// on cmd or its descendants are preserved using Cobra's normal inheritance.
func ExecuteCommand(cmd *cobra.Command, stderr io.Writer) int {
	return executeCommand(cmd, stderr, cmd.Execute)
}

// ExecuteCommandContext executes cmd with ctx, writes one user-facing error,
// and returns the stable process exit code without terminating the process.
func ExecuteCommandContext(ctx context.Context, cmd *cobra.Command, stderr io.Writer) int {
	// Cobra retains inherited contexts on children between executions.
	// Set the new execution context on the full tree so no child can retain a
	// canceled context from an earlier execution.
	setCommandContexts(cmd.Root(), ctx)
	return executeCommand(cmd, stderr, func() error { return cmd.ExecuteContext(ctx) })
}

func executeCommand(cmd *cobra.Command, stderr io.Writer, execute func() error) int {
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetErr(stderr)

	err := execute()
	if err == nil {
		return exitcode.Success
	}
	if stderr != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
	}
	return exitcode.FromError(err)
}

func setCommandContexts(cmd *cobra.Command, ctx context.Context) {
	cmd.SetContext(ctx)
	for _, child := range cmd.Commands() {
		setCommandContexts(child, ctx)
	}
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
