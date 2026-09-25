package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type guardedActionParserFactory struct{ calls []string }

func (f *guardedActionParserFactory) called(method string) error {
	f.calls = append(f.calls, method)
	return errors.New("unexpected factory acquisition")
}

func (f *guardedActionParserFactory) Namespace() (string, bool, error) {
	return "", false, f.called("Namespace")
}

func (f *guardedActionParserFactory) RESTConfig() (*rest.Config, error) {
	return nil, f.called("RESTConfig")
}

func (f *guardedActionParserFactory) ContextName() (string, error) {
	return "", f.called("ContextName")
}

func (f *guardedActionParserFactory) KubeClient() (kubernetes.Interface, error) {
	return nil, f.called("KubeClient")
}

func (f *guardedActionParserFactory) OMEClient() (versioned.Interface, error) {
	return nil, f.called("OMEClient")
}

func (f *guardedActionParserFactory) RuntimeClient() (ctrlclient.Client, error) {
	return nil, f.called("RuntimeClient")
}

func (f *guardedActionParserFactory) RuntimeClientForAction(context.Context) (ctrlclient.Client, error) {
	return nil, f.called("RuntimeClientForAction")
}

// A parser diagnostic containing the original flag error would disclose private
// values before RunE. Both exported execution boundaries must fail before clients.
func TestGuardedActionParserPrivacyThroughExportedExecution(t *testing.T) {
	const private = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	for _, action := range []string{"pause", "resume"} {
		cases := []struct {
			name   string
			suffix []string
		}{
			{"malformed confirmation", []string{"--yes=" + private}},
			{"long control value", []string{"--yes=" + private + "\x1b[2J\n" + strings.Repeat("x", 4096)}},
			{"unknown private flag", []string{"--unknown-" + private + "=value"}},
			{"missing output", []string{"--output"}},
			{"missing dry-run", []string{"--dry-run"}},
			{"missing inherited context", []string{"--context"}},
			{"malformed inherited boolean", []string{"--insecure-skip-tls-verify=" + private}},
		}
		if action == "resume" {
			cases = append(cases, struct {
				name   string
				suffix []string
			}{"malformed discard", []string{"--discard-pending-actions=" + private}})
		}
		for _, tc := range cases {
			for _, production := range []bool{false, true} {
				entry := "injected root"
				if production {
					entry = "production Run"
				}
				t.Run(action+" "+tc.name+" "+entry, func(t *testing.T) {
					var stdout, stderr bytes.Buffer
					streams := genericiooptions.IOStreams{In: strings.NewReader(""), Out: &stdout, ErrOut: &stderr}
					args := append([]string{"rollout", action, "example"}, tc.suffix...)
					f := &guardedActionParserFactory{}
					var code int
					if production {
						args = append([]string{"--kubeconfig=/private/tmp/ome-cli-parser-no-such-config-20260915"}, args...)
						code = Run(args, streams)
					} else {
						cmd := NewRootCmdWithFactory(f, streams)
						cmd.SetArgs(args)
						code = ExecuteCommand(cmd, &stderr)
					}
					if code != 1 || stdout.Len() != 0 {
						t.Fatalf("parser refusal: code=%d stdoutBytes=%d", code, stdout.Len())
					}
					if !production && len(f.calls) != 0 {
						t.Fatalf("parser acquired factory methods: %v", f.calls)
					}
					if strings.Contains(stderr.String(), private) || stderr.String() != "error: invalid rollout action flags; use --help\n" {
						t.Fatalf("parser diagnostic disclosed private input or was not closed/bounded")
					}
				})
			}
		}
	}
}

func TestExecuteCommandMapsErrorsAndPrintsOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "success", want: exitcode.Success},
		{name: "ordinary", err: errors.New("ordinary failure"), want: exitcode.GeneralError},
		{name: "assertion", err: &exitcode.UnmetAssertionError{Err: errors.New("not ready")}, want: exitcode.AssertionUnmet},
		{name: "conflict", err: &exitcode.PreconditionError{Err: errors.New("stale target")}, want: exitcode.MutationConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stderr bytes.Buffer
			cmd := &cobra.Command{
				Use: "test",
				RunE: func(*cobra.Command, []string) error {
					return test.err
				},
			}
			cmd.SetErr(&stderr)

			if got := ExecuteCommand(cmd, &stderr); got != test.want {
				t.Fatalf("ExecuteCommand() = %d, want %d", got, test.want)
			}
			if test.err == nil {
				if stderr.Len() != 0 {
					t.Fatalf("stderr = %q, want empty", stderr.String())
				}
				return
			}
			want := "error: " + test.err.Error() + "\n"
			if stderr.String() != want {
				t.Fatalf("stderr = %q, want %q", stderr.String(), want)
			}
		})
	}
}

func TestExecuteCommandContextPropagatesCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stderr bytes.Buffer
	observed := make(chan context.Context, 1)
	done := make(chan int, 1)
	cmd := &cobra.Command{
		Use: "test",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			observed <- ctx
			<-ctx.Done()
			return ctx.Err()
		},
	}
	cmd.SetArgs([]string{})
	go func() { done <- ExecuteCommandContext(ctx, cmd, &stderr) }()

	select {
	case got := <-observed:
		if got != ctx {
			t.Fatal("handler did not receive the caller's context")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case code := <-done:
		if code != exitcode.GeneralError {
			t.Fatalf("ExecuteCommandContext() = %d, want %d", code, exitcode.GeneralError)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not exit after cancellation")
	}
	if got := stderr.String(); got != "error: context canceled\n" {
		t.Fatalf("stderr = %q, want one cancellation diagnostic", got)
	}
}

func TestExecuteCommandContextCancellationBeforeFactoryAcquisition(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	streams := genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr}
	f := &guardedActionParserFactory{}
	cmd := NewRootCmdWithFactory(f, streams)
	cmd.SetArgs([]string{"cluster", "status"})
	if code := ExecuteCommandContext(ctx, cmd, &stderr); code != exitcode.GeneralError {
		t.Fatalf("ExecuteCommandContext() = %d, want %d", code, exitcode.GeneralError)
	}
	if len(f.calls) != 0 {
		t.Fatalf("canceled command acquired factory methods: %v", f.calls)
	}
	if stdout.Len() != 0 || stderr.String() != "error: context canceled\n" {
		t.Fatalf("canceled command stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunContextPropagatesCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	streams := genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr}
	args := []string{"--kubeconfig=" + t.TempDir() + "/missing", "cluster", "status"}
	if code := RunContext(ctx, args, streams); code != exitcode.GeneralError {
		t.Fatalf("RunContext() = %d, want %d", code, exitcode.GeneralError)
	}
	if stdout.Len() != 0 || stderr.String() != "error: context canceled\n" {
		t.Fatalf("canceled command stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunUsesRootAndProvidedStreams(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	streams := genericiooptions.IOStreams{
		In:     strings.NewReader(""),
		Out:    &stdout,
		ErrOut: &stderr,
	}
	if got := Run([]string{"--help"}, streams); got != exitcode.Success {
		t.Fatalf("Run(--help) = %d, want %d; stderr=%q", got, exitcode.Success, stderr.String())
	}
	if !strings.Contains(stdout.String(), "kubectl ome <command>") || stderr.Len() != 0 {
		t.Fatalf("Run(--help) stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunReturnsGeneralErrorForBadInvocation(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	streams := genericiooptions.IOStreams{
		In:     strings.NewReader(""),
		Out:    &bytes.Buffer{},
		ErrOut: &stderr,
	}
	if got := Run([]string{"not-a-command"}, streams); got != exitcode.GeneralError {
		t.Fatalf("Run(bad command) = %d, want %d", got, exitcode.GeneralError)
	}
	if !strings.Contains(stderr.String(), "error: unknown command") {
		t.Fatalf("stderr = %q, want unknown-command diagnostic", stderr.String())
	}
}
