package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelpDoesNotCollideWithControllerRuntimeFlags(t *testing.T) {
	if flag.Lookup("kubeconfig") == nil {
		t.Fatal("expected controller-runtime to register its global kubeconfig flag")
	}
	originalArgs := os.Args
	t.Cleanup(func() { os.Args = originalArgs })
	os.Args = []string{"worker-result", "--help"}
	main()
	main()
}

func TestRunParsesArgumentsWithoutAPIConnection(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing-kubeconfig")
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing required flags", want: "explicit kubeconfig"},
		{name: "unknown flag", args: []string{"--unknown"}, want: "flag provided but not defined"},
		{
			name: "valid flags reach private config loading",
			args: []string{"--kubeconfig", missingConfig, "--context", "kind-alfred-e2e",
				"--namespace", "alfred-e2e", "--name", "single", "--node", "source"},
			want: "load private kubeconfig",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := run(tc.args); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) = %v, want error containing %q", tc.args, err, tc.want)
			}
		})
	}
}
