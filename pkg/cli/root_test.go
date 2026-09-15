package cli

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

type waitFlagFactory struct {
	factory.Static
	calls int
}

func (f *waitFlagFactory) Namespace() (string, bool, error) {
	f.calls++
	return "work", false, nil
}

func (f *waitFlagFactory) RESTConfig() (*rest.Config, error) {
	f.calls++
	return nil, nil
}

func TestRootWaitRejectedFlagsHavePrivateDiagnosticsWithoutAcquisition(t *testing.T) {
	const credential = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	for _, tc := range []struct {
		name string
		flag string
	}{
		{name: "malformed timeout", flag: "--timeout=" + credential},
		{name: "missing timeout", flag: "--timeout"},
		{name: "unknown flag", flag: "--" + credential},
		{name: "malformed inherited flag", flag: "--insecure-skip-tls-verify=" + credential},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			f := &waitFlagFactory{}
			root := NewRootCmdWithFactory(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			root.SetArgs([]string{"wait", "service", "--for=condition=Ready", tc.flag})
			code := ExecuteCommand(root, &stderr)
			if code != 1 || out.Len() != 0 || f.calls != 0 {
				t.Fatalf("invalid flags acquired config or emitted report: code=%d stdout=%q acquisitions=%d", code, out.String(), f.calls)
			}
			if strings.Contains(stderr.String(), credential) {
				t.Fatalf("rejected flag leaked into stderr: %s", stderr.String())
			}
			if stderr.String() != "error: InvalidWaitFlags\n" {
				t.Fatalf("diagnostic is not closed: %q", stderr.String())
			}
		})
	}
}

func TestRootCommandTree(t *testing.T) {
	t.Parallel()

	root := NewRootCmdWithFactory(factory.Static{NS: "default"}, genericiooptions.IOStreams{
		In:     &bytes.Buffer{},
		Out:    &bytes.Buffer{},
		ErrOut: &bytes.Buffer{},
	})
	got := commandPaths(root)
	want := []string{
		"ome accelerator",
		"ome accelerator explain",
		"ome admin",
		"ome admin recommendations",
		"ome autoscale",
		"ome autoscale explain",
		"ome autoscale status",
		"ome cluster",
		"ome cluster status",
		"ome get",
		"ome instance",
		"ome instance list",
		"ome instance retry-blocks",
		"ome instance status",
		"ome logs",
		"ome migration",
		"ome migration history",
		"ome migration status",
		"ome quota",
		"ome quota status",
		"ome quota tree",
		"ome quota validate",
		"ome rollout",
		"ome rollout explain",
		"ome rollout history",
		"ome rollout status",
		"ome rollout validate",
		"ome runtime",
		"ome runtime effective",
		"ome runtime explain",
		"ome runtime history",
		"ome runtime tree",
		"ome status",
		"ome traffic",
		"ome traffic explain",
		"ome traffic status",
		"ome version",
		"ome wait",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command tree = %v, want %v", got, want)
	}
	if !root.SilenceErrors || !root.SilenceUsage {
		t.Fatalf("root error policy = (SilenceErrors=%t, SilenceUsage=%t), want both true", root.SilenceErrors, root.SilenceUsage)
	}
	explain, _, err := root.Find([]string{"runtime", "explain"})
	if err != nil {
		t.Fatalf("find runtime explain: %v", err)
	}
	if explain.Use != "explain (--model NAME | --isvc NAME)" || explain.Short != "Explain which serving runtimes match a model and why" {
		t.Fatalf("runtime explain contract changed: Use=%q Short=%q", explain.Use, explain.Short)
	}
	tree, _, err := root.Find([]string{"runtime", "tree"})
	if err != nil {
		t.Fatalf("find runtime tree: %v", err)
	}
	if tree.Use != "tree RUNTIME" || tree.Short != "Show runtime inheritance and InferenceService users" {
		t.Fatalf("runtime tree contract changed: Use=%q Short=%q", tree.Use, tree.Short)
	}
	validate, _, err := root.Find([]string{"rollout", "validate"})
	if err != nil {
		t.Fatalf("find rollout validate: %v", err)
	}
	if validate.Use != "validate INFERENCESERVICE" || validate.Short != "Validate rollout, traffic, and autoscaling configuration" {
		t.Fatalf("rollout validate contract changed: Use=%q Short=%q", validate.Use, validate.Short)
	}
	history, _, err := root.Find([]string{"rollout", "history"})
	if err != nil {
		t.Fatalf("find rollout history: %v", err)
	}
	if history.Use != "history INFERENCESERVICE" || history.Short != "Show bounded retained rollout evidence" {
		t.Fatalf("rollout history contract changed: Use=%q Short=%q", history.Use, history.Short)
	}
	trafficExplain, _, err := root.Find([]string{"traffic", "explain"})
	if err != nil {
		t.Fatalf("find traffic explain: %v", err)
	}
	if trafficExplain.Use != "explain INFERENCESERVICE" ||
		trafficExplain.Short != "Explain declared and reported traffic behavior" {
		t.Fatalf("traffic explain contract changed: Use=%q Short=%q",
			trafficExplain.Use, trafficExplain.Short)
	}
	retryBlocks, _, err := root.Find([]string{"instance", "retry-blocks"})
	if err != nil {
		t.Fatalf("find instance retry-blocks: %v", err)
	}
	if retryBlocks.Use != "retry-blocks INFERENCESERVICE --component COMPONENT" ||
		retryBlocks.Short != "Show controller-reported retry authority" {
		t.Fatalf("instance retry-blocks contract changed: Use=%q Short=%q", retryBlocks.Use, retryBlocks.Short)
	}
	acceleratorExplain, _, err := root.Find([]string{"accelerator", "explain"})
	if err != nil {
		t.Fatalf("find accelerator explain: %v", err)
	}
	if acceleratorExplain.Use != "explain INFERENCESERVICE" ||
		acceleratorExplain.Short != "Explain accelerator intent and reported selection" {
		t.Fatalf("accelerator explain contract changed: Use=%q Short=%q",
			acceleratorExplain.Use, acceleratorExplain.Short)
	}
	if !strings.Contains(root.Long, "accelerator-selection evidence") {
		t.Fatalf("root overview does not describe accelerator diagnostics: %q", root.Long)
	}
	migrationHistory, _, err := root.Find([]string{"migration", "history"})
	if err != nil {
		t.Fatalf("find migration history: %v", err)
	}
	if migrationHistory.Use != "history INFERENCESERVICE" || migrationHistory.Short != "Show bounded migration evidence history" {
		t.Fatalf("migration history contract changed: Use=%q Short=%q", migrationHistory.Use, migrationHistory.Short)
	}
	status, _, err := root.Find([]string{"instance", "status"})
	if err != nil {
		t.Fatalf("find instance status: %v", err)
	}
	if status.Use != "status INFERENCESERVICE INDEX --component COMPONENT" ||
		status.Short != "Show one logical instance and bounded live evidence" {
		t.Fatalf("instance status contract changed: Use=%q Short=%q", status.Use, status.Short)
	}
}

func TestRootHelpListsLogicalInstanceInspection(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	root := NewRootCmdWithFactory(factory.Static{NS: "default"}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	root.SetArgs([]string{"--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !bytes.Contains(output.Bytes(), []byte("  instance    Inspect controller-reported logical instances\n")) {
		t.Fatalf("root help does not list instance command:\n%s", output.String())
	}
}

func TestRootHelpListsMigrationStatus(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	root := NewRootCmdWithFactory(factory.Static{NS: "default"}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	root.SetArgs([]string{"--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(output.String(), "migration") || !strings.Contains(output.String(), "Inspect OMENative migrations") {
		t.Fatalf("root help does not list migration status:\n%s", output.String())
	}
}

func TestRootHelpListsTrafficEvidenceCommand(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	root := NewRootCmdWithFactory(factory.Static{NS: "default"}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	root.SetArgs([]string{"--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !bytes.Contains(output.Bytes(), []byte("  traffic     Inspect controller-reported traffic evidence\n")) {
		t.Fatalf("root help does not list traffic command:\n%s", output.String())
	}
}

func TestRootHelpListsAutoscaleEvidenceCommand(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	root := NewRootCmdWithFactory(factory.Static{NS: "default"}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	root.SetArgs([]string{"--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !bytes.Contains(output.Bytes(), []byte("  autoscale   Inspect autoscaling configuration and evidence\n")) {
		t.Fatalf("root help does not list autoscale command:\n%s", output.String())
	}
}

func TestInjectedRootCarriesAllKubectlConfigFlags(t *testing.T) {
	t.Parallel()

	root := NewRootCmdWithFactory(factory.Static{NS: "default"}, genericiooptions.IOStreams{})
	wantCmd := &cobra.Command{Use: "expected"}
	genericclioptions.NewConfigFlags(true).AddFlags(wantCmd.PersistentFlags())

	got := flagNames(root.PersistentFlags())
	want := flagNames(wantCmd.PersistentFlags())
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persistent kube flags = %v, want %v", got, want)
	}
	if flag := root.PersistentFlags().Lookup("namespace"); flag == nil || flag.Shorthand != "n" {
		t.Fatalf("namespace flag = %#v, want -n shorthand", flag)
	}
}

func TestRootHelpListsRolloutInspection(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	root := NewRootCmdWithFactory(factory.Static{NS: "default"}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	root.SetArgs([]string{"--help"})

	if err := root.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	help := output.String()
	for _, text := range []string{"rollout", "Inspect InferenceService rollouts", "-n, --namespace"} {
		if !strings.Contains(help, text) {
			t.Fatalf("root help missing %q:\n%s", text, help)
		}
	}
}

func commandPaths(root *cobra.Command) []string {
	var paths []string
	var walk func(*cobra.Command)
	walk = func(parent *cobra.Command) {
		for _, child := range parent.Commands() {
			paths = append(paths, child.CommandPath())
			walk(child)
		}
	}
	walk(root)
	sort.Strings(paths)
	return paths
}

func flagNames(flags *pflag.FlagSet) []string {
	var names []string
	flags.VisitAll(func(flag *pflag.Flag) {
		names = append(names, flag.Name)
	})
	sort.Strings(names)
	return names
}
