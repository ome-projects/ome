// Command docs-examples checks the YAML examples in the documentation
// site against the API server's validation.
//
// It reads every fenced yaml block in the Markdown files under -content
// that isn't marked check=skip, then starts an envtest API server with the
// CRDs in -crds. It creates the namespaces the examples use, then dry-run
// creates each object with strict field validation. That catches unknown
// fields, wrong types, enum values, missing required fields and CEL rules,
// but not rules that only the admission webhooks enforce, because envtest
// doesn't run them.
//
// Documents without apiVersion or kind, such as fragments of objects and
// Helm values, aren't objects and aren't checked. An object without
// metadata.name or metadata.generateName fails, so a config file with an
// apiVersion and kind, such as a kubeconfig, needs check=skip. Objects
// whose API group the server doesn't serve, such as KEDA's, are skipped
// and listed.
//
// It exits 0 when every example passes, 1 when one fails, and 2 when it
// can't run or finds no objects.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/go-logr/logr"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run runs the command with args, the arguments after the program name,
// and returns its exit code.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("docs-examples", flag.ContinueOnError)
	flags.SetOutput(stderr)
	content := flags.String("content", "website/src/lib/content", "`directory` of Markdown pages")
	crds := flags.String("crds", "config/crd/full", "`directory` of CRDs to install")
	if err := flags.Parse(args); err != nil {
		// Parse has printed the error, or the usage for -h.
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	// controller-runtime warns when nothing sets its logger.
	log.SetLogger(logr.Discard())

	// Read the examples first, so a wrong -content fails without starting
	// envtest.
	docs, err := readDocuments(*content)
	if err != nil {
		fmt.Fprintf(stderr, "docs-examples: %v\n", err)
		return 2
	}
	examples, rep := parse(docs)
	if len(examples) == 0 && len(rep.failures) == 0 {
		fmt.Fprintf(stderr, "docs-examples: found no Kubernetes objects in the Markdown files under %s; is -content right?\n", *content)
		return 2
	}

	// etcd and the API server run in their own process group, so Ctrl-C
	// doesn't reach them. Catch it, and SIGTERM, so that env.Stop runs.
	// The deferred env.Stop runs before stop, while signals are still
	// caught, so a second Ctrl-C can't cut it short.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env := &envtest.Environment{CRDDirectoryPaths: []string{*crds}, ErrorIfCRDPathMissing: true}
	cfg, err := startEnvironment(env)
	if err != nil {
		fmt.Fprintf(stderr, "docs-examples: start the envtest API server: %v\n", err)
		// Without KUBEBUILDER_ASSETS, envtest can't find its binaries. With
		// it, the error is something else, and the hint would mislead.
		if os.Getenv("KUBEBUILDER_ASSETS") == "" {
			fmt.Fprintln(stderr, "Run make docs-examples, which installs envtest and sets KUBEBUILDER_ASSETS.")
		}
		return 2
	}
	defer func() {
		if err := env.Stop(); err != nil {
			fmt.Fprintf(stderr, "docs-examples: stop the envtest API server: %v\n", err)
		}
	}()
	if ctx.Err() != nil {
		fmt.Fprintln(stderr, "docs-examples: interrupted")
		return 2
	}

	c, err := newChecker(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "docs-examples: %v\n", err)
		return 2
	}
	err = c.check(ctx, examples, rep)
	if ctx.Err() != nil {
		fmt.Fprintln(stderr, "docs-examples: interrupted")
		return 2
	}
	if err != nil {
		fmt.Fprintf(stderr, "docs-examples: %v\n", err)
		return 2
	}
	rep.print(stdout)
	if len(rep.failures) > 0 {
		return 1
	}
	return 0
}

// startEnvironment starts env. env.Start can fail after etcd and the API
// server are running, such as when it can't install the CRDs, and leave
// them running, so startEnvironment stops them.
func startEnvironment(env *envtest.Environment) (*rest.Config, error) {
	cfg, err := env.Start()
	if err != nil {
		if stopErr := env.Stop(); stopErr != nil {
			return nil, errors.Join(err, fmt.Errorf("stop the envtest API server: %w", stopErr))
		}
		return nil, err
	}
	return cfg, nil
}

// readDocuments returns the YAML documents in the Markdown files under dir.
func readDocuments(dir string) ([]document, error) {
	var docs []document
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".md" {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		docs = append(docs, extractDocuments(path, string(data))...)
		return nil
	})
	return docs, err
}
