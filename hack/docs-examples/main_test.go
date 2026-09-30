package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// crdDir is the directory of OME's CRDs, from this package's directory.
var crdDir = filepath.Join("..", "..", "config", "crd", "full")

// requireEnvtest skips the test when KUBEBUILDER_ASSETS, which tells
// envtest where its binaries are, isn't set. In CI, where a skip would
// look like a pass, it fails the test instead.
func requireEnvtest(t *testing.T) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		return
	}
	if os.Getenv("CI") != "" {
		t.Fatal("KUBEBUILDER_ASSETS is not set, and CI is, so this envtest test fails instead of skipping; the Website workflow shows how to set it")
	}
	t.Skip("KUBEBUILDER_ASSETS is not set; the Website workflow shows how to set it")
}

func writeFile(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRun(t *testing.T) {
	content := t.TempDir()
	writeFile(t, filepath.Join(content, "page.md"), "```yaml\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n```\n")
	noObjects := t.TempDir()
	writeFile(t, filepath.Join(noObjects, "page.md"), "```yaml\nspec:\n  replicas: 1\n```\n")
	missing := filepath.Join(t.TempDir(), "missing")
	// noEnvtest makes envtest fail to start, for the cases that mustn't
	// get that far.
	noEnvtest := map[string]string{
		"KUBEBUILDER_ASSETS":        "",
		"TEST_ASSET_ETCD":           missing,
		"TEST_ASSET_KUBE_APISERVER": missing,
	}
	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		wantCode   int
		wantStderr []string
	}{
		{
			name:       "help",
			args:       []string{"-h"},
			wantCode:   0,
			wantStderr: []string{"Usage of docs-examples:", "-content directory"},
		},
		{
			name:       "unknown flag",
			args:       []string{"-contents", content},
			wantCode:   2,
			wantStderr: []string{"flag provided but not defined: -contents", "Usage of docs-examples:"},
		},
		{
			name:       "missing content directory",
			args:       []string{"-content", missing},
			env:        noEnvtest,
			wantCode:   2,
			wantStderr: []string{"docs-examples: lstat " + missing + ": no such file or directory"},
		},
		{
			name:       "no objects",
			args:       []string{"-content", noObjects, "-crds", crdDir},
			env:        noEnvtest,
			wantCode:   2,
			wantStderr: []string{"docs-examples: found no Kubernetes objects in the Markdown files under " + noObjects + "; is -content right?"},
		},
		{
			name:     "no envtest binaries",
			args:     []string{"-content", content, "-crds", crdDir},
			env:      noEnvtest,
			wantCode: 2,
			wantStderr: []string{
				"docs-examples: start the envtest API server:",
				"Run make docs-examples, which installs envtest and sets KUBEBUILDER_ASSETS.",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			// A failed envtest start can leave a temporary directory
			// behind. Keep it in one that the test removes.
			t.Setenv("TMPDIR", t.TempDir())
			var stdout, stderr strings.Builder
			if got := run(tc.args, &stdout, &stderr); got != tc.wantCode {
				t.Errorf("run() = %d, want %d; stderr:\n%s", got, tc.wantCode, stderr.String())
			}
			if stdout.Len() > 0 {
				t.Errorf("run() wrote to stdout:\n%s", stdout.String())
			}
			for _, want := range tc.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr doesn't contain %q:\n%s", want, stderr.String())
				}
			}
		})
	}
}

func TestRunWithEnvtest(t *testing.T) {
	requireEnvtest(t)
	content := t.TempDir()
	page := filepath.Join(content, "page.md")
	writeFile(t, page, `~~~yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
---
apiVersion: v1
kind: Service
metadata:
  name: bad-port
spec:
  ports:
    - port: 700000
~~~
`)
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout []string
		wantStderr []string
	}{
		{
			name:     "a failing example",
			args:     []string{"-content", content, "-crds", crdDir},
			wantCode: 1,
			wantStdout: []string{
				page + ":7: Service bad-port: ",
				"spec.ports[0].port: Invalid value: 700000",
				"Checked 2 objects: 1 problem.\n",
			},
		},
		{
			name:       "missing CRD directory",
			args:       []string{"-content", content, "-crds", filepath.Join(content, "missing")},
			wantCode:   2,
			wantStderr: []string{"docs-examples: start the envtest API server: unable to install CRDs"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr strings.Builder
			if got := run(tc.args, &stdout, &stderr); got != tc.wantCode {
				t.Errorf("run() = %d, want %d; stderr:\n%s", got, tc.wantCode, stderr.String())
			}
			for _, want := range tc.wantStdout {
				if !strings.Contains(stdout.String(), want) {
					t.Errorf("stdout doesn't contain %q:\n%s", want, stdout.String())
				}
			}
			for _, want := range tc.wantStderr {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr doesn't contain %q:\n%s", want, stderr.String())
				}
			}
			// KUBEBUILDER_ASSETS is set, so a failed start isn't about
			// finding the binaries.
			if strings.Contains(stderr.String(), "Run make docs-examples") {
				t.Errorf("stderr has the KUBEBUILDER_ASSETS hint:\n%s", stderr.String())
			}
		})
	}
}

func TestStartEnvironmentStopsAfterFailure(t *testing.T) {
	requireEnvtest(t)
	log.SetLogger(logr.Discard())
	// env.Start fails after etcd and the API server are up, when it
	// installs the CRDs.
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(t.TempDir(), "missing")},
		ErrorIfCRDPathMissing: true,
	}
	// Stop what's left running even when the test fails. Stop is safe to
	// call twice.
	t.Cleanup(func() { _ = env.Stop() })

	if _, err := startEnvironment(env); err == nil {
		t.Fatal("startEnvironment() succeeded with a missing CRD directory, want an error")
	}
	for name, addr := range map[string]string{
		"etcd":           env.ControlPlane.Etcd.URL.Host,
		"kube-apiserver": env.ControlPlane.APIServer.SecureServing.ListenAddr.HostPort(),
	} {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			t.Errorf("%s still listens on %s after the failed start", name, addr)
		} else if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Errorf("dial %s on %s: %v, want connection refused", name, addr, err)
		}
	}
}

func TestReadDocuments(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.md")
	writeFile(t, a, "# A\n\n```yaml\nkind: One\n---\nkind: Two\n```\n\n```yaml check=skip\nkind: Skipped\n```\n\n```bash\nkind: Bash\n```\n")
	b := filepath.Join(dir, "sub", "b.md")
	writeFile(t, b, "Text\n\n~~~yml\n\nkind: Three\n~~~\n")
	writeFile(t, filepath.Join(dir, "notes.txt"), "```yaml\nkind: NotMarkdown\n```\n")

	got, err := readDocuments(dir)
	if err != nil {
		t.Fatalf("readDocuments() error: %v", err)
	}
	want := []document{
		{path: a, line: 4, text: "kind: One"},
		{path: a, line: 6, text: "kind: Two"},
		{path: b, line: 5, text: "kind: Three"},
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(document{})); diff != "" {
		t.Errorf("readDocuments() mismatch (-want +got):\n%s", diff)
	}
}
