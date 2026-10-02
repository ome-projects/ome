package modelagent

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/llmman"
	"sigs.k8s.io/ome/pkg/modelparser"
)

func TestMaterializeModelPack(t *testing.T) {
	t.Run("directory", func(t *testing.T) {
		src, dest := t.TempDir(), filepath.Join(t.TempDir(), "out")
		require.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(src, "config.json"), []byte("{}"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(src, "sub", "w.bin"), []byte("weights"), 0o644))

		require.NoError(t, materializeModelPack(context.Background(), src, dest))

		got, err := os.ReadFile(filepath.Join(dest, "sub", "w.bin"))
		require.NoError(t, err)
		require.Equal(t, "weights", string(got))
		require.FileExists(t, filepath.Join(dest, "config.json"))
	})

	t.Run("single file", func(t *testing.T) {
		src := filepath.Join(t.TempDir(), "model.gguf")
		dest := filepath.Join(t.TempDir(), "out")
		require.NoError(t, os.WriteFile(src, []byte("gguf"), 0o644))

		require.NoError(t, materializeModelPack(context.Background(), src, dest))

		got, err := os.ReadFile(filepath.Join(dest, "model.gguf"))
		require.NoError(t, err)
		require.Equal(t, "gguf", string(got))
	})

	t.Run("missing source", func(t *testing.T) {
		require.Error(t, materializeModelPack(context.Background(), filepath.Join(t.TempDir(), "nope"), t.TempDir()))
	})
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(root, 0o755))
	for name, content := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
}

func readTree(t *testing.T, root string) map[string]string {
	t.Helper()
	got := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		require.NoError(t, err)
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		got[rel] = string(b)
		return nil
	}))
	return got
}

// requireOnlyEntry fails if dir holds anything but name, e.g. a leftover
// staging directory.
func requireOnlyEntry(t *testing.T, dir, name string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, name, entries[0].Name())
}

func TestMaterializeModelPackReplacesDestination(t *testing.T) {
	tests := []struct {
		name     string
		existing map[string]string // nil: destination does not exist
		src      map[string]string
		single   bool // src is the one file in src, not a directory
		slash    bool // dest is given with a trailing slash
	}{
		{
			name: "fresh destination",
			src:  map[string]string{"config.json": "{}", "sub/w.bin": "weights"},
		},
		{
			name:     "files of the previous artifact are dropped",
			existing: map[string]string{"config.json": "old", "old.bin": "old", "olddir/x": "old"},
			src:      map[string]string{"config.json": "new", "w.bin": "new"},
		},
		{
			name:     "empty destination directory",
			existing: map[string]string{},
			src:      map[string]string{"config.json": "{}"},
		},
		{
			name:     "trailing slash on destination",
			existing: map[string]string{"old.bin": "old"},
			src:      map[string]string{"config.json": "{}"},
			slash:    true,
		},
		{
			name:     "single file replaces a directory",
			existing: map[string]string{"old.bin": "old", "sub/x": "old"},
			src:      map[string]string{"model.gguf": "gguf"},
			single:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcDir, parent := t.TempDir(), t.TempDir()
			dest := filepath.Join(parent, "out")
			if tt.existing != nil {
				writeTree(t, dest, tt.existing)
			}
			writeTree(t, srcDir, tt.src)
			src := srcDir
			if tt.single {
				for name := range tt.src {
					src = filepath.Join(srcDir, name)
				}
			}
			arg := dest
			if tt.slash {
				arg += "/"
			}

			require.NoError(t, materializeModelPack(context.Background(), src, arg))

			require.Equal(t, tt.src, readTree(t, dest))
			requireOnlyEntry(t, parent, "out")
		})
	}
}

func TestMaterializeModelPackKeepsDestinationOnFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions are not enforced for root")
	}
	src, parent := t.TempDir(), t.TempDir()
	dest := filepath.Join(parent, "out")
	writeTree(t, src, map[string]string{"a.bin": "new"})
	locked := filepath.Join(src, "locked")
	require.NoError(t, os.Mkdir(locked, 0o000))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	writeTree(t, dest, map[string]string{"old.bin": "old"})

	require.Error(t, materializeModelPack(context.Background(), src, dest))

	require.Equal(t, map[string]string{"old.bin": "old"}, readTree(t, dest))
	requireOnlyEntry(t, parent, "out")
}

func TestMaterializeModelPackRefusesSourceInsideDestination(t *testing.T) {
	dest := t.TempDir()
	src := filepath.Join(dest, "store", "model")
	writeTree(t, src, map[string]string{"w.bin": "weights"})

	require.Error(t, materializeModelPack(context.Background(), src, dest))

	require.Equal(t, map[string]string{"store/model/w.bin": "weights"}, readTree(t, dest))
}

func TestMaterializeModelPackReplacesSymlinkNotItsTarget(t *testing.T) {
	// A destination that links to a shared directory must not write into it.
	shared, src, parent := t.TempDir(), t.TempDir(), t.TempDir()
	dest := filepath.Join(parent, "out")
	writeTree(t, shared, map[string]string{"shared.bin": "shared"})
	writeTree(t, src, map[string]string{"config.json": "{}"})
	require.NoError(t, os.Symlink(shared, dest))

	require.NoError(t, materializeModelPack(context.Background(), src, dest))

	fi, err := os.Lstat(dest)
	require.NoError(t, err)
	require.True(t, fi.IsDir())
	require.Equal(t, map[string]string{"config.json": "{}"}, readTree(t, dest))
	require.Equal(t, map[string]string{"shared.bin": "shared"}, readTree(t, shared))
}

func TestMaterializeModelPackCanceledKeepsDestination(t *testing.T) {
	src, parent := t.TempDir(), t.TempDir()
	dest := filepath.Join(parent, "out")
	writeTree(t, src, map[string]string{"a.bin": "new", "sub/b.bin": "new"})
	writeTree(t, dest, map[string]string{"old.bin": "old"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, materializeModelPack(ctx, src, dest), context.Canceled)

	require.Equal(t, map[string]string{"old.bin": "old"}, readTree(t, dest))
	requireOnlyEntry(t, parent, "out")
}

// cancelAfter reports context.Canceled once Err has been called n times, so a
// test can cancel at each checkpoint in turn. calls counts every Err call.
type cancelAfter struct {
	context.Context
	n, calls int
}

func (c *cancelAfter) Err() error {
	c.calls++
	if c.calls > c.n {
		return context.Canceled
	}
	return nil
}

func TestMaterializeModelPackCancelAtAnyPointNeverInstalls(t *testing.T) {
	src := t.TempDir()
	want := map[string]string{"a.bin": "new", "sub/b.bin": "new"}
	writeTree(t, src, want)

	run := func(n int) (dest string, parent string, calls int, err error) {
		parent = t.TempDir()
		dest = filepath.Join(parent, "out")
		writeTree(t, dest, map[string]string{"old.bin": "old"})
		ctx := &cancelAfter{Context: context.Background(), n: n}
		err = materializeModelPack(ctx, src, dest)
		return dest, parent, ctx.calls, err
	}

	// An uncanceled run tells how many checkpoints there are.
	dest, _, checkpoints, err := run(1 << 20)
	require.NoError(t, err)
	require.Equal(t, want, readTree(t, dest))
	require.Greater(t, checkpoints, 2)

	// Canceling at any earlier checkpoint, including the last one before the
	// swap, must leave the previous artifact in place.
	for n := 0; n < checkpoints; n++ {
		dest, parent, _, err := run(n)
		require.ErrorIs(t, err, context.Canceled, "n=%d", n)
		require.Equal(t, map[string]string{"old.bin": "old"}, readTree(t, dest), "n=%d", n)
		requireOnlyEntry(t, parent, "out")
	}
}

func TestMaterializeModelPackCanceledAfterStagingDoesNotSwap(t *testing.T) {
	// A single file is staged without per-entry checks, so the only check
	// after the one at entry is the one guarding the swap.
	srcDir, parent := t.TempDir(), t.TempDir()
	dest := filepath.Join(parent, "out")
	writeTree(t, srcDir, map[string]string{"model.gguf": "gguf"})
	writeTree(t, dest, map[string]string{"old.bin": "old"})

	ctx := &cancelAfter{Context: context.Background(), n: 1}
	err := materializeModelPack(ctx, filepath.Join(srcDir, "model.gguf"), dest)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, ctx.calls)
	require.Equal(t, map[string]string{"old.bin": "old"}, readTree(t, dest))
	requireOnlyEntry(t, parent, "out")
}

// fakeLlmman starts a daemon that answers /api/version and /api/pull, and a
// llmman binary whose `resolve` prints resolved. It returns the references the
// daemon was asked to pull.
func fakeLlmman(t *testing.T, resolved string) *[]string {
	t.Helper()
	pulled := &[]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/version":
			_, _ = w.Write([]byte(`{"version":"test"}`))
		case "/api/pull":
			var req map[string]string
			_ = json.NewDecoder(r.Body).Decode(&req)
			*pulled = append(*pulled, req["model"])
			_, _ = w.Write([]byte("{\"status\":\"pulling\",\"completed\":1,\"total\":2}\n{\"status\":\"success\"}\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	bin := filepath.Join(t.TempDir(), "llmman")
	script := "#!/bin/sh\necho '{\"path\":\"" + resolved + "\"}'\n"
	require.NoError(t, os.WriteFile(bin, []byte(script), 0o755))
	t.Setenv(llmman.HostEnv, srv.URL)
	t.Setenv(llmman.BinEnv, bin)
	return pulled
}

func TestProcessTaskModelPackPublishesReady(t *testing.T) {
	src := t.TempDir()
	writeTree(t, src, map[string]string{"weights.bin": "weights"})
	pulled := fakeLlmman(t, src)

	g, task := newCancellationCoverageGopher(t, "modelpack://ghcr.io/org/model:tag")
	g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
	dest := *task.BaseModel.Spec.Storage.Path

	require.NoError(t, g.processTask(task))

	assert.Equal(t, []string{"ghcr.io/org/model:tag"}, *pulled)
	assert.Equal(t, map[string]string{"weights.bin": "weights"}, readTree(t, dest))
	assertCancellationCoverageStatus(t, g, task, ModelStatusReady)
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)
	assert.Equal(t, float64(1), testutil.ToFloat64(g.metrics.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name)))
}

func TestProcessTaskModelPackPullFailurePublishesFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.URL
	srv.Close() // nothing listens at addr
	t.Setenv(llmman.HostEnv, addr)

	g, task := newCancellationCoverageGopher(t, "modelpack://ghcr.io/org/model:tag")

	err := g.processTask(task)

	require.ErrorContains(t, err, "llmman serve")
	assert.NotErrorIs(t, err, context.Canceled)
	assertCancellationCoverageStatus(t, g, task, ModelStatusFailed)
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)
	assert.Equal(t, float64(1), testutil.ToFloat64(g.metrics.modelDownloadsFailedTotal.WithLabelValues(modelType, namespace, name)))
}

func TestProcessTaskModelPackDeleteRemovesFiles(t *testing.T) {
	g, task := newCancellationCoverageGopher(t, "modelpack://ghcr.io/org/model:tag")
	task.TaskType = Delete
	dest := *task.BaseModel.Spec.Storage.Path
	writeTree(t, dest, map[string]string{"weights.bin": "weights"})

	require.NoError(t, g.processTask(task))

	require.NoDirExists(t, dest)
}
