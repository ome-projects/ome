package modelagent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestHfArtifactFileLockProcess(t *testing.T) {
	root := os.Getenv("OME_TEST_HF_LOCK_ROOT")
	if root == "" {
		return
	}
	input := testHfArtifactTaskInput(t, root, "child")
	lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, root)
	require.NoError(t, err)
	require.True(t, acquired)
	fmt.Println("locked")
	// The parent kills this process, without running Close or any deferred work.
	defer lock.Close()
	time.Sleep(time.Minute)
}

func TestHfArtifactFileLockSurvivesDirectoryRemovalAndClose(t *testing.T) {
	input := testHfArtifactTaskInput(t, t.TempDir(), "child")
	lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
	require.NoError(t, err)
	require.True(t, acquired)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	require.False(t, strings.HasPrefix(lock.Path(), input.Parent.LocalPath+string(filepath.Separator)))
	require.NoError(t, os.MkdirAll(input.Parent.LocalPath, 0o755))
	require.NoError(t, os.RemoveAll(input.Parent.LocalPath))
	_, acquired, err = tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
	require.NoError(t, err)
	require.False(t, acquired)
	require.NoError(t, lock.Close())
	assert.FileExists(t, lock.Path(), "lock inode must never be unlinked")
	next, acquired, err := tryHfArtifactParentFileLock(input.Parent, input.ModelStoreRoot)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, next.Close())
}

func TestHfArtifactFileLockReleasedOnProcessExit(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHfArtifactFileLockProcess$")
	cmd.Env = append(os.Environ(), "OME_TEST_HF_LOCK_ROOT="+root)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	scanner := bufio.NewScanner(stdout)
	require.True(t, scanner.Scan())
	require.Equal(t, "locked", scanner.Text())
	input := testHfArtifactTaskInput(t, root, "child")
	_, acquired, err := tryHfArtifactParentFileLock(input.Parent, root)
	require.NoError(t, err)
	require.False(t, acquired)
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait())
	lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, root)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, lock.Close())
}

func TestHfArtifactHandlersUseOSLocks(t *testing.T) {
	root := t.TempDir()
	input := testHfArtifactTaskInput(t, root, "child")
	for _, childLock := range []bool{false, true} {
		t.Run(fmt.Sprint("child=", childLock), func(t *testing.T) {
			lock, acquired, err := tryHfArtifactParentFileLock(input.Parent, root)
			if childLock {
				require.NoError(t, err)
				require.NoError(t, lock.Close())
				lock, acquired, err = tryHfArtifactChildFileLock(input)
			}
			require.NoError(t, err)
			require.True(t, acquired)
			defer lock.Close()
			for i := 0; i < 2; i++ {
				repository, _ := newTestHfArtifactRepository(t, map[string]string{})
				handler := newHfArtifactTaskHandler(repository)
				result, err := handler.handleDownload(context.Background(), input, func(string) error {
					t.Fatal("a contending handler must not download")
					return nil
				})
				require.NoError(t, err)
				assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
			}
		})
	}
}

func TestHfArtifactPathsRejectTraversalAndSymlinkAncestors(t *testing.T) {
	for _, scenario := range []string{"traversal", "relative root", "outside child", "outside parent", "child in artifacts", "parent symlink", "ancestor symlink", "child ancestor symlink", "stored parent outside"} {
		t.Run(scenario, func(t *testing.T) {
			repository, _ := newTestHfArtifactRepository(t, map[string]string{})
			handler := newHfArtifactTaskHandler(repository)
			input := testHfArtifactTaskInput(t, t.TempDir(), "child")
			outside := t.TempDir()
			switch scenario {
			case "traversal":
				input.ChildModelPath = input.ModelStoreRoot + "/../" + filepath.Base(input.ModelStoreRoot) + "/child"
			case "relative root":
				input.ModelStoreRoot = "."
			case "outside child":
				input.ChildModelPath = filepath.Join(outside, "child")
			case "outside parent":
				input.Parent.LocalPath = canonicalHfArtifactPath(filepath.Join(outside, "child"), input.Parent.Identity)
			case "child in artifacts":
				input.ChildModelPath = filepath.Join(input.ModelStoreRoot, "_artifacts", "child")
			case "parent symlink":
				require.NoError(t, os.MkdirAll(filepath.Dir(input.Parent.LocalPath), 0o755))
				require.NoError(t, os.Symlink(outside, input.Parent.LocalPath))
			case "ancestor symlink":
				require.NoError(t, os.Symlink(outside, filepath.Join(input.ModelStoreRoot, "_artifacts")))
			case "child ancestor symlink":
				require.NoError(t, os.Symlink(outside, filepath.Join(input.ModelStoreRoot, "nested")))
				input.ChildModelPath = filepath.Join(input.ModelStoreRoot, "nested", "child")
			case "stored parent outside":
				parent := input.Parent
				parent.LocalPath = canonicalHfArtifactPath(filepath.Join(outside, "child"), parent.Identity)
				locked, acquired, err := repository.TryAcquireLock(context.Background(), parent)
				require.NoError(t, err)
				require.True(t, acquired)
				require.NoError(t, repository.MarkReady(context.Background(), locked))
			}
			result, err := handler.handleDownload(context.Background(), input, func(string) error {
				t.Fatal("unsafe path must not reach download")
				return nil
			})
			require.NoError(t, err)
			assert.Equal(t, hfArtifactTaskRetry, result.Outcome)
			assert.Error(t, result.RetryReason)
		})
	}
}

func testHfArtifactStoreRootAlias(t *testing.T) (string, string) {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	canonicalRoot := filepath.Join(directory, "real", "store")
	require.NoError(t, os.MkdirAll(canonicalRoot, 0o755))
	alias := filepath.Join(directory, "alias")
	// The alias is above the store; symlinks inside the store remain forbidden.
	require.NoError(t, os.Symlink(filepath.Dir(canonicalRoot), alias))
	return canonicalRoot, filepath.Join(alias, "store")
}

func TestHfArtifactPathsAllowSystemRootAlias(t *testing.T) {
	canonicalRoot, aliasRoot := testHfArtifactStoreRootAlias(t)
	input := testHfArtifactTaskInput(t, aliasRoot, "child")
	input.ModelStoreRoot = canonicalRoot
	repository, _ := newTestHfArtifactRepository(t, map[string]string{})
	seedTestChildModelEntry(t, repository, input)
	result, err := newHfArtifactTaskHandler(repository).handleDownload(context.Background(), input, writeTestHfArtifactFiles)
	require.NoError(t, err)
	require.Equal(t, hfArtifactTaskDone, result.Outcome, "%v", result.RetryReason)
}

func TestHfArtifactFileLocksContendAcrossRootAliases(t *testing.T) {
	for _, scenario := range []string{"same parent", "same child with different parents"} {
		t.Run(scenario, func(t *testing.T) {
			canonicalRoot, aliasRoot := testHfArtifactStoreRootAlias(t)
			first := testHfArtifactTaskInput(t, canonicalRoot, "first")
			second := testHfArtifactTaskInput(t, aliasRoot, "second")
			if scenario == "same child with different parents" {
				second.ChildModelPath = filepath.Join(aliasRoot, "first")
				second.Parent.Identity.ModelID = "Qwen/Qwen3-14B"
				second.Parent.Key = hfArtifactConfigMapKey(second.Parent.Identity)
				second.Parent.LocalPath = canonicalHfArtifactPath(second.ChildModelPath, second.Parent.Identity)
			}
			firstRepository, _ := newTestHfArtifactRepository(t, map[string]string{})
			secondRepository, _ := newTestHfArtifactRepository(t, map[string]string{})
			firstHandler := newHfArtifactTaskHandler(firstRepository)
			secondHandler := newHfArtifactTaskHandler(secondRepository)
			unlock, acquired, err := firstHandler.tryArtifactOperation(context.Background(), first)
			require.NoError(t, err)
			require.True(t, acquired)
			firstReleased := false
			defer func() {
				if !firstReleased {
					unlock()
				}
			}()

			if scenario == "same child with different parents" {
				// Prove that the child lock, not parent contention, blocks this owner.
				parentUnlock, acquired, err := secondHandler.tryParentFileOperation(context.Background(), second.Parent, second.ModelStoreRoot)
				require.NoError(t, err)
				require.True(t, acquired)
				parentUnlock()
			}
			_, acquired, err = secondHandler.tryArtifactOperation(context.Background(), second)
			require.NoError(t, err)
			require.False(t, acquired)

			unlock()
			firstReleased = true
			secondUnlock, acquired, err := secondHandler.tryArtifactOperation(context.Background(), second)
			require.NoError(t, err)
			require.True(t, acquired, "the competing owner can acquire after release")
			defer secondUnlock()
			_, acquired, err = firstHandler.tryArtifactOperation(context.Background(), first)
			require.NoError(t, err)
			require.False(t, acquired, "alias ownership also excludes the canonical owner")
		})
	}
}

// The transport waits for request cancellation, with a test-only escape hatch
// so a missing caller context fails rather than hanging the test.
type artifactCheckTransport func(*http.Request) (*http.Response, error)

func (f artifactCheckTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSharedFileCheckHonorsCallerDeadlineAndReleasesLocks(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel"} {
		for _, blockedRead := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/%d", mode, blockedRead), func(t *testing.T) {
				input := testHfArtifactTaskInput(t, t.TempDir(), "child")
				repo, fakeClient := newTestHfArtifactRepository(t, map[string]string{})
				h := newHfArtifactTaskHandler(repo)
				reads := 0
				var cancel context.CancelFunc
				client, err := kubernetes.NewForConfig(&rest.Config{Host: "https://test.invalid", Transport: artifactCheckTransport(func(r *http.Request) (*http.Response, error) {
					reads++
					if reads == blockedRead {
						if mode == "cancel" {
							cancel()
						}
						select {
						case <-r.Context().Done():
							return nil, r.Context().Err()
						case <-time.After(time.Second):
							return nil, fmt.Errorf("request ignored caller deadline")
						}
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"node-1","namespace":"ome"},"data":{}}`))}, nil
				})})
				require.NoError(t, err)
				repo.configMaps.kubeClient = client
				ctx := context.Background()
				if mode == "cancel" {
					ctx, cancel = context.WithCancel(ctx)
				} else {
					ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				}
				defer cancel()
				result, err := h.handleDownload(ctx, input, writeTestHfArtifactFiles)
				require.NoError(t, err)
				if mode == "cancel" {
					require.ErrorIs(t, result.RetryReason, context.Canceled)
				} else {
					require.ErrorIs(t, result.RetryReason, context.DeadlineExceeded)
				}
				repo.configMaps.kubeClient = fakeClient
				unlock, acquired, err := h.tryArtifactOperation(context.Background(), input)
				require.NoError(t, err)
				require.True(t, acquired, "all file and operation locks must be released")
				unlock()
			})
		}
	}
}

func TestSharedFileCheckHasDefaultTimeout(t *testing.T) {
	repo, _ := newTestHfArtifactRepository(t, map[string]string{})
	before := time.Now()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: "https://test.invalid", Transport: artifactCheckTransport(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		require.True(t, ok, "lock-held checks must have a deadline even without a caller deadline")
		require.WithinDuration(t, before.Add(10*time.Second), deadline, time.Second)
		return nil, context.Canceled
	})})
	require.NoError(t, err)
	repo.configMaps.kubeClient = client
	require.ErrorIs(t, repo.configMaps.checkDirectCleanupForWriter(context.Background(), t.TempDir()), context.Canceled)
}
