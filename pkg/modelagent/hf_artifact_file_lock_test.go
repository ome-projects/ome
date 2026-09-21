package modelagent

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			unlock, acquired, err := firstHandler.tryArtifactOperation(first)
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
				parentUnlock, acquired, err := secondHandler.tryParentFileOperation(second.Parent, second.ModelStoreRoot)
				require.NoError(t, err)
				require.True(t, acquired)
				parentUnlock()
			}
			_, acquired, err = secondHandler.tryArtifactOperation(second)
			require.NoError(t, err)
			require.False(t, acquired)

			unlock()
			firstReleased = true
			secondUnlock, acquired, err := secondHandler.tryArtifactOperation(second)
			require.NoError(t, err)
			require.True(t, acquired, "the competing owner can acquire after release")
			defer secondUnlock()
			_, acquired, err = firstHandler.tryArtifactOperation(first)
			require.NoError(t, err)
			require.False(t, acquired, "alias ownership also excludes the canonical owner")
		})
	}
}
