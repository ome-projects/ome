// Package stage implements staging a model from an already-mounted source
// directory onto the node's local disk, which is what the stage:// storage
// protocol does. It deliberately knows nothing about Kubernetes or about the
// model agent: everything here is plain filesystem work.
package stage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResolveSource checks that sourcePath is a directory contained in one of the
// configured roots, and returns its symlink-resolved absolute path.
//
// The roots are a security boundary, not a convenience: stage:// lets the
// author of a BaseModel name any path the agent can read, and whatever is
// staged ends up mounted into inference pods. Resolution happens before the
// containment check so a symlink under a root cannot be used to escape it.
func ResolveSource(sourcePath string, roots []string) (string, error) {
	if len(roots) == 0 {
		return "", fmt.Errorf("no stage source roots configured: staging is disabled unless --stage-source-roots is set")
	}

	resolvedSource, err := filepath.EvalSymlinks(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("stage source %q does not exist", sourcePath)
		}
		return "", fmt.Errorf("failed to resolve stage source %q: %w", sourcePath, err)
	}

	info, err := os.Stat(resolvedSource)
	if err != nil {
		return "", fmt.Errorf("failed to stat stage source %q: %w", sourcePath, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("stage source %q is not a directory", sourcePath)
	}

	for _, root := range roots {
		resolvedRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			// A misconfigured root must not silently widen or narrow the
			// boundary; skip it and let the remaining roots decide.
			continue
		}
		if contains(resolvedRoot, resolvedSource) {
			return resolvedSource, nil
		}
	}

	return "", fmt.Errorf("stage source %q resolves to %q, which is outside the configured stage source roots %v",
		sourcePath, resolvedSource, roots)
}

// contains reports whether path is root itself or lives beneath it. Comparison
// is per path segment: a string prefix check would let /mnt/nfs authorize
// /mnt/nfs-evil.
func contains(root, path string) bool {
	if root == path {
		return true
	}
	return strings.HasPrefix(path, strings.TrimSuffix(root, string(os.PathSeparator))+string(os.PathSeparator))
}
