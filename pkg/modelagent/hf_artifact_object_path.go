package modelagent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/constants"
)

// hfArtifactObjectPath maps a literal OCI object key into a shared parent without
// cleaning or decoding remote names. It performs no writes. The caller must hold
// the parent operation lock and validate the parent's store path; these checks
// reject existing symlinks from the supplied parent through the object leaf.
func hfArtifactObjectPath(parentPath, prefix, objectName string) (string, error) {
	if err := validateHfArtifactCleanPath(parentPath); err != nil {
		return "", err
	}
	if parentPath == string(filepath.Separator) {
		return "", fmt.Errorf("shared HF object parent cannot be the filesystem root")
	}
	if !strings.HasPrefix(objectName, prefix) {
		return "", fmt.Errorf("OCI object %q does not match prefix %q", objectName, prefix)
	}
	relative := strings.TrimPrefix(objectName, prefix)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		if !strings.HasPrefix(relative, "/") {
			return "", fmt.Errorf("OCI object %q does not lie below prefix %q", objectName, prefix)
		}
		relative = strings.TrimPrefix(relative, "/")
	}
	if strings.ContainsAny(relative, "\\\x00") {
		return "", fmt.Errorf("unsafe OCI artifact object name %q", objectName)
	}
	for _, part := range strings.Split(relative, "/") {
		if part == "" || part == "." || part == ".." || part == constants.HfArtifactReadyMarkerFileName || constants.IsInternalArtifactObjectName(part) {
			return "", fmt.Errorf("unsafe OCI artifact object name %q", objectName)
		}
	}
	destination := filepath.Join(parentPath, filepath.FromSlash(relative))
	if err := validateHfArtifactPathAncestors(parentPath, destination, false); err != nil {
		return "", err
	}
	if info, err := os.Lstat(destination); err == nil && !info.Mode().IsRegular() {
		return "", fmt.Errorf("OCI artifact object destination %s is not a regular file", destination)
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return destination, nil
}
