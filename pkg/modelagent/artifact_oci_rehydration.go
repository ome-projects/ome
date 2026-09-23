package modelagent

import (
	"context"
	"fmt"
)

// A consuming Pod permits validation, but never in-place repair. The caller
// supplies verification of the complete, shape-filtered object list.
func (s *Gopher) reuseConsumedOCIArtifact(ctx context.Context, task *GopherTask, path string, verify func() map[string]error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if artifactRehydrationID(task) == "" {
		return false, nil
	}
	used, err := s.pathHasLocalPodConsumers(ctx, path)
	if err != nil || !used {
		return false, err
	}
	verificationErrors := verify()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if len(verificationErrors) != 0 {
		return false, fmt.Errorf("cannot repair OCI artifact while a Pod uses %s: %d existing files failed verification", path, len(verificationErrors))
	}
	return true, nil
}
