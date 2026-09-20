package modelagent

import (
	"context"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// hfArtifactStartup separates process-local validation work from durable parent
// ownership. A parent's Ready state survives a restart; this validation cache
// deliberately does not.
type hfArtifactStartup struct {
	mu        sync.Mutex
	recovered bool
	pending   map[string]bool // true while a worker validates this startup parent
	handler   *hfArtifactTaskHandler
}

func newHfArtifactStartup(handler *hfArtifactTaskHandler) *hfArtifactStartup {
	return &hfArtifactStartup{handler: handler, pending: make(map[string]bool)}
}

// recover must run before any shared-artifact task can acquire a parent. Keeping
// this gate closed on API failure prevents a retry scan from mistaking a newly
// started worker for an abandoned startup owner.
func (s *hfArtifactStartup) recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recovered {
		return nil
	}
	cm, err := s.handler.repository.configMaps.getConfigMap(ctx)
	if apierrors.IsNotFound(err) {
		s.recovered = true
		return nil
	}
	if err != nil {
		return err
	}
	for key, raw := range cm.Data {
		if !isHfArtifactConfigMapKey(key) {
			continue
		}
		parent, err := decodeHfArtifactEntry(key, raw)
		if err == nil {
			err = validateHfArtifactIdentityAndPath(parent)
		}
		if err == nil && parent.Status == HfArtifactStatusUpdating && parent.LockID == "" {
			err = fmt.Errorf("Updating shared artifact has no lock owner")
		}
		if err != nil || !isValidHfArtifactStatus(parent.Status) {
			if err == nil {
				err = fmt.Errorf("invalid shared artifact status %q", parent.Status)
			}
			// A corrupt foreign record must not block unrelated model downloads.
			// Its own tasks fail closed until state can be reconstructed.
			s.pending[key] = false
			s.handler.repository.configMaps.logger.Warnf("Cannot recover shared artifact %s; preserving state for reconstruction: %v", key, err)
			continue
		}
		if parent.Status == HfArtifactStatusUpdating {
			if s.handler.files.ParentReadyMarkerMatchesLock(parent) {
				if _, err := hfArtifactChildStatusesToRestore(cm.Data, parent, parent); err != nil {
					// A malformed relationship blocks this identity only. API errors
					// below still keep the global scan pending until it can finish.
					s.pending[key] = false
					s.handler.repository.configMaps.logger.Warnf("Cannot finalize shared artifact %s during startup: %v", key, err)
					continue
				}
				err = s.handler.markParentReady(ctx, parent)
				parent.Status = HfArtifactStatusReady
			} else {
				err = s.handler.repository.MarkFailed(ctx, parent)
				parent.Status = HfArtifactStatusFailed
			}
			if err != nil {
				return fmt.Errorf("recover shared artifact %s: %w", key, err)
			}
		}
		if parent.Status == HfArtifactStatusReady {
			s.pending[key] = false
		}
	}
	s.recovered = true
	return nil
}

func (s *hfArtifactStartup) needsValidation(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, pending := s.pending[key]
	return pending
}

// validateOnce serializes the first integrity check across sibling children.
// Failure keeps the key pending; a transient lookup or status-write error must
// not turn a stale parent into a trusted one for the remainder of this process.
func (s *hfArtifactStartup) validateOnce(ctx context.Context, input hfArtifactTaskInput, validate hfArtifactValidateFunc, download hfArtifactDownloadFunc) (hfArtifactTaskResult, error) {
	s.mu.Lock()
	inProgress, pending := s.pending[input.Parent.Key]
	if !pending {
		s.mu.Unlock()
		return s.handler.handleDownload(ctx, input, download)
	}
	if inProgress {
		s.mu.Unlock()
		return newHfArtifactRetryResult(input.Parent.Key, nil), nil
	}
	s.pending[input.Parent.Key] = true
	s.mu.Unlock()
	validationPerformed := false
	validateParent := validate
	if validate != nil {
		validateParent = func(path string) (bool, error) {
			validationPerformed = true
			return validate(path)
		}
	}
	result, err := s.handler.handleDownloadOverride(ctx, input, validateParent, download)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[input.Parent.Key] = false
	if err == nil && result.Outcome == hfArtifactTaskDone &&
		!s.handler.repository.isChildMutationBlocked(input.ChildModelKey, input.ChildModelUID) {
		if !validationPerformed {
			// Completing a previous owner's marker-backed update did not check
			// its files in this process. Requeue before publishing child Ready.
			return newHfArtifactRetryResult(input.Parent.Key, nil), nil
		}
		delete(s.pending, input.Parent.Key)
	}
	return result, err
}
