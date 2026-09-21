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
	pending   map[string]bool            // true while a worker validates this startup parent
	deferred  map[string]HfArtifactEntry // Updating parents awaiting ownership recovery.
	handler   *hfArtifactTaskHandler
}

func newHfArtifactStartup(handler *hfArtifactTaskHandler) *hfArtifactStartup {
	return &hfArtifactStartup{handler: handler, pending: make(map[string]bool), deferred: make(map[string]HfArtifactEntry)}
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
	cm, err := s.handler.repository.configMaps.getConfigMapForRecovery(ctx)
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
		if err == nil && parent.Key != key {
			err = fmt.Errorf("shared artifact entry %s records key %s", key, parent.Key)
		}
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
			unlock, acquired, err := s.handler.tryParentFileOperation(parent, hfArtifactParentStoreRoot(parent))
			if err != nil {
				s.deferred[key] = parent
				s.pending[key] = false
				s.handler.repository.configMaps.logger.Warnf("Cannot inspect startup parent ownership %s: %v", key, err)
				continue
			}
			if !acquired {
				// Another agent process still owns this directory. Its completion
				// must be validated by this process, never reset merely by age.
				s.pending[key] = false
				s.deferred[key] = parent
				continue
			}
			if s.handler.files.ParentReadyMarkerMatchesLock(parent) {
				if _, err := hfArtifactChildStatusesToRestore(cm.Data, parent, parent); err != nil {
					// A malformed relationship blocks this identity only. API errors
					// below still keep the global scan pending until it can finish.
					s.pending[key] = false
					s.handler.repository.configMaps.logger.Warnf("Cannot finalize shared artifact %s during startup: %v", key, err)
					unlock()
					continue
				}
				err = s.handler.markParentReady(ctx, parent)
				parent.Status = HfArtifactStatusReady
			} else {
				err = s.handler.repository.MarkFailed(ctx, parent)
				parent.Status = HfArtifactStatusFailed
			}
			unlock()
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

// recoverParent checks current ownership, including work started by another
// process after our startup snapshot. Only an available OS lock permits recovery;
// a cached Updating record alone never proves that its owner has stopped.
func (s *hfArtifactStartup) recoverParent(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, deferred := s.deferred[key]
	if !deferred {
		exists, raw, err := s.handler.repository.configMaps.getDataEntryBasedOnModelKey(ctx, key)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil || !exists {
			return err
		}
		expected, err = decodeHfArtifactEntry(key, raw)
		if err != nil {
			return err
		}
		if expected.Key != key {
			return fmt.Errorf("shared artifact entry %s records key %s", key, expected.Key)
		}
		if err := validateHfArtifactIdentityAndPath(expected); err != nil {
			return err
		}
		if expected.Status != HfArtifactStatusUpdating {
			return nil
		}
		if expected.LockID == "" {
			return fmt.Errorf("Updating shared artifact %s has no lock owner", key)
		}
		s.deferred[key] = expected
	}
	unlock, acquired, err := s.handler.tryParentFileOperation(expected, hfArtifactParentStoreRoot(expected))
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("shared artifact %s still has an active owner", key)
	}
	defer unlock()
	parent, found, err := s.handler.repository.Get(ctx, expected.Identity)
	if err != nil {
		return err
	}
	if found && parent.LocalPath != expected.LocalPath {
		s.deferred[key] = parent
		return fmt.Errorf("startup parent %s path changed", key)
	}
	if found && parent.Status == HfArtifactStatusUpdating {
		if s.handler.files.ParentReadyMarkerMatchesLock(parent) {
			err = s.handler.markParentReady(ctx, parent)
		} else {
			err = s.handler.repository.MarkFailed(ctx, parent)
		}
		if err != nil {
			return err
		}
	}
	delete(s.deferred, key)
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
