package modelagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

var startupConfigMapContext = func() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultStartupConfigMapSnapshotTimeout)
}

func (s *Gopher) deferStartupReadyRevalidationIfLocalPathExists(task *GopherTask) bool {
	if s.classifyStartupRevalidation(task) {
		s.logger.Infof("Deferring MD5 validation for %s behind normal download work", getModelInfoForLogging(task))
		s.enqueueTask(task)
		return true
	}
	return false
}

func (s *Gopher) recoverStartupHuggingFaceArtifactParents(ctx context.Context) bool {
	s.startupHuggingFaceParentRecoveryMutex.Lock()
	defer s.startupHuggingFaceParentRecoveryMutex.Unlock()

	recovered := s.recoverStartupHuggingFaceArtifactParentsLocked(ctx)
	s.startupHuggingFaceParentRecoveryPending = !recovered
	return recovered
}

func (s *Gopher) recoverStartupHuggingFaceArtifactParentsLocked(ctx context.Context) bool {
	if s.configMapReconciler == nil {
		return true
	}
	configMap, err := s.configMapReconciler.getConfigMap(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			s.logger.Info("No startup ConfigMap found for Hugging Face artifact parent recovery")
			return true
		}
		s.logger.Warnf("Cannot recover startup Hugging Face artifact parents: %v", err)
		return false
	}
	recovered := true
	for parentKey, dataEntry := range configMap.Data {
		if !isHuggingFaceArtifactConfigMapKey(parentKey) {
			continue
		}
		var entry ModelEntry
		if err := json.Unmarshal([]byte(dataEntry), &entry); err != nil {
			s.logger.Warnf("Cannot parse Hugging Face artifact parent %s during startup recovery: %v", parentKey, err)
			continue
		}
		if entry.Status != ModelStatusUpdating {
			continue
		}
		identity, parentPath, ok := huggingFaceArtifactParentIdentityAndPath(parentKey, entry)
		if !ok {
			s.logger.Warnf("Cannot recover Updating Hugging Face artifact parent %s because it is missing valid identity or parent path", parentKey)
			if err := s.configMapReconciler.deleteConfigMapDataEntry(ctx, parentKey); err != nil {
				s.logger.Warnf("Cannot delete invalid Updating Hugging Face artifact parent %s during startup recovery: %v", parentKey, err)
				recovered = false
			} else {
				s.logger.Infof("Deleted invalid Updating Hugging Face artifact parent %s during startup recovery so a later child task can rebuild it", parentKey)
			}
			continue
		}
		if hasHuggingFaceArtifactReadyMarker(parentPath) {
			if err := s.markHuggingFaceArtifactParentReady(ctx, parentKey, parentPath, identity); err != nil {
				s.logger.Warnf("Cannot mark Hugging Face artifact parent %s Ready during startup recovery: %v", parentKey, err)
				recovered = false
			} else {
				s.logger.Infof("Recovered Hugging Face artifact parent %s as Ready from startup ready marker at %s", parentKey, parentPath)
			}
			continue
		}
		if err := s.configMapReconciler.markHuggingFaceArtifactParentFailed(ctx, parentKey, identity, parentPath); err != nil {
			s.logger.Warnf("Cannot mark stale Hugging Face artifact parent %s Failed during startup recovery: %v", parentKey, err)
			recovered = false
		} else {
			s.logger.Infof("Marked stale Hugging Face artifact parent %s Failed during startup recovery because ready marker is missing at %s", parentKey, parentPath)
		}
	}
	return recovered
}

func (s *Gopher) isStartupHuggingFaceArtifactParentRecoveryPending() bool {
	s.startupHuggingFaceParentRecoveryMutex.Lock()
	defer s.startupHuggingFaceParentRecoveryMutex.Unlock()
	return s.startupHuggingFaceParentRecoveryPending
}

func (s *Gopher) ensureStartupHuggingFaceArtifactParentsRecovered(ctx context.Context, task *GopherTask, parentKey string) (bool, error) {
	if !s.isStartupHuggingFaceArtifactParentRecoveryPending() {
		return false, nil
	}
	if s.recoverStartupHuggingFaceArtifactParents(ctx) {
		return false, nil
	}
	if s.requeueStartupHuggingFaceArtifactParentRecoveryWait(task, parentKey) {
		return true, nil
	}
	return true, fmt.Errorf("timed out waiting for startup Hugging Face artifact parent recovery before processing %s", parentKey)
}

func (s *Gopher) requeueStartupHuggingFaceArtifactParentRecoveryWait(task *GopherTask, parentKey string) bool {
	if task == nil || s.gopherChan == nil {
		return false
	}
	now := time.Now()
	timeout := s.samePathWaitTimeout
	if timeout <= 0 {
		timeout = defaultSamePathWaitTimeout
	}
	if task.SamePathWaitStartedAt.IsZero() {
		task.SamePathWaitStartedAt = now
	} else if now.Sub(task.SamePathWaitStartedAt) >= timeout {
		s.logger.Warnf("Timed out waiting for startup Hugging Face artifact parent recovery for %s before processing %s", parentKey, getModelInfoForLogging(task))
		return false
	}

	delay := s.samePathWaitDelay
	if delay <= 0 {
		delay = defaultSamePathWaitDelay
	}
	s.logger.Infof("Startup Hugging Face artifact parent recovery is pending for %s; requeueing %s after %s", parentKey, getModelInfoForLogging(task), delay)
	time.AfterFunc(delay, func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Warnf("Cannot requeue startup Hugging Face artifact parent recovery task for %s because gopher channel is closed: %v", getModelInfoForLogging(task), r)
			}
		}()
		s.gopherChan <- task
	})
	return true
}

func (s *Gopher) claimStartupHuggingFaceArtifactParentValidation(parentKey string) bool {
	if strings.TrimSpace(parentKey) == "" {
		return false
	}
	s.startupHuggingFaceParentValidationMutex.Lock()
	defer s.startupHuggingFaceParentValidationMutex.Unlock()
	return s.claimStartupHuggingFaceArtifactParentValidationLocked(parentKey)
}

func (s *Gopher) claimStartupHuggingFaceArtifactParentValidationLocked(parentKey string) bool {
	if len(s.startupHuggingFaceParentValidationKeys) > 0 {
		if _, ok := s.startupHuggingFaceParentValidationKeys[parentKey]; ok {
			delete(s.startupHuggingFaceParentValidationKeys, parentKey)
			return true
		}
	}
	if !s.startupHuggingFaceParentSnapshotMissing {
		return false
	}
	if s.startupHuggingFaceParentValidationTried == nil {
		s.startupHuggingFaceParentValidationTried = map[string]struct{}{}
	}
	if _, ok := s.startupHuggingFaceParentValidationTried[parentKey]; ok {
		return false
	}
	s.startupHuggingFaceParentValidationTried[parentKey] = struct{}{}
	return true
}

func (s *Gopher) restoreStartupHuggingFaceArtifactParentValidationLocked(parentKey string) {
	if strings.TrimSpace(parentKey) == "" {
		return
	}
	if s.startupHuggingFaceParentValidationKeys == nil {
		s.startupHuggingFaceParentValidationKeys = map[string]struct{}{}
	}
	s.startupHuggingFaceParentValidationKeys[parentKey] = struct{}{}
}

func (s *Gopher) validateStartupHuggingFaceArtifactParentIfNeeded(ctx context.Context, task *GopherTask, parentKey string, identity ArtifactIdentity, downloadObjectStorageModel func(string) error) (bool, error) {
	if strings.TrimSpace(parentKey) == "" {
		return false, nil
	}
	var validationPath string
	var acquired bool
	var parentBusy bool
	s.startupHuggingFaceParentValidationMutex.Lock()
	if s.claimStartupHuggingFaceArtifactParentValidationLocked(parentKey) {
		observedParentKey, parentPath, parentStatus, ok, lookupErr := s.getHuggingFaceArtifactParentWithError(ctx, identity)
		if lookupErr != nil {
			s.restoreStartupHuggingFaceArtifactParentValidationLocked(parentKey)
			s.startupHuggingFaceParentValidationMutex.Unlock()
			return false, nil
		}
		if ok && observedParentKey == parentKey && parentStatus == ModelStatusReady {
			var err error
			validationPath, acquired, err = s.tryAcquireHuggingFaceArtifactParentRebuild(ctx, parentKey, parentPath, identity)
			if err != nil {
				s.restoreStartupHuggingFaceArtifactParentValidationLocked(parentKey)
				s.startupHuggingFaceParentValidationMutex.Unlock()
				return false, err
			}
			parentBusy = !acquired
		}
	}
	s.startupHuggingFaceParentValidationMutex.Unlock()
	if parentBusy {
		return true, s.waitForHuggingFaceArtifactParent(task, parentKey)
	}
	if strings.TrimSpace(validationPath) == "" {
		return false, nil
	}

	s.logger.Infof("Validating startup Hugging Face artifact parent %s at %s", parentKey, validationPath)
	if err := downloadObjectStorageModel(validationPath); err != nil {
		if failErr := s.configMapReconciler.markHuggingFaceArtifactParentFailed(ctx, parentKey, identity, validationPath); failErr != nil {
			s.logger.Warnf("failed to mark Hugging Face artifact parent %s at %s as Failed after startup validation error: %v", parentKey, validationPath, failErr)
		}
		return false, err
	}
	if markerErr := writeHuggingFaceArtifactReadyMarker(validationPath); markerErr != nil {
		s.logger.Warnf("failed to write Hugging Face artifact ready marker for startup-validated parent %s at %s: %v", parentKey, validationPath, markerErr)
	}
	if err := s.markHuggingFaceArtifactParentReady(ctx, parentKey, validationPath, identity); err != nil {
		s.logger.Errorf("validated startup Hugging Face artifact parent %s at %s but failed to mark it Ready: %v", parentKey, validationPath, err)
		return false, err
	}
	s.logger.Infof("Validated startup Hugging Face artifact parent %s at %s", parentKey, validationPath)
	return false, nil
}
