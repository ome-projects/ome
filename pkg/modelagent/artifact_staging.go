package modelagent

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Keep lock-aware stages separate from older, uncoordinated staging writers.
const directArtifactDownloadStages = "downloads"

// Stages contain unpublished bytes, not durable model state. The independent
// lock protects another process's download while startup reclaims crash debris.
func (s *Gopher) cleanupArtifactStaging() error {
	root, err := canonicalHfArtifactStoreRoot(s.modelRootDir)
	if err != nil {
		return err
	}
	store := filepath.Join(root, directArtifactStagingDirectory, directArtifactDownloadStages)
	if err := validateHfArtifactPathAncestors(root, store, true); err != nil {
		return err
	}
	entries, err := os.ReadDir(store)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var cleanupErr error
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 64 {
			continue
		}
		if _, err := hex.DecodeString(entry.Name()); err != nil {
			continue
		}
		stage := filepath.Join(store, entry.Name())
		lock, acquired, err := tryHfArtifactFileLock(root, filepath.Join(root, hfArtifactLockDirectory), "staging:"+stage)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		if !acquired {
			continue
		}
		cleanupErr = errors.Join(cleanupErr, removeArtifactStage(root, stage), lock.Close())
	}
	return cleanupErr
}

func removeArtifactStage(root, stage string) error {
	if err := validateHfArtifactPathAncestors(root, stage, true); err != nil {
		return fmt.Errorf("validate artifact staging cleanup: %w", err)
	}
	return os.RemoveAll(stage)
}
