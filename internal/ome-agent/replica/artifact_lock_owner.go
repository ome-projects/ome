package replica

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"sigs.k8s.io/ome/pkg/constants"
)

const maxArtifactLockOwnerBytes = 4096

// The consumer supplies the same owner ID for retries of one replication
// operation and a different ID for each independent operation.
type artifactLockOwner struct {
	OwnerID string `json:"ownerID"`
}

func (c *Config) validateArtifactLockOwner() error {
	if c.ArtifactUploadLockOwnerID != strings.TrimSpace(c.ArtifactUploadLockOwnerID) {
		return fmt.Errorf("artifact_upload_lock_owner_id must not contain surrounding whitespace")
	}
	if c.ArtifactUploadLockOwnerID == "" {
		return nil
	}
	body, err := json.Marshal(artifactLockOwner{OwnerID: c.ArtifactUploadLockOwnerID})
	if err != nil {
		return fmt.Errorf("cannot encode artifact_upload_lock_owner_id: %w", err)
	}
	// Retries reject larger lock bodies, so check the JSON size before creating one.
	if len(body) > maxArtifactLockOwnerBytes {
		return fmt.Errorf("artifact_upload_lock_owner_id must produce a JSON lock body of at most %d bytes", maxArtifactLockOwnerBytes)
	}
	return nil
}

func (r *ReplicaAgent) artifactUploadLockBody() (string, error) {
	if err := r.Config.validateArtifactLockOwner(); err != nil {
		return "", err
	}
	if r.Config.ArtifactUploadLockOwnerID == "" {
		return constants.ArtifactUploadLockBody, nil
	}
	body, err := json.Marshal(artifactLockOwner{OwnerID: r.Config.ArtifactUploadLockOwnerID})
	return string(body), err
}

// A retry with the same owner ID keeps the existing lock. Deleting and reacquiring
// it would let another owner take it between those steps. The consumer must ensure
// the previous uploader has stopped before retrying with the same ID.
func (r *ReplicaAgent) reuseOwnedUploadLock() *targetArtifactUploadLock {
	if r.Config.ArtifactUploadLockOwnerID == "" {
		return nil
	}
	response, err := r.Config.Target.OCIOSDataStore.GetObject(r.targetArtifactUploadLockURI())
	if err != nil {
		r.Logger.Warnf("Cannot read upload lock owner; will check again while waiting: %v", err)
		return nil
	}
	defer response.Content.Close()
	// Keep the ETag from the same response as the owner. Cleanup must not delete
	// a different owner's lock if ownership changes before this upload finishes.
	if response.ETag == nil || *response.ETag == "" {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Content, maxArtifactLockOwnerBytes+1))
	if err != nil || len(body) > maxArtifactLockOwnerBytes {
		return nil
	}
	var owner artifactLockOwner
	if json.Unmarshal(body, &owner) != nil || owner.OwnerID != r.Config.ArtifactUploadLockOwnerID {
		// Other owners and locks without an owner ID keep the existing wait/timeout behavior.
		return nil
	}
	r.Logger.Infof("Continuing replication with the existing upload lock for owner %s", owner.OwnerID)
	return &targetArtifactUploadLock{ETag: *response.ETag}
}
