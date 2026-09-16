package modelagent

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	"sigs.k8s.io/ome/pkg/constants"
)

const (
	hfModelIDAnnotationKey = "hf-model-id"
	hfSHAAnnotationKey     = "hf-model-sha"
)

func newHfArtifactIdentity(modelID, commitSHA string) (HfArtifactIdentity, error) {
	identity := HfArtifactIdentity{
		ModelID:   strings.TrimSpace(modelID),
		CommitSHA: strings.ToLower(strings.TrimSpace(commitSHA)),
	}
	if !isValidHfArtifactIdentity(identity) {
		return HfArtifactIdentity{}, fmt.Errorf("invalid Hugging Face artifact identity %q@%q", modelID, commitSHA)
	}
	return identity, nil
}

func isValidHfArtifactIdentity(identity HfArtifactIdentity) bool {
	return isValidHfModelID(identity.ModelID) &&
		isValidHfCommitSHA(identity.CommitSHA)
}

func isValidHfCommitSHA(sha string) bool {
	if len(sha) != 40 {
		return false
	}
	for _, character := range sha {
		if !strings.ContainsRune("0123456789abcdefABCDEF", character) {
			return false
		}
	}
	return true
}

func isValidHfModelID(modelID string) bool {
	modelID = strings.TrimSpace(modelID)
	if modelID == "" || len(modelID) > 96 || strings.HasPrefix(modelID, "/") || strings.HasSuffix(modelID, "/") {
		return false
	}
	if strings.Contains(modelID, "\\") || strings.Contains(modelID, "//") || strings.Contains(modelID, "..") || strings.Contains(modelID, "--") || strings.HasSuffix(modelID, ".git") {
		return false
	}
	parts := strings.Split(modelID, "/")
	if len(parts) > 2 {
		return false
	}
	for _, part := range parts {
		if !isValidHfModelIDSegment(part) {
			return false
		}
	}
	return true
}

func isValidHfModelIDSegment(segment string) bool {
	if segment == "" || segment == "." || segment == ".." || len(segment) > 96 {
		return false
	}
	if strings.HasPrefix(segment, ".") || strings.HasPrefix(segment, "-") || strings.HasSuffix(segment, ".") || strings.HasSuffix(segment, "-") {
		return false
	}
	for _, character := range segment {
		valid := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.'
		if !valid {
			return false
		}
	}
	return true
}

func hfArtifactIdentityFromTask(task *GopherTask) (HfArtifactIdentity, bool) {
	if task == nil {
		return HfArtifactIdentity{}, false
	}
	if task.BaseModel != nil {
		return hfArtifactIdentityFromAnnotations(task.BaseModel.Annotations)
	}
	if task.ClusterBaseModel != nil {
		return hfArtifactIdentityFromAnnotations(task.ClusterBaseModel.Annotations)
	}
	return HfArtifactIdentity{}, false
}

// For annotation-based artifact reuse, the Model CR producer resolves the
// Hugging Face revision. This helper only validates the model ID and commit SHA
// before using them in artifact keys and paths; it performs no remote check.
func hfArtifactIdentityFromAnnotations(annotations map[string]string) (HfArtifactIdentity, bool) {
	identity, err := newHfArtifactIdentity(
		annotations[hfModelIDAnnotationKey],
		annotations[hfSHAAnnotationKey],
	)
	return identity, err == nil
}

func hfArtifactConfigMapKey(identity HfArtifactIdentity) string {
	return constants.HfArtifactConfigMapKeyPrefix +
		sanitizeConfigMapKeyComponent(identity.ModelID) + "." +
		shortConfigMapKeyHash(identity.ModelID) + "." +
		strings.ToLower(identity.CommitSHA)
}

func isHfArtifactConfigMapKey(key string) bool {
	return strings.HasPrefix(key, constants.HfArtifactConfigMapKeyPrefix)
}

func shortConfigMapKeyHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])[:12]
}

func sanitizeConfigMapKeyComponent(value string) string {
	var result strings.Builder
	for _, character := range strings.TrimSpace(value) {
		valid := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.'
		if valid {
			result.WriteRune(character)
		} else {
			result.WriteByte('.')
		}
	}
	sanitized := strings.Trim(result.String(), ".")
	if sanitized == "" {
		return "unknown"
	}
	return sanitized
}

// canonicalHfArtifactPath returns the shared artifact directory for a model path.
// The model ID and commit SHA distinguish parents for different artifact identities.
func canonicalHfArtifactPath(modelPath string, identity HfArtifactIdentity) string {
	return filepath.Join(
		filepath.Dir(modelPath),
		constants.ModelArtifactsDirectory,
		filepath.FromSlash(identity.ModelID),
		strings.ToLower(identity.CommitSHA),
	)
}
