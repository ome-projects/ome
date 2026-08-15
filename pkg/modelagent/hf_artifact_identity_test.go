package modelagent

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const testHFCommitSHA = "b968826d9c46dd6066d109eabc6255188de91218"

func TestNewHfArtifactIdentity(t *testing.T) {
	identity, err := newHfArtifactIdentity(" Qwen/Qwen3-8B ", strings.ToUpper(testHFCommitSHA))
	require.NoError(t, err)
	assert.Equal(t, HfArtifactIdentity{
		ModelID:   "Qwen/Qwen3-8B",
		CommitSHA: testHFCommitSHA,
	}, identity)
}

func TestNewHfArtifactIdentityRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		modelID string
		sha     string
	}{
		{name: "empty model id", sha: testHFCommitSHA},
		{name: "path traversal", modelID: "Qwen/../Qwen3-8B", sha: testHFCommitSHA},
		{name: "too many segments", modelID: "org/team/model", sha: testHFCommitSHA},
		{name: "repository id too long", modelID: "org/" + strings.Repeat("m", 93), sha: testHFCommitSHA},
		{name: "short sha", modelID: "Qwen/Qwen3-8B", sha: "abc123"},
		{name: "non hexadecimal sha", modelID: "Qwen/Qwen3-8B", sha: strings.Repeat("z", 40)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newHfArtifactIdentity(tt.modelID, tt.sha)
			assert.Error(t, err)
		})
	}
}

func TestHfArtifactIdentityFromTask(t *testing.T) {
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		hfModelIDAnnotationKey: "Qwen/Qwen3-8B",
		hfSHAAnnotationKey:     testHFCommitSHA,
	}}}

	identity, ok := hfArtifactIdentityFromTask(&GopherTask{BaseModel: model})
	require.True(t, ok)
	assert.Equal(t, "Qwen/Qwen3-8B", identity.ModelID)
	assert.Equal(t, testHFCommitSHA, identity.CommitSHA)
}

func TestHfArtifactIdentityFromClusterBaseModelTask(t *testing.T) {
	model := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		hfModelIDAnnotationKey: "Qwen/Qwen3-8B",
		hfSHAAnnotationKey:     testHFCommitSHA,
	}}}

	identity, ok := hfArtifactIdentityFromTask(&GopherTask{ClusterBaseModel: model})

	require.True(t, ok)
	assert.Equal(t, "Qwen/Qwen3-8B", identity.ModelID)
	assert.Equal(t, testHFCommitSHA, identity.CommitSHA)
}

func TestHfArtifactIdentityFromTaskRequiresCompleteIdentity(t *testing.T) {
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		hfModelIDAnnotationKey: "Qwen/Qwen3-8B",
	}}}

	_, ok := hfArtifactIdentityFromTask(&GopherTask{BaseModel: model})
	assert.False(t, ok)
}

func TestHfArtifactConfigMapKeyAndCanonicalPath(t *testing.T) {
	identity, err := newHfArtifactIdentity("Qwen/Qwen3-8B", testHFCommitSHA)
	require.NoError(t, err)

	key := hfArtifactConfigMapKey(identity)
	assert.True(t, strings.HasPrefix(key, "artifact.huggingface.Qwen.Qwen3-8B."))
	assert.True(t, strings.HasSuffix(key, "."+testHFCommitSHA))

	path := canonicalHfArtifactPath("/models/store/model-1", identity)
	assert.Equal(t, filepath.Join(
		"/models/store",
		"_artifacts",
		"Qwen",
		"Qwen3-8B",
		testHFCommitSHA,
	), path)
}

func TestHfArtifactConfigMapKeyAvoidsSanitizationCollision(t *testing.T) {
	first, err := newHfArtifactIdentity("org/model_name", testHFCommitSHA)
	require.NoError(t, err)
	second, err := newHfArtifactIdentity("org/model.name", testHFCommitSHA)
	require.NoError(t, err)

	assert.NotEqual(t, hfArtifactConfigMapKey(first), hfArtifactConfigMapKey(second))
}
