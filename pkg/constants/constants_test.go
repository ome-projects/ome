package constants

import (
	"strings"
	"testing"
)

func TestLWSNameTruncates(t *testing.T) {
	tests := []struct {
		name              string
		componentName     string
		expectedTruncated bool
	}{
		{
			name:              "short component name",
			componentName:     "my-model-engine",
			expectedTruncated: false,
		},
		{
			name:              "actual long engine component name",
			componentName:     "amaaaaaabgjpxjqamiuior4qamufon2clgneukbxomingadlfcsgq67sicoa-engine",
			expectedTruncated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				maxLabelValueLength        = 63
				maxLWSNameLength           = 35
				lwsNamePrefix              = "lws-"
				workerStatefulSetSuffix    = "-0"
				statefulSetRevisionHash    = "75965d8c9f"
				kueueLeaderWorkerSetPrefix = "leaderworkerset-"
				kueueWorkloadHashSuffix    = "-ece5c"
			)

			got := LWSName(tt.componentName)
			workerStatefulSetName := got + workerStatefulSetSuffix
			revisionLabelValue := workerStatefulSetName + "-" + statefulSetRevisionHash
			kueueWorkloadLabelValue := kueueLeaderWorkerSetPrefix + got + workerStatefulSetSuffix + kueueWorkloadHashSuffix
			expected := TruncateNameWithPrefix(tt.componentName, lwsNamePrefix, maxLWSNameLength)

			if len(got) > maxLWSNameLength {
				t.Fatalf("LWSName length = %d, want <= %d: %q", len(got), maxLWSNameLength, got)
			}
			if !strings.HasPrefix(got, lwsNamePrefix) {
				t.Fatalf("LWSName = %q, want prefix %q", got, lwsNamePrefix)
			}
			if tt.expectedTruncated && got == lwsNamePrefix+tt.componentName {
				t.Fatalf("LWSName = %q, want truncated name", got)
			}
			if len(revisionLabelValue) > maxLabelValueLength {
				t.Fatalf("StatefulSet revision label value length = %d, want <= %d: %q", len(revisionLabelValue), maxLabelValueLength, revisionLabelValue)
			}
			if len(kueueWorkloadLabelValue) > maxLabelValueLength {
				t.Fatalf("Kueue workload label value length = %d, want <= %d: %q", len(kueueWorkloadLabelValue), maxLabelValueLength, kueueWorkloadLabelValue)
			}
			if got != expected {
				t.Fatalf("LWSName = %q, want %q", got, expected)
			}
		})
	}
}

func TestIsArtifactCompleteMarkerObjectName(t *testing.T) {
	tests := []struct {
		name       string
		objectName string
		want       bool
	}{
		{
			name:       "marker at root",
			objectName: ArtifactCompleteMarkerFileName,
			want:       true,
		},
		{
			name:       "marker under model prefix",
			objectName: "customer-imported-basemodels/deepseek-ai/DeepSeek-V4-Pro/abc123/" + ArtifactCompleteMarkerFileName,
			want:       true,
		},
		{
			name:       "regular model file",
			objectName: "customer-imported-basemodels/deepseek-ai/DeepSeek-V4-Pro/abc123/config.json",
		},
		{
			name:       "similar suffix without path separator",
			objectName: "customer-imported-basemodels/deepseek-ai/DeepSeek-V4-Pro/abc123/not-" + ArtifactCompleteMarkerFileName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsArtifactCompleteMarkerObjectName(tt.objectName); got != tt.want {
				t.Fatalf("IsArtifactCompleteMarkerObjectName(%q) = %v, want %v", tt.objectName, got, tt.want)
			}
		})
	}
}

func TestIsArtifactUploadLockObjectName(t *testing.T) {
	tests := []struct {
		name       string
		objectName string
		want       bool
	}{
		{
			name:       "lock at root",
			objectName: ArtifactUploadLockFileName,
			want:       true,
		},
		{
			name:       "lock under model prefix",
			objectName: "customer-imported-basemodels/deepseek-ai/DeepSeek-V4-Pro/abc123/" + ArtifactUploadLockFileName,
			want:       true,
		},
		{
			name:       "regular model file",
			objectName: "customer-imported-basemodels/deepseek-ai/DeepSeek-V4-Pro/abc123/tokenizer.json",
		},
		{
			name:       "similar suffix without path separator",
			objectName: "customer-imported-basemodels/deepseek-ai/DeepSeek-V4-Pro/abc123/not-" + ArtifactUploadLockFileName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsArtifactUploadLockObjectName(tt.objectName); got != tt.want {
				t.Fatalf("IsArtifactUploadLockObjectName(%q) = %v, want %v", tt.objectName, got, tt.want)
			}
		})
	}
}

func TestHuggingFaceArtifactConstants(t *testing.T) {
	if HuggingFaceArtifactConfigMapKeyPrefix != "artifact.huggingface." {
		t.Fatalf("HuggingFaceArtifactConfigMapKeyPrefix = %q", HuggingFaceArtifactConfigMapKeyPrefix)
	}
	if HuggingFaceArtifactReadyMarkerFileName != ".ome-hf-artifact-ready" {
		t.Fatalf("HuggingFaceArtifactReadyMarkerFileName = %q", HuggingFaceArtifactReadyMarkerFileName)
	}
}
