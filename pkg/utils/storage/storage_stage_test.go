package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseStageStorageURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		want        *StageStorageComponents
		wantErr     bool
		errContains string
	}{
		{
			name: "valid uri with absolute path",
			uri:  "stage:///mnt/nfs-src/qwen/Qwen3-32B",
			want: &StageStorageComponents{
				SourcePath: "/mnt/nfs-src/qwen/Qwen3-32B",
			},
		},
		{
			name: "valid uri with trailing slash is normalized",
			uri:  "stage:///mnt/nfs-src/qwen/Qwen3-32B/",
			want: &StageStorageComponents{
				SourcePath: "/mnt/nfs-src/qwen/Qwen3-32B",
			},
		},
		{
			name:        "missing stage prefix",
			uri:         "/mnt/nfs-src/qwen",
			wantErr:     true,
			errContains: "missing stage:// prefix",
		},
		{
			name:        "empty uri",
			uri:         "",
			wantErr:     true,
			errContains: "missing stage:// prefix",
		},
		{
			name:        "only prefix",
			uri:         "stage://",
			wantErr:     true,
			errContains: "missing source path",
		},
		{
			name:        "relative path is rejected",
			uri:         "stage://models/qwen",
			wantErr:     true,
			errContains: "must be an absolute path",
		},
		{
			name:        "parent directory traversal is rejected",
			uri:         "stage:///mnt/nfs-src/../../etc",
			wantErr:     true,
			errContains: "must not contain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseStageStorageURI(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetStorageTypeStage(t *testing.T) {
	got, err := GetStorageType("stage:///mnt/nfs-src/qwen/Qwen3-32B")
	assert.NoError(t, err)
	assert.Equal(t, StorageTypeStage, got)
}

func TestValidateStorageURIDispatchesStage(t *testing.T) {
	assert.NoError(t, ValidateStorageURI("stage:///mnt/nfs-src/qwen/Qwen3-32B"))
	assert.Error(t, ValidateStorageURI("stage://relative/path"))
}
