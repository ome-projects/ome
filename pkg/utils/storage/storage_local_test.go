package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

func TestParseLocalStorageURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		want        *LocalStorageComponents
		wantErr     bool
		errContains string
	}{
		{
			name: "valid uri with absolute path",
			uri:  "local:///opt/models/llama",
			want: &LocalStorageComponents{
				Path: "/opt/models/llama",
			},
			wantErr: false,
		},
		{
			name: "valid uri with relative path",
			uri:  "local://./models/gpt",
			want: &LocalStorageComponents{
				Path: "./models/gpt",
			},
			wantErr: false,
		},
		{
			name: "valid uri with home directory path",
			uri:  "local://~/models/bert",
			want: &LocalStorageComponents{
				Path: "~/models/bert",
			},
			wantErr: false,
		},
		{
			name: "valid uri with complex path",
			uri:  "local:///usr/local/share/models/language-models/v1.0",
			want: &LocalStorageComponents{
				Path: "/usr/local/share/models/language-models/v1.0",
			},
			wantErr: false,
		},
		{
			name:        "missing local prefix",
			uri:         "/opt/models/llama",
			wantErr:     true,
			errContains: "missing local:// prefix",
		},
		{
			name:        "empty uri",
			uri:         "",
			wantErr:     true,
			errContains: "missing local:// prefix",
		},
		{
			name:        "only prefix",
			uri:         "local://",
			wantErr:     true,
			errContains: "missing path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseLocalStorageURI(tt.uri)
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

func TestValidateLocalStorageURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{
			name:    "valid absolute path",
			uri:     "local:///opt/models/llama",
			wantErr: false,
		},
		{
			name:    "valid relative path",
			uri:     "local://./models/gpt",
			wantErr: false,
		},
		{
			name:    "invalid - missing prefix",
			uri:     "/opt/models/llama",
			wantErr: true,
		},
		{
			name:    "invalid - empty path",
			uri:     "local://",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatorFor(t, StorageTypeLocal)(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetStorageTypeWithLocal(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    StorageType
		wantErr bool
	}{
		{
			name:    "local storage",
			uri:     "local:///opt/models/llama",
			want:    StorageTypeLocal,
			wantErr: false,
		},
		{
			name:    "oci storage",
			uri:     "oci://n/myns/b/mybucket/o/mypath",
			want:    StorageTypeOCI,
			wantErr: false,
		},
		{
			name:    "pvc storage",
			uri:     "pvc://my-pvc/model",
			want:    StorageTypePVC,
			wantErr: false,
		},
		{
			name:    "huggingface storage",
			uri:     "hf://meta-llama/Llama-2-7b",
			want:    StorageTypeHuggingFace,
			wantErr: false,
		},
		{
			name:    "unknown storage",
			uri:     "unknown://something",
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetStorageType(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateStorageURIWithLocal(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{
			name:    "valid local storage",
			uri:     "local:///opt/models/llama",
			wantErr: false,
		},
		{
			name:    "valid oci storage",
			uri:     "oci://n/myns/b/mybucket/o/mypath",
			wantErr: false,
		},
		{
			name:    "invalid local storage",
			uri:     "local://",
			wantErr: true,
		},
		{
			name:    "unknown storage type",
			uri:     "unknown://something",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStorageURI(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewObjectURIWithLocal(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		expect      *ociobjectstore.ObjectURI
		wantErr     bool
		errContains string
	}{
		{
			name: "local storage absolute path",
			uri:  "local:///opt/models/llama",
			expect: &ociobjectstore.ObjectURI{
				Namespace: "local",
				Prefix:    "/opt/models/llama",
			},
			wantErr: false,
		},
		{
			name: "local storage relative path",
			uri:  "local://./models/gpt",
			expect: &ociobjectstore.ObjectURI{
				Namespace: "local",
				Prefix:    "./models/gpt",
			},
			wantErr: false,
		},
		{
			name:        "invalid local storage",
			uri:         "local://",
			wantErr:     true,
			errContains: "missing path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewObjectURI(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expect, got)
		})
	}
}
