package storage

import "testing"

func TestParseModelPackStorageURI(t *testing.T) {
	// The whole remainder is the reference: a repository name may contain
	// slashes, so there is nothing further to split.
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{"tag", "modelpack://ghcr.io/org/model:tag", "ghcr.io/org/model:tag"},
		{"no tag", "modelpack://ghcr.io/org/model", "ghcr.io/org/model"},
		{"port and digest", "modelpack://registry.example.com:5000/a/b/c@sha256:abc", "registry.example.com:5000/a/b/c@sha256:abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseModelPackStorageURI(tt.uri)
			if err != nil {
				t.Fatal(err)
			}
			if got.Reference != tt.want {
				t.Fatalf("got %q, want %q", got.Reference, tt.want)
			}
		})
	}
}

func TestParseModelPackStorageURIRejectsBadInput(t *testing.T) {
	for _, uri := range []string{
		"ghcr.io/org/model:tag", // no scheme
		"oci://ghcr.io/org/model:tag",
		"modelpack://", // empty reference
		"modelpack://   ",
		"modelpack://model",    // no repository component
		"modelpack://ghcr.io/", // empty repository
		"modelpack:///model",   // empty registry
	} {
		if _, err := ParseModelPackStorageURI(uri); err == nil {
			t.Errorf("expected an error for %q", uri)
		}
	}
}

func TestModelPackIsDistinctFromObjectStorage(t *testing.T) {
	cases := map[string]StorageType{
		"modelpack://ghcr.io/org/model:tag": StorageTypeModelPack,
		"oci://n/ns/b/bucket/o/path":        StorageTypeOCI,
	}
	for uri, want := range cases {
		got, err := GetStorageType(uri)
		if err != nil {
			t.Errorf("%s: %v", uri, err)
			continue
		}
		if got != want {
			t.Errorf("GetStorageType(%q) = %q, want %q", uri, got, want)
		}
	}
}

func TestValidateStorageURIModelPack(t *testing.T) {
	if err := ValidateStorageURI("modelpack://ghcr.io/org/model:tag"); err != nil {
		t.Errorf("ModelPack URI should validate: %v", err)
	}
	if err := ValidateStorageURI("modelpack://model"); err == nil {
		t.Error("a ModelPack URI without a repository should fail")
	}
	if err := ValidateStorageURI("oci://n/ns/b/bucket/o/path"); err != nil {
		t.Errorf("Object Storage URI should be unchanged: %v", err)
	}
}
