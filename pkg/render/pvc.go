package render

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// isPVCStorage reports whether the storage spec names a PersistentVolumeClaim.
func isPVCStorage(s *v1beta1.StorageSpec) bool {
	if s == nil || s.StorageUri == nil {
		return false
	}
	t, err := storage.GetStorageType(*s.StorageUri)
	if err != nil {
		return false
	}
	return t == storage.StorageTypePVC
}

// parsePVCStorage returns the claim a PVC storage URI names, or nil when the
// spec names no claim or the URI does not parse. Admission and the BaseModel
// reconciler validate the URI upstream, so a parse failure here means "no
// claim", not an error.
func parsePVCStorage(s *v1beta1.StorageSpec) *storage.PVCStorageComponents {
	if !isPVCStorage(s) {
		return nil
	}
	components, err := storage.ParsePVCStorageURI(*s.StorageUri)
	if err != nil {
		return nil
	}
	return components
}
