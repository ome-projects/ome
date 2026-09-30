package render

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// isPVCBaseModel reports whether p's referenced BaseModel is PVC-backed.
func isPVCBaseModel(p *Piece) bool {
	if p == nil || p.BaseModel == nil {
		return false
	}
	return isPVCStorage(p.BaseModel.Storage)
}

// parsePVCComponents returns the parsed PVC URI for p's BaseModel. Returns
// nil if the model is not PVC-backed or the URI is malformed; the caller
// is expected to have validated the model upstream (via the BaseModel
// reconciler / admission webhook), so a parse error here is treated as
// "no PVC" rather than fatal.
func parsePVCComponents(p *Piece) *storage.PVCStorageComponents {
	if !isPVCBaseModel(p) {
		return nil
	}
	return parsePVCStorage(p.BaseModel.Storage)
}

// isPVCStorage / parsePVCStorage: Storage-spec-level forms used by the
// overlay helpers, which have no Piece.
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
