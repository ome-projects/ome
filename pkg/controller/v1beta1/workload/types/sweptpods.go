package types

import k8stypes "k8s.io/apimachinery/pkg/types"

// SweptPods is the pass-scoped record of the pods the force-delete sweep
// removed this pass. The Component-wide sweep runs ahead of the verb
// passes on the same observation they read, so a pass that reaches one of
// those pods finds it here and issues no second delete for an object that
// is already gone. All methods are nil-safe.
type SweptPods struct {
	uids map[k8stypes.UID]struct{}
}

// Record notes that the pod with uid was force-deleted this pass.
func (s *SweptPods) Record(uid k8stypes.UID) {
	if s == nil {
		return
	}
	if s.uids == nil {
		s.uids = make(map[k8stypes.UID]struct{})
	}
	s.uids[uid] = struct{}{}
}

// Swept reports whether the pod with uid was force-deleted this pass.
func (s *SweptPods) Swept(uid k8stypes.UID) bool {
	if s == nil {
		return false
	}
	_, ok := s.uids[uid]
	return ok
}
