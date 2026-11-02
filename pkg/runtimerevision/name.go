package runtimerevision

import (
	"fmt"
	"strings"
)

// SourceKind identifies which runtime CRD a revision was captured
// from. Used as a label value and as the name-prefix discriminator.
type SourceKind string

const (
	KindClusterServingRuntime SourceKind = "ClusterServingRuntime"
	KindServingRuntime        SourceKind = "ServingRuntime"
)

// Name builds the revision's metadata.name from the source runtime's
// scope, namespace, name, plus the short content hash. All revisions
// live in the OME system namespace; the prefix and embedded
// source-namespace prevent collisions between
//
//	team-a/srt-foo  and  team-b/srt-foo
func Name(kind SourceKind, sourceNamespace, runtimeName, shortHash string) string {
	switch kind {
	case KindClusterServingRuntime:
		return fmt.Sprintf("cr-%s-%s", runtimeName, shortHash)
	case KindServingRuntime:
		return fmt.Sprintf("r-%s-%s-%s", sourceNamespace, runtimeName, shortHash)
	default:
		// Defensive: a typo'd SourceKind shouldn't silently produce
		// the cluster-scoped shape; surface it.
		return fmt.Sprintf("unknown-%s-%s-%s", sourceNamespace, runtimeName, shortHash)
	}
}

// MatchesRuntime reports whether revName conforms to the naming
// convention for a revision of the given (kind, sourceNamespace,
// runtimeName). Used by the ISVC admission validator to reject a
// spec.runtime.revision that obviously can't refer to the named runtime,
// without an API call.
//
// Validates the prefix shape and that the trailing shortHashLength
// characters are lowercase hex. Does NOT confirm existence — that's
// deferred to reconcile.
func MatchesRuntime(revName string, kind SourceKind, sourceNamespace, runtimeName string) bool {
	var prefix string
	switch kind {
	case KindClusterServingRuntime:
		prefix = fmt.Sprintf("cr-%s-", runtimeName)
	case KindServingRuntime:
		prefix = fmt.Sprintf("r-%s-%s-", sourceNamespace, runtimeName)
	default:
		return false
	}
	if !strings.HasPrefix(revName, prefix) {
		return false
	}
	suffix := revName[len(prefix):]
	return len(suffix) == shortHashLength && isLowerHex(suffix)
}

// isLowerHex reports whether every character of s is a lowercase hex digit.
func isLowerHex(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
