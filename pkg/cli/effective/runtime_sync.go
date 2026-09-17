package effective

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/actionbounds"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

var ErrRuntimeSyncEvidence = errors.New("action refused: managed runtime sync safety evidence is unavailable or inconsistent")
var syncIdentity = regexp.MustCompile(`^[A-Za-z0-9_.:/@+-]{1,256}$`)

// RuntimeSyncEvidence is an action-grade private snapshot, never runtime content.
type RuntimeSyncEvidence struct {
	parentDigest  string
	snapshot      string
	oldAnnotation string
	oldStatus     string
	targetHash    string
	rows          [][2]string
	native        []string
	valid         bool
}

func (RuntimeSyncEvidence) MarshalJSON() ([]byte, error) { return nil, ErrUnsafeRuntimeSerialization }
func (RuntimeSyncEvidence) MarshalYAML() (any, error)    { return nil, ErrUnsafeRuntimeSerialization }
func (RuntimeSyncEvidence) String() string               { return "<effective.RuntimeSyncEvidence redacted>" }
func (RuntimeSyncEvidence) GoString() string             { return "<effective.RuntimeSyncEvidence redacted>" }
func (e RuntimeSyncEvidence) SameSnapshot(other RuntimeSyncEvidence) bool {
	return e.valid && other.valid && e.snapshot == other.snapshot && e.parentDigest == other.parentDigest
}
func (e RuntimeSyncEvidence) MatchesInferenceService(v *v1beta1.InferenceService) bool {
	return e.valid && v != nil && actionbounds.PrivatePayload(v) && e.parentDigest == syncDigest(v)
}
func (e RuntimeSyncEvidence) PreviewRows() [][2]string   { return append([][2]string{}, e.rows...) }
func (e RuntimeSyncEvidence) NativeComponents() []string { return append([]string{}, e.native...) }
func (e RuntimeSyncEvidence) TargetHash() string {
	if !e.valid {
		return ""
	}
	return e.targetHash
}
func (e RuntimeSyncEvidence) TokenAvailable(value string) bool {
	return e.valid && value != "" && value != e.oldAnnotation && value != e.oldStatus
}

func syncDigest(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func prepareRuntimeSyncEvidence(v *v1beta1.InferenceService, s *RuntimeState, target RuntimeRevisionObservation, targetAbsent bool, reads map[string]string) (RuntimeSyncEvidence, error) {
	refuse := func() (RuntimeSyncEvidence, error) { return RuntimeSyncEvidence{}, ErrRuntimeSyncEvidence }
	if v == nil || s == nil || !actionbounds.PrivatePayload(v) || !s.MatchesInferenceService(v) || v.Generation <= 0 || s.Generation != v.Generation {
		return refuse()
	}
	ref := v.Spec.Runtime
	if ref == nil || ref.Name == "" || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || ref.AutoSync == nil || *ref.AutoSync || ref.Revision != nil && *ref.Revision != "" || !validDeclaredRuntimeKind(ref.Kind) || ref.APIGroup != nil && *ref.APIGroup != "ome.io" {
		return refuse()
	}
	if s.PinMode != RuntimePinModeManagedPin || s.PinState != RuntimePinStateResolved || s.ActiveRevisionName == "" || s.ActiveRevisionName != v.Status.PinnedRevisionName || s.ReportedRevisionName != v.Status.PinnedRevisionName || s.RequestedRevisionName != "" || s.active == nil || s.active.Origin != ConfigurationOriginControllerRevision || s.active.Consistency != RevisionConsistencyConsistent {
		return refuse()
	}
	if s.DriftState != RuntimeDriftStateReportedTrue || s.DriftReason != RuntimeDriftReasonRevisionMismatch || s.LiveToActive != RuntimeHashRelationDifferent || s.liveAvailability != liveAvailable || len(s.issues) != 0 || !s.HistoryRequested || !s.HistoryComplete || s.HistoryTruncated || len(s.revisions) > 32 {
		return refuse()
	}
	if deriveSyncTokenState(v.Annotations[constants.RuntimeSyncAnnotationKey], v.Status.LastRuntimeSyncToken) == SyncTokenStatePending {
		return refuse()
	}
	live := s.live
	if live == nil || live.Runtime.spec == nil || live.Runtime.spec.IsDisabled() || !live.Runtime.IdentityObserved || live.Runtime.Generation <= 0 || live.Runtime.Name != ref.Name || live.Runtime.SelectionSource != RuntimeExplicit || live.Runtime.PinSource.Validate() != nil || live.Runtime.PinSource.State != RuntimePinSourceResolved || live.Runtime.PinSource.Kind != s.DeclaredSourceKind || live.Runtime.PinSource.Namespace != s.DeclaredSourceNamespace {
		return refuse()
	}
	chain := live.Runtime.DeclaredInheritance.chain
	if live.Runtime.DeclaredInheritance.Validate() != nil || live.Runtime.DeclaredInheritance.state != InheritanceObserved || len(chain) == 0 || len(chain) > constants.RuntimeInheritMaxDepth {
		return refuse()
	}
	rows := [][2]string{{"Generation", strconv.FormatInt(v.Generation, 10)}, {"Reported gen", strconv.FormatInt(v.Status.ObservedGeneration, 10)}, {"Freshness", "Unverifiable (advisory global status)"}, {"Pin", s.ActiveRevisionName}, {"Pin source", s.DeclaredSourceKind + "/" + s.DeclaredSourceNamespace + "/" + ref.Name}}
	seen := map[string]bool{}
	for _, source := range chain {
		key := source.Kind + "/" + source.Namespace + "/" + source.Name
		if source.APIVersion != "ome.io/v1beta1" || source.Kind != "ServingRuntime" && source.Kind != "ClusterServingRuntime" || !syncIdentity.MatchString(source.Name) || len(validation.IsDNS1123Subdomain(source.Name)) != 0 || !syncIdentity.MatchString(string(source.UID)) || !syncIdentity.MatchString(source.ResourceVersion) || source.Generation <= 0 || source.Kind == "ClusterServingRuntime" && source.Namespace != "" || source.Kind == "ServingRuntime" && (source.Namespace != v.Namespace || len(validation.IsDNS1123Label(source.Namespace)) != 0) || seen[key] {
			return refuse()
		}
		seen[key] = true
		rows = append(rows, [2]string{"Source", key}, [2]string{"Source UID", string(source.UID)}, [2]string{"Source RV", source.ResourceVersion}, [2]string{"Source gen", strconv.FormatInt(source.Generation, 10)})
	}
	leaf := chain[len(chain)-1]
	if leaf.Kind != live.Runtime.Kind || leaf.Namespace != live.Runtime.Namespace || leaf.Name != live.Runtime.Name || string(leaf.UID) != live.Runtime.UID || leaf.ResourceVersion != live.Runtime.resourceVersion || leaf.Generation != live.Runtime.Generation {
		return refuse()
	}
	full, short, err := runtimerevision.Hash(live.Runtime.spec)
	if err != nil || !validShortHash(short) || s.LiveShortHash != short {
		return refuse()
	}
	activeFull, activeShort, err := runtimerevision.Hash(s.active.spec)
	if err != nil || full == activeFull || short == activeShort {
		return refuse()
	}
	expected := runtimerevision.Name(runtimerevision.SourceKind(s.DeclaredSourceKind), s.DeclaredSourceNamespace, ref.Name, short)
	targetCount := 0
	activeSeen := false
	for _, revision := range s.revisions {
		if !revision.objectReturned || revision.Consistency != RevisionConsistencyConsistent || revision.Disabled || !syncIdentity.MatchString(revision.UID) || !syncIdentity.MatchString(revision.ResourceVersion) || revision.Namespace != s.historyNamespace {
			return refuse()
		}
		if revision.Name == s.ActiveRevisionName {
			if !slices.Contains(revision.roles, RuntimeRevisionRoleHistory) {
				return refuse()
			}
			if revision.fullHash != activeFull || revision.SourceKind != s.DeclaredSourceKind || revision.SourceNamespace != s.DeclaredSourceNamespace || revision.SourceName != ref.Name {
				return refuse()
			}
			activeSeen = true
			rows = append(rows, [2]string{"Pin UID", revision.UID}, [2]string{"Pin RV", revision.ResourceVersion})
		}
		// The real writer filters only source NAME + SHORT hash, in OME namespace.
		if revision.rawShortHash == short {
			targetCount++
			if revision.Name != expected || revision.fullHash != full || revision.SourceKind != s.DeclaredSourceKind || revision.SourceNamespace != s.DeclaredSourceNamespace || revision.SourceName != ref.Name {
				return refuse()
			}
		}
	}
	if !activeSeen || targetCount > 1 {
		return refuse()
	}
	if targetAbsent {
		if targetCount != 0 || target.objectReturned {
			return refuse()
		}
	} else {
		if !target.objectReturned || target.Name != expected || target.Namespace != s.historyNamespace || target.Consistency != RevisionConsistencyConsistent || target.Disabled || target.fullHash != full || target.SourceName != ref.Name || target.SourceKind != s.DeclaredSourceKind || target.SourceNamespace != s.DeclaredSourceNamespace || !syncIdentity.MatchString(target.UID) || !syncIdentity.MatchString(target.ResourceVersion) {
			return refuse()
		}
		// LIST and exact predicted-key GET must agree, not just share a digest.
		if targetCount != 1 {
			return refuse()
		}
		for _, revision := range s.revisions {
			if revision.Name == expected && (revision.objectFingerprint != target.objectFingerprint || revision.UID != target.UID || revision.ResourceVersion != target.ResourceVersion) {
				return refuse()
			}
		}
	}
	native := []string{}
	if len(s.active.components) == 0 || len(s.active.components) > 3 || len(live.Components) == 0 || len(live.Components) > 3 {
		return refuse()
	}
	for _, components := range [][]EffectiveComponent{s.active.components, live.Components} {
		componentSeen := map[v1beta1.ComponentType]bool{}
		for _, component := range components {
			if componentSeen[component.Type] || component.Type != v1beta1.EngineComponent && component.Type != v1beta1.DecoderComponent && component.Type != v1beta1.RouterComponent {
				return refuse()
			}
			componentSeen[component.Type] = true
			if !component.DeploymentMode.IsValid() {
				return refuse()
			}
		}
	}
	for _, component := range s.active.components {
		if component.DeploymentMode == constants.OMENative {
			native = append(native, string(component.Type))
		}
	}
	stateName := "Existing"
	if targetAbsent {
		stateName = "NotFound"
	}
	rows = append(rows, [2]string{"Current hash", activeShort}, [2]string{"Target hash", short}, [2]string{"Predicted pin", expected}, [2]string{"Target evidence", stateName}, [2]string{"Old sync token", string(s.SyncTokenState)})
	if !targetAbsent {
		rows = append(rows, [2]string{"Target UID", target.UID}, [2]string{"Target RV", target.ResourceVersion})
	}
	digest := syncDigest(v)
	snapshot := syncDigest(struct {
		Parent         string
		Reads          map[string]string
		Target, Active string
		Absent         bool
	}{digest, reads, full, activeFull, targetAbsent})
	if digest == "" || snapshot == "" {
		return refuse()
	}
	return RuntimeSyncEvidence{valid: true, parentDigest: digest, snapshot: snapshot, oldAnnotation: v.Annotations[constants.RuntimeSyncAnnotationKey], oldStatus: v.Status.LastRuntimeSyncToken, targetHash: short, rows: rows, native: native}, nil
}
