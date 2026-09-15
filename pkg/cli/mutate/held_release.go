package mutate

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

// HeldReleasePlan owns one IR CAS request and private preview bindings.
type HeldReleasePlan struct {
	patch                     []byte
	target                    reportv1alpha1.ActionTarget
	evidence                  HeldReleaseEvidence
	active                    *effective.ActiveConfiguration
	live                      *effective.LiveConfiguration
	pinMode                   effective.RuntimePinMode
	component, revision, hash string
	block                     v1beta1.RetryBlock
}

func (p HeldReleasePlan) Patch() []byte                       { return append([]byte{}, p.patch...) }
func (p HeldReleasePlan) Target() reportv1alpha1.ActionTarget { return p.target }
func (p HeldReleasePlan) RevisionHash() string                { return p.hash }
func (HeldReleasePlan) String() string                        { return "<HeldReleasePlan redacted>" }
func (HeldReleasePlan) GoString() string                      { return "<HeldReleasePlan redacted>" }

// PrepareHeldRelease selects only after complete private source admission.
func PrepareHeldRelease(parent *v1beta1.InferenceService, state *effective.RuntimeState, evidence HeldReleaseEvidence, component, revision string, clock reportv1alpha1.Clock) (HeldReleasePlan, error) {
	if err := ValidateTarget(parent); err != nil {
		return HeldReleasePlan{}, err
	}
	components, err := RequireNativeRuntime(parent, state)
	if err != nil {
		return HeldReleasePlan{}, err
	}
	if !slices.Contains(components, component) || !SafeScalar(revision) || evidence.parent == nil || evidence.parent.UID != parent.UID || evidence.parent.ResourceVersion != parent.ResourceVersion || evidence.parent.Generation != parent.Generation || evidence.selected < 0 || evidence.selected >= len(evidence.items) {
		return HeldReleasePlan{}, ErrHeldRelease
	}
	ir := &evidence.items[evidence.selected]
	if string(ir.Spec.Component) != component {
		return HeldReleasePlan{}, ErrHeldRelease
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	if err = validateHeldBlocks(ir, parent, clock.Now()); err != nil {
		return HeldReleasePlan{}, err
	}
	matched := -1
	for i, block := range ir.Status.RetryBlocks {
		if block.TargetRevision == revision || revisionHash.MatchString(revision) && strings.HasSuffix(block.TargetRevision, "-"+revision) {
			if matched >= 0 {
				return HeldReleasePlan{}, ErrHeldRelease
			}
			matched = i
		}
	}
	if matched < 0 || ir.Status.RetryBlocks[matched].State != v1beta1.RetryBlockHeld {
		return HeldReleasePlan{}, ErrHeldRelease
	}
	block := ir.Status.RetryBlocks[matched]
	active, err := state.RequireActive()
	if err != nil {
		return HeldReleasePlan{}, ErrRuntime
	}
	p := HeldReleasePlan{target: reportv1alpha1.ActionTarget{Kind: "InferenceReplica", Namespace: ir.Namespace, Name: ir.Name, UID: string(ir.UID), ResourceVersion: ir.ResourceVersion}, evidence: evidence, active: active, live: state.LiveConfiguration(), pinMode: state.PinMode, component: component, revision: block.TargetRevision, hash: block.TargetRevision[len(block.TargetRevision)-8:], block: block}
	p.patch, err = json.Marshal([]patchOperation{{Op: "test", Path: "/metadata/uid", Value: p.target.UID}, {Op: "test", Path: "/metadata/resourceVersion", Value: p.target.ResourceVersion}, {Op: "add", Path: "/metadata/annotations/ome.io~1release-held-revision", Value: p.revision}})
	if err != nil {
		return HeldReleasePlan{}, errors.New("encode held-release patch failed")
	}
	return p, nil
}

// MatchesRefresh compares current private originals without global parent
// observedGeneration becoming a freshness gate or acknowledgement.
func (p HeldReleasePlan) MatchesRefresh(parent *v1beta1.InferenceService, state *effective.RuntimeState, evidence HeldReleaseEvidence) bool {
	if parent == nil || p.evidence.parent == nil || parent.UID != p.evidence.parent.UID || parent.ResourceVersion != p.evidence.parent.ResourceVersion || parent.Generation != p.evidence.parent.Generation {
		return false
	}
	if _, err := RequireNativeRuntime(parent, state); err != nil {
		return false
	}
	active, err := state.RequireActive()
	return err == nil && state.PinMode == p.pinMode && reflect.DeepEqual(active, p.active) && reflect.DeepEqual(state.LiveConfiguration(), p.live) && reflect.DeepEqual(evidence.items, p.evidence.items) && evidence.selected == p.evidence.selected
}

func (p HeldReleasePlan) WritePreview(out io.Writer, contextName, omeNamespace string, dryRun reportv1alpha1.DryRunMode) error {
	if !SafeScalar(contextName) || !SafeScalar(omeNamespace) || p.evidence.parent == nil || p.target.UID == "" {
		return errors.New("held-release preview identity is unavailable or unsafe")
	}
	parent := p.evidence.parent
	ir := p.evidence.items[p.evidence.selected]
	rows := [][]string{}
	add := func(field, value string) {
		if value == "" {
			value = "<absent>"
		}
		for len(value) > 54 {
			rows = append(rows, []string{field, value[:54]})
			value = value[54:]
			field = "(continued)"
		}
		rows = append(rows, []string{field, value})
	}
	add("Action", "Alpha instance release-held")
	add("Context", contextName)
	add("Workload NS", p.target.Namespace)
	add("OME NS", omeNamespace)
	add("Parent", "InferenceService/"+parent.Name)
	add("Parent UID", string(parent.UID))
	add("Parent RV", parent.ResourceVersion)
	add("Parent generation", strconv.FormatInt(parent.Generation, 10))
	add("Global parent status", strconv.FormatInt(parent.Status.ObservedGeneration, 10)+" advisory Unverifiable; not acknowledgement")
	add("Component", p.component)
	add("Target", p.target.Kind+"/"+p.target.Name)
	add("IR UID", p.target.UID)
	add("IR RV", p.target.ResourceVersion)
	add("IR generation", strconv.FormatInt(ir.Generation, 10))
	add("Native observed", strconv.FormatInt(ir.Status.ObservedGeneration, 10))
	add("Parent stamp", ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey])
	add("Revision", p.revision)
	add("Hash", p.hash)
	add("State", "Held")
	add("Attempts", strconv.FormatInt(int64(p.block.AttemptsStarted), 10))
	add("Dry-run", string(dryRun))
	pauseDepth := "none"
	if paused, freeze := constants.RolloutPauseState(parent.Annotations); freeze {
		pauseDepth = "freeze"
	} else if paused {
		pauseDepth = "true"
	}
	add("Pause depth", pauseDepth)
	add("Membership", strconv.Itoa(len(p.evidence.items))+" IRs / "+strconv.Itoa(p.evidence.pages)+" pages")
	add("Test UID", p.target.UID)
	add("Test RV", p.target.ResourceVersion)
	add("Add annotation", constants.ReleaseHeldRevisionAnnotationKey)
	add("Exact value", p.revision)
	add("Controller stamp", "existing controller-write=true preserved")
	add("Pause effect", "Mailbox may be consumed; release does not resume work")
	add("Held check", "Actor initial snapshot; fresh removal does not recheck")
	add("Consumption", "Fresh delete is not handled value/UID compare-and-delete")
	add("Later observation", "Both absent on original UID is consistent, not attribution")
	rows = append(rows, []string{"Scope", "One IR release request; no spec/status/scale mutation"}, []string{"Acceptance", "Not release, retry, garbage collection or availability"}, []string{"Race", "IR CAS is not a parent/runtime/membership transaction"}, []string{"Actor limits", "Consumption has no durable request-correlated receipt"}, []string{"Actor retention", "Historical retry records may be retention-pruned"})
	if err := (report.Table{Headers: []string{"HELD RELEASE PREVIEW", "VALUE"}, Rows: rows}).Write(out); err != nil {
		return errors.New("write held-release preview failed")
	}
	return nil
}
