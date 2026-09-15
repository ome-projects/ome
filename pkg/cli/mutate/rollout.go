package mutate

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/canaryevidence"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

var (
	ErrPaused             = errors.New("action refused: service is already paused; freeze is preserved")
	ErrNotPaused          = errors.New("action refused: service has no recognized pause")
	ErrPending            = errors.New("action refused: a rollout promote or rollback mailbox is present")
	ErrStrongConfirmation = errors.New("discarding pending actions requires --yes")
	ErrUnsafeValue        = errors.New("action refused: a logical annotation value is unsafe to preview")
	ErrIdle               = errors.New("action refused: no applicable active rollout or lifecycle work was observed")
	ErrStale              = errors.New("action refused: controller safety evidence is stale or inconsistent")
	ErrRuntime            = errors.New("action refused: active runtime is unavailable, inconsistent or unbound")
)

type ReplicaEvidence struct {
	complete        bool
	active          bool
	operations      int
	migrations      int
	uid             string
	resourceVersion string
	sources         map[v1beta1.ComponentType]string
}
type RolloutPlan struct {
	patch         []byte
	target        reportv1alpha1.ActionTarget
	action        string
	components    []string
	previousPause string
	removed       []annotationValue
	work          ReplicaEvidence
	revisionHash  string
	canary        *canaryPreview
}
type annotationValue struct {
	key   string
	value string
}
type patchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

func (p RolloutPlan) Patch() []byte { return append([]byte{}, p.patch...) }

// RevisionHash identifies the exact primary canary target, not convergence.
func (p RolloutPlan) RevisionHash() string { return p.revisionHash }
func PrepareRollout(v *v1beta1.InferenceService, state *effective.RuntimeState, work ReplicaEvidence, action string, discard, yes bool, clock reportv1alpha1.Clock) (RolloutPlan, error) {
	if err := ValidateTarget(v); err != nil {
		return RolloutPlan{}, err
	}
	components, err := RequireNativeRuntime(v, state)
	if err != nil {
		return RolloutPlan{}, err
	}
	if action != "pause" && action != "resume" {
		return RolloutPlan{}, errors.New("unsupported rollout action")
	}
	if discard && (action != "resume" || !yes) {
		return RolloutPlan{}, ErrStrongConfirmation
	}
	paused, _ := constants.RolloutPauseState(v.Annotations)
	if action == "pause" && paused {
		return RolloutPlan{}, ErrPaused
	}
	if action == "resume" && !paused {
		return RolloutPlan{}, ErrNotPaused
	}
	p := RolloutPlan{target: reportv1alpha1.ActionTarget{Kind: "InferenceService", Namespace: v.Namespace, Name: v.Name, UID: string(v.UID), ResourceVersion: v.ResourceVersion}, action: action, components: components, previousPause: v.Annotations[constants.PausedRolloutAnnotation], work: work}
	for _, key := range []string{constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation} {
		value, exists := v.Annotations[key]
		if !exists {
			continue
		}
		if !discard {
			return RolloutPlan{}, ErrPending
		}
		if value != "" && !SafeScalar(value) {
			return RolloutPlan{}, ErrUnsafeValue
		}
		p.removed = append(p.removed, annotationValue{key: key, value: value})
	}
	if action == "pause" {
		if !work.complete {
			return RolloutPlan{}, ErrBounds
		}
		if work.uid != string(v.UID) || work.resourceVersion != v.ResourceVersion {
			return RolloutPlan{}, ErrStale
		}
		pinnedActive, err := pinnedWorkActive(v, work, clock)
		if err != nil {
			return RolloutPlan{}, err
		}
		if !work.active && !pinnedActive {
			return RolloutPlan{}, ErrIdle
		}
		if p.previousPause != "" && !SafeScalar(p.previousPause) {
			return RolloutPlan{}, ErrUnsafeValue
		}
	}
	patch := []patchOperation{{Op: "test", Path: "/metadata/uid", Value: string(v.UID)}, {Op: "test", Path: "/metadata/resourceVersion", Value: v.ResourceVersion}}
	path := func(key string) string {
		return "/metadata/annotations/" + strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
	}
	if action == "pause" {
		if v.Annotations == nil {
			patch = append(patch, patchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]string{}})
		}
		patch = append(patch, patchOperation{Op: "add", Path: path(constants.PausedRolloutAnnotation), Value: "true"})
	} else {
		patch = append(patch, patchOperation{Op: "remove", Path: path(constants.PausedRolloutAnnotation)})
		for _, value := range p.removed {
			patch = append(patch, patchOperation{Op: "remove", Path: path(value.key)})
		}
	}
	p.patch, err = json.Marshal(patch)
	if err != nil {
		return RolloutPlan{}, errors.New("encode guarded patch")
	}
	return p, nil
}

// RequireNativeRuntime treats revision-writer consistency as a pinned-source
// gate. Live sources deliberately have unknown revision consistency and instead
// require the resolver's verified source identity.
func RequireNativeRuntime(v *v1beta1.InferenceService, state *effective.RuntimeState) ([]string, error) {
	if state == nil || !state.MatchesInferenceService(v) {
		return nil, ErrRuntime
	}
	active, err := state.RequireActive()
	if err != nil {
		return nil, ErrRuntime
	}
	if ref := v.Spec.Runtime; ref != nil && ref.Name != "" {
		kind := "ClusterServingRuntime"
		ns := ""
		if ref.Kind != nil {
			kind = *ref.Kind
		}
		if kind != "ClusterServingRuntime" && kind != "ServingRuntime" || ref.APIGroup != nil && *ref.APIGroup != "ome.io" {
			return nil, ErrRuntime
		}
		if kind == "ServingRuntime" {
			ns = v.Namespace
		}
		if active.RuntimeName != ref.Name || active.RuntimeKind != kind || active.RuntimeNamespace != ns {
			return nil, ErrRuntime
		}
	}
	if err := requireActiveOrigin(active, state); err != nil {
		return nil, err
	}
	values := active.Components()
	if len(values) == 0 || len(values) > 3 {
		return nil, ErrRuntime
	}
	result := []string{}
	seen := map[v1beta1.ComponentType]bool{}
	for _, value := range values {
		if seen[value.Type] || value.Type != v1beta1.EngineComponent && value.Type != v1beta1.DecoderComponent && value.Type != v1beta1.RouterComponent {
			return nil, ErrRuntime
		}
		seen[value.Type] = true
		if value.DeploymentMode == constants.OMENative {
			result = append(result, string(value.Type))
		}
	}
	if len(result) == 0 {
		return nil, ErrRuntime
	}
	return result, nil
}

func requireActiveOrigin(active *effective.ActiveConfiguration, state *effective.RuntimeState) error {
	if active.Origin == effective.ConfigurationOriginControllerRevision {
		if _, err := state.RequireConsistentActive(); err != nil {
			return ErrRuntime
		}
	} else if active.Origin == effective.ConfigurationOriginLiveRuntime {
		live := state.LiveConfiguration()
		if live == nil || !live.Runtime.IdentityObserved || live.Runtime.Generation <= 0 ||
			live.Runtime.Kind != active.RuntimeKind || live.Runtime.Name != active.RuntimeName || live.Runtime.Namespace != active.RuntimeNamespace || state.PinMode != effective.RuntimePinModeAutoSync {
			return ErrRuntime
		}
	} else {
		return ErrRuntime
	}
	return nil
}

func pinnedWorkActive(v *v1beta1.InferenceService, work ReplicaEvidence, clock reportv1alpha1.Clock) (bool, error) {
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	projection, err := rolloutprojection.Project(v, clock)
	if err != nil {
		return false, ErrStale
	}
	for _, issue := range projection.Content.Issues {
		if issue.Code == reportv1alpha1.RolloutIssueEpochUnverifiable || issue.Code == reportv1alpha1.RolloutIssueAnalysisInconclusive {
			continue
		}
		if (v.Status.Rollout == nil || v.Status.Rollout.ActiveRun == nil) && issue.Code == reportv1alpha1.RolloutIssueComponentStatusMissing && issue.Group == nil && work.sources[v1beta1.ComponentType(issue.Component)] != "" {
			continue
		}
		return false, ErrStale
	}
	if v.Status.Rollout == nil || v.Status.Rollout.ActiveRun == nil {
		return false, nil
	}
	if !pinnedevidence.ValidActiveRun(v) {
		return false, ErrStale
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	run := v.Status.Rollout.ActiveRun
	if run.PinnedAt.Time.After(clock.Now()) {
		return false, ErrStale
	}
	for _, target := range run.TargetRevisions {
		if work.sources[target.Component] != target.Revision {
			return false, ErrStale
		}
	}
	for _, pinned := range run.Plan.Groups {
		group := pinned.Group
		if group.Canary == nil {
			continue
		}
		primary, valid := canaryevidence.Primary(group.Components)
		if !valid || v.Status.Canary == nil {
			return false, ErrStale
		}
		parts := make([]string, 0, len(group.Components))
		for _, component := range group.Components {
			for _, target := range run.TargetRevisions {
				if target.Component == component {
					parts = append(parts, string(component)+"="+target.Revision)
					if component == primary && target.Revision != v.Status.Canary.CanaryRevisionHash {
						return false, ErrStale
					}
				}
			}
		}
		sort.Strings(parts)
		if v.Status.Canary.TargetID != "ct1:"+rolloutpolicy.ShortHash([]byte(strings.Join(parts, ";"))) {
			return false, ErrStale
		}
		for _, value := range []*metav1.Time{v.Status.Canary.StepEnteredTime, v.Status.Canary.LastEvaluationTime, v.Status.Canary.LastConclusiveEvaluationTime} {
			if value != nil && (value.IsZero() || value.Time.After(clock.Now())) {
				return false, ErrStale
			}
		}
	}
	active := false
	for _, group := range projection.Content.Groups {
		switch group.Phase {
		case reportv1alpha1.RolloutPhaseSurging, reportv1alpha1.RolloutPhaseWaiting, reportv1alpha1.RolloutPhaseShifting, reportv1alpha1.RolloutPhaseDraining, reportv1alpha1.RolloutPhaseScalingDown, reportv1alpha1.RolloutPhaseRollingBack, reportv1alpha1.RolloutPhasePaused, reportv1alpha1.RolloutPhaseCanarying, reportv1alpha1.RolloutPhasePending, reportv1alpha1.RolloutPhasePromoting, reportv1alpha1.RolloutPhaseBlueGreenStandby, reportv1alpha1.RolloutPhaseUpdating:
			active = true
		}
	}
	return active, nil
}
