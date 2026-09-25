package mutate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

const maxRepinMetadataBytes = 64 * 1024

var (
	ErrRepinIdle             = errors.New("rollout repin refused: no active pinned run")
	ErrRepinEmptyPlan        = errors.New("rollout repin refused: empty live plans are not supported by this command")
	ErrRepinEvidence         = errors.New("rollout repin refused: controller plan evidence is missing, stale or inconsistent")
	ErrRepinInSync           = errors.New("rollout repin refused: the current plan is already pinned")
	ErrRepinTopology         = errors.New("rollout repin refused: live and pinned rollout topology differ")
	ErrRepinMultipleCanaries = errors.New("rollout repin refused: this command supports at most one canary group")
	ErrRepinPending          = errors.New("rollout repin refused: a rollout action mailbox is already present")
	ErrRepinPlan             = errors.New("rollout repin plan is private")

	rolloutPortableDigestPattern = regexp.MustCompile(`^rp1:[0-9a-f]{12}$`)
)

// RolloutRepinPlan is an immutable UID/resourceVersion-guarded request to ask
// the controller to replace one active run's pinned progression bodies. It
// carries only allowlisted identity and digest fields, never policy bodies or
// arbitrary condition messages.
type RolloutRepinPlan struct {
	patch   []byte
	target  reportv1alpha1.ActionTarget
	details reportv1alpha1.RolloutActionDetails
}

func (RolloutRepinPlan) MarshalJSON() ([]byte, error)          { return nil, ErrRepinPlan }
func (RolloutRepinPlan) MarshalYAML() (any, error)             { return nil, ErrRepinPlan }
func (RolloutRepinPlan) String() string                        { return "<mutate.RolloutRepinPlan redacted>" }
func (RolloutRepinPlan) GoString() string                      { return "<mutate.RolloutRepinPlan redacted>" }
func (p RolloutRepinPlan) Patch() []byte                       { return append([]byte(nil), p.patch...) }
func (p RolloutRepinPlan) Target() reportv1alpha1.ActionTarget { return p.target }
func (p RolloutRepinPlan) Details() reportv1alpha1.RolloutActionDetails {
	return p.details
}

// PrepareRolloutRepin proves that one current InferenceService snapshot has a
// non-empty valid active run, an equally shaped live plan, a complete current
// resolution view, and exact drift evidence. The controller still re-renders
// from live policy reads and CAS-checks the supplied digest when consuming the
// one-shot annotation.
func PrepareRolloutRepin(service *omev1beta1.InferenceService, clock reportv1alpha1.Clock) (RolloutRepinPlan, error) {
	if err := ValidateTarget(service); err != nil {
		return RolloutRepinPlan{}, err
	}
	for _, key := range []string{constants.RolloutRepinAnnotation, constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation} {
		if _, present := service.Annotations[key]; present {
			return RolloutRepinPlan{}, ErrRepinPending
		}
	}
	if service.Status.Rollout == nil || service.Status.Rollout.ActiveRun == nil {
		return RolloutRepinPlan{}, ErrRepinIdle
	}
	if service.Spec.Rollout == nil || len(service.Spec.Rollout.Groups) == 0 {
		return RolloutRepinPlan{}, ErrRepinEmptyPlan
	}
	run := service.Status.Rollout.ActiveRun
	if len(run.Plan.Groups) == 0 || !pinnedevidence.ValidActiveRun(service) {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}
	if len(service.Spec.Rollout.Groups) != len(run.Plan.Groups) {
		return RolloutRepinPlan{}, ErrRepinTopology
	}
	for i := range service.Spec.Rollout.Groups {
		if !sameRepinTopology(&service.Spec.Rollout.Groups[i], &run.Plan.Groups[i].Group) {
			return RolloutRepinPlan{}, ErrRepinTopology
		}
	}
	if err := validateRepinLiveSpec(service); err != nil {
		return RolloutRepinPlan{}, err
	}
	if len(service.Status.Rollout.Groups) != len(service.Spec.Rollout.Groups) {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}

	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	now := clock.Now()
	if run.PinnedAt.IsZero() || run.PinnedAt.Time.After(now) || !validRepinConditions(service, run, now) {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}
	drift := uniqueRepinCondition(service.Status.Conditions, omev1beta1.RolloutPlanDriftCondition)
	if drift == nil {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}
	if drift.Status != corev1.ConditionTrue {
		return RolloutRepinPlan{}, ErrRepinInSync
	}

	pinnedDigests := make([]string, 0, len(run.Plan.Groups))
	currentDigests := make([]string, 0, len(run.Plan.Groups))
	canaryGroups := 0
	firstRenderedChange := -1
	firstReportedChange := -1
	for i := range service.Spec.Rollout.Groups {
		live := &service.Spec.Rollout.Groups[i]
		pinned := &run.Plan.Groups[i]
		resolution := &service.Status.Rollout.Groups[i]
		if live.DeclaredProgression() == omev1beta1.RolloutProgressionCanary {
			canaryGroups++
		}
		if !rolloutPortableDigestPattern.MatchString(pinned.PortableDigest) {
			return RolloutRepinPlan{}, ErrRepinEvidence
		}
		current, valid := currentRepinDigest(live, resolution, i)
		if !valid {
			return RolloutRepinPlan{}, ErrRepinEvidence
		}
		pinnedDigests = append(pinnedDigests, pinned.PortableDigest)
		currentDigests = append(currentDigests, current)
		if firstRenderedChange < 0 && current != pinned.PortableDigest {
			firstRenderedChange = i
		}
		// Drift conditions are produced by the controller's raw source scan.
		// It currently reports an empty digest for an implicit default
		// blue-green group, while current is the composed explicit render used
		// for the repin CAS. Track both scans so the condition proof matches the
		// controller without constructing the wrong requested digest.
		if firstReportedChange < 0 && resolution.ObservedDigest != pinned.PortableDigest {
			firstReportedChange = i
		}
	}
	if canaryGroups > 1 {
		return RolloutRepinPlan{}, ErrRepinMultipleCanaries
	}
	if firstRenderedChange < 0 {
		return RolloutRepinPlan{}, ErrRepinInSync
	}
	if firstReportedChange < 0 {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}
	wantReason := omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun
	if service.Status.Rollout.Groups[firstReportedChange].Source == omev1beta1.RolloutPlanSourcePolicy {
		wantReason = omev1beta1.RolloutPlanDriftReasonPolicyNewerThanRun
	}
	if drift.Reason != wantReason {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}
	liveLabel := service.Status.Rollout.Groups[firstReportedChange].ObservedDigest
	if liveLabel == "" {
		liveLabel = "(unresolvable)"
	}
	wantMessage := fmt.Sprintf(
		"groups[%d]: live render %s differs from pinned %s; the edit applies at the next run (or via ome.io/rollout-repin)",
		firstReportedChange,
		liveLabel,
		run.Plan.Groups[firstReportedChange].PortableDigest,
	)
	if drift.Message != wantMessage {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}

	pinnedDigest := rolloutpolicy.CombinedDigest(pinnedDigests)
	currentDigest := rolloutpolicy.CombinedDigest(currentDigests)
	if !rolloutPortableDigestPattern.MatchString(pinnedDigest) || !rolloutPortableDigestPattern.MatchString(currentDigest) || currentDigest == "now" {
		return RolloutRepinPlan{}, ErrRepinEvidence
	}
	if pinnedDigest == currentDigest {
		return RolloutRepinPlan{}, ErrRepinInSync
	}
	if len(service.Annotations) >= 256 || repinMetadataBytes(service)+len(constants.RolloutRepinAnnotation)+len(currentDigest) > maxRepinMetadataBytes {
		return RolloutRepinPlan{}, ErrBounds
	}

	patch := []patchOperation{
		{Op: "test", Path: "/metadata/uid", Value: string(service.UID)},
		{Op: "test", Path: "/metadata/resourceVersion", Value: service.ResourceVersion},
	}
	if service.Annotations == nil {
		patch = append(patch, patchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]string{}})
	}
	patch = append(patch, patchOperation{
		Op: "add", Path: "/metadata/annotations/" + escapeJSONPointer(constants.RolloutRepinAnnotation), Value: currentDigest,
	})
	encoded, err := json.Marshal(patch)
	if err != nil {
		return RolloutRepinPlan{}, errors.New("encode guarded rollout repin patch")
	}
	return RolloutRepinPlan{
		patch: encoded,
		target: reportv1alpha1.ActionTarget{
			Kind: "InferenceService", Namespace: service.Namespace, Name: service.Name,
			UID: string(service.UID), ResourceVersion: service.ResourceVersion,
		},
		details: reportv1alpha1.RolloutActionDetails{
			RunID: run.RunID, PinnedPlanDigest: pinnedDigest,
			RequestedPlanDigest: currentDigest, GroupCount: len(run.Plan.Groups),
		},
	}, nil
}

func validateRepinLiveSpec(service *omev1beta1.InferenceService) error {
	if len(service.Spec.Rollout.Groups) > 3 ||
		validation.ValidateCanary(&service.Spec) != nil ||
		validation.ValidateCoordination(&service.Spec) != nil ||
		validation.ValidateLifecycle(&service.Spec) != nil ||
		validation.ValidateRolloutPolicyRefs(&service.Spec, true) != nil ||
		validation.ValidateRolloutOrderingEnforced(&service.Spec) != nil {
		return ErrRepinEvidence
	}
	canaries := 0
	for i := range service.Spec.Rollout.Groups {
		if ref := service.Spec.Rollout.Groups[i].PolicyRef; ref != nil &&
			(!SafeScalar(ref.Name) || len(k8svalidation.IsDNS1123Subdomain(ref.Name)) != 0) {
			return ErrRepinEvidence
		}
		if service.Spec.Rollout.Groups[i].DeclaredProgression() == omev1beta1.RolloutProgressionCanary {
			canaries++
		}
	}
	if canaries > 1 {
		return ErrRepinMultipleCanaries
	}
	return nil
}

func validRepinConditions(service *omev1beta1.InferenceService, run *omev1beta1.RolloutRun, now time.Time) bool {
	ready := uniqueRepinCondition(service.Status.Conditions, omev1beta1.RolloutPlanReadyCondition)
	drift := uniqueRepinCondition(service.Status.Conditions, omev1beta1.RolloutPlanDriftCondition)
	if ready == nil || drift == nil || ready.Status != corev1.ConditionTrue ||
		ready.Reason != omev1beta1.RolloutPlanReasonPinned ||
		ready.Message != "run "+run.RunID+" pinned" ||
		!validRepinConditionTime(ready.LastTransitionTime, run.OpenedAt.Time, now) ||
		!validRepinConditionTime(drift.LastTransitionTime, run.PinnedAt.Time, now) {
		return false
	}
	if drift.Status == corev1.ConditionTrue {
		return drift.Reason == omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun ||
			drift.Reason == omev1beta1.RolloutPlanDriftReasonPolicyNewerThanRun
	}
	return drift.Status == corev1.ConditionFalse && drift.Reason == omev1beta1.RolloutPlanDriftReasonInSync
}

func uniqueRepinCondition(conditions duckv1.Conditions, conditionType string) *apis.Condition {
	var found *apis.Condition
	for i := range conditions {
		if conditions[i].Type != apis.ConditionType(conditionType) {
			continue
		}
		if found != nil {
			return nil
		}
		found = &conditions[i]
	}
	return found
}

func validRepinConditionTime(value apis.VolatileTime, notBefore, now time.Time) bool {
	return !value.Inner.IsZero() && !value.Inner.Time.Before(notBefore) && !value.Inner.Time.After(now)
}

func sameRepinTopology(live, pinned *omev1beta1.RolloutGroup) bool {
	if live == nil || pinned == nil || live.DeclaredProgression() != pinned.DeclaredProgression() {
		return false
	}
	return reflect.DeepEqual(live.Components, pinned.Components) &&
		reflect.DeepEqual(live.Order, pinned.Order) &&
		reflect.DeepEqual(live.Soak, pinned.Soak) &&
		reflect.DeepEqual(live.MaintainRatio, pinned.MaintainRatio)
}

func currentRepinDigest(live *omev1beta1.RolloutGroup, resolution *omev1beta1.RolloutGroupResolution, index int) (string, bool) {
	if live == nil || resolution == nil || resolution.Index != int32(index) ||
		resolution.Source != rolloutpolicy.GroupSource(live) ||
		!reflect.DeepEqual(resolution.PolicyRef, live.PolicyRef) {
		return "", false
	}
	if resolution.Source == omev1beta1.RolloutPlanSourcePolicy {
		return resolution.ObservedDigest, live.PolicyRef != nil && resolution.ShadowedPolicyRef == nil &&
			rolloutPortableDigestPattern.MatchString(resolution.ObservedDigest)
	}
	resolved, err := rolloutpolicy.ComposeGroup(live, nil)
	if err != nil {
		return "", false
	}
	computed, err := rolloutpolicy.ProgressionDigest(&resolved)
	if err != nil || computed == "" || computed != resolution.ObservedDigest {
		// The current controller reports an empty resolution digest for the
		// implicit default blue-green spelling. Compute that self-describing
		// render locally; every explicit inline body must still match status.
		defaultBlueGreen := live.PolicyRef == nil && live.Canary == nil && live.BlueGreen == nil && live.RollingUpdate == nil
		if !defaultBlueGreen || resolution.ObservedDigest != "" || err != nil || computed == "" {
			return "", false
		}
	}
	if live.PolicyRef == nil {
		return computed, resolution.ShadowedPolicyRef == nil
	}
	shadow := resolution.ShadowedPolicyRef
	if shadow == nil || shadow.Name != live.PolicyRef.Name ||
		shadow.WouldPinDigest != "" && !rolloutPortableDigestPattern.MatchString(shadow.WouldPinDigest) {
		return "", false
	}
	return computed, true
}

func repinMetadataBytes(service *omev1beta1.InferenceService) int {
	total := 0
	for key, value := range service.Annotations {
		total += len(key) + len(value)
	}
	for key, value := range service.Labels {
		total += len(key) + len(value)
	}
	for _, value := range service.Finalizers {
		total += len(value)
	}
	return total
}

// WritePreview renders only reviewed identity and digest scalars. Every line
// is bounded for an 80-column terminal while the patch retains exact values.
func (p RolloutRepinPlan) WritePreview(out io.Writer, contextName string, mode reportv1alpha1.DryRunMode) error {
	if len(p.patch) == 0 || !SafeScalar(contextName) || p.target.UID == "" ||
		!rolloutPortableDigestPattern.MatchString(p.details.RequestedPlanDigest) || p.details.RequestedPlanDigest == "now" {
		return ErrUnsafeValue
	}
	rows := make([][]string, 0, 18)
	add := func(field, value string) {
		if value == "" {
			value = "<absent>"
		}
		for value != "" {
			chunk := repinDisplayPrefix(value, 56)
			rows = append(rows, []string{field, chunk})
			value = strings.TrimPrefix(value, chunk)
			field = "(continued)"
		}
	}
	for _, row := range [][2]string{
		{"Action", "rollout repin"},
		{"Context", contextName},
		{"Workload NS", p.target.Namespace},
		{"Target", p.target.Kind + "/" + p.target.Name},
		{"UID", p.target.UID},
		{"ResourceVersion", p.target.ResourceVersion},
		{"Dry-run", string(mode)},
		{"Run", p.details.RunID},
		{"Pinned plan digest", p.details.PinnedPlanDigest},
		{"Requested digest", p.details.RequestedPlanDigest},
		{"Group count", strconv.Itoa(p.details.GroupCount)},
		{"Set annotation", constants.RolloutRepinAnnotation},
		{"Value", p.details.RequestedPlanDigest},
	} {
		add(row[0], row[1])
	}
	if _, err := fmt.Fprintln(out, "ALPHA guarded rollout repin (not controller convergence)"); err != nil {
		return errors.New("write rollout repin preview failed")
	}
	if err := (report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}).Write(out); err != nil {
		return errors.New("write rollout repin preview failed")
	}
	for _, warning := range []string{
		"Run identity and progress remain; only pinned progression bodies change.",
		"A clamped canary step may hold before raising traffic.",
		"The controller CAS covers progression renders, not full plan topology.",
		"The CLI separately refuses topology changes before sending a PATCH.",
		"The exact InferenceService UID/resourceVersion is tested atomically.",
	} {
		if _, err := fmt.Fprintln(out, warning); err != nil {
			return errors.New("write rollout repin preview failed")
		}
	}
	return nil
}

func repinDisplayPrefix(value string, width int) string {
	result := ""
	for _, r := range value {
		candidate := result + string(r)
		if result != "" && printers.CellDisplayWidth(candidate) > width {
			break
		}
		result = candidate
	}
	return result
}
