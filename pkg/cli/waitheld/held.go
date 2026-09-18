// Package waitheld observes whether one exact InferenceReplica revision is no
// longer Held. It never attributes that state to a release request.
package waitheld

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/actionbounds"
	"sigs.k8s.io/ome/pkg/cli/safetext"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

const (
	maxSources    = 1
	maxBlocks     = 64
	maxStatusRows = 2048
)

var (
	hashPattern     = regexp.MustCompile(`^[0-9a-f]{8}$`)
	identityPattern = regexp.MustCompile(`^[A-Za-z0-9_.:/@+-]+$`)
	unsafeIdentity  = regexp.MustCompile(`(?:^|[^A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}|^[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}$`)
)

// Target is the exact action target. IRUID must come from the release-held
// ActionResult, not be inferred from the first wait poll.
type Target struct {
	Namespace, ParentName, Component, IRName, Revision, IRUID string
}

// Evidence is private API evidence from one bounded acquisition pass.
type Evidence struct {
	Parent   *v1beta1.InferenceService
	Replica  *v1beta1.InferenceReplica
	Complete bool
	Sources  int
}

type Reason string

const (
	ReasonHeld                 Reason = "Held"
	ReasonUnheld               Reason = "Unheld"
	ReasonInvalidTarget        Reason = "InvalidTarget"
	ReasonSourceIncomplete     Reason = "SourceIncomplete"
	ReasonReplicaMissing       Reason = "ReplicaMissing"
	ReasonReplicaReplaced      Reason = "ReplicaReplaced"
	ReasonDeleting             Reason = "Deleting"
	ReasonUnsupportedPlacement Reason = "UnsupportedPlacement"
	ReasonSourceStale          Reason = "SourceStale"
	ReasonSourceInvalid        Reason = "SourceInvalid"
	ReasonMailboxPending       Reason = "MailboxPending"
	ReasonMailboxSuperseded    Reason = "MailboxSuperseded"
	ReasonInvalidRetryBlocks   Reason = "InvalidRetryBlocks"
	ReasonInvalidStatus        Reason = "InvalidStatus"
)

type TargetState string

const (
	TargetUnknown         TargetState = "Unknown"
	TargetHeld            TargetState = "Held"
	TargetAbsent          TargetState = "Absent"
	TargetBackoff         TargetState = "Backoff"
	TargetRetryInProgress TargetState = "RetryInProgress"
)

type MailboxState string

const (
	MailboxUnknown    MailboxState = "Unknown"
	MailboxAbsent     MailboxState = "Absent"
	MailboxPending    MailboxState = "Pending"
	MailboxSuperseded MailboxState = "Superseded"
)

type Validity string

const (
	ValidityValid       Validity = "Valid"
	ValidityPartial     Validity = "Partial"
	ValidityUnavailable Validity = "Unavailable"
	ValidityInvalid     Validity = "Invalid"
)

type Attribution string

const AttributionUnverifiable Attribution = "Unverifiable"

// Observation contains only bounded validated identities and fixed states.
// Raw retry reasons and mailbox values never enter the output.
type Observation struct {
	Matched             bool              `json:"matched"`
	Reason              Reason            `json:"reason"`
	Validity            Validity          `json:"validity"`
	TargetState         TargetState       `json:"targetState"`
	MailboxState        MailboxState      `json:"mailboxState"`
	Attribution         Attribution       `json:"attribution"`
	Component           string            `json:"component,omitempty"`
	Revision            string            `json:"revision,omitempty"`
	InferenceReplica    string            `json:"inferenceReplica,omitempty"`
	InferenceReplicaUID string            `json:"inferenceReplicaUID,omitempty"`
	Encoding            irstatus.Encoding `json:"encoding,omitempty"`
	InspectedSources    int               `json:"inspectedSources"`
	InspectedBlocks     int               `json:"inspectedBlocks"`
}

func validTarget(target Target) bool {
	if len(validation.IsDNS1123Label(target.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(target.ParentName)) != 0 || !validIdentity(target.IRUID) {
		return false
	}
	switch target.Component {
	case "engine", "decoder", "router":
	default:
		return false
	}
	prefix := target.ParentName + "-" + target.Component + "-"
	return len(validation.IsDNS1123Subdomain(target.IRName)) == 0 &&
		len(target.Revision) == len(prefix)+8 && len(validation.IsDNS1123Subdomain(target.Revision)) == 0 &&
		strings.HasPrefix(target.Revision, prefix) && hashPattern.MatchString(target.Revision[len(prefix):])
}

// ValidTarget checks the exact action identity before any API acquisition.
func ValidTarget(target Target) bool { return validTarget(target) }

// Evaluate reports the current same-UID state. An absent block at the first
// poll is state evidence, not proof of request consumption; natural pruning
// or a re-Held race can produce it.
func Evaluate(target Target, evidence Evidence) Observation {
	o := Observation{Reason: ReasonSourceInvalid, Validity: ValidityInvalid, TargetState: TargetUnknown,
		MailboxState: MailboxUnknown, Attribution: AttributionUnverifiable}
	if !validTarget(target) {
		o.Reason = ReasonInvalidTarget
		return o
	}
	o.Component, o.Revision = target.Component, safetext.Sanitize(target.Revision, 253)
	if evidence.Parent == nil || !validParent(evidence.Parent, target) {
		return o
	}
	if evidence.Parent.DeletionTimestamp != nil {
		o.Reason = ReasonDeleting
		return o
	}
	if placementOwned(evidence.Parent.Annotations, evidence.Parent.Labels, evidence.Parent.Finalizers) ||
		evidence.Parent.Spec.Placement != nil || evidence.Parent.Status.Placement != nil {
		o.Reason = ReasonUnsupportedPlacement
		return o
	}
	ir := evidence.Replica
	if ir != nil {
		if !validIdentity(string(ir.UID)) {
			return o
		}
		if string(ir.UID) != target.IRUID {
			o.Reason, o.Validity = ReasonReplicaReplaced, ValidityUnavailable
			return o
		}
	}
	if !evidence.Complete || evidence.Sources < 0 || evidence.Sources > maxSources ||
		evidence.Replica != nil && evidence.Sources != 1 || evidence.Replica == nil && evidence.Sources != 0 {
		o.Reason, o.Validity = ReasonSourceIncomplete, ValidityPartial
		return o
	}
	o.InspectedSources = evidence.Sources
	if ir == nil {
		o.Reason, o.Validity = ReasonReplicaMissing, ValidityUnavailable
		return o
	}
	if ir.DeletionTimestamp != nil {
		o.Reason = ReasonDeleting
		return o
	}
	if placementOwned(ir.Annotations, ir.Labels, ir.Finalizers) {
		o.Reason = ReasonUnsupportedPlacement
		return o
	}
	if !validReplicaIdentity(ir, evidence.Parent, target) {
		return o
	}
	o.InferenceReplica = safetext.Sanitize(ir.Name, 253)
	o.InferenceReplicaUID = safetext.Sanitize(string(ir.UID), 256)
	if ir.Status.ObservedGeneration != ir.Generation || ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] != strconv.FormatInt(evidence.Parent.Generation, 10) {
		o.Reason, o.Validity = ReasonSourceStale, ValidityPartial
		return o
	}
	if ir.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] != constants.InferenceReplicaControllerWriteAnnotationVal {
		return o
	}
	if len(ir.Status.RetryBlocks) > maxBlocks {
		o.Reason = ReasonInvalidRetryBlocks
		return o
	}
	// RetryBlocks is top-level and independent of instance rows, but malformed
	// compacted status cannot be promoted to complete authoritative evidence.
	if !actionbounds.PrivatePayload(ir) {
		o.Reason = ReasonInvalidStatus
		return o
	}
	logical := ir.DeepCopy()
	encoding, err := irstatus.NewDecoder(maxStatusRows).Decode(logical)
	if err != nil || !actionbounds.PrivatePayload(logical) {
		o.Reason = ReasonInvalidStatus
		return o
	}
	o.Encoding, o.InspectedBlocks = encoding, len(ir.Status.RetryBlocks)
	state, reason := inspectBlocks(ir.Status.RetryBlocks, target.Revision, target.ParentName+"-"+target.Component+"-", time.Now())
	if reason != "" {
		o.Reason = reason
		return o
	}
	o.TargetState = state
	if mailbox, present := ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey]; present {
		o.Validity = ValidityValid
		if mailbox == target.Revision {
			o.Reason, o.MailboxState = ReasonMailboxPending, MailboxPending
		} else {
			o.Reason, o.MailboxState = ReasonMailboxSuperseded, MailboxSuperseded
		}
		return o
	}
	o.MailboxState, o.Validity = MailboxAbsent, ValidityValid
	if state == TargetHeld {
		o.Reason = ReasonHeld
		return o
	}
	o.Matched, o.Reason = o.Validity == ValidityValid, ReasonUnheld
	return o
}

func validParent(parent *v1beta1.InferenceService, target Target) bool {
	return (parent.Kind == "" || parent.Kind == "InferenceService") && (parent.APIVersion == "" || parent.APIVersion == "ome.io/v1beta1") &&
		parent.Name == target.ParentName && parent.Namespace == target.Namespace && validIdentity(string(parent.UID)) && validIdentity(parent.ResourceVersion) &&
		parent.Generation > 0 && len(parent.Annotations) <= 256 && len(parent.Labels) <= 256 && len(parent.Finalizers) <= 64 &&
		len(parent.Status.Conditions) <= 64 && len(parent.Status.Components) <= 3 && actionbounds.PrivatePayload(parent)
}

func validReplicaIdentity(ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, target Target) bool {
	if (ir.Kind != "" && ir.Kind != "InferenceReplica") || (ir.APIVersion != "" && ir.APIVersion != "ome.io/v1beta1") ||
		ir.Name != target.IRName || len(validation.IsDNS1123Subdomain(ir.Name)) != 0 || ir.Namespace != target.Namespace || !validIdentity(ir.ResourceVersion) || ir.Generation <= 0 ||
		ir.Spec.ParentRef.Name != target.ParentName || string(ir.Spec.Component) != target.Component ||
		len(parent.Name) <= 63 && ir.Labels[constants.InferenceServiceLabel] != target.ParentName ||
		len(ir.Annotations) > 256 || len(ir.Labels) > 256 || len(ir.Finalizers) > 64 ||
		len(ir.OwnerReferences) > 16 || len(ir.Status.Conditions) > 64 || len(ir.Status.Migrations) > 256 || len(ir.Status.Traffic) > 8 ||
		!actionbounds.PrivatePayload(ir) {
		return false
	}
	controllers := 0
	for _, owner := range ir.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		controllers++
		if owner.APIVersion != "ome.io/v1beta1" || owner.Kind != "InferenceService" || owner.Name != parent.Name || owner.UID != parent.UID {
			return false
		}
	}
	return controllers == 1
}

func placementOwned(annotations, labels map[string]string, finalizers []string) bool {
	for _, value := range finalizers {
		if value == "ome.io/placement" {
			return true
		}
	}
	for _, key := range []string{"ome.io/accelerator-requirements", "ome.io/cluster-selector", constants.PlacementOrigin, constants.PlacementOriginUID, constants.PlacementControlPlane} {
		if _, found := annotations[key]; found {
			return true
		}
		if _, found := labels[key]; found {
			return true
		}
	}
	return false
}

// ValidIdentity admits the same bounded opaque UID/resource-version shape as
// release-held while keeping credentials and control characters out of the
// action-derived wait target.
func ValidIdentity(value string) bool { return validIdentity(value) }

func validIdentity(value string) bool {
	if len(value) == 0 || len(value) > 256 || !identityPattern.MatchString(value) || unsafeIdentity.MatchString(value) {
		return false
	}
	if at := strings.LastIndexByte(value, '@'); at >= 0 {
		userinfo := value[:at]
		if scheme := strings.Index(userinfo, "://"); scheme >= 0 {
			userinfo = userinfo[scheme+3:]
		}
		if strings.ContainsRune(userinfo, ':') {
			return false
		}
	}
	return true
}

func inspectBlocks(blocks []v1beta1.RetryBlock, revision, prefix string, now time.Time) (TargetState, Reason) {
	state := TargetAbsent
	seen := make(map[string]bool, len(blocks))
	for _, block := range blocks {
		if len(block.TargetRevision) != len(prefix)+8 || !strings.HasPrefix(block.TargetRevision, prefix) ||
			!hashPattern.MatchString(block.TargetRevision[len(prefix):]) || seen[block.TargetRevision] || block.AttemptsStarted < 0 ||
			len(block.Reason) > 4096 || !validBlockTimes(block, now) {
			return TargetUnknown, ReasonInvalidRetryBlocks
		}
		seen[block.TargetRevision] = true
		switch block.State {
		case v1beta1.RetryBlockHeld:
			if block.AttemptsStarted == 0 || block.NextRetryAt != nil {
				return TargetUnknown, ReasonInvalidRetryBlocks
			}
		case v1beta1.RetryBlockBackoff:
			if block.NextRetryAt == nil {
				return TargetUnknown, ReasonInvalidRetryBlocks
			}
		case v1beta1.RetryBlockRetryInProgress:
		default:
			return TargetUnknown, ReasonInvalidRetryBlocks
		}
		if block.TargetRevision == revision {
			switch block.State {
			case v1beta1.RetryBlockHeld:
				state = TargetHeld
			case v1beta1.RetryBlockBackoff:
				state = TargetBackoff
			case v1beta1.RetryBlockRetryInProgress:
				state = TargetRetryInProgress
			}
		}
	}
	return state, ""
}

func validBlockTimes(block v1beta1.RetryBlock, now time.Time) bool {
	for _, stamp := range []*metav1.Time{block.NextRetryAt, block.FirstFailureAt, block.LastFailureAt} {
		if stamp != nil && stamp.IsZero() {
			return false
		}
	}
	if block.FirstFailureAt != nil && block.FirstFailureAt.Time.After(now) || block.LastFailureAt != nil && block.LastFailureAt.Time.After(now) {
		return false
	}
	if block.FirstFailureAt != nil && block.LastFailureAt != nil && block.LastFailureAt.Before(block.FirstFailureAt) ||
		block.NextRetryAt != nil && block.FirstFailureAt != nil && block.NextRetryAt.Before(block.FirstFailureAt) {
		return false
	}
	return true
}
