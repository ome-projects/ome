package v1alpha1

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/safetext"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitheld"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

var heldWaitHash = regexp.MustCompile(`^[0-9a-f]{8}$`)

// WaitHeldRevisionObservation is a bounded, allowlisted state observation.
// No release request receipt exists, so attribution is always unverifiable.
type WaitHeldRevisionObservation waitheld.Observation

func (r WaitRequested) IsHeldRevision() bool { return r == WaitRequestedHeldRevisionUnheld }

func (o WaitHeldRevisionObservation) Canonical() WaitHeldRevisionObservation {
	switch o.Reason {
	case waitheld.ReasonHeld, waitheld.ReasonUnheld, waitheld.ReasonInvalidTarget,
		waitheld.ReasonSourceIncomplete, waitheld.ReasonReplicaMissing, waitheld.ReasonReplicaReplaced,
		waitheld.ReasonDeleting, waitheld.ReasonUnsupportedPlacement, waitheld.ReasonSourceStale,
		waitheld.ReasonSourceInvalid, waitheld.ReasonMailboxPending, waitheld.ReasonMailboxSuperseded,
		waitheld.ReasonInvalidRetryBlocks, waitheld.ReasonInvalidStatus:
	default:
		o.Reason, o.Validity = waitheld.ReasonSourceInvalid, waitheld.ValidityInvalid
	}
	switch o.Validity {
	case waitheld.ValidityValid, waitheld.ValidityPartial, waitheld.ValidityUnavailable, waitheld.ValidityInvalid:
	default:
		o.Validity = waitheld.ValidityInvalid
	}
	switch o.TargetState {
	case waitheld.TargetUnknown, waitheld.TargetHeld, waitheld.TargetAbsent,
		waitheld.TargetBackoff, waitheld.TargetRetryInProgress:
	default:
		o.TargetState, o.Validity = waitheld.TargetUnknown, waitheld.ValidityInvalid
	}
	switch o.MailboxState {
	case waitheld.MailboxUnknown, waitheld.MailboxAbsent, waitheld.MailboxPending, waitheld.MailboxSuperseded:
	default:
		o.MailboxState, o.Validity = waitheld.MailboxUnknown, waitheld.ValidityInvalid
	}
	if o.Attribution != waitheld.AttributionUnverifiable {
		o.Validity = waitheld.ValidityInvalid
	}
	o.Attribution = waitheld.AttributionUnverifiable
	if o.Component != "" && o.Component != "engine" && o.Component != "decoder" && o.Component != "router" {
		o.Component, o.Validity = "", waitheld.ValidityInvalid
	}
	if o.Revision != "" && o.Revision != "[REDACTED]" {
		if !validHeldWaitRevision(o.Revision, o.Component) {
			o.Revision, o.Validity = "", waitheld.ValidityInvalid
		} else {
			o.Revision = safetext.Sanitize(o.Revision, 253)
		}
	}
	if o.InferenceReplica != "" && o.InferenceReplica != "[REDACTED]" {
		if len(validation.IsDNS1123Subdomain(o.InferenceReplica)) != 0 {
			o.InferenceReplica, o.Validity = "", waitheld.ValidityInvalid
		} else {
			o.InferenceReplica = safetext.Sanitize(o.InferenceReplica, 253)
		}
	}
	if o.InferenceReplicaUID != "" && o.InferenceReplicaUID != "[REDACTED]" {
		if !waitheld.ValidIdentity(o.InferenceReplicaUID) {
			o.InferenceReplicaUID, o.Validity = "", waitheld.ValidityInvalid
		} else {
			o.InferenceReplicaUID = safetext.Sanitize(o.InferenceReplicaUID, 256)
		}
	}
	if o.Encoding != "" && o.Encoding != irstatus.EncodingDenseV1 && o.Encoding != irstatus.EncodingColumnarV2 {
		o.Encoding, o.Validity = "", waitheld.ValidityInvalid
	}
	if o.InspectedSources < 0 || o.InspectedSources > 1 || o.InspectedBlocks < 0 || o.InspectedBlocks > 64 {
		o.Validity = waitheld.ValidityInvalid
	}
	o.InspectedSources = max(0, min(o.InspectedSources, 1))
	o.InspectedBlocks = max(0, min(o.InspectedBlocks, 64))
	validUnheld := o.Validity == waitheld.ValidityValid && o.Reason == waitheld.ReasonUnheld &&
		(o.TargetState == waitheld.TargetAbsent || o.TargetState == waitheld.TargetBackoff || o.TargetState == waitheld.TargetRetryInProgress) &&
		o.MailboxState == waitheld.MailboxAbsent && o.Component != "" && o.Revision != "" &&
		o.InferenceReplica != "" && o.InferenceReplicaUID != "" && o.Encoding != "" && o.InspectedSources == 1
	if o.Matched && !validUnheld {
		o.Validity = waitheld.ValidityInvalid
	}
	o.Matched = o.Matched && validUnheld
	return o
}

func validHeldWaitRevision(revision, component string) bool {
	if component == "" || len(validation.IsDNS1123Subdomain(revision)) != 0 {
		return false
	}
	suffix := "-" + component + "-"
	i := len(revision) - len(suffix) - 8
	return i > 0 && strings.HasPrefix(revision[i:], suffix) &&
		len(validation.IsDNS1123Subdomain(revision[:i])) == 0 && heldWaitHash.MatchString(revision[len(revision)-8:])
}

func (r WaitReport) heldRevisionTable(wide bool) report.Table {
	c := r.Content
	o := c.HeldRevision
	interpretation := "No verified current unheld state"
	if c.Outcome == waitengine.OutcomeMatched && o.Matched {
		interpretation = "Exact revision no longer Held on original IR"
	} else if c.Outcome == waitengine.OutcomeTimedOut && o.Validity == waitheld.ValidityValid {
		interpretation = "Last observation; currentness unverified"
	}
	rows := [][]string{
		{"Service", r.Metadata.Namespace + "/" + r.Metadata.Name},
		{"Requested", string(c.Requested)}, {"Component", o.Component},
		{"Revision", o.Revision}, {"InferenceReplica", o.InferenceReplica},
		{"Outcome", string(c.Outcome)}, {"Target state", string(o.TargetState)},
		{"Mailbox", string(o.MailboxState)}, {"Validity", string(o.Validity)},
		{"Reason", string(c.Reason)}, {"Evidence", string(c.Evidence)},
		{"Interpretation", interpretation},
		{"Attribution", "Unverifiable; not action attribution"},
		{"State caveat", "Pruning/re-Held races; no request receipt"},
		{"Source method", string(c.Method)},
	}
	if wide {
		rows = append(rows, []string{"InferenceReplica UID", o.InferenceReplicaUID},
			[]string{"Status encoding", string(o.Encoding)},
			[]string{"Inspected sources / blocks", fmt.Sprintf("%d / %d", o.InspectedSources, o.InspectedBlocks)},
			[]string{"Read scope", "Parent GET / exact IR GET / parent refresh"},
			[]string{"Cancellation", "Cooperative requests; plugins may ignore it"})
	}
	rows = append(rows, []string{"GET / WATCH / polls", fmt.Sprintf("%d / %d / %d", c.Counts.Gets, c.Counts.Watches, c.Counts.Polls)},
		[]string{"Elapsed milliseconds", strconv.FormatInt(c.ElapsedMilliseconds, 10)})
	for i := range rows {
		rows[i][0] = printers.BoundedCell(rows[i][0], 22)
		rows[i][1] = printers.BoundedCell(rows[i][1], 54)
	}
	return report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}
}
