package status

import (
	"context"
	"slices"
	"strconv"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A once-only message is one an operator needs to read once per episode
// and not once per reconcile: the gang has no co-location term, the
// PodGroup was rebuilt, a repair is held, a drain is past its deadline.
// Its dedup key is the row, because the row is what the message is about
// — a key held in controller memory answers "has this process said it
// yet", which is a different question, and one no replay can ask twice.

// scope names the episode a once-only message belongs to. A message
// states which scope it has; the row does not decide for it.
type scope int

const (
	// attempt is a message about the operation in flight, whose episode
	// is that operation. A row carrying no operation keys it on the
	// incarnation: a message about a row with no attempt is about the
	// Instance as built. The zero value, so a reason the table does not
	// name gets the narrower episode.
	attempt scope = iota
	// standing is a message about the Instance as built — its template,
	// its topology — whose episode is the incarnation. An attempt opening
	// or concluding on the row does not change what it says.
	standing
)

// scopes names the standing messages; every other reason is
// attempt-scoped.
var scopes = map[types.EventReason]scope{
	types.EventReasonMaybeNoGangScheduler: standing,
	types.EventReasonGangSplitRisk:        standing,
}

// incarnationEpisode is the episode a standing message keys on.
func incarnationEpisode(s *types.InstanceStatus) string {
	incarnation := int64(0)
	if s != nil {
		incarnation = s.Incarnation
	}
	return "#" + strconv.FormatInt(incarnation, 10)
}

// attemptEpisode is the episode an attempt-scoped message keys on: the
// operation in flight, else the incarnation.
func attemptEpisode(s *types.InstanceStatus) string {
	if s != nil && s.Operation != nil && s.Operation.ID != "" {
		return s.Operation.ID
	}
	return incarnationEpisode(s)
}

// episode is the row state reason belongs to, by its scope.
func episode(reason types.EventReason, s *types.InstanceStatus) string {
	if scopes[reason] == attempt {
		return attemptEpisode(s)
	}
	return incarnationEpisode(s)
}

// announcementMarker is what one delivered message records: the reason
// and the episode it was delivered in.
func announcementMarker(reason types.EventReason, s *types.InstanceStatus) string {
	return string(reason) + "@" + episode(reason, s)
}

// Announced reports whether reason has already been delivered for the
// row's current episode.
func Announced(s types.InstanceStatus, reason types.EventReason) bool {
	return slices.Contains(s.Announced, announcementMarker(reason, &s))
}

// markAnnounced records reason against the row's current episode and
// drops the markers of every episode that has ended: a marker survives
// while its episode is the row's current attempt or the row's current
// incarnation, so an attempt concluding leaves the standing markers in
// place and a rebuilt Instance starts clean. Reports whether it changed
// the row: false means the message was already delivered, and the caller
// must not emit it again.
//
// The prune is part of the same write rather than a separate pass: the
// only reader of a marker is this file, so an ended episode's markers are
// collected the next time the row has something to say.
func markAnnounced(s *types.InstanceStatus, reason types.EventReason) bool {
	if s == nil {
		return false
	}
	marker := announcementMarker(reason, s)
	live := map[string]struct{}{
		attemptEpisode(s):     {},
		incarnationEpisode(s): {},
	}
	kept := make([]string, 0, len(s.Announced)+1)
	for _, entry := range s.Announced {
		if entry == marker {
			// Already delivered. Any stale markers alongside it are
			// collected by the write that does have something to say.
			return false
		}
		if _, ok := live[markerEpisode(entry)]; ok {
			kept = append(kept, entry)
		}
	}
	s.Announced = append(kept, marker)
	return true
}

// markerEpisode is the episode half of a marker. An entry with no
// separator belongs to no episode this build can name, so it is treated
// as ended.
func markerEpisode(entry string) string {
	for i := len(entry) - 1; i >= 0; i-- {
		if entry[i] == '@' {
			return entry[i+1:]
		}
	}
	return ""
}

// Announce records reason on row idx and reports whether this pass is
// the one that recorded it. Callers emit their message from that answer,
// so a message reaches an operator exactly as often as the write that
// earned it: a refused or failed write emits nothing and the next pass
// tries again.
//
// Eligibility is re-tested inside the mutation, against the row the
// write lands on. The pass-start observation the caller decided from can
// be stale by then, and a row that has since opened a new operation is a
// row the message must be delivered to again.
func Announce(ctx context.Context, input types.ReconcileInput, idx int32, reason types.EventReason) (bool, error) {
	if input.MutateInstance == nil {
		// No row writer, so no row to remember on: the message is
		// delivered rather than dropped.
		return true, nil
	}
	// Edge-triggered off the observation as well as the write, so a
	// message already delivered costs no mutation at all — which is every
	// pass over every workload whose warning stands.
	if row := input.ObservedState.Instance(idx); row != nil && Announced(*row, reason) {
		return false, nil
	}
	recorded := false
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		recorded = markAnnounced(s, reason)
		return recorded
	})
	if err != nil {
		return false, err
	}
	return recorded, nil
}
