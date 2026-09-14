package coordination

import (
	"sort"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Traffic tags: the short, cosmetic identifiers the API documents for
// ComponentTrafficTarget.Tag. Consumers do not route on them; they exist so an
// operator reading status can tell the incoming revision from the outgoing one
// without comparing hashes.
const (
	TrafficTagLatest = "latest"
	TrafficTagPrev   = "prev"
)

// RevisionWeight is the (per-revision Service name, percent, tag) tuple
// that the coordination layer materializes into one
// ComponentTrafficTarget on Status.Components.<c>.Traffic[].
type RevisionWeight struct {
	// RevisionHash is the underlying ControllerRevision hash. The
	// writer converts this into the per-revision Service name via
	// PerRevisionServiceName.
	RevisionHash string

	// Percent is the traffic percentage routed to this revision. Sum
	// across all weights for one Component must be 100.
	Percent int32

	// Tag is a short human-readable tag for the revision ("latest",
	// "prev"). Cosmetic.
	Tag string

	// LatestRevision marks this entry as the LatestRolledoutRevision
	// for the Component.
	LatestRevision bool

	// PairingProtocol is the P/D pairing token the revision was minted
	// under; copied onto the emitted ComponentTrafficTarget so routing
	// consumers can pair engine/decoder targets by equal values. Empty
	// pairs with anything.
	PairingProtocol string
}

// BuildTrafficTargets converts a sorted weight list into the
// Status.Components.<c>.Traffic[] entries the HTTPRoute weighted-
// backendRef consumer reads. The writer fills RevisionName with the
// per-revision Service name (`<isvc>-<component>-rev-<hash>`) so the
// consumer can use it directly as a backend reference.
//
// Weights naming the same revision are merged into one target, summing
// their percent. That list is +listType=map +listMapKey=revisionName, so
// emitting the same revisionName twice makes the apiserver reject the whole
// status write -- not just the traffic field -- and every subsequent
// reconcile for that ISVC fails with it, freezing conditions, rollout phase
// and replica counts until the underlying state changes.
//
// It is reachable, and by an ordinary operation: canaryWeights always emits
// a canary and a stable entry, and a revision hash is content-addressed over
// the rendered pod template. Reverting a failed rollout to its pre-rollout
// template therefore resolves the canary target back onto the revision
// already serving, so canary and stable carry the same hash while the step
// weight is still strictly between 0 and 100 and neither entry is dropped by
// the Percent<=0 filter below.
func BuildTrafficTargets(isvcName string, component v1beta1.ComponentType, weights []RevisionWeight) []v1beta1.ComponentTrafficTarget {
	if len(weights) == 0 {
		return nil
	}
	out := make([]v1beta1.ComponentTrafficTarget, 0, len(weights))
	at := make(map[string]int, len(weights))
	for _, w := range weights {
		if w.Percent <= 0 || w.RevisionHash == "" {
			continue
		}
		name := PerRevisionServiceName(isvcName, component, w.RevisionHash)
		if i, ok := at[name]; ok {
			out[i].Percent += w.Percent
			// The latest-revision weight owns the merged target's cosmetic
			// identity, so an operator reading status still sees the incoming
			// revision tagged as the incoming one.
			if w.LatestRevision {
				out[i].LatestRevision = true
				out[i].Tag = w.Tag
				out[i].PairingProtocol = w.PairingProtocol
			}
			continue
		}
		at[name] = len(out)
		out = append(out, v1beta1.ComponentTrafficTarget{
			RevisionName:    name,
			Percent:         w.Percent,
			Tag:             w.Tag,
			LatestRevision:  w.LatestRevision,
			PairingProtocol: w.PairingProtocol,
		})
	}
	return out
}

// SortedWeights returns weights sorted by descending Percent, breaking
// ties by RevisionHash, so the producer writes a stable slice across
// reconciles (a no-op write doesn't bounce the apiserver).
func SortedWeights(weights []RevisionWeight) []RevisionWeight {
	out := append([]RevisionWeight(nil), weights...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Percent != out[j].Percent {
			return out[i].Percent > out[j].Percent
		}
		return out[i].RevisionHash < out[j].RevisionHash
	})
	return out
}

// NormalizeWeights snaps a slice of revision weights so the values sum
// to 100 even when float division round-off would produce 99 or 101.
// The largest entry absorbs the residual to minimize visible drift.
//
// Inputs with sum == 0 are returned unchanged; the caller is expected
// to filter out empty traffic before invoking this.
func NormalizeWeights(in []RevisionWeight) []RevisionWeight {
	if len(in) == 0 {
		return in
	}
	total := int32(0)
	maxIdx := 0
	for i, w := range in {
		total += w.Percent
		if w.Percent > in[maxIdx].Percent {
			maxIdx = i
		}
	}
	if total == 0 {
		return in
	}
	delta := int32(100) - total
	if delta == 0 {
		return in
	}
	out := append([]RevisionWeight(nil), in...)
	out[maxIdx].Percent += delta
	if out[maxIdx].Percent < 0 {
		// Pathological input — clamp at zero so the consumer
		// doesn't see a negative weight.
		out[maxIdx].Percent = 0
	}
	return out
}

// ComputeWeightsFromPods produces a RevisionWeight slice from observed
// per-revision pod counts. New-vs-old apportioning is the natural
// "what fraction of capacity is the new revision" — mirrors the traffic
// producer contract.
//
// totals must be the total live pod count for the Component (any
// revision); a zero total returns nil.
//
// latestHash, when non-empty, tags the matching weight as
// LatestRevision=true and carries the tag vocabulary: that weight gets
// "latest" and every other weight "prev".
func ComputeWeightsFromPods(perRevisionPods map[string]int32, total int32, latestHash string) []RevisionWeight {
	if total <= 0 || len(perRevisionPods) == 0 {
		return nil
	}
	out := make([]RevisionWeight, 0, len(perRevisionPods))
	for hash, count := range perRevisionPods {
		if hash == "" || count <= 0 {
			continue
		}
		pct := int32((int64(count) * 100) / int64(total))
		// Tag stays empty when the caller could not resolve a latest hash:
		// with nothing to compare against, every revision would read "prev",
		// which is worse than saying nothing.
		tag := ""
		if latestHash != "" {
			tag = TrafficTagPrev
			if hash == latestHash {
				tag = TrafficTagLatest
			}
		}
		out = append(out, RevisionWeight{
			RevisionHash:   hash,
			Percent:        pct,
			Tag:            tag,
			LatestRevision: hash == latestHash,
		})
	}
	if len(out) == 0 {
		return nil
	}
	out = SortedWeights(out)
	out = NormalizeWeights(out)
	return out
}

// TrafficDiffersMeaningfully reports whether the desired traffic
// targets differ from the observed ones in a way the controller should
// write. Used to avoid no-op status writes that would bounce the
// apiserver and add reconcile pressure.
//
// The comparison is set-aware on RevisionName + Percent + LatestRevision +
// PairingProtocol and ignores Tag drift (Tag is cosmetic; LatestRevision is
// operator-visible and consumed by HTTPRoute reconcilers, and
// PairingProtocol is what routing consumers pair engine/decoder targets on,
// so a flip of either MUST trigger a write).
//
// Bug context: when the very first reconcile that observed pods saw an
// empty LifecycleStatus.UpdateRevision (the IR controller had
// not yet committed Status; common on Component bring-up), the writer
// stamped LatestRevision=false. A subsequent reconcile saw
// UpdateRevision populated and computed LatestRevision=true, but the
// old comparison treated the flag as cosmetic and the no-op short-
// circuit suppressed the write — leaving traffic[].LatestRevision stuck
// at false even after the rollout converged at 100% on a single
// revision. This surfaced as engine.traffic[0] missing the flag while
// decoder/router (whose Status arrived first in the IR controller's
// processing order) had it correctly set.
func TrafficDiffersMeaningfully(desired, observed []v1beta1.ComponentTrafficTarget) bool {
	if len(desired) != len(observed) {
		return true
	}
	type want struct {
		percent         int32
		latestRevision  bool
		pairingProtocol string
	}
	wantBy := make(map[string]want, len(desired))
	for _, t := range desired {
		wantBy[t.RevisionName] = want{percent: t.Percent, latestRevision: t.LatestRevision, pairingProtocol: t.PairingProtocol}
	}
	for _, t := range observed {
		w, ok := wantBy[t.RevisionName]
		if !ok || w.percent != t.Percent || w.latestRevision != t.LatestRevision || w.pairingProtocol != t.PairingProtocol {
			return true
		}
	}
	return false
}

// TrafficWithinDeadband reports whether desired is close enough to the
// already-written observed targets that the write should be SUPPRESSED
// as pod-count jitter rather than a real ratio shift.
//
// Why: weights are derived from RAW live pod counts. A pod momentarily
// Pending between reconciles changes a revision's count, flipping the
// computed percentage; with deadband=0 every flip clears
// TrafficDiffersMeaningfully and triggers a status write + HTTPRoute
// re-enqueue. At ~6000 pods this is continuous churn. The deadband adds
// hysteresis: a write is suppressed only when the revision SET is
// identical, no LatestRevision flag flips, and every per-revision percent
// moves by strictly less than deadband from the value already in status.
//
// Eventual correctness is preserved: a sustained ratio change always
// moves at least one revision's percent by >= deadband (the deadband is a
// per-revision band, not a cumulative tolerance), so it is never
// permanently suppressed — only transient sub-band jitter is.
//
// deadband <= 0 disables the band entirely (TrafficWithinDeadband is
// always false), preserving the exact prior write-on-any-diff behavior.
//
// Terminal/boundary convergence is always written, never suppressed: if any
// revision's desired percent is exactly 0 or 100 and differs from its last-
// written value, the write proceeds regardless of deadband. Without this, an
// operator who sets deadband larger than the final canary step (e.g. 95/5 ->
// 100/0 with deadband 10, where both revisions move only 5) would have the
// terminal write permanently suppressed, sticking traffic at 95/5 and never
// reaching a clean 100/0. The band only ever gates genuinely mid-range jitter.
func TrafficWithinDeadband(desired, observed []v1beta1.ComponentTrafficTarget, deadband int32) bool {
	if deadband <= 0 {
		return false
	}
	if len(desired) != len(observed) {
		return false
	}
	type have struct {
		percent         int32
		latestRevision  bool
		pairingProtocol string
	}
	haveBy := make(map[string]have, len(observed))
	for _, t := range observed {
		haveBy[t.RevisionName] = have{percent: t.Percent, latestRevision: t.LatestRevision, pairingProtocol: t.PairingProtocol}
	}
	for _, t := range desired {
		h, ok := haveBy[t.RevisionName]
		if !ok {
			// Revision set changed (a revision appeared/disappeared);
			// never suppress that — it's a real topology change.
			return false
		}
		// A LatestRevision flag flip is operator-visible and consumed by
		// HTTPRoute reconcilers; never suppress it (mirrors the contract
		// in TrafficDiffersMeaningfully). A PairingProtocol change is what
		// consumers pair on; never suppress it either.
		if h.latestRevision != t.LatestRevision {
			return false
		}
		if h.pairingProtocol != t.PairingProtocol {
			return false
		}
		// A revision arriving at or leaving a boundary (0 or 100) is a
		// terminal convergence event, not mid-range jitter; never suppress
		// it even when the per-revision move is under the band. Otherwise a
		// deadband larger than the final canary step could permanently
		// suppress the converging write.
		if (t.Percent == 0 || t.Percent == 100) && t.Percent != h.percent {
			return false
		}
		if absInt32(t.Percent-h.percent) >= deadband {
			return false
		}
	}
	return true
}

func absInt32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}
