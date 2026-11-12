package coordination

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The waiver is a reading of the ratio state: a held-still peer is left out
// of the lockstep's reference on both signs of a step and for an in-place
// patch, a peer named that the reference does not list changes nothing,
// and the band is unchanged.
func TestLockstepHold_APeerHeldStillPacesNothing(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	pacing := v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced, RatioTolerancePercent: ptr.To(int32(5))}
	ahead := func(heldStill v1beta1.ComponentType) RatioState {
		return RatioState{
			Original:      map[v1beta1.ComponentType]int32{eng: 12, dec: 4},
			NewPods:       map[v1beta1.ComponentType]int32{eng: 10, dec: 3},
			Serving:       map[v1beta1.ComponentType]int32{eng: 12, dec: 4},
			HeldStillPeer: heldStill,
		}
	}
	if held, reason := LockstepHold(pacing, ahead(""), eng); !held || !strings.Contains(reason, "outpace") {
		t.Fatalf("10 of 12 beside 3 of 4 at a tolerance of 5 stands past the allowance: held=%v reason=%q", held, reason)
	}
	if held, reason := LockstepHold(pacing, ahead(dec), eng); held {
		t.Fatalf("a decoder the pairing gates hold still paces nothing: %s", reason)
	}
	if held, _ := LockstepHold(pacing, ahead(rtr), eng); !held {
		t.Fatalf("a held-still peer the reference does not list changes nothing")
	}
	// Only the held-still peer leaves the reading: a third member of the
	// group that still rolls paces the Component as before.
	withRouter := ahead(dec)
	withRouter.Original[rtr], withRouter.NewPods[rtr], withRouter.Serving[rtr] = 2, 0, 2
	if held, reason := LockstepHold(pacing, withRouter, eng); !held || !strings.Contains(reason, "outpace") {
		t.Fatalf("a router at 0 of 2 still paces an engine at 10 of 12 while the decoder is held still: held=%v reason=%q", held, reason)
	}
	for _, delta := range []int32{+1, -1} {
		if d := EvaluateSurge(pacing, ahead(""), eng, delta); d.AllowedSurgeDelta != 0 || !strings.Contains(d.Reason, "outpace") {
			t.Fatalf("delta %+d: the lockstep must hold the engine beside a decoder it paces by: %+v", delta, d)
		}
		d := EvaluateSurge(pacing, ahead(dec), eng, delta)
		if strings.Contains(d.Reason, "outpace") {
			t.Fatalf("delta %+d: the lockstep must not hold the engine beside a decoder held still: %+v", delta, d)
		}
	}
	// With the lockstep waived the band decides: a drain from 12:4 to 11:4
	// is 8.3 points off the 3:1 reference, past the tolerance and inside
	// twice it, so the one drain is admitted; a second with one open is not.
	if d := EvaluateSurge(pacing, ahead(dec), eng, -1); d.AllowedSurgeDelta != -1 {
		t.Fatalf("the band's one-drain tiebreaker must still admit the drain: %+v", d)
	}
	open := ahead(dec)
	open.InFlightUnavail = map[v1beta1.ComponentType]int32{eng: 1}
	open.Serving[eng] = 11
	if d := EvaluateSurge(pacing, open, eng, -1); d.AllowedSurgeDelta != 0 || !strings.Contains(d.Reason, "would skew") {
		t.Fatalf("the band must still refuse a second drain beside an open one: %+v", d)
	}
}
