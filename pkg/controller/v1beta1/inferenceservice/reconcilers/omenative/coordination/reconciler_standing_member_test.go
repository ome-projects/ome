package coordination

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestReconcile_AMemberThatKeepsItsTemplateIsNeverHeldByTheRatio drives the
// coordination pass over a one-member change of a maintainRatio pair: 12
// prefill beside 8 decode, both SurgeThenDrain at tolerance 20, and one
// side's image alone is pushed. The side that keeps its template rests with
// every Instance on the revision it runs, so no replica will ask the ratio
// gate about a start of it. The pass writes the group Surging with
// surgeHeldByRatio off and records no RatioSkewRejected.
func TestReconcile_AMemberThatKeepsItsTemplateIsNeverHeldByTheRatio(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	for _, tc := range []struct {
		name     string
		changed  v1beta1.ComponentType
		standing v1beta1.ComponentType
	}{
		{"decoder changes alone", dec, eng},
		{"engine changes alone", eng, dec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{
				eng: {desired: 12, surge: "10%", strategy: surge},
				dec: {desired: 8, surge: "10%", strategy: surge},
			}
			kept := members[tc.standing]
			kept.promoted = kept.desired
			members[tc.standing] = kept
			isvc := batchService(batchGroupFixture{tolerance: int32Ptr(20)}, members)
			resting := batchReplica(tc.standing, members[tc.standing])
			resting.Status.CurrentRevision = resting.Status.UpdateRevision
			c := testClient(resting, batchReplica(tc.changed, members[tc.changed]))
			rec := record.NewFakeRecorder(16)
			if _, err := Reconcile(context.Background(), ReconcileInputs{
				ISVC: isvc, Client: c, Reader: c, Recorder: rec, Now: time.Now(),
				ComponentDeploymentModes: testOMENativeModes(eng, dec),
				ComponentRunnerPorts:     testComponentRunnerPorts(),
			}); err != nil {
				t.Fatalf("pass: %v", err)
			}
			group := isvc.Status.RolloutCoordination.Groups[0]
			if group.Phase != v1beta1.CoordinationPhaseSurging {
				t.Fatalf("the group reads %s, want Surging on the %s alone", group.Phase, tc.changed)
			}
			if group.SurgeHeldByRatio {
				t.Errorf("surgeHeldByRatio is written while the %s keeps its template and the %s rolls alone; nothing is held", tc.standing, tc.changed)
			}
			for _, e := range drainEvents(rec) {
				if strings.Contains(e, " "+EventReasonRatioSkewRejected+" ") {
					t.Errorf("%s recorded on a one-member change: %q", EventReasonRatioSkewRejected, e)
				}
			}
		})
	}
}
