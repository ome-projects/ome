package endpoint

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// preclaim writes the claims a pass may touch into the map's claim record
// from fresh state and decides whether a retained positive plan is replayed.
// One row per rule: what it reads (the live map, the live owner, the claim
// record, the other maps, the plan capture), the error class it answers
// with, the preparation it hands on, and the claim record it leaves behind.

// preclaimRow is one shape of map, owner, record and plan.
type preclaimRow struct {
	name string
	// objects builds the owner and the map as the store holds them.
	objects func() (*v1beta1.InferenceService, *v1beta1.TrafficMap)
	// record is the claim record the stored map starts with.
	record *v1beta1.TrafficMapPublisherStatus
	// recorded is the options digest this publisher wrote with its own status
	// apply before the pass, so the field is its alone; "" writes none.
	recorded string
	// others are other TrafficMaps in the store.
	others []client.Object
	// handed edits the copies the pass hands in, after the store is seeded.
	handed func(owner *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap)
	// clientOptions change the store the pass reads and writes.
	clientOptions []fakeclient.Option
	desired       []string
	capture       *TrafficMapPublishPlanCapture
	// wantErr is the named error the answer wraps; wantTerminal a terminal refusal.
	wantErr      error
	wantTerminal bool
	wantErrText  string
	// want is the preparation handed on; nil when the pass answers an error.
	want *trafficMapPublicationPreparation
	// wantRecord is the claim record left on the live map; nil means none.
	wantRecord *v1beta1.TrafficMapPublisherStatus
	// wantReason is the reason of both publication conditions; "" means the
	// pass wrote neither.
	wantReason string
}

func retainedPlan(generation, ownerGeneration int64, digit string, targets ...v1beta1.TrafficMapPublisherTarget) *v1beta1.TrafficMapPublisherLastPositive {
	return &v1beta1.TrafficMapPublisherLastPositive{
		TrafficMapGeneration:    generation,
		ObservedISVCGeneration:  ownerGeneration,
		PlanCompatibilityDigest: testPublisherDigest(digit),
		Targets:                 targets,
	}
}

func target(name string, weight int64) v1beta1.TrafficMapPublisherTarget {
	return v1beta1.TrafficMapPublisherTarget{Target: name, Weight: weight}
}

func claimRecord(claims []string, lastPositive *v1beta1.TrafficMapPublisherLastPositive) *v1beta1.TrafficMapPublisherStatus {
	return &v1beta1.TrafficMapPublisherStatus{
		PublisherName: "example-publisher", ClaimedTargets: claims, LastPositive: lastPositive,
	}
}

// withOptionsDigest is a claim record that remembers the options digest of the
// last applied plan.
func withOptionsDigest(record *v1beta1.TrafficMapPublisherStatus, digit string) *v1beta1.TrafficMapPublisherStatus {
	record.ObservedOptionsDigest = testPublisherDigest(digit)
	return record
}

func retainPolicy(digit string, targets ...v1beta1.TrafficMapPublisherTarget) *TrafficMapPublishPlanCapture {
	return &TrafficMapPublishPlanCapture{
		Targets:                 targets,
		PlanCompatibilityDigest: testPublisherDigest(digit),
		NoPositivePlanPolicy:    TrafficMapNoPositivePlanPolicyRetainLastPositive,
	}
}

// finalizedPublisherObjects is the plain owner and map with the finalizer
// the claim record needs.
func finalizedPublisherObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	owner, trafficMap := publisherTestObjects()
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	return owner, trafficMap
}

// drainingPlacementLossObjects is the lost placement whose owner drains.
func drainingPlacementLossObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	owner.Annotations = map[string]string{constants.TrafficDrainAnnotation: ""}
	return owner, trafficMap
}

// otherMapClaiming is another map of the same publisher that claims the targets.
func otherMapClaiming(claims ...string) *v1beta1.TrafficMap {
	other := publisherClaimedTrafficMap("other", "example-publisher", claims)
	other.Finalizers = []string{TrafficMapPublisherFinalizer}
	return other
}

// transitioning is the reason both publication conditions carry while a
// plan is being applied.
var transitioning = v1beta1.TrafficMapReasonPublicationTransitioning

// lostPlan is the retained plan of a map whose placement was lost.
var lostPlan = retainedPlan(2, 2, "a", target("target-a", 75), target("target-b", 25))

// preclaimRows are the rows of TestPreclaim.
var preclaimRows = []preclaimRow{
	{
		name:    "a map replaced since the read is a source change",
		objects: finalizedPublisherObjects,
		handed: func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) {
			trafficMap.UID = types.UID("replaced")
		},
		desired: []string{"target-a"},
		wantErr: errTrafficMapPublisherSourceChanged,
	},
	{
		name:    "a map whose generation moved is stale",
		objects: finalizedPublisherObjects,
		handed:  func(_ *v1beta1.InferenceService, trafficMap *v1beta1.TrafficMap) { trafficMap.Generation-- },
		desired: []string{"target-a"},
		wantErr: errTrafficMapPublisherPlanStale,
	},
	{
		name: "a deleting map is stale",
		objects: func() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
			owner, trafficMap := finalizedPublisherObjects()
			trafficMap.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
			return owner, trafficMap
		},
		desired: []string{"target-a"},
		wantErr: errTrafficMapPublisherPlanStale,
	},
	{
		name:    "an owner that cannot be read is an owner read failure",
		objects: finalizedPublisherObjects,
		clientOptions: []fakeclient.Option{
			fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, errors.New("request timed out")).OnKind(&v1beta1.InferenceService{}),
		},
		desired: []string{"target-a"},
		wantErr: errTrafficMapPublisherOwnerRead,
	},
	{
		name:    "an owner whose generation moved is a source change",
		objects: finalizedPublisherObjects,
		handed:  func(owner *v1beta1.InferenceService, _ *v1beta1.TrafficMap) { owner.Generation++ },
		desired: []string{"target-a"},
		wantErr: errTrafficMapPublisherSourceChanged,
	},
	{
		name:    "a map without the publisher finalizer is stale",
		objects: publisherTestObjects,
		desired: []string{"target-a"},
		wantErr: errTrafficMapPublisherPlanStale,
	},
	{
		name:         "another publisher's claims refuse the pass",
		objects:      finalizedPublisherObjects,
		record:       &v1beta1.TrafficMapPublisherStatus{PublisherName: "earlier-publisher", ClaimedTargets: []string{"target-a"}},
		desired:      []string{"target-a"},
		wantTerminal: true,
		wantErr:      errTrafficMapPublisherChanged,
		wantRecord:   &v1beta1.TrafficMapPublisherStatus{PublisherName: "earlier-publisher", ClaimedTargets: []string{"target-a"}},
	},
	{
		name:       "a fresh map claims the sorted union",
		objects:    finalizedPublisherObjects,
		desired:    []string{"target-b", "target-a"},
		want:       &trafficMapPublicationPreparation{expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:       "retired claims stay in the union",
		objects:    finalizedPublisherObjects,
		record:     claimRecord([]string{"retired"}, nil),
		desired:    []string{"target-a"},
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"retired"}, expectedClaims: []string{"retired", "target-a"}},
		wantRecord: claimRecord([]string{"retired", "target-a"}, nil),
		wantReason: transitioning,
	},
	{
		name:       "the union keeps the recorded options digest",
		objects:    finalizedPublisherObjects,
		record:     claimRecord([]string{"retired"}, nil),
		recorded:   "f",
		desired:    []string{"target-a"},
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"retired"}, expectedClaims: []string{"retired", "target-a"}},
		wantRecord: withOptionsDigest(claimRecord([]string{"retired", "target-a"}, nil), "f"),
		wantReason: transitioning,
	},
	{
		name:         "a union over the limit is refused",
		objects:      finalizedPublisherObjects,
		record:       claimRecord(numberedClaims(v1beta1.MaxTrafficMapPublisherTargets), nil),
		desired:      []string{"target-z"},
		wantTerminal: true,
		wantErrText:  "publisher claim union has 129 targets, maximum is 128",
		wantRecord:   claimRecord(numberedClaims(v1beta1.MaxTrafficMapPublisherTargets), nil),
	},
	{
		name:       "a union at the limit is accepted",
		objects:    finalizedPublisherObjects,
		record:     claimRecord(numberedClaims(v1beta1.MaxTrafficMapPublisherTargets-1), nil),
		desired:    []string{"target-z"},
		want:       &trafficMapPublicationPreparation{previousClaims: numberedClaims(127), expectedClaims: append(numberedClaims(127), "target-z")},
		wantRecord: claimRecord(append(numberedClaims(127), "target-z"), nil),
		wantReason: transitioning,
	},
	{
		name:         "a target another map claims is refused",
		objects:      finalizedPublisherObjects,
		others:       []client.Object{otherMapClaiming("target-a")},
		desired:      []string{"target-a"},
		wantTerminal: true,
		wantErrText:  `publisher target "target-a" is already claimed by TrafficMap team-a/other`,
	},
	{
		name:       "a positive plan over the same claims keeps the retained plan",
		objects:    finalizedPublisherObjects,
		record:     claimRecord([]string{"target-a"}, retainedPlan(2, 2, "a", target("target-a", 100))),
		desired:    []string{"target-a"},
		capture:    retainPolicy("a", target("target-a", 60)),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a"}, expectedClaims: []string{"target-a"}, lastPositive: retainedPlan(2, 2, "a", target("target-a", 100))},
		wantRecord: claimRecord([]string{"target-a"}, retainedPlan(2, 2, "a", target("target-a", 100))),
		wantReason: transitioning,
	},
	{
		name:       "a positive plan over new claims drops the retained plan",
		objects:    finalizedPublisherObjects,
		record:     claimRecord([]string{"target-a"}, retainedPlan(2, 2, "a", target("target-a", 100))),
		desired:    []string{"target-a", "target-b"},
		capture:    retainPolicy("a", target("target-a", 60), target("target-b", 40)),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:       "a positive plan over fewer claims drops the retained plan",
		objects:    finalizedPublisherObjects,
		record:     claimRecord([]string{"target-a", "target-b"}, retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0))),
		desired:    []string{"target-a"},
		capture:    retainPolicy("a", target("target-a", 100)),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:       "a positive plan under other options drops the retained plan",
		objects:    finalizedPublisherObjects,
		record:     claimRecord([]string{"target-a"}, retainedPlan(2, 2, "a", target("target-a", 100))),
		desired:    []string{"target-a"},
		capture:    retainPolicy("b", target("target-a", 60)),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a"}, expectedClaims: []string{"target-a"}},
		wantRecord: claimRecord([]string{"target-a"}, nil),
		wantReason: transitioning,
	},
	{
		name:    "a positive plan while no member serves keeps a retained plan from an earlier source generation",
		objects: allHomesUnreadyPublisherObjects,
		record:  claimRecord([]string{"target-a", "target-b"}, retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0))),
		desired: []string{"target-a"},
		capture: func() *TrafficMapPublishPlanCapture {
			capture := retainPolicy("a", target("target-a", 100))
			capture.AllowAllHomesUnreadyFallback = true
			return capture
		}(),
		want: &trafficMapPublicationPreparation{
			previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"},
			lastPositive: retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0)),
		},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0))),
		wantReason: transitioning,
	},
	{
		name:       "no positive plan and unknown eligibility writes the unknown condition and stops",
		objects:    finalizedPublisherObjects,
		record:     claimRecord([]string{"target-a"}, retainedPlan(2, 2, "a", target("target-a", 100))),
		desired:    []string{"target-a"},
		capture:    retainPolicy("a", target("target-a", 0)),
		wantErr:    errTrafficMapPublisherEligibilityUnknown,
		wantRecord: claimRecord([]string{"target-a"}, retainedPlan(2, 2, "a", target("target-a", 100))),
		wantReason: trafficMapPublisherReasonEligibilityUnknown,
	},
	{
		name:    "no positive plan on a lost placement replays the retained plan",
		objects: eligiblePlacementLossPublisherObjects,
		record:  claimRecord([]string{"target-a", "target-b"}, lostPlan),
		capture: retainPolicy("a"),
		want: &trafficMapPublicationPreparation{
			previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"},
			useFallback: true, lastPositive: lostPlan,
		},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, lostPlan),
		wantReason: transitioning,
	},
	{
		name:     "a replay keeps the recorded options digest",
		objects:  eligiblePlacementLossPublisherObjects,
		record:   claimRecord([]string{"target-a", "target-b"}, lostPlan),
		recorded: "f",
		capture:  retainPolicy("a"),
		want: &trafficMapPublicationPreparation{
			previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"},
			useFallback: true, lastPositive: lostPlan,
		},
		wantRecord: withOptionsDigest(claimRecord([]string{"target-a", "target-b"}, lostPlan), "f"),
		wantReason: transitioning,
	},
	{
		name:       "no positive plan with an invalid retained plan claims the current plan",
		objects:    eligiblePlacementLossPublisherObjects,
		record:     claimRecord([]string{"target-a", "target-b"}, lostPlan),
		capture:    retainPolicy("b"),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:       "no positive plan with other claims claims the union",
		objects:    eligiblePlacementLossPublisherObjects,
		record:     claimRecord([]string{"target-a", "target-b"}, lostPlan),
		desired:    []string{"target-c"},
		capture:    retainPolicy("a"),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b", "target-c"}},
		wantRecord: claimRecord([]string{"target-a", "target-b", "target-c"}, nil),
		wantReason: transitioning,
	},
	{
		name:       "no positive plan while the source drains claims the current plan",
		objects:    drainingPlacementLossObjects,
		record:     claimRecord([]string{"target-a", "target-b"}, lostPlan),
		capture:    retainPolicy("a"),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:    "the apply-current policy never replays",
		objects: eligiblePlacementLossPublisherObjects,
		record:  claimRecord([]string{"target-a", "target-b"}, lostPlan),
		capture: &TrafficMapPublishPlanCapture{
			PlanCompatibilityDigest: testPublisherDigest("a"), NoPositivePlanPolicy: TrafficMapNoPositivePlanPolicyApplyCurrent,
		},
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:    "a plan that removes publication never replays",
		objects: eligiblePlacementLossPublisherObjects,
		record:  claimRecord([]string{"target-a", "target-b"}, lostPlan),
		capture: func() *TrafficMapPublishPlanCapture {
			capture := retainPolicy("a")
			capture.Withdrawn = true
			return capture
		}(),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:       "a publisher without retained plans drops the record",
		objects:    eligiblePlacementLossPublisherObjects,
		record:     claimRecord([]string{"target-a", "target-b"}, lostPlan),
		desired:    []string{"target-a", "target-b"},
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
	{
		name:         "a collision is refused before a replay is recorded",
		objects:      eligiblePlacementLossPublisherObjects,
		record:       claimRecord([]string{"target-a", "target-b"}, lostPlan),
		others:       []client.Object{otherMapClaiming("target-b")},
		capture:      retainPolicy("a"),
		wantTerminal: true,
		wantErrText:  `publisher target "target-b" is already claimed by TrafficMap team-a/other`,
		wantRecord:   claimRecord([]string{"target-a", "target-b"}, lostPlan),
	},
	{
		name:    "no positive plan while no member serves replays a compatible subset",
		objects: allHomesUnreadyPublisherObjects,
		record:  claimRecord([]string{"target-a", "target-b"}, retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0))),
		desired: []string{"target-a"},
		capture: func() *TrafficMapPublishPlanCapture {
			capture := retainPolicy("a", target("target-a", 0))
			capture.AllowAllHomesUnreadyFallback = true
			return capture
		}(),
		want: &trafficMapPublicationPreparation{
			previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"},
			useFallback: true, lastPositive: retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0)),
		},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0))),
		wantReason: transitioning,
	},
	{
		name:       "no positive plan while no member serves needs the opt-in",
		objects:    allHomesUnreadyPublisherObjects,
		record:     claimRecord([]string{"target-a", "target-b"}, retainedPlan(2, 2, "a", target("target-a", 100), target("target-b", 0))),
		desired:    []string{"target-a"},
		capture:    retainPolicy("a", target("target-a", 0)),
		want:       &trafficMapPublicationPreparation{previousClaims: []string{"target-a", "target-b"}, expectedClaims: []string{"target-a", "target-b"}},
		wantRecord: claimRecord([]string{"target-a", "target-b"}, nil),
		wantReason: transitioning,
	},
}

func TestPreclaim(t *testing.T) {
	for _, row := range preclaimRows {
		t.Run(row.name, func(t *testing.T) {
			owner, trafficMap := row.objects()
			trafficMap.Status.Publisher = copyTrafficMapPublisherJournal(row.record)
			objects := append([]client.Object{owner, trafficMap}, row.others...)
			options := append([]fakeclient.Option{
				fakeclient.WithStatusSubresource(&v1beta1.TrafficMap{}), fakeclient.WithObjects(objects...),
			}, row.clientOptions...)
			kubeClient := fakeclient.New(t, options...)
			publisher := &fallbackTrafficMapPublisher{recordingTrafficMapPublisher: &recordingTrafficMapPublisher{name: "example-publisher", stateful: true}}
			reconciler := &TrafficMapPublisherReconciler{Client: kubeClient, APIReader: kubeClient, Publisher: publisher, Active: true}
			if row.recorded != "" {
				recorded := trafficMap.DeepCopy()
				recorded.Status.Publisher.ObservedOptionsDigest = testPublisherDigest(row.recorded)
				require.NoError(t, reconciler.applyPublisherStatus(context.Background(), recorded))
			}
			handedOwner, handedMap := owner.DeepCopy(), trafficMap.DeepCopy()
			if row.handed != nil {
				row.handed(handedOwner, handedMap)
			}

			preparation, err := reconciler.preclaim(context.Background(), handedMap, handedOwner, row.desired, row.capture)

			switch {
			case row.wantErr != nil:
				require.Error(t, err)
				assert.True(t, errors.Is(err, row.wantErr), "error %q does not wrap %q", err, row.wantErr)
			case row.wantTerminal || row.wantErrText != "":
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
			var terminal *terminalPublisherError
			assert.Equal(t, row.wantTerminal, errors.As(err, &terminal), "terminal refusal: %v", err)
			if row.wantErrText != "" {
				assert.Contains(t, err.Error(), row.wantErrText)
			}
			if row.want == nil {
				assert.Nil(t, preparation)
			} else {
				require.NotNil(t, preparation)
				assert.Equal(t, row.want.previousClaims, preparation.previousClaims, "previous claims")
				assert.Equal(t, row.want.expectedClaims, preparation.expectedClaims, "expected claims")
				assert.Equal(t, row.want.useFallback, preparation.useFallback, "replay of the retained plan")
				assert.Equal(t, row.want.lastPositive, preparation.lastPositive, "retained plan handed on")
			}
			live := &v1beta1.TrafficMap{}
			require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), live))
			assert.Equal(t, row.wantRecord, live.Status.Publisher, "claim record")
			for _, conditionType := range []string{v1beta1.TrafficMapPublished, v1beta1.TrafficMapPublicationFallback} {
				condition := apimeta.FindStatusCondition(live.Status.Conditions, conditionType)
				if row.wantReason == "" {
					assert.Nil(t, condition, "%s written", conditionType)
					continue
				}
				require.NotNil(t, condition, "%s not written", conditionType)
				assert.Equal(t, row.wantReason, condition.Reason, conditionType)
			}
			assert.False(t, live.Status.Published)
		})
	}
}
