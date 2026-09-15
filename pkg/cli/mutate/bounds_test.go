package mutate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestCompleteTargetPrivatePayloadIsBoundedBeforePinnedCopy(t *testing.T) {
	v := safeTarget()
	v.Status.Conditions = []apis.Condition{{Type: "Ready", Status: corev1.ConditionTrue, Message: strings.Repeat("PRIVATE", 300000)}}
	require.ErrorIs(t, ValidateTarget(v), ErrBounds)
}

func TestPrivatePayloadBudgetsCoverCompleteNestedTypedValues(t *testing.T) {
	for _, input := range []any{nil, (*string)(nil), time.Now(), map[string]any{"good": []any{"small", [2]byte{1, 2}}}, strings.Repeat("x", 1024*1024-8)} {
		require.True(t, boundedPrivatePayload(input))
	}
	for _, input := range []any{strings.Repeat("x", 1024*1024-7), make([]byte, 1024*1024), make([]int, 32769), make([]int, 32768), map[string]string{"private": strings.Repeat("x", 1024*1024)}} {
		require.False(t, boundedPrivatePayload(input))
	}
	largeMap := map[int]int{}
	for i := 0; i < 32769; i++ {
		largeMap[i] = 0
	}
	require.False(t, boundedPrivatePayload(largeMap))
	type nested struct{ Next *nested }
	root, current := &nested{}, (*nested)(nil)
	current = root
	for i := 0; i < 33; i++ {
		current.Next = &nested{}
		current = current.Next
	}
	require.False(t, boundedPrivatePayload(root))
}

func TestNoRunStillRefusesUnexpectedParentCanaryEvidence(t *testing.T) {
	v, state := nativeTarget(t)
	v.Status.Canary = &v1beta1.CanaryStatus{TargetID: "PRIVATE_UNKNOWN", CanaryRevisionHash: "not-a-hash"}
	_, err := PrepareRollout(v, state, activeEvidence(v), "pause", false, true, testClock)
	require.ErrorIs(t, err, ErrStale)
}

func TestPinnedWorkRefusesFreshButRetargetedIR(t *testing.T) {
	v, state := nativeTarget(t)
	pinnedCanary(t, v)
	ir := replicaFor(v)
	ir.Status.UpdateRevision = "chat-engine-cccccccc"
	work, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	_, err = PrepareRollout(v, state, work, "pause", false, true, testClock)
	require.ErrorIs(t, err, ErrStale)
}
