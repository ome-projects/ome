package mutate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func heldReplica(v *v1beta1.InferenceService) *v1beta1.InferenceReplica {
	ir := replicaFor(v)
	ir.Name = "actual-native-engine"
	ir.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] = "true"
	ir.Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: "chat-engine-aaaaaaaa", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, Reason: "PRIVATE_BLOCK_REASON"}}
	return ir
}

// A parent-global gate would incorrectly reject current native children.
func TestHeldReleaseGlobalGenerationAdvisoryAndCanonicalPatch(t *testing.T) {
	for _, observed := range []int64{0, 6, 7, 8} {
		t.Run(strconv.FormatInt(observed, 10), func(t *testing.T) {
			v, state := nativeTarget(t)
			v.Status.ObservedGeneration = observed
			ir := heldReplica(v)
			before := ir.DeepCopy()
			evidence, err := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, "engine", testClock)
			require.NoError(t, err)
			for _, revision := range []string{"aaaaaaaa", "chat-engine-aaaaaaaa"} {
				plan, err := PrepareHeldRelease(v, state, evidence, "engine", revision, testClock)
				require.NoError(t, err)
				require.Equal(t, "actual-native-engine", plan.Target().Name)
				require.Equal(t, "aaaaaaaa", plan.RevisionHash())
				require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-ir"},{"op":"test","path":"/metadata/resourceVersion","value":"81"},{"op":"add","path":"/metadata/annotations/ome.io~1release-held-revision","value":"chat-engine-aaaaaaaa"}]`, string(plan.Patch()))
				patch := plan.Patch()
				patch[0] = 'x'
				require.Equal(t, byte('['), plan.Patch()[0])
				var preview bytes.Buffer
				require.NoError(t, plan.WritePreview(&preview, "synthetic", "ome", "client"))
				require.Contains(t, preview.String(), "Unverifiable")
				require.Contains(t, preview.String(), "actual-native-engine")
				require.NotContains(t, preview.String(), "PRIVATE_BLOCK_REASON")
			}
			require.Equal(t, before, ir)
		})
	}
}

func TestHeldReleaseOpaquePlanValidationAndRefreshRefusals(t *testing.T) {
	v, state := nativeTarget(t)
	ir := heldReplica(v)
	evidence, err := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, "engine", testClock)
	require.NoError(t, err)
	plan, err := PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", nil)
	require.NoError(t, err)
	require.Equal(t, "<HeldReleasePlan redacted>", fmt.Sprint(plan))
	for _, scenario := range []string{"nil parent", "nil state", "missing evidence", "wrong component", "selected mismatch", "not Held"} {
		parent, current, source, component := v, state, evidence, "engine"
		switch scenario {
		case "nil parent":
			parent = nil
		case "nil state":
			current = nil
		case "missing evidence":
			source = HeldReleaseEvidence{}
		case "wrong component":
			component = "router"
		case "selected mismatch":
			source.items = append([]v1beta1.InferenceReplica{}, source.items...)
			source.items[0].Spec.Component = v1beta1.DecoderComponent
		case "not Held":
			source.items = append([]v1beta1.InferenceReplica{}, source.items...)
			source.items[0].Status.RetryBlocks = append([]v1beta1.RetryBlock{}, source.items[0].Status.RetryBlocks...)
			source.items[0].Status.RetryBlocks[0].State = v1beta1.RetryBlockRetryInProgress
		}
		_, err = PrepareHeldRelease(parent, current, source, component, "aaaaaaaa", testClock)
		require.Error(t, err)
	}
	require.False(t, plan.MatchesRefresh(nil, state, evidence))
	require.False(t, plan.MatchesRefresh(v, nil, evidence))
	changed := v.DeepCopy()
	changed.ResourceVersion = "43"
	require.False(t, plan.MatchesRefresh(changed, state, evidence))
	var preview bytes.Buffer
	require.Error(t, (HeldReleasePlan{}).WritePreview(&preview, "safe", "ome", "client"))
	require.Error(t, plan.WritePreview(&preview, "unsafe\n", "ome", "client"))
	require.Error(t, plan.WritePreview(heldPreviewFailure{}, "safe", "ome", "client"))
	_, err = CollectHeldReleaseEvidence(context.Background(), nil, v, "engine", testClock)
	require.Error(t, err)
	_, err = CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, "unknown", testClock)
	require.Error(t, err)
}

type heldPreviewFailure struct{}

func (heldPreviewFailure) Write([]byte) (int, error) { return 0, errors.New("PRIVATE_IO_ERROR") }

func TestHeldReleasePausePreviewUsesRecognizedDepthOnly(t *testing.T) {
	for _, pause := range []string{"", "false", "PRIVATE_PAUSE_VALUE", "true", "freeze"} {
		v, state := nativeTarget(t)
		v.Annotations = map[string]string{constants.PausedRolloutAnnotation: pause}
		evidence, err := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(heldReplica(v)).OmeV1beta1(), v, "engine", testClock)
		require.NoError(t, err)
		plan, err := PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
		require.NoError(t, err)
		var preview bytes.Buffer
		require.NoError(t, plan.WritePreview(&preview, "synthetic", "ome", "client"))
		require.NotContains(t, preview.String(), "PRIVATE_PAUSE_VALUE")
		require.Contains(t, preview.String(), "Pause depth")
	}
}

// Any skipped unsafe tail, stale child, forged stamp or occupied mailbox
// would permit an unsafe positive plan; every fixture must refuse.
func TestHeldReleaseRefusesUnsafeSourceAndWholeBlockList(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.InferenceReplica)
	}{
		{"own unobserved", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 0 }},
		{"own behind", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 1 }},
		{"own ahead", func(ir *v1beta1.InferenceReplica) { ir.Status.ObservedGeneration = 3 }},
		{"parent stamp leading zero", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "07"
		}},
		{"missing provenance", func(ir *v1beta1.InferenceReplica) {
			delete(ir.Annotations, constants.InferenceReplicaControllerWriteAnnotationKey)
		}},
		{"pending empty", func(ir *v1beta1.InferenceReplica) { ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "" }},
		{"pending false", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "false"
		}},
		{"pending same", func(ir *v1beta1.InferenceReplica) {
			ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "chat-engine-aaaaaaaa"
		}},
		{"not held", func(ir *v1beta1.InferenceReplica) { ir.Status.RetryBlocks[0].State = v1beta1.RetryBlockBackoff }},
		{"unknown bystander", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RetryBlocks = append(ir.Status.RetryBlocks, v1beta1.RetryBlock{TargetRevision: "chat-engine-bbbbbbbb", State: "PRIVATE_UNKNOWN_STATE", AttemptsStarted: 1})
		}},
		{"duplicate bystander", func(ir *v1beta1.InferenceReplica) {
			ir.Status.RetryBlocks = append(ir.Status.RetryBlocks, ir.Status.RetryBlocks[0])
		}},
		{"zero attempts", func(ir *v1beta1.InferenceReplica) { ir.Status.RetryBlocks[0].AttemptsStarted = 0 }},
		{"future failure", func(ir *v1beta1.InferenceReplica) {
			tm := metav1.NewTime(testNow.Add(time.Hour))
			ir.Status.RetryBlocks[0].LastFailureAt = &tm
		}},
		{"held next", func(ir *v1beta1.InferenceReplica) {
			tm := metav1.NewTime(testNow)
			ir.Status.RetryBlocks[0].NextRetryAt = &tm
		}},
		{"oversized private payload", func(ir *v1beta1.InferenceReplica) { ir.Annotations["private"] = strings.Repeat("PRIVATE", 160000) }},
		{"derived label", func(ir *v1beta1.InferenceReplica) { ir.Labels[constants.PlacementOrigin] = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, state := nativeTarget(t)
			ir := heldReplica(v)
			tc.change(ir)
			evidence, err := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, "engine", testClock)
			if err == nil {
				_, err = PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
			}
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}
