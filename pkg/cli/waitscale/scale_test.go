package waitscale_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitscale"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func scaleEvidence(spec, current, ready int32) waitscale.Evidence {
	controller := true
	parent := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("parent-uid"), ResourceVersion: "11", Generation: 3}}
	parent.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {
		ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"},
	}}
	ir := &ome.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: types.UID("engine-uid"), ResourceVersion: "12", Generation: 2,
			Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(parent.Generation, 10)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}}},
		Spec:   ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent, Replicas: &spec},
		Status: ome.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: current, ReadyReplicas: ready, ServingReplicas: ready, AvailableReplicas: ready},
	}
	for i := int32(0); i < current; i++ {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, ome.OMENativeInstanceStatus{Index: i, Incarnation: 1, Phase: ome.OMENativeInstanceReady, PodCount: 1})
	}
	return waitscale.Evidence{Parent: parent, Replica: ir}
}

func TestScaleMatchesFreshExactSpecAndLogicalInstances(t *testing.T) {
	for _, tc := range []struct {
		name         string
		spec, actual int32
		wantMatch    bool
		wantReason   string
	}{
		{"matched", 2, 2, true, "ReplicaScaleMatched"},
		{"spec pending", 1, 2, false, "ReplicaScaleNotMatched"},
		{"instances pending", 2, 1, false, "ReplicaScaleNotMatched"},
		{"instances above", 2, 3, false, "ReplicaScaleNotMatched"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(scaleEvidence(tc.spec, tc.actual, 0))
			require.Equal(t, tc.wantMatch, decision.Matched)
			require.Equal(t, tc.wantReason, string(decision.Reason))
			require.Equal(t, "Valid", observation.Validity)
			require.Equal(t, "DenseV1", observation.Encoding)
			require.Equal(t, tc.spec, *observation.SpecReplicas)
			require.Equal(t, tc.actual, *observation.CurrentReplicas)
			require.Zero(t, *observation.ReadyReplicas)
		})
	}
}

func TestScaleDoesNotMistakeUnobservedZeroForConvergence(t *testing.T) {
	evidence := scaleEvidence(1, 0, 0)
	evidence.Replica.Status.ObservedGeneration = 0
	decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 1}).Evaluate(evidence)
	require.False(t, decision.Matched)
	require.NotEqual(t, "Valid", observation.Validity)
	require.Nil(t, observation.CurrentReplicas)
}

func TestScaleColumnarAndDenseGiveSameCountDecision(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	base, baseObserved := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
	require.True(t, base.Matched)
	columns, err := irstatus.EncodeColumns(evidence.Replica.Status.InstanceStatuses, 2)
	require.NoError(t, err)
	encoding := ome.InstanceStatusEncodingColumnarV2
	evidence.Replica.Status.InstanceStatuses = nil
	evidence.Replica.Status.InstanceStatusEncoding = &encoding
	evidence.Replica.Status.InstanceStatusColumns = columns
	decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
	require.Equal(t, base.Matched, decision.Matched)
	require.Equal(t, baseObserved.CurrentReplicas, observation.CurrentReplicas)
	require.Equal(t, "ColumnarV2", observation.Encoding)

	// A mixed or malformed representation must not inherit aggregate counts.
	evidence.Replica.Status.InstanceStatuses = []ome.OMENativeInstanceStatus{{Index: 0}}
	decision, observation = waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
	require.False(t, decision.Matched)
	require.Equal(t, "Invalid", observation.Validity)
	require.Nil(t, observation.CurrentReplicas)
}

func TestScaleWaitAcceptsCompactedFourThousandInstanceStatus(t *testing.T) {
	evidence := scaleEvidence(4000, 4000, 2400)
	columns, err := irstatus.EncodeColumns(evidence.Replica.Status.InstanceStatuses, 4000)
	require.NoError(t, err)
	encoding := ome.InstanceStatusEncodingColumnarV2
	evidence.Replica.Status.InstanceStatuses = nil
	evidence.Replica.Status.InstanceStatusEncoding = &encoding
	evidence.Replica.Status.InstanceStatusColumns = columns
	decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 4000}).Evaluate(evidence)
	require.True(t, decision.Matched)
	require.Equal(t, "ColumnarV2", observation.Encoding)
	require.Equal(t, int32(4000), *observation.CurrentReplicas)
	require.Equal(t, int32(2400), *observation.ReadyReplicas)
}

func TestScaleRejectsStaleOrForeignEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*waitscale.Evidence)
	}{
		{"parent generation stamp", func(e *waitscale.Evidence) {
			e.Replica.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "2"
		}},
		{"stale IR generation", func(e *waitscale.Evidence) { e.Replica.Status.ObservedGeneration = 1 }},
		{"owner UID", func(e *waitscale.Evidence) { e.Replica.OwnerReferences[0].UID = "other" }},
		{"component", func(e *waitscale.Evidence) { e.Replica.Spec.Component = ome.DecoderComponent }},
		{"target name", func(e *waitscale.Evidence) {
			e.Parent.Status.Components[ome.EngineComponent] = ome.ComponentStatusSpec{ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "other"}}
		}},
		{"negative count", func(e *waitscale.Evidence) { e.Replica.Status.Replicas = -1 }},
		{"excess ready", func(e *waitscale.Evidence) { e.Replica.Status.ReadyReplicas = 3 }},
		{"excess updated", func(e *waitscale.Evidence) { e.Replica.Status.UpdatedReplicas = 3 }},
		{"unknown dense phase", func(e *waitscale.Evidence) { e.Replica.Status.InstanceStatuses[0].Phase = "future" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := scaleEvidence(2, 2, 1)
			tc.change(&evidence)
			decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
			require.False(t, decision.Matched)
			require.Nil(t, observation.CurrentReplicas)
			require.NotEqual(t, "Valid", observation.Validity)
		})
	}
}

func TestScalePinsSelectedIRAndOptionalActionIdentity(t *testing.T) {
	target := waitscale.Target{Component: ome.EngineComponent, Replicas: 2, IRName: "chat-engine", IRUID: types.UID("engine-uid")}
	evaluator := waitscale.NewEvaluator(target)
	first := scaleEvidence(2, 1, 0)
	decision, _ := evaluator.Evaluate(first)
	require.False(t, decision.Matched)
	replaced := scaleEvidence(2, 2, 0)
	replaced.Replica.UID = "new-uid"
	decision, observation := evaluator.Evaluate(replaced)
	require.False(t, decision.Matched)
	require.Equal(t, "Invalid", observation.Validity)
	require.Nil(t, observation.CurrentReplicas)
}

func TestScaleAllowsOneControllerOwnerAmongBoundedReferences(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	evidence.Replica.OwnerReferences = append(evidence.Replica.OwnerReferences,
		metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "diagnostic", UID: "other"})
	decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
	require.True(t, decision.Matched)
	require.Equal(t, "Valid", observation.Validity)
}

func TestScaleRejectsStatusRowMismatchAndUnsafeActionTarget(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	evidence.Replica.Status.InstanceStatuses = evidence.Replica.Status.InstanceStatuses[:1]
	decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
	require.False(t, decision.Matched)
	require.Equal(t, "Invalid", observation.Validity)

	evidence = scaleEvidence(2, 2, 1)
	decision, observation = waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2, IRName: "chat-engine"}).Evaluate(evidence)
	require.False(t, decision.Matched)
	require.Equal(t, "Invalid", observation.Validity)
}

func TestScaleUnavailableTargetAndSafeObservation(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	evidence.Parent.Status.Components = nil
	evidence.Replica = nil
	decision, observation := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
	require.False(t, decision.Matched)
	require.Equal(t, "ReplicaScaleNotRecorded", string(decision.Reason))
	require.Equal(t, "Unavailable", observation.Validity)

	evidence = scaleEvidence(2, 2, 1)
	evidence.Replica.Annotations["private"] = "Bearer secret-leak"
	evidence.Replica.Status.Conditions = []metav1.Condition{{Type: "Ready", Message: "Bearer secret-leak"}}
	decision, observation = waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(evidence)
	require.True(t, decision.Matched)
	raw, err := json.Marshal(observation)
	require.NoError(t, err)
	require.False(t, strings.Contains(string(raw), "secret-leak"))
}

func TestScaleRawEvidenceCannotBeSerializedOrFormatted(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	evidence.Replica.Annotations["private"] = "Bearer secret-leak"
	_, err := json.Marshal(evidence)
	require.Error(t, err)
	require.False(t, strings.Contains(fmt.Sprintf("%v", evidence), "secret-leak"))
}
