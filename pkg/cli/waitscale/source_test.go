package waitscale_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitscale"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

type fakeScaleReplicaReader struct{ client omeclient.OmeV1beta1Interface }

func (r fakeScaleReplicaReader) GetInferenceReplica(ctx context.Context, namespace, name string, options metav1.GetOptions) (*ome.InferenceReplica, error) {
	return r.client.InferenceReplicas(namespace).Get(ctx, name, options)
}

func newScaleTestSource(client omeclient.OmeV1beta1Interface, namespace, name string, target waitscale.Target) *waitscale.Source {
	return waitscale.NewSource(client, fakeScaleReplicaReader{client: client}, namespace, name, target)
}

func TestScaleSourceReadsOnlyExactParentAndSelectedIR(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	source := newScaleTestSource(client.OmeV1beta1(), "prod", "chat", waitscale.Target{Component: ome.EngineComponent, Replicas: 2})
	snapshot, err := source.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, evidence.Parent.UID, snapshot.UID)
	require.Equal(t, evidence.Parent.ResourceVersion, snapshot.ResourceVersion)
	decision, observed := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(snapshot.Value)
	require.True(t, decision.Matched)
	require.Equal(t, "Valid", observed.Validity)
	require.Len(t, client.Actions(), 3)
	for i, expected := range []struct{ resource, name string }{{"inferenceservices", "chat"}, {"inferencereplicas", "chat-engine"}, {"inferenceservices", "chat"}} {
		get, ok := client.Actions()[i].(ktesting.GetAction)
		require.True(t, ok, "action %d must be a named GET, not LIST", i)
		require.Equal(t, expected.resource, get.GetResource().Resource)
		require.Equal(t, "prod", get.GetNamespace())
		require.Equal(t, expected.name, get.GetName())
	}
}

func TestScaleSourceAllowsMissingIRButNotBroadFallback(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	client := omefake.NewSimpleClientset(evidence.Parent)
	source := newScaleTestSource(client.OmeV1beta1(), "prod", "chat", waitscale.Target{Component: ome.EngineComponent, Replicas: 2})
	snapshot, err := source.Get(context.Background())
	require.NoError(t, err)
	require.Nil(t, snapshot.Value.Replica)
	decision, observed := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(snapshot.Value)
	require.False(t, decision.Matched)
	require.Equal(t, "Unavailable", observed.Validity)
	require.Len(t, client.Actions(), 3)
	require.Equal(t, "get", client.Actions()[1].GetVerb())
	require.Equal(t, "inferenceservices", client.Actions()[2].GetResource().Resource)
}

func TestScaleSourceMissingTargetMakesOnlyParentGET(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	evidence.Parent.Status.Components = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	snapshot, err := newScaleTestSource(client.OmeV1beta1(), "prod", "chat", waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Get(context.Background())
	require.NoError(t, err)
	require.Nil(t, snapshot.Value.Replica)
	require.Len(t, client.Actions(), 1)
	require.Equal(t, "inferenceservices", client.Actions()[0].GetResource().Resource)
}

func TestScaleSourceOptionalActionIdentitySelectsOneExactIR(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	evidence.Parent.Status.Components = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	target := waitscale.Target{Component: ome.EngineComponent, Replicas: 2, IRName: evidence.Replica.Name, IRUID: evidence.Replica.UID}
	snapshot, err := newScaleTestSource(client.OmeV1beta1(), "prod", "chat", target).Get(context.Background())
	require.NoError(t, err)
	require.Len(t, client.Actions(), 3)
	require.Equal(t, "chat-engine", client.Actions()[1].(ktesting.GetAction).GetName())
	decision, _ := waitscale.NewEvaluator(target).Evaluate(snapshot.Value)
	require.True(t, decision.Matched)
}

func TestScaleSourceDiscardsTornParentAndIRSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ome.InferenceService)
	}{
		{"generation changed", func(p *ome.InferenceService) { p.Generation++ }},
		{"selected ref changed", func(p *ome.InferenceService) {
			status := p.Status.Components[ome.EngineComponent]
			status.ScaleTargetRef.Name = "other-engine"
			p.Status.Components[ome.EngineComponent] = status
		}},
		{"UID replaced", func(p *ome.InferenceService) { p.UID = "replacement-parent" }},
		{"deletion", func(p *ome.InferenceService) { now := metav1.NewTime(time.Unix(3000, 0)); p.DeletionTimestamp = &now }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := scaleEvidence(2, 2, 1)
			client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
			client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
				changed := evidence.Parent.DeepCopy()
				changed.ResourceVersion = "13"
				tc.change(changed)
				err := client.Tracker().Update(schema.GroupVersionResource{Group: "ome.io", Version: "v1beta1", Resource: "inferenceservices"}, changed, "prod")
				require.NoError(t, err)
				return false, nil, nil
			})
			snapshot, err := newScaleTestSource(client.OmeV1beta1(), "prod", "chat", waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Get(context.Background())
			require.NoError(t, err)
			require.Len(t, client.Actions(), 3)
			require.Equal(t, "13", snapshot.ResourceVersion)
			require.Nil(t, snapshot.Value.Replica)
			decision, _ := waitscale.NewEvaluator(waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Evaluate(snapshot.Value)
			require.False(t, decision.Matched)
			if tc.name == "UID replaced" {
				require.Equal(t, "replacement-parent", string(snapshot.UID))
			}
			if tc.name == "deletion" {
				require.True(t, snapshot.Deleting)
			}
		})
	}
}

func TestScaleSourcePropagatesIRReadDenialAndCancellation(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"}, "chat-engine", errors.New("secret error"))
	})
	_, err := newScaleTestSource(client.OmeV1beta1(), "prod", "chat", waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Get(context.Background())
	require.Error(t, err)
	require.True(t, apierrors.IsForbidden(err))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	client = omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	_, err = newScaleTestSource(client.OmeV1beta1(), "prod", "chat", waitscale.Target{Component: ome.EngineComponent, Replicas: 2}).Get(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, client.Actions())
}

func TestScaleSourceRejectsInvalidInputWithoutCallsAndDisablesWatch(t *testing.T) {
	evidence := scaleEvidence(2, 2, 1)
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	source := newScaleTestSource(client.OmeV1beta1(), "prod", "chat", waitscale.Target{Component: ome.EngineComponent, Replicas: 0})
	_, err := source.Get(context.Background())
	require.Error(t, err)
	require.Empty(t, client.Actions())
	_, err = source.Watch(context.Background(), "11")
	require.Error(t, err)
	_, err = source.Decode(evidence.Parent)
	require.Error(t, err)
}
