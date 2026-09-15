package mutate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func completedScaleEvidenceFixture(t *testing.T, edits ...func(*v1beta1.InferenceService)) (ScaleEvidence, *effective.RuntimeState, *v1beta1.InferenceReplica) {
	t.Helper()
	p, _, selected, scale, _ := scaleFixture(t)
	p.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: p.Spec.Engine.ComponentExtensionSpec}
	engine := p.Status.Components[v1beta1.EngineComponent]
	pinnedCanary(t, p)
	group := &p.Status.Rollout.ActiveRun.Plan.Groups[0]
	group.Group.Components = append(group.Group.Components, v1beta1.DecoderComponent)
	digest, err := rolloutpolicy.ProgressionDigest(&group.Group)
	require.NoError(t, err)
	group.PortableDigest = digest
	p.Status.Rollout.ActiveRun.TargetRevisions = append(p.Status.Rollout.ActiveRun.TargetRevisions, v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "bbbbbbbb"})
	for _, component := range group.Group.Components {
		status := engine
		name := p.Name + "-" + string(component)
		status.LatestReadyRevision, status.LatestRolledoutRevision = name+"-rev-bbbbbbbb", name+"-rev-bbbbbbbb"
		status.Traffic = []v1beta1.ComponentTrafficTarget{{RevisionName: name + "-rev-bbbbbbbb", Percent: 100}}
		status.ScaleTargetRef = &v1beta1.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: name}
		p.Status.Components[component] = status
	}
	p.Status.Canary.CurrentStep, p.Status.Canary.ObservedTrafficWeight = 2, 100
	p.Status.Canary.StableRevisionHash = ""
	p.Status.Canary.TargetID = "ct1:" + rolloutpolicy.ShortHash([]byte("decoder=bbbbbbbb;engine=bbbbbbbb"))
	for _, edit := range edits {
		edit(p)
	}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	reader := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: p.Namespace, UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}}).Build()
	live, err := effective.NewBoundedRuntimeResolver(reader, paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 1e9})
	require.NoError(t, err)
	pins, err := effective.NewRuntimePinResolver(kubefake.NewClientset().AppsV1(), live, "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 1e9})
	require.NoError(t, err)
	state, err := pins.Resolve(context.Background(), p, effective.RuntimeResolveOptions{})
	require.NoError(t, err)
	source, err := effective.ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
	require.NoError(t, err)
	evidence, err := InspectScaleEvidence(p, selected, scale, source, testClock)
	require.NoError(t, err)
	sibling := selected.DeepCopy()
	sibling.Name, sibling.UID, sibling.ResourceVersion = "chat-decoder", "uid-decoder", "91"
	sibling.Spec.Component = v1beta1.DecoderComponent
	sibling.Status.CurrentRevision, sibling.Status.UpdateRevision = "chat-decoder-bbbbbbbb", "chat-decoder-bbbbbbbb"
	return evidence, state, sibling
}

func TestCollectScalePinnedTargetsExactPrivateSnapshots(t *testing.T) {
	evidence, state, sibling := completedScaleEvidenceFixture(t)
	client := omefake.NewSimpleClientset(evidence.replica, sibling)
	collected, err := CollectScalePinnedTargets(context.Background(), client.OmeV1beta1(), evidence, testClock)
	require.NoError(t, err)
	require.Len(t, collected.replicas, 2)
	require.Len(t, client.Actions(), 1)
	require.Equal(t, "get", client.Actions()[0].GetVerb())
	require.Equal(t, sibling.Name, client.Actions()[0].(ktesting.GetAction).GetName())
	require.Equal(t, evidence.parent.Namespace, client.Actions()[0].GetNamespace())
	require.NoError(t, collected.Revalidate(context.Background(), client.OmeV1beta1(), testClock))
	require.Len(t, client.Actions(), 3)
	plan, err := PrepareScale(evidence.parent, state, collected, v1beta1.EngineComponent, 3, true, true, testClock)
	require.NoError(t, err)
	require.Contains(t, plan.Details().Warnings, "ConditionalPinnedIRReads")
	require.Len(t, plan.Details().Sources, 2)
	// Caller mutations cannot alter retained other-target authority.
	sibling.Status.UpdateRevision = "chat-decoder-cccccccc"
	require.Equal(t, "bbbbbbbb", collected.pinnedSources().sources[v1beta1.DecoderComponent])
	require.Len(t, evidence.replicas, 1, "collection does not mutate its input")
	_, err = PrepareScale(evidence.parent, state, evidence, v1beta1.EngineComponent, 3, true, true, testClock)
	require.ErrorIs(t, err, ErrStale, "missing sibling authority must never pass")
}

func TestCollectScalePinnedTargetsAndRevalidationFailureMatrix(t *testing.T) {
	for _, mode := range []string{"none", "get error", "wrong component", "wrong key", "missing replicas", "invalid generation", "invalid ref", "invalid pinned run", "final drift", "final error"} {
		t.Run(mode, func(t *testing.T) {
			evidence, _, sibling := completedScaleEvidenceFixture(t, func(p *v1beta1.InferenceService) {
				if mode == "invalid ref" {
					p.Status.Components[v1beta1.DecoderComponent].ScaleTargetRef.Kind = "Deployment"
				}
				if mode == "invalid pinned run" {
					p.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest = "other"
				}
			})
			client := omefake.NewSimpleClientset(evidence.replica, sibling)
			switch mode {
			case "none":
				p, _, r, s, source := scaleFixture(t)
				var err error
				evidence, err = InspectScaleEvidence(p, r, s, source, nil)
				require.NoError(t, err)
			case "get error":
				client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("private-pinned-error")
				})
			case "wrong component", "wrong key", "missing replicas", "invalid generation":
				client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
					r := sibling.DeepCopy()
					switch mode {
					case "wrong component":
						r.Spec.Component = v1beta1.RouterComponent
					case "wrong key":
						r.Name = "other"
					case "missing replicas":
						r.Spec.Replicas = nil
					case "invalid generation":
						r.Generation = 0
					}
					return true, r, nil
				})
			}
			collected, err := CollectScalePinnedTargets(context.Background(), client.OmeV1beta1(), evidence, nil)
			if mode != "none" && mode != "final drift" && mode != "final error" {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "private")
				return
			}
			require.NoError(t, err)
			if mode == "none" {
				require.Empty(t, client.Actions())
				return
			}
			client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
				if mode == "final error" {
					return true, nil, errors.New("private-pinned-error")
				}
				r := evidence.replica.DeepCopy()
				if action.(ktesting.GetAction).GetName() == sibling.Name {
					r = sibling.DeepCopy()
				}
				r.Spec.Replicas = ptr.To[int32](2)
				return true, r, nil
			})
			err = collected.Revalidate(context.Background(), client.OmeV1beta1(), testClock)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func TestCollectScalePinnedTargetsAtMostTwoExactSiblingReads(t *testing.T) {
	evidence, state, sibling := completedScaleEvidenceFixture(t, func(p *v1beta1.InferenceService) {
		p.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: p.Spec.Engine.ComponentExtensionSpec}
		group := &p.Status.Rollout.ActiveRun.Plan.Groups[0]
		group.Group.Components = append(group.Group.Components, v1beta1.RouterComponent)
		digest, err := rolloutpolicy.ProgressionDigest(&group.Group)
		require.NoError(t, err)
		group.PortableDigest = digest
		p.Status.Rollout.ActiveRun.TargetRevisions = append(p.Status.Rollout.ActiveRun.TargetRevisions, v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "bbbbbbbb"})
		status := p.Status.Components[v1beta1.DecoderComponent]
		status.ScaleTargetRef = &v1beta1.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-router"}
		status.LatestReadyRevision, status.LatestRolledoutRevision = "chat-router-rev-bbbbbbbb", "chat-router-rev-bbbbbbbb"
		status.Traffic = []v1beta1.ComponentTrafficTarget{{RevisionName: "chat-router-rev-bbbbbbbb", Percent: 100}}
		p.Status.Components[v1beta1.RouterComponent] = status
		p.Status.Canary.TargetID = "ct1:" + rolloutpolicy.ShortHash([]byte("decoder=bbbbbbbb;engine=bbbbbbbb;router=bbbbbbbb"))
	})
	router := sibling.DeepCopy()
	router.Name, router.UID, router.ResourceVersion = "chat-router", "uid-router", "101"
	router.Spec.Component = v1beta1.RouterComponent
	router.Status.CurrentRevision, router.Status.UpdateRevision = "chat-router-bbbbbbbb", "chat-router-bbbbbbbb"
	client := omefake.NewSimpleClientset(evidence.replica, sibling, router)
	collected, err := CollectScalePinnedTargets(context.Background(), client.OmeV1beta1(), evidence, testClock)
	require.NoError(t, err)
	require.Len(t, client.Actions(), 2)
	require.Len(t, collected.replicas, 3)
	for _, action := range client.Actions() {
		require.Equal(t, "get", action.GetVerb())
		require.Empty(t, action.GetSubresource())
	}
	require.NoError(t, collected.Revalidate(context.Background(), client.OmeV1beta1(), testClock))
	require.Len(t, client.Actions(), 5)
	_, err = PrepareScale(evidence.parent, state, collected, v1beta1.EngineComponent, 3, true, true, testClock)
	require.NoError(t, err)
}

func TestScalePinnedZeroNilCancelledAndUnknownRefsRefuse(t *testing.T) {
	client := omefake.NewSimpleClientset().OmeV1beta1()
	evidence, _, _ := completedScaleEvidenceFixture(t)
	for _, ctx := range []context.Context{nil, context.Background()} {
		_, err := CollectScalePinnedTargets(ctx, client, ScaleEvidence{}, testClock)
		require.Error(t, err)
		require.Error(t, (ScaleEvidence{}).Revalidate(ctx, client, testClock))
	}
	_, err := CollectScalePinnedTargets(context.Background(), nil, evidence, testClock)
	require.Error(t, err)
	require.Error(t, evidence.Revalidate(context.Background(), nil, testClock))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = CollectScalePinnedTargets(ctx, client, evidence, testClock)
	require.Error(t, err)
	require.Error(t, evidence.Revalidate(ctx, client, testClock))
	for _, ref := range []*v1beta1.ScaleTargetRef{nil, {}, {APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "../../other"}, {APIVersion: "ome.io/v1beta1", Kind: "Deployment", Name: "x"}, {APIVersion: "apps/v1", Kind: "InferenceReplica", Name: "x"}} {
		require.False(t, validScaleReplicaRef(ref))
	}
	require.NoError(t, inspectScaleTargetReplica(evidence.parent, evidence.replica, v1beta1.EngineComponent, evidence.replica.Name, nil))
	require.Error(t, inspectScaleTargetReplica(evidence.parent, nil, v1beta1.EngineComponent, "x", nil))
	copy := evidence.replica.DeepCopy()
	copy.Status.UpdateRevision = ""
	evidence.replicas[v1beta1.EngineComponent] = copy
	require.Equal(t, "bbbbbbbb", evidence.pinnedSources().sources[v1beta1.EngineComponent])
}

func TestScalePinnedCancellationDuringProofReadRefuses(t *testing.T) {
	for _, phase := range []string{"collection", "final revalidation"} {
		t.Run(phase, func(t *testing.T) {
			evidence, _, sibling := completedScaleEvidenceFixture(t)
			client := omefake.NewSimpleClientset(evidence.replica, sibling)
			collected, err := CollectScalePinnedTargets(context.Background(), client.OmeV1beta1(), evidence, testClock)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.(ktesting.GetAction).GetName() == sibling.Name {
					cancel()
					return true, sibling.DeepCopy(), nil
				}
				return true, evidence.replica.DeepCopy(), nil
			})
			if phase == "collection" {
				_, err = CollectScalePinnedTargets(ctx, client.OmeV1beta1(), evidence, testClock)
			} else {
				err = collected.Revalidate(ctx, client.OmeV1beta1(), testClock)
			}
			require.Error(t, err, "a client ignoring canceled context cannot authorize success")
		})
	}
}
