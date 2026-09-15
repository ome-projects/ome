package effective

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestAutoscalingRetainsPrivateProjectedBlockForGuardedScale(t *testing.T) {
	parent := autoscaleISVC(constants.OMENative)
	parent.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
	resolution, err := ResolveAutoscaling(parent, autoscaleRuntimeState(t, parent, baseAutoscaleRuntime()))
	require.NoError(t, err)
	field := reflect.ValueOf(resolution.Components[0]).FieldByName("autoscaler")
	require.True(t, field.IsValid(), "scale proof must retain controller-resolved payload privately")
	require.False(t, field.IsNil())
	require.False(t, field.CanInterface(), "projected payload must remain private")
}

func manualSourceFixture(t *testing.T) (*v1beta1.InferenceService, *RuntimeState, *v1beta1.ServingRuntime) {
	t.Helper()
	p := autoscaleISVC(constants.OMENative)
	p.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
	p.Spec.Engine.MinReplicas = ptr.To(1)
	p.Spec.Engine.MaxReplicas = 10
	p.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {ScaleTargetRef: &v1beta1.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "svc-engine"}, Autoscaler: &v1beta1.ComponentAutoscalerStatus{Class: v1beta1.AutoscalerNone, ManagedBy: "none", SpecSource: "isvc"}}}
	rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: p.Namespace, UID: "runtime-uid", ResourceVersion: "29", Generation: 2}, Spec: *baseAutoscaleRuntime()}
	state := autoscaleRuntimeState(t, p, &rt.Spec)
	state.PinMode = RuntimePinModeAutoSync
	state.SyncTokenState = SyncTokenStateAbsent
	state.DriftState = RuntimeDriftStateNotReported
	state.live.Runtime.IdentityObserved = true
	state.live.Runtime.UID = "runtime-uid"
	state.live.Runtime.Generation = 2
	state.live.Runtime.resourceVersion = "29"
	return p, state, rt
}

func TestManualScaleSourceGlobalGenerationAdvisoryAndPrivateProof(t *testing.T) {
	for _, observed := range []int64{0, 3, 4, 8} {
		t.Run(fmt.Sprint(observed), func(t *testing.T) {
			p, state, _ := manualSourceFixture(t)
			p.Status.ObservedGeneration = observed
			source, err := ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
			require.NoError(t, err)
			summary := source.Summary()
			require.Equal(t, int32(1), summary.Minimum)
			require.Equal(t, int32(10), summary.Maximum)
			summary.Sources[0].UID = "altered"
			require.NotEqual(t, "altered", source.Summary().Sources[0].UID)
			r := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "svc-engine", Namespace: p.Namespace}, Spec: v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent, Autoscaler: p.Spec.Engine.Autoscaler.DeepCopy()}}
			require.NoError(t, source.ValidateReplica(r))
			r.Spec.Autoscaler.Class = v1beta1.AutoscalerExternal
			require.ErrorIs(t, source.ValidateReplica(r), ErrManualScaleSource)
			_, err = json.Marshal(source)
			require.Error(t, err)
			_, err = source.MarshalYAML()
			require.Error(t, err)
			require.Equal(t, "<effective.ManualScaleSource redacted>", fmt.Sprintf("%v", source))
			require.NotContains(t, fmt.Sprintf("%#v", source), "29")
		})
	}
}

func TestManualScaleSourceOwnershipAndUnavailableMatrix(t *testing.T) {
	changes := []struct {
		name string
		edit func(*v1beta1.InferenceService, *RuntimeState)
	}{
		{"unbound", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.inferenceService.resourceVersion = "other" }},
		{"generation", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.Generation++ }},
		{"sync pending", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.SyncTokenState = SyncTokenStatePending }},
		{"sync malformed", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.SyncTokenState = "private-sync" }},
		{"drift malformed", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.DriftState = RuntimeDriftStateMalformed }},
		{"live drift", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.DriftState = RuntimeDriftStateReportedTrue }},
		{"managed live", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.PinMode = RuntimePinModeManagedPin }},
		{"unobserved runtime", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.live.Runtime.IdentityObserved = false }},
		{"missing chain", func(_ *v1beta1.InferenceService, s *RuntimeState) {
			s.live.Runtime.DeclaredInheritance = InheritanceObservation{}
		}},
		{"wrong component", func(_ *v1beta1.InferenceService, s *RuntimeState) { s.active.components = nil }},
		{"unknown class", func(p *v1beta1.InferenceService, _ *RuntimeState) {
			value := p.Status.Components[v1beta1.EngineComponent]
			value.Autoscaler.Class = "private-class"
		}},
		{"owner mismatch", func(p *v1beta1.InferenceService, _ *RuntimeState) {
			p.Status.Components[v1beta1.EngineComponent].Autoscaler.ManagedBy = "external"
		}},
		{"source mismatch", func(p *v1beta1.InferenceService, _ *RuntimeState) {
			p.Status.Components[v1beta1.EngineComponent].Autoscaler.SpecSource = "runtime"
		}},
		{"remote target", func(p *v1beta1.InferenceService, _ *RuntimeState) {
			p.Status.Components[v1beta1.EngineComponent].ScaleTargetRef.Kind = "Deployment"
		}},
		{"held LKG", func(p *v1beta1.InferenceService, _ *RuntimeState) {
			p.Status.Components[v1beta1.EngineComponent].Autoscaler.Policy = &v1beta1.AutoscalerPolicyProvenance{Name: "private-policy"}
		}},
		{"stale resolution", func(p *v1beta1.InferenceService, _ *RuntimeState) {
			p.Status.Components[v1beta1.EngineComponent].Autoscaler.Conditions = []metav1.Condition{{Type: v1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionTrue, ObservedGeneration: 3, Reason: v1beta1.AutoscalerResolvedReasonInlinePrecedence}}
		}},
		{"unsupported scaling", func(p *v1beta1.InferenceService, _ *RuntimeState) {
			p.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional}
		}},
	}
	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			p, state, _ := manualSourceFixture(t)
			tc.edit(p, state)
			_, err := ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private-")
		})
	}
	p, state, _ := manualSourceFixture(t)
	p.Spec.Engine.Autoscaler = nil
	p.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{Name: "fleet"}
	_, err := ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
	require.ErrorIs(t, err, ErrManualScalePolicy)
	p, state, _ = manualSourceFixture(t)
	p.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{Name: "fleet"}
	status := p.Status.Components[v1beta1.EngineComponent].Autoscaler
	status.ShadowedPolicyRef = &v1beta1.ShadowedAutoscalerPolicy{Name: "fleet"}
	status.Conditions = []metav1.Condition{{Type: v1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionTrue, ObservedGeneration: p.Generation, Reason: v1beta1.AutoscalerResolvedReasonInlinePrecedence}}
	_, err = ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
	require.NoError(t, err)
}

type sourceReader struct {
	ctrlclient.Reader
	edit  func(ctrlclient.Object)
	calls int
}

func (r *sourceReader) Get(ctx context.Context, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	r.calls++
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if r.edit != nil {
		r.edit(obj)
	}
	return nil
}

func TestManualScaleSourceExactRevalidationAndCancellation(t *testing.T) {
	p, state, rt := manualSourceFixture(t)
	source, err := ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
	require.NoError(t, err)
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	reader := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(p, rt).Build()
	require.NoError(t, source.Revalidate(context.Background(), reader))
	for _, field := range []string{"name", "namespace", "uid", "rv", "generation", "deletion", "gvk"} {
		t.Run(field, func(t *testing.T) {
			r := &sourceReader{Reader: reader, edit: func(obj ctrlclient.Object) {
				switch field {
				case "name":
					obj.SetName("other")
				case "namespace":
					obj.SetNamespace("other")
				case "uid":
					obj.SetUID("other")
				case "rv":
					obj.SetResourceVersion("other")
				case "generation":
					obj.SetGeneration(8)
				case "deletion":
					now := metav1.Now()
					obj.SetDeletionTimestamp(&now)
				case "gvk":
					obj.GetObjectKind().SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
				}
			}}
			require.ErrorIs(t, source.Revalidate(context.Background(), r), ErrManualScaleSourceChanged)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &sourceReader{Reader: reader}
	require.ErrorIs(t, source.Revalidate(ctx, r), context.Canceled)
	require.Zero(t, r.calls)
	require.ErrorIs(t, (ManualScaleSource{}).Revalidate(context.Background(), reader), ErrManualScaleSource)
	require.False(t, (ManualScaleSource{}).MatchesParent(p))
	require.Error(t, (ManualScaleSource{}).ValidateReplica(nil))
	for _, text := range []string{fmt.Sprint(source), fmt.Sprintf("%#v", source)} {
		require.False(t, strings.Contains(text, "resourceVersion"))
	}
}

func TestManualScaleSourceRejectsUnsafeOrUnboundInheritanceHead(t *testing.T) {
	for _, field := range []string{"name", "uid", "generation", "namespace"} {
		t.Run(field, func(t *testing.T) {
			p, state, _ := manualSourceFixture(t)
			chain := state.live.Runtime.DeclaredInheritance.Chain()
			switch field {
			case "name":
				chain[0].Name = "other"
			case "uid":
				chain[0].UID = "private@credential"
			case "generation":
				chain[0].Generation = 3
			case "namespace":
				chain[0].Namespace = "elsewhere"
			}
			state.live.Runtime.DeclaredInheritance = observedInheritance(chain)
			_, err := ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
			require.ErrorIs(t, err, ErrManualScaleSource)
		})
	}
}

func TestManualScaleSourceRealRootFirstInheritanceRemainsBound(t *testing.T) {
	p, _, head := manualSourceFixture(t)
	kind := "ServingRuntime"
	p.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: head.Name, Kind: &kind, AutoSync: ptr.To(true)}
	head.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: "base"}
	base := head.DeepCopy()
	base.Name = "base"
	base.UID = "base-uid"
	base.ResourceVersion = "20"
	base.Annotations = nil
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	reader := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(p, head, base).Build()
	live, err := NewBoundedRuntimeResolver(reader, paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 1e9})
	require.NoError(t, err)
	pins, err := NewRuntimePinResolver(kubefake.NewClientset().AppsV1(), live, "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: 1e9})
	require.NoError(t, err)
	state, err := pins.Resolve(context.Background(), p, RuntimeResolveOptions{})
	require.NoError(t, err)
	source, err := ResolveManualScaleSource(p, state, v1beta1.EngineComponent)
	require.NoError(t, err)
	require.Len(t, source.Summary().Sources, 2)
	require.NoError(t, source.Revalidate(context.Background(), reader))
}
