package endpoint

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// Fixtures of the TrafficMap publisher tests: the recording publisher and its
// preflighting and fallback variants, the owner and map builders, and the client
// options that fail or change a read on cue.

type testTrafficMapPublishPlan struct {
	claims []string
}

func (p testTrafficMapPublishPlan) Claims() []string { return p.claims }

type recordingTrafficMapPublisher struct {
	name      string
	stateful  bool
	claims    []string
	events    []string
	resolve   func(map[string]string, map[string]string) (map[string]string, error)
	plan      func(*v1beta1.InferenceService, *v1beta1.TrafficMap, map[string]string) (TrafficMapPublishPlan, error)
	drain     func(context.Context, types.NamespacedName, []string) error
	apply     func(context.Context, TrafficMapPublishPlan) (TrafficMapPublishResult, error)
	unpublish func(context.Context, types.NamespacedName, v1beta1.TrafficMapPublisherStatus) error
}

type preflightingTrafficMapPublisher struct {
	*recordingTrafficMapPublisher
	preflight func(context.Context, TrafficMapPublishPlan) error
}

type fallbackTrafficMapPublisher struct {
	*recordingTrafficMapPublisher
	capture func(TrafficMapPublishPlan) (TrafficMapPublishPlanCapture, error)
	replay  func(v1beta1.TrafficMapPublisherLastPositive) (TrafficMapPublishPlan, error)
}

func (p *fallbackTrafficMapPublisher) Capture(
	plan TrafficMapPublishPlan,
) (TrafficMapPublishPlanCapture, error) {
	if p.capture != nil {
		return p.capture(plan)
	}
	return TrafficMapPublishPlanCapture{}, errors.New("capture is not configured")
}

func (p *fallbackTrafficMapPublisher) Replay(
	lastPositive v1beta1.TrafficMapPublisherLastPositive,
) (TrafficMapPublishPlan, error) {
	p.events = append(p.events, "replay")
	if p.replay != nil {
		return p.replay(lastPositive)
	}
	return nil, errors.New("replay is not configured")
}

func (p *preflightingTrafficMapPublisher) Preflight(
	ctx context.Context,
	plan TrafficMapPublishPlan,
) error {
	p.events = append(p.events, "preflight")
	if p.preflight != nil {
		return p.preflight(ctx, plan)
	}
	return nil
}

func (p *recordingTrafficMapPublisher) Name() string { return p.name }

func (p *recordingTrafficMapPublisher) Stateful() bool {
	return p.stateful
}

func (p *recordingTrafficMapPublisher) ResolveOptions(global, inline map[string]string) (map[string]string, error) {
	if p.resolve != nil {
		return p.resolve(global, inline)
	}
	resolved := clonePublisherOptions(global)
	for key, value := range inline {
		resolved[key] = value
	}
	return resolved, nil
}

func (p *recordingTrafficMapPublisher) Plan(
	owner *v1beta1.InferenceService,
	trafficMap *v1beta1.TrafficMap,
	options map[string]string,
) (TrafficMapPublishPlan, error) {
	if p.plan != nil {
		return p.plan(owner, trafficMap, options)
	}
	return testTrafficMapPublishPlan{claims: p.claims}, nil
}

func (p *recordingTrafficMapPublisher) Drain(
	ctx context.Context,
	key types.NamespacedName,
	claims []string,
) error {
	p.events = append(p.events, "drain:"+strings.Join(claims, ","))
	if p.drain != nil {
		return p.drain(ctx, key, claims)
	}
	return nil
}

func (p *recordingTrafficMapPublisher) Apply(
	ctx context.Context,
	plan TrafficMapPublishPlan,
) (TrafficMapPublishResult, error) {
	p.events = append(p.events, "apply")
	if p.apply != nil {
		return p.apply(ctx, plan)
	}
	return TrafficMapPublishResult{}, nil
}

func (p *recordingTrafficMapPublisher) Unpublish(
	ctx context.Context,
	key types.NamespacedName,
	journal v1beta1.TrafficMapPublisherStatus,
) error {
	p.events = append(p.events, "unpublish:"+strings.Join(journal.ClaimedTargets, ","))
	if p.unpublish != nil {
		return p.unpublish(ctx, key, journal)
	}
	return nil
}

func publisherTestObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	controller := true
	owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "team-a", UID: types.UID("owner-uid"), Generation: 2,
	}, Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{
		Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle,
	}}}
	trafficMap := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "model", Namespace: "team-a", UID: types.UID("map-uid"), Generation: 3,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: owner.Name, UID: owner.UID, Controller: &controller,
			}},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service: "model", ObservedISVCGeneration: owner.Generation,
		},
		Status: v1beta1.TrafficMapStatus{SourceUID: owner.UID},
	}
	return owner, trafficMap
}

func eligiblePlacementLossPublisherObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	owner, trafficMap := publisherTestObjects()
	owner.Status.ObservedGeneration = owner.Generation
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase:   v1beta1.PlacementPhasePending,
		Cluster: "cluster-a",
	}
	owner.Status.Conditions = duckv1.Conditions{{
		Type:   apis.ConditionReady,
		Status: corev1.ConditionUnknown,
		Reason: placementcontroller.PlacementReadyReasonLost,
	}}
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Spec.Mode = v1beta1.PlacementModeSingle
	trafficMap.Status.Conditions = []metav1.Condition{
		{
			Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
			Reason: v1beta1.TrafficMapReasonNotPlaced, ObservedGeneration: trafficMap.Generation,
		},
		{
			Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
			Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: trafficMap.Generation,
		},
	}
	return owner, trafficMap
}

func allHomesUnreadyPublisherObjects() (*v1beta1.InferenceService, *v1beta1.TrafficMap) {
	owner, trafficMap := eligiblePlacementLossPublisherObjects()
	owner.Generation++
	owner.Status.ObservedGeneration = owner.Generation
	owner.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	owner.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{
			{Cluster: "cluster-a", Endpoint: &apis.URL{Scheme: "https", Host: "a.example"}, AdmittedReplicas: 1},
			{Cluster: "cluster-b", Endpoint: &apis.URL{Scheme: "https", Host: "b.example"}, AdmittedReplicas: 1},
			{Cluster: "cluster-c", Endpoint: &apis.URL{Scheme: "https", Host: "c.example"}, AdmittedReplicas: 1},
		},
	}
	owner.Status.Conditions[0].Status = corev1.ConditionFalse
	owner.Status.Conditions[0].Reason = "PlacementNotReady"
	trafficMap.Spec.Mode = v1beta1.PlacementModeSplit
	trafficMap.Spec.ObservedISVCGeneration = owner.Generation
	trafficMap.Spec.Entries = []v1beta1.TrafficMapEntry{
		{Cluster: "cluster-a", Endpoint: &apis.URL{Scheme: "https", Host: "a.example"}, Capacity: &v1beta1.TrafficMapCapacity{Allocated: 1}},
		{Cluster: "cluster-b", Endpoint: &apis.URL{Scheme: "https", Host: "b.example"}, Capacity: &v1beta1.TrafficMapCapacity{Allocated: 1}},
		{Cluster: "cluster-c", Endpoint: &apis.URL{Scheme: "https", Host: "c.example"}, Capacity: &v1beta1.TrafficMapCapacity{Allocated: 1}},
	}
	apimeta.FindStatusCondition(
		trafficMap.Status.Conditions, v1beta1.TrafficMapRoutable,
	).Reason = v1beta1.TrafficMapReasonAllHomesUnready
	return owner, trafficMap
}

// withZeroReports gives every entry an accepted report of zero capacity.
func withZeroReports(trafficMap *v1beta1.TrafficMap) {
	for i := range trafficMap.Spec.Entries {
		zero := resource.MustParse("0")
		trafficMap.Spec.Entries[i].Capacity.ReportedWeight = &zero
	}
}

func restoreEligiblePublisherRoutingConditions(
	t *testing.T,
	kubeClient client.Client,
	trafficMap *v1beta1.TrafficMap,
) {
	t.Helper()
	live := getPublisherTrafficMap(t, kubeClient, trafficMap)
	apimeta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionFalse,
		Reason: v1beta1.TrafficMapReasonNotPlaced, ObservedGeneration: live.Generation,
	})
	apimeta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse,
		Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: live.Generation,
	})
	require.NoError(t, kubeClient.Status().Update(context.Background(), live))
}

func testPublisherDigest(hexDigit string) string {
	return "sha256:" + strings.Repeat(hexDigit, sha256.Size*2)
}

func publisherClaimedTrafficMap(name, publisher string, claims []string) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID("uid-" + name)},
		Status: v1beta1.TrafficMapStatus{Publisher: &v1beta1.TrafficMapPublisherStatus{
			PublisherName: publisher, ClaimedTargets: claims,
		}},
	}
}

func numberedClaims(count int) []string {
	claims := make([]string, count)
	for i := range claims {
		claims[i] = fmt.Sprintf("target-%03d", i)
	}
	return claims
}

func newTrafficMapPublisherReconciler(
	t *testing.T,
	publisher TrafficMapPublisher,
	objects ...client.Object,
) (*TrafficMapPublisherReconciler, client.WithWatch) {
	t.Helper()
	kubeClient := newTrafficMapPublisherClient(t, objects...)
	return &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}, kubeClient
}

func newTrafficMapPublisherClient(t *testing.T, objects ...client.Object) client.WithWatch {
	t.Helper()
	return fakeclient.New(t, fakeclient.WithStatusSubresource(&v1beta1.TrafficMap{}), fakeclient.WithObjects(objects...))
}

func reconcileTrafficMapPublisher(
	t *testing.T,
	reconciler *TrafficMapPublisherReconciler,
	trafficMap *v1beta1.TrafficMap,
) (ctrl.Result, error) {
	t.Helper()
	return reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(trafficMap),
	})
}

func getPublisherTrafficMap(t *testing.T, kubeClient client.Client, object client.Object) *v1beta1.TrafficMap {
	t.Helper()
	current := &v1beta1.TrafficMap{}
	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(object), current))
	return current
}

func publisherApplyContent(t *testing.T, apply runtime.ApplyConfiguration) map[string]any {
	t.Helper()
	unstructuredApply, ok := apply.(interface{ UnstructuredContent() map[string]any })
	require.True(t, ok)
	return unstructuredApply.UnstructuredContent()
}

// changedOnLaterReads answers the first read of the TrafficMap and of its
// owner from the store and changes every later one: mutateTrafficMap and
// mutateOwner run on the copy read, or the owner read fails with ownerErr.
// That is what a reconciler meets when the objects move between its first
// authoritative read and the one before its effects.
func changedOnLaterReads(
	mutateTrafficMap func(*v1beta1.TrafficMap),
	mutateOwner func(*v1beta1.InferenceService),
	ownerErr error,
) fakeclient.Option {
	trafficMapGets, ownerGets := 0, 0
	return fakeclient.WithInterceptors(interceptor.Funcs{
		Get: func(ctx context.Context, next client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
			switch object.(type) {
			case *v1beta1.TrafficMap:
				trafficMapGets++
			case *v1beta1.InferenceService:
				ownerGets++
				if ownerGets > 1 && ownerErr != nil {
					return ownerErr
				}
			}
			if err := next.Get(ctx, key, object, opts...); err != nil {
				return err
			}
			switch typed := object.(type) {
			case *v1beta1.TrafficMap:
				if trafficMapGets > 1 && mutateTrafficMap != nil {
					mutateTrafficMap(typed)
				}
			case *v1beta1.InferenceService:
				if ownerGets > 1 && mutateOwner != nil {
					mutateOwner(typed)
				}
			}
			return nil
		},
	})
}

// statusWriteConflictsOnce refuses the first status write, an apply or a
// patch, with a conflict after running beforeConflict, which stands for the
// writer that won; every later write reaches the store.
func statusWriteConflictsOnce(beforeConflict func()) fakeclient.Option {
	conflicted := false
	conflict := func(name string) error {
		if conflicted {
			return nil
		}
		conflicted = true
		beforeConflict()
		return apierrors.NewConflict(
			schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps"},
			name, errors.New("injected status conflict"),
		)
	}
	return fakeclient.WithInterceptors(interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, next client.Client, sub string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			if err := conflict("model"); err != nil {
				return err
			}
			return next.SubResource(sub).Apply(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, next client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if err := conflict(obj.GetName()); err != nil {
				return err
			}
			return next.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}
