package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	return s
}

type fakeClusters struct {
	m map[string]workloadcluster.SelectivelyCachingClient
}

func (f fakeClusters) ClientFor(name string) (workloadcluster.SelectivelyCachingClient, bool) {
	c, ok := f.m[name]
	return c, ok
}

func (f fakeClusters) ClientForUID(name string, uid types.UID) (workloadcluster.SelectivelyCachingClient, bool) {
	if uid != types.UID(name+"-uid") {
		return nil, false
	}
	return f.ClientFor(name)
}

func (f fakeClusters) Connected() []string {
	names := make([]string, 0, len(f.m))
	for n := range f.m {
		names = append(names, n)
	}
	return names
}

func readyWC(name string, labels map[string]string) *v1beta1.WorkloadCluster {
	return &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), Labels: labels},
		Status: v1beta1.WorkloadClusterStatus{Conditions: []metav1.Condition{
			{Type: v1beta1.WorkloadClusterReady, Status: metav1.ConditionTrue, Reason: "Reachable"},
		}},
	}
}

func srcISVC(selector string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "uid-1", Generation: 1,
			Annotations: map[string]string{},
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "placement-runtime"},
			Placement:      &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: testAffinity(selector)},
			Engine:         &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}},
		},
	}
}

func backendTestRuntime() *v1beta1.ClusterServingRuntime {
	return &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "placement-runtime", UID: "runtime-uid"}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}}}}
}

// emptyWorker is a fresh fake worker client (status subresource registered).
func emptyWorker(s *runtime.Scheme) client.WithWatch {
	return fakeclient.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(backendTestRuntime()).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*v1beta1.InferenceService); ok && obj.GetUID() == "" {
			obj.SetUID(types.UID("member-" + obj.GetName()))
			obj.SetGeneration(1)
		}
		return cl.Create(ctx, obj, opts...)
	}}).Build()
}

// workerWithAdmittedDerived returns a worker whose derived ISVC already reports
// an admitted engine instance (it won the race).
func workerWithAdmittedDerived(t *testing.T, s *runtime.Scheme) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent, true) // from admission_test.go
	derived.UID = "member-uid"
	derived.Namespace = "prod"
	derived.Name = "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	// The reconcile reads admission from the authoritative IR on the worker
	// cluster, so seed it alongside the derived ISVC.
	engineIR := irWithInstances(v1beta1.EngineComponent, true)
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(observedWorkerObjects(derived, engineIR)...).Build()
	// Ensure the admitted status is stored (if the builder didn't persist it).
	cur := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if len(cur.Status.Components) == 0 {
		cur.Status = derived.Status
		require.NoError(t, w.Status().Update(context.Background(), cur))
	}
	return w
}

func newPlacer(s *runtime.Scheme, clusters ClusterClients, objs ...client.Object) (*Reconciler, client.Client) {
	c := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
	return &Reconciler{Client: c, APIReader: c, Scheme: s, Log: log.Log, Clusters: clusters, Requeue: time.Second}, c
}

func req() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "prod", Name: "svc"}}
}

func hasDerived(t *testing.T, w client.Client) bool {
	t.Helper()
	o := &v1beta1.InferenceService{}
	err := w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o)
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func cpPlacement(t *testing.T, cp client.Client) *v1beta1.PlacementStatus {
	t.Helper()
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	return o.Status.Placement
}

func workerWithAdmittedURL(t *testing.T, s *runtime.Scheme, host string) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent, true)
	derived.UID = "member-uid"
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status.URL = &apis.URL{Scheme: "https", Host: host}
	engineIR := irWithInstances(v1beta1.EngineComponent, true)
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(observedWorkerObjects(derived, engineIR)...).Build()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if cur.Status.URL == nil {
		cur.Status = derived.Status
		require.NoError(t, w.Status().Update(context.Background(), cur))
	}
	return w
}

func workerWithReadyURL(t *testing.T, s *runtime.Scheme, host string, ready int32) client.WithWatch {
	return workerWithReplicaCounts(t, s, host, ready, ready)
}

func workerWithReplicaCounts(t *testing.T, s *runtime.Scheme, host string, admitted, ready int32) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent, true)
	derived.UID = "member-uid"
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status.URL = &apis.URL{Scheme: "https", Host: host}
	if ready > 0 && host != "" {
		derived.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	}
	instances := make([]bool, admitted)
	for i := range instances {
		instances[i] = true
	}
	engineIR := irWithInstances(v1beta1.EngineComponent, instances...)
	engineIR.Status.ReadyReplicas = ready
	return fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(observedWorkerObjects(derived, engineIR)...).Build()
}

func workerWithPDReplicaCounts(
	t *testing.T,
	s *runtime.Scheme,
	host string,
	engineAdmitted, engineReady, decoderAdmitted, decoderReady int32,
) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent, true)
	derived.UID = "member-uid"
	declareComponent(derived, v1beta1.DecoderComponent)
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status.URL = &apis.URL{Scheme: "https", Host: host}
	if min(engineReady, decoderReady) > 0 && host != "" {
		derived.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	}

	engineInstances := make([]bool, engineAdmitted)
	for i := range engineInstances {
		engineInstances[i] = true
	}
	engineIR := irWithInstances(v1beta1.EngineComponent, engineInstances...)
	engineIR.Status.ReadyReplicas = engineReady

	decoderInstances := make([]bool, decoderAdmitted)
	for i := range decoderInstances {
		decoderInstances[i] = true
	}
	decoderIR := irWithInstances(v1beta1.DecoderComponent, decoderInstances...)
	decoderIR.Status.ReadyReplicas = decoderReady

	return fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(observedWorkerObjects(derived, engineIR, decoderIR)...).Build()
}

func cpStatusURL(t *testing.T, cp client.Client) *apis.URL {
	t.Helper()
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	return o.Status.URL
}

func cpReadyCondition(t *testing.T, cp client.Client) *apis.Condition {
	t.Helper()
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	ready := o.Status.GetCondition(apis.ConditionReady)
	require.NotNil(t, ready)
	return ready
}

func TestWritePlacementProjectsSourceReady(t *testing.T) {
	stableAt := metav1.NewTime(time.Unix(123, 0).UTC())
	servingCandidate := v1beta1.CandidatePlacement{
		Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
		ReadyReplicas: 1, Endpoint: &apis.URL{Scheme: "https", Host: "svc.a.example"},
	}
	tests := []struct {
		name            string
		result          placementResult
		wantStatus      corev1.ConditionStatus
		wantReason      string
		wantMessage     string
		stableReadyTime bool
	}{
		{
			name:       "pending",
			result:     placementResult{phase: v1beta1.PlacementPhasePending},
			wantStatus: corev1.ConditionUnknown, wantReason: placementReadyReasonPending,
			wantMessage: placementReadyMessagePending,
		},
		{
			name: "confirmed placement loss",
			result: placementResult{
				phase: v1beta1.PlacementPhasePending, placementLost: true,
			},
			wantStatus: corev1.ConditionUnknown, wantReason: PlacementReadyReasonLost,
			wantMessage: placementReadyMessageLost,
		},
		{
			name: "admitting",
			result: placementResult{
				phase:      v1beta1.PlacementPhaseAdmitting,
				candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitting}},
			},
			wantStatus: corev1.ConditionUnknown, wantReason: placementReadyReasonAdmitting,
			wantMessage: placementReadyMessageAdmitting,
		},
		{
			name: "placed without a ready endpoint",
			result: placementResult{
				phase: v1beta1.PlacementPhasePlaced,
				candidates: []v1beta1.CandidatePlacement{{
					Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
					ReadyReplicas: 1,
				}},
			},
			wantStatus: corev1.ConditionFalse, wantReason: placementReadyReasonNotReady,
			wantMessage: placementReadyMessageNotReady,
		},
		{
			name: "placed with an unreadable home",
			result: placementResult{
				phase:            v1beta1.PlacementPhasePlaced,
				candidates:       []v1beta1.CandidatePlacement{servingCandidate},
				readinessUnknown: true,
			},
			wantStatus: corev1.ConditionUnknown, wantReason: PlacementReadyReasonUnknown,
			wantMessage: placementReadyMessageUnknown,
		},
		{
			name: "placed and serving",
			result: placementResult{
				phase:      v1beta1.PlacementPhasePlaced,
				candidates: []v1beta1.CandidatePlacement{servingCandidate},
				ready:      true,
			},
			wantStatus: corev1.ConditionTrue, wantReason: placementReadyReasonReady,
			wantMessage: placementReadyMessageReady, stableReadyTime: true,
		},
		{
			name: "failed",
			result: placementResult{
				phase:      v1beta1.PlacementPhaseFailed,
				candidates: []v1beta1.CandidatePlacement{servingCandidate},
			},
			wantStatus: corev1.ConditionFalse, wantReason: placementReadyReasonFailed,
			wantMessage: placementReadyMessageFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testScheme(t)
			source := srcISVC("gpu=gb300")
			source.Generation = 7
			unrelated := apis.Condition{
				Type: "ExternalHealth", Status: corev1.ConditionTrue,
				Reason: "PolicyResolved", Message: "policy is usable",
				LastTransitionTime: apis.VolatileTime{Inner: stableAt},
			}
			conditions := apis.Conditions{unrelated}
			if tt.stableReadyTime {
				conditions = append(conditions, apis.Condition{
					Type: apis.ConditionReady, Status: tt.wantStatus,
					Reason: tt.wantReason, Message: tt.wantMessage,
					LastTransitionTime: apis.VolatileTime{Inner: stableAt},
				})
			}
			source.Status.SetConditions(conditions)

			r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, source)
			seeded := &v1beta1.InferenceService{}
			require.NoError(t, cp.Get(context.Background(), client.ObjectKeyFromObject(source), seeded))
			seeded.Status.SetConditions(conditions)
			require.NoError(t, cp.Status().Update(context.Background(), seeded))
			_, err := r.writePlacement(context.Background(), seeded, tt.result)
			require.NoError(t, err)

			got := &v1beta1.InferenceService{}
			require.NoError(t, cp.Get(context.Background(), client.ObjectKeyFromObject(source), got))
			ready := got.Status.GetCondition(apis.ConditionReady)
			require.NotNil(t, ready)
			assert.Equal(t, tt.wantStatus, ready.Status)
			assert.Equal(t, tt.wantReason, ready.Reason)
			assert.Equal(t, tt.wantMessage, ready.Message)
			assert.Equal(t, int64(7), got.Status.ObservedGeneration)

			preserved := got.Status.GetCondition(unrelated.Type)
			require.NotNil(t, preserved)
			assert.Equal(t, unrelated.Type, preserved.Type)
			assert.Equal(t, unrelated.Status, preserved.Status)
			assert.Equal(t, unrelated.Reason, preserved.Reason)
			assert.Equal(t, unrelated.Message, preserved.Message)
			assert.True(t, preserved.LastTransitionTime.Inner.Equal(&stableAt),
				"an unrelated condition must be preserved")
			if tt.stableReadyTime {
				assert.True(t, ready.LastTransitionTime.Inner.Equal(&stableAt),
					"an unchanged Ready verdict must preserve LastTransitionTime")
			}
		})
	}
}

func TestPlacementCandidateServingRequiresReadyIngress(t *testing.T) {
	derived := srcISVC("gpu=gb300")
	candidate := v1beta1.CandidatePlacement{
		Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
		ReadyReplicas: 1, Endpoint: &apis.URL{Scheme: "https", Host: "svc.a.example"},
	}
	assert.False(t, placementCandidateServing(derived, candidate),
		"a rendered URL and ready replicas do not prove that ingress is programmed")

	derived.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	assert.True(t, placementCandidateServing(derived, candidate))
}

func TestReconcile_NoCandidate(t *testing.T) {
	s := testScheme(t)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"), readyWC("cloud-a", map[string]string{"gpu": "h100"}))
	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
}

func TestReconcile_FansOutToAllCandidates(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.True(t, hasDerived(t, wa), "derived on a")
	assert.True(t, hasDerived(t, wb), "derived on b")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	assert.Empty(t, p.Cluster)
	assert.Len(t, p.Candidates, 2)
	for _, candidate := range p.Candidates {
		assert.Equal(t, v1beta1.CandidatePhaseAdmitting, candidate.Phase)
	}
}

// TestReconcile_AdmittingRequeuesAtPollCadence guards the convergence bug where an
// race in progress requeued at the long steady-state safety backstop
// (DefaultStatusSafetyRequeue, 10m) instead of the fast poll cadence. With the
// status funnel off (the default), that backstop is the only re-trigger, so the
// winner is not observed until it fires — admission appears stuck. Admitting must
// re-poll at r.requeue() (the harness's 1s).
func TestReconcile_AdmittingRequeuesAtPollCadence(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	require.Equal(t, v1beta1.PlacementPhaseAdmitting, cpPlacement(t, cp).Phase)
	assert.Equal(t, time.Second, res.RequeueAfter,
		"admitting must requeue at the poll cadence, not the steady-state backstop")
	assert.Less(t, res.RequeueAfter, DefaultStatusSafetyRequeue)
}

func TestReconcile_FirstAdmittedWinsAndDeletesLosers(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s) // b already admitted -> winner
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.False(t, hasDerived(t, wa), "loser a's derived deleted")
	assert.True(t, hasDerived(t, wb), "winner b's derived kept")
}

func TestReconcile_SinglePublishesWinnerReplicaCounts(t *testing.T) {
	s := testScheme(t)
	worker := workerWithReplicaCounts(t, s, "svc.a.example", 4, 2)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	placement := cpPlacement(t, cp)
	require.NotNil(t, placement)
	require.Len(t, placement.Candidates, 1)
	assert.Equal(t, int32(4), placement.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(2), placement.Candidates[0].ReadyReplicas)
}

func TestReconcile_SinglePublishesPDReplicaMinimum(t *testing.T) {
	s := testScheme(t)
	worker := workerWithPDReplicaCounts(t, s, "svc.a.example", 4, 3, 2, 1)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	r, cp := newPlacer(s, clusters, isvc, readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	placement := cpPlacement(t, cp)
	require.NotNil(t, placement)
	require.Len(t, placement.Candidates, 1)
	assert.Equal(t, int32(2), placement.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(1), placement.Candidates[0].ReadyReplicas)
}

func TestReconcile_StickyWinner(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.False(t, hasDerived(t, wa), "sticky winner must NOT fan out to a")
	assert.True(t, hasDerived(t, wb))
}

func TestReconcile_SingleRefreshesStickyWinnerReplicaCounts(t *testing.T) {
	s := testScheme(t)
	worker := workerWithReplicaCounts(t, s, "svc.a.example", 5, 3)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a",
		Phase:   v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	r, cp := newPlacer(s, clusters, isvc, readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	placement := cpPlacement(t, cp)
	require.NotNil(t, placement)
	require.Len(t, placement.Candidates, 1)
	assert.Equal(t, int32(5), placement.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(3), placement.Candidates[0].ReadyReplicas)
}

// A confirmed gap retains only the winner identity during grace. Routing
// evidence is cleared immediately while re-racing remains held.
func TestReconcile_WinnerLostWithinGraceHolds(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s) // b's derived is GONE (winner lost)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute // non-trivial: first pass is within grace

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "requeues to re-check after grace")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster, "winner held during grace")
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
	assert.Empty(t, p.Candidates, "an absent derived is not an active candidate")
	ready := cpReadyCondition(t, cp)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonLost, ready.Reason)
	assert.False(t, hasDerived(t, wa), "must NOT re-fan-out to a within grace")
}

func TestReconcile_RemovedWinnerClusterRetainsOnlyIdentityDuringGrace(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced, Endpoint: apis.HTTPS("stale.example.com"),
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("stale.example.com"), AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	isvc.Status.URL = apis.HTTPS("stale.example.com")
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc)
	r.WinnerLostGracePeriod = time.Minute

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "a", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
	assert.Empty(t, p.Candidates, "a removed WorkloadCluster has no active candidate")
	assert.Nil(t, cpStatusURL(t, cp))
	ready := cpReadyCondition(t, cp)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonLost, ready.Reason)
}

func TestReconcile_RemovedWinnerClusterRetainsLossProvenanceAfterGrace(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("stale.example.com"), AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc)
	r.WinnerLostGracePeriod = time.Minute
	r.winnerLostSince.Store(isvc.UID, time.Now().Add(-2*time.Minute))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "a", p.Cluster, "the last-winner identity preserves loss provenance")
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
	assert.Empty(t, p.Candidates)
	assert.Nil(t, p.Endpoint)
	ready := cpReadyCondition(t, cp)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonLost, ready.Reason)
}

func TestReconcile_LostWinnerBecomesUnknownWhenMemberReadFails(t *testing.T) {
	s := testScheme(t)
	workers := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(emptyWorker(s)),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("stale.example.com"), AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	r, cp := newPlacer(s, workers, isvc, readyWC("a", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, PlacementReadyReasonLost, cpReadyCondition(t, cp).Reason)

	workers.m["a"] = workloadcluster.NewNeverCachingClient(getFailClient{WithWatch: emptyWorker(s)})
	for range 2 {
		_, err = r.Reconcile(context.Background(), req())
		require.NoError(t, err)
		p := cpPlacement(t, cp)
		require.NotNil(t, p)
		assert.Equal(t, "a", p.Cluster)
		assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
		assert.Empty(t, p.Candidates)
		assert.Equal(t, PlacementReadyReasonUnknown, cpReadyCondition(t, cp).Reason)
	}
}

// Once the winner-lost grace window has elapsed (the derived stayed gone), the
// controller gives up on the winner and re-races: it
// clears the winner and re-fans-out to every candidate.
func TestReconcile_ReplaceOnWinnerLostAfterGrace(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s) // b's derived is GONE (winner lost)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute
	// Pre-seed the grace clock so the window is already elapsed on this pass.
	r.winnerLostSince.Store(isvc.UID, time.Now().Add(-2*time.Minute))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Empty(t, p.Cluster, "lost winner cleared after grace")
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	assert.True(t, hasDerived(t, wa), "re-fanned out to a")
	assert.True(t, hasDerived(t, wb), "re-fanned out to b")
	// Marker cleared once re-raced.
	_, ok := r.winnerLostSince.Load(isvc.UID)
	assert.False(t, ok, "grace marker cleared after re-race")
}

// workerWithUnadmittedDerived returns a worker whose derived ISVC exists but
// whose engine IR reports NO admitted instance — the shape of a winner that
// Kueue preempted (pods evicted back behind the admission gate).
func workerWithUnadmittedDerived(t *testing.T, s *runtime.Scheme) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent)
	derived.UID = "member-uid"
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	engineIR := irWithInstances(v1beta1.EngineComponent, false) // present, NOT admitted
	return fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(observedWorkerObjects(derived, engineIR)...).Build()
}

func TestReconcile_WinnerLostAdmissionWithinGraceHolds(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithUnadmittedDerived(t, s) // b's derived present but de-admitted (preempted)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute // first pass arms the clock, within window

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "requeues to re-check after grace")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster, "winner held during grace")
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	require.Len(t, p.Candidates, 1)
	assert.Nil(t, p.Candidates[0].Endpoint)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)
	assert.False(t, hasDerived(t, wa), "must NOT re-fan-out to a while within grace")
}

func TestReconcile_WinnerLostAdmissionReRacesAfterGrace(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithUnadmittedDerived(t, s) // b's derived present but de-admitted (preempted)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute
	// Pre-seed the grace clock so the window is already elapsed on this pass.
	r.winnerLostSince.Store(isvc.UID, time.Now().Add(-2*time.Minute))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Empty(t, p.Cluster, "de-admitted winner cleared after grace")
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	assert.True(t, hasDerived(t, wa), "re-fanned out to a after grace")
	_, ok := r.winnerLostSince.Load(isvc.UID)
	assert.False(t, ok, "grace marker cleared after re-race")
}

func TestReconcile_WinnerFailedSurfacesFailed(t *testing.T) {
	s := testScheme(t)
	// b's derived ISVC reports an engine instance Phase=Failed.
	derivedFailed := isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	derivedFailed.Namespace = "prod"
	derivedFailed.Name = "svc"
	derivedFailed.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	// The reconcile reads the Failed phase from the authoritative IR on the
	// worker cluster; seed it alongside the derived ISVC.
	failedIR := irWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	wb := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(derivedFailed, failedIR).Build()
	// Ensure the failed status is persisted.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, wb.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	cur.Status = derivedFailed.Status
	require.NoError(t, wb.Status().Update(context.Background(), cur))

	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc, readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseFailed, p.Phase, "placement escalated to Failed")
	assert.Equal(t, "b", p.Cluster, "failed cluster recorded")
	assert.True(t, hasDerived(t, wb), "failed derived kept (not deleted)")
}

func TestReconcile_StickyWinnerSweepsStrayLoser(t *testing.T) {
	s := testScheme(t)
	// a is connected and carries a STRAY derived (origin-labeled), not in status.
	wa := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "stray-uid", Labels: map[string]string{PlacementOriginLabel: "uid-1"}},
	}).Build()
	wb := workerWithAdmittedDerived(t, s) // winner b, admitted
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}}, // a is NOT listed
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, wa), "stray derived on non-winner cluster a must be swept even though it's not in status.Candidates")
	assert.True(t, hasDerived(t, wb), "winner b kept")
	p := cpPlacement(t, cp)
	assert.Equal(t, "b", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
}

func TestReconcile_DeleteRemovesDerivedEverywhere(t *testing.T) {
	s := testScheme(t)
	wa := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "stray-uid", Labels: map[string]string{PlacementOriginLabel: "uid-1"}},
	}).Build()
	wb := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "stray-uid", Labels: map[string]string{PlacementOriginLabel: "uid-1"}},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b",
		Candidates: []v1beta1.CandidatePlacement{
			{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitting}, {Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted},
		},
	}
	r, cp := newPlacer(s, clusters, isvc)

	require.NoError(t, cp.Delete(context.Background(), isvc)) // finalizer -> DeletionTimestamp

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, wa), "derived removed from a")
	assert.False(t, hasDerived(t, wb), "derived removed from b")
	got := &v1beta1.InferenceService{}
	err = cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got)
	assert.True(t, apierrors.IsNotFound(err), "CP ISVC gone after finalizer cleared")
}

// createFailClient wraps a fake client but fails every Create (simulates a
// cluster that rejects the derived ISVC).
type createFailClient struct {
	client.WithWatch
}

func (c createFailClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return apierrors.NewInternalError(errors.New("create rejected"))
}

func TestReconcile_FanOutPartialFailureStillRaces(t *testing.T) {
	s := testScheme(t)
	// a rejects Create; b accepts.
	wa := createFailClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).Build()}
	wb := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "one bad cluster must not fail the whole reconcile")

	assert.True(t, hasDerived(t, wb), "healthy cluster b still got the derived")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
}

func TestReconcile_PublishesEndpointWhenAdmitted(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedURL(t, s, "svc.prod.b.example")
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Equal(t, "b", p.Cluster)
	require.NotNil(t, p.Endpoint)
	assert.Equal(t, "svc.prod.b.example", p.Endpoint.Host)
	u := cpStatusURL(t, cp)
	require.NotNil(t, u)
	assert.Equal(t, "svc.prod.b.example", u.Host)

	// The winner's per-candidate Endpoint is populated too (the per-home model
	// All/Split build on; in Single the top-level Endpoint stays authoritative).
	winner := candidatesByCluster(p.Candidates)["b"]
	require.NotNil(t, winner.Allocation)
	assert.Positive(t, winner.Allocation.DesiredReplicas)
	require.NotNil(t, winner.Endpoint)
	assert.Equal(t, "svc.prod.b.example", winner.Endpoint.Host)
	for _, candidate := range p.Candidates {
		if candidate.Cluster != "b" {
			assert.Zero(t, candidate.Allocation.DesiredReplicas)
		}
	}
}

func TestReconcile_PlacedButNoEndpointYet(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s) // admitted, but NO status.url
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Nil(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
}

func TestReconcile_ClearsEndpointOnWinnerLost(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s) // b's derived GONE
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	oldURL := &apis.URL{Scheme: "https", Host: "old.example"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: oldURL,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	isvc.Status.URL = oldURL
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute
	// Expire grace so this pass also exercises the re-race path after clearing
	// the missing winner's endpoint.
	r.winnerLostSince.Store(isvc.UID, time.Now().Add(-2*time.Minute))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	assert.Nil(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
}

func TestReconcile_ConfirmedWinnerGapClearsRoutingWithinGrace(t *testing.T) {
	s := testScheme(t)
	wb := emptyWorker(s) // b's derived GONE this pass (transient)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	oldURL := &apis.URL{Scheme: "https", Host: "svc.prod.b.example"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: oldURL,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	isvc.Status.URL = oldURL
	r, cp := newPlacer(s, clusters, isvc, readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Minute // within grace on first pass

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
	assert.Equal(t, "b", p.Cluster, "grace retains the winner identity")
	assert.Empty(t, p.Candidates, "a confirmed absence is not an active candidate")
	assert.Nil(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
	ready := cpReadyCondition(t, cp)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonLost, ready.Reason)
}

func TestReconcile_ObservedEndpointRemovalClearsSourceURL(t *testing.T) {
	s := testScheme(t)
	wb := workerWithAdmittedDerived(t, s) // admitted, NO status.url this pass
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	prior := &apis.URL{Scheme: "https", Host: "svc.prod.b.example"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: prior,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	isvc.Status.URL = prior
	r, cp := newPlacer(s, clusters, isvc, readyWC("b", map[string]string{"gpu": "gb300"}))

	// The newPlacer fake client should persist the initial status, but ensure it:
	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	if got.Status.URL == nil || got.Status.Placement == nil {
		got.Status = isvc.Status
		require.NoError(t, cp.Status().Update(context.Background(), got))
	}

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Nil(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
}

func TestReconcile_TerminalWinnerClearsRouting(t *testing.T) {
	s := testScheme(t)
	// derived: engine instance Failed, but a status.url is present.
	derived := isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status.URL = &apis.URL{Scheme: "https", Host: "svc.prod.b.example"}
	failedIR := irWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	wb := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(derived, failedIR).Build()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, wb.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if cur.Status.URL == nil {
		cur.Status = derived.Status
		require.NoError(t, wb.Status().Update(context.Background(), cur))
	}
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc, readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseFailed, p.Phase)
	assert.Nil(t, p.Endpoint)
	assert.Nil(t, cpStatusURL(t, cp))
}

func TestEndpointFor_NilSafe(t *testing.T) {
	assert.Nil(t, endpointFor(nil))
	assert.Nil(t, endpointFor(&v1beta1.InferenceService{}))
}

// deleteFailClient fails every Delete (simulates a cluster that errors on
// loser cleanup).
type deleteFailClient struct {
	client.WithWatch
}

func (c deleteFailClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return apierrors.NewInternalError(errors.New("delete rejected"))
}

// getFailClient fails every Get (simulates a transient remote read error).
type getFailClient struct {
	client.WithWatch
}

func (c getFailClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return apierrors.NewInternalError(errors.New("get rejected"))
}

type hangingGetClient struct {
	client.WithWatch
}

func (c hangingGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	<-ctx.Done()
	return ctx.Err()
}

// irGetFailClient allows derived-ISVC reads but fails authoritative IR reads.
type irGetFailClient struct {
	client.WithWatch
}

func (c irGetFailClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.InferenceReplica); ok {
		return apierrors.NewInternalError(errors.New("IR get rejected"))
	}
	return c.WithWatch.Get(ctx, key, obj, opts...)
}

// failIRGetAfterClient allows a fixed number of authoritative IR reads, then
// fails later reads while recording successful remote updates.
type failIRGetAfterClient struct {
	client.WithWatch
	successfulIRGets int
	irGets           int
	updates          int
}

func (c *failIRGetAfterClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.InferenceReplica); ok {
		c.irGets++
		if c.irGets > c.successfulIRGets {
			return apierrors.NewInternalError(errors.New("IR get rejected"))
		}
	}
	return c.WithWatch.Get(ctx, key, obj, opts...)
}

func (c *failIRGetAfterClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updates++
	return c.WithWatch.Update(ctx, obj, opts...)
}

type mutationCountingClient struct {
	client.WithWatch
	mutations *int
}

func (c mutationCountingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	(*c.mutations)++
	return c.WithWatch.Create(ctx, obj, opts...)
}

func (c mutationCountingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	(*c.mutations)++
	return c.WithWatch.Update(ctx, obj, opts...)
}

func (c mutationCountingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	(*c.mutations)++
	return c.WithWatch.Patch(ctx, obj, patch, opts...)
}

func (c mutationCountingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	(*c.mutations)++
	return c.WithWatch.Delete(ctx, obj, opts...)
}

// foreignDerived builds a same-named ISVC NOT created by this control plane (no
// origin label, or an origin label for a different source UID).
func foreignDerived(originUID string) *v1beta1.InferenceService {
	o := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"}}
	if originUID != "" {
		o.Labels = map[string]string{PlacementOriginLabel: originUID}
	}
	return o
}

// Cross-tenant data-loss guard: the loser sweep must NOT delete a same-named
// ISVC on a connected NON-candidate cluster that this control plane did not
// derive. deleteLosers sweeps only candidate clusters and deleteDerivedOn is
// origin-guarded, so a user's same-named ISVC on an unrelated connected
// cluster survives.
func TestReconcile_LoserSweepDoesNotTouchForeignISVCOnNonCandidate(t *testing.T) {
	s := testScheme(t)
	// "other" is connected but NOT a candidate (label does not match the
	// selector) and carries a user-created same-named ISVC with no origin label.
	wOther := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(foreignDerived("")).Build()
	wb := workerWithAdmittedDerived(t, s) // winner (candidate)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"other": workloadcluster.NewNeverCachingClient(wOther),
		"b":     workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("other", map[string]string{"gpu": "h100"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.True(t, hasDerived(t, wOther), "foreign same-named ISVC on non-candidate connected cluster must NOT be deleted")
	assert.True(t, hasDerived(t, wb), "winner kept")
	assert.Equal(t, "b", cpPlacement(t, cp).Cluster)
}

// Even on a candidate (loser) cluster, an object derived from a
// DIFFERENT source UID must survive the origin-guarded sweep.
func TestReconcile_LoserSweepDoesNotDeleteOtherSourceDerived(t *testing.T) {
	s := testScheme(t)
	// a is a candidate and holds an ISVC derived from a different source.
	wa := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(foreignDerived("some-other-uid")).Build()
	wb := workerWithAdmittedDerived(t, s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	// Make this a sticky-winner pass so a's foreign object is only ever subject
	// to the loser sweep (not adopted by a fan-out CreateOrUpdate).
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, _ := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.True(t, hasDerived(t, wa), "derived owned by a different source UID must NOT be deleted")
}

// Bug: deleteLosers must tolerate a per-cluster delete failure instead of
// aborting the whole reconcile on the first error.
func TestReconcile_LoserCleanupToleratesPerClusterError(t *testing.T) {
	s := testScheme(t)
	// a holds OUR stray derived but errors on Delete; the reconcile must still
	// succeed (place the winner) rather than abort.
	wa := deleteFailClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).
		WithObjects(&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}}}).Build()}
	wb := workerWithAdmittedDerived(t, s) // winner
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "a single loser-cluster delete error must not abort the reconcile")
	assert.Equal(t, v1beta1.PlacementPhasePlaced, cpPlacement(t, cp).Phase)
	assert.Equal(t, "b", cpPlacement(t, cp).Cluster)
}

// Bug: a transient remote GET error in the sticky-winner path must hold the
// existing placement and requeue, not abort and not re-race.
func TestReconcile_StickyWinnerTransientGetHoldsPlacement(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := getFailClient{WithWatch: emptyWorker(s)} // winner's GET fails transiently
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "transient remote GET error must not abort the reconcile")
	assert.Positive(t, res.RequeueAfter, "should requeue to retry")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster, "existing placement held")
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.False(t, hasDerived(t, wa), "must NOT re-race / fan out to a on a transient read error")
}

// Bug: placeOn must merge control-plane-owned metadata onto the derived rather
// than clobbering worker-added labels/annotations every poll.
func TestPlaceOn_PreservesWorkerMetadata(t *testing.T) {
	s := testScheme(t)
	// Pre-existing derived that IS ours (carries the origin marker) plus a
	// worker-added label + annotation the worker reconciler stamped on it.
	existing := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "svc", Namespace: "prod", UID: "member-uid",
		Labels:      map[string]string{"worker.io/managed": "yes", PlacementOriginLabel: "uid-1"},
		Annotations: map[string]string{"worker.io/state": "bookkeeping"},
	}}
	w := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(existing, backendTestRuntime()).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, _ := newPlacer(s, clusters, srcISVC(""), readyWC("a", nil))

	require.NoError(t, r.placeOn(context.Background(), "a", srcISVC(""), false))

	got := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	assert.Equal(t, "yes", got.Labels["worker.io/managed"], "worker label preserved")
	assert.Equal(t, "bookkeeping", got.Annotations["worker.io/state"], "worker annotation preserved")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel], "control-plane origin label set")
}

// Bug (cross-tenant data loss): placeOn must NOT overwrite a same-named ISVC on
// the candidate cluster that is NOT a derived of this control plane (a user's
// own ISVC, or another control plane's object). The apply must abort — leaving
// the foreign object's Spec untouched and unstamped — mirroring the origin guard
// already on the delete path.
func TestPlaceOn_RefusesToClobberForeignISVC(t *testing.T) {
	s := testScheme(t)
	// A foreign object: same name/namespace, NO origin markers, a distinct Spec.
	foreign := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "member-uid",
			Labels:      map[string]string{"owner": "some-user"},
			Annotations: map[string]string{"note": "hand-rolled"},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Router: &v1beta1.RouterSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "user-container", Image: "user-img"}}},
		},
	}
	w := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(foreign, backendTestRuntime()).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, _ := newPlacer(s, clusters, srcISVC(""), readyWC("a", nil))

	err := r.placeOn(context.Background(), "a", srcISVC(""), false)
	require.Error(t, err, "placeOn must refuse to clobber a foreign same-named ISVC")

	got := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	// Foreign Spec is unchanged: still the user's router, no engine stamped in.
	assert.NotNil(t, got.Spec.Router, "foreign Spec preserved (router intact)")
	assert.Nil(t, got.Spec.Engine, "foreign Spec preserved (no engine grafted on)")
	// No origin markers stamped onto the foreign object.
	assert.Empty(t, got.Labels[PlacementOriginLabel], "origin label not stamped on foreign object")
	assert.Empty(t, got.Annotations[PlacementOriginUIDAnnotation], "origin annotation not stamped on foreign object")
	assert.Equal(t, "some-user", got.Labels["owner"], "foreign label untouched")
}

// placeOn must update OUR OWN existing derived (origin marker present) normally,
// without erroring — the guard distinguishes our derived from a foreign object.
func TestPlaceOn_UpdatesOwnDerived(t *testing.T) {
	s := testScheme(t)
	existing := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "svc", Namespace: "prod", UID: "member-uid",
		Labels: map[string]string{PlacementOriginLabel: "uid-1"},
	}}
	w := fakeclient.NewClientBuilder().WithScheme(s).WithObjects(existing, backendTestRuntime()).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, _ := newPlacer(s, clusters, srcISVC(""), readyWC("a", nil))

	require.NoError(t, r.placeOn(context.Background(), "a", srcISVC(""), false), "re-apply of our own derived must succeed")

	got := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	assert.NotNil(t, got.Spec.Engine, "desired Spec applied to our derived")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel], "origin label retained")
}

// placeOn must create the derived when no object exists on the candidate.
func TestPlaceOn_CreatesWhenAbsent(t *testing.T) {
	s := testScheme(t)
	w := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, _ := newPlacer(s, clusters, srcISVC(""), readyWC("a", nil))

	require.NoError(t, r.placeOn(context.Background(), "a", srcISVC(""), false), "first-time create must succeed")

	got := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	assert.NotNil(t, got.Spec.Engine, "derived created with desired Spec")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel], "origin label stamped on created derived")
}

// Bug: reconcileDelete must NOT remove the finalizer while a connected cluster
// still holds our derived because its delete errored (would orphan it). It
// requeues instead.
func TestReconcileDelete_HoldsFinalizerWhenConnectedDeleteFails(t *testing.T) {
	s := testScheme(t)
	wa := deleteFailClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).
		WithObjects(&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", Labels: map[string]string{PlacementOriginLabel: "uid-1"}}}).Build()}
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	r, cp := newPlacer(s, clusters, isvc, readyWC("a", map[string]string{"gpu": "gb300"}))
	require.NoError(t, cp.Delete(context.Background(), isvc)) // -> DeletionTimestamp (finalizer present)

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "should requeue to retry teardown")

	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, got))
	assert.Contains(t, got.Finalizers, PlacementFinalizer, "finalizer held while connected cluster still holds derived")
}

// An unreadable winner is unknown rather than lost. It does not arm the grace
// clock, which tracks confirmed absence or loss of admission, and it cannot
// trigger a re-race or loser sweep even with a near-zero grace.
func TestReconcile_StickyWinnerDisconnectedHoldsWithoutArmingGrace(t *testing.T) {
	s := testScheme(t)
	// Winner "b" is a Ready candidate WorkloadCluster but is NOT in the connected
	// client set (transport flap): ClientFor("b") -> (nil, false).
	wa := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Endpoint: apis.HTTPS("svc.b.example"),
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("svc.b.example"), AdmittedReplicas: 3, ReadyReplicas: 3,
		}},
	}
	isvc.Status.URL = apis.HTTPS("svc.b.example")
	isvc.Status.SetCondition(apis.ConditionReady, &apis.Condition{
		Status: corev1.ConditionTrue, Reason: placementReadyReasonReady,
	})
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.WinnerLostGracePeriod = time.Nanosecond // even a ~zero grace must not re-race a disconnected winner

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter, "disconnected winner: hold and requeue")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "b", p.Cluster, "disconnected winner held, not cleared")
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	require.Len(t, p.Candidates, 1)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)
	assert.Equal(t, int32(3), p.Candidates[0].AdmittedReplicas)
	require.NotNil(t, p.Candidates[0].Endpoint)
	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), req().NamespacedName, got))
	ready := got.Status.GetCondition(apis.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonUnknown, ready.Reason)
	assert.False(t, hasDerived(t, wa), "must NOT re-race / fan out to a on a disconnected winner")
	_, armed := r.winnerLostSince.Load(isvc.UID)
	assert.False(t, armed, "grace clock must NOT be armed for a disconnected winner")
}

func TestReconcile_DisconnectedLegacySingleSynthesizesZeroReadyCandidate(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced, Endpoint: apis.HTTPS("svc.a.example"),
	}
	isvc.Status.URL = apis.HTTPS("svc.a.example")
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, "a", p.Candidates[0].Cluster)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, p.Candidates[0].Phase)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)
	require.NotNil(t, p.Candidates[0].Endpoint)
}

func TestReconcile_DisconnectedLegacyCandidateInheritsTopLevelEndpoint(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
		Endpoint: apis.HTTPS("svc.a.example"),
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			AdmittedReplicas: 2, ReadyReplicas: 1,
		}},
	}
	isvc.Status.URL = apis.HTTPS("svc.a.example")
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	require.NotNil(t, p.Candidates[0].Endpoint)
	assert.Equal(t, "svc.a.example", p.Candidates[0].Endpoint.Host)
	assert.Equal(t, int32(2), p.Candidates[0].AdmittedReplicas)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)
	require.NotNil(t, p.Endpoint)
	assert.Equal(t, "svc.a.example", p.Endpoint.Host)
}

func TestReconcile_StandingObservationHasPerMemberDeadline(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("svc.a.example"), AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	worker := hangingGetClient{WithWatch: emptyWorker(s)}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, isvc, readyWC("a", map[string]string{"gpu": "gb300"}))
	r.PlaceTimeout = 25 * time.Millisecond

	started := time.Now()
	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Less(t, time.Since(started), time.Second)
	assert.Positive(t, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)
	ready := cpReadyCondition(t, cp)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonUnknown, ready.Reason)
}

// Once the winner reappears, its pending grace marker is cleared so another
// disappearance starts a fresh window.
func TestReconcile_StickyWinnerPresentClearsGraceMarker(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	wb := workerWithAdmittedDerived(t, s) // present + admitted
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	// Pre-seed a stale grace marker (as if the derived had briefly vanished).
	r.winnerLostSince.Store(isvc.UID, time.Now())

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.Equal(t, v1beta1.PlacementPhasePlaced, cpPlacement(t, cp).Phase)
	_, ok := r.winnerLostSince.Load(isvc.UID)
	assert.False(t, ok, "grace marker cleared once derived is present again")
}

// Fault isolation: a candidate that is NOT in the connected snapshot is
// skipped during fan-out — its absence must not block placing on the connected
// candidates, and the placement still proceeds to Admitting.
func TestReconcile_FanOutSkipsDisconnectedCandidate(t *testing.T) {
	s := testScheme(t)
	// Both "a" and "b" are Ready candidates, but only "a" is connected.
	wa := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err, "a disconnected candidate must not fail the reconcile")

	assert.True(t, hasDerived(t, wa), "connected candidate a got the derived")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
}

// Fleet readiness is sampled, so an established placement must survive a pass
// that observes zero Ready clusters: publishing Pending there would clear the
// winner, candidates and endpoint and deroute still-serving traffic.
func TestReconcile_NoReadyClustersPreservesLastKnownPlacement(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced, Endpoint: apis.HTTPS("svc.a.example"),
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("svc.a.example")}},
	}
	isvc.Status.URL = apis.HTTPS("svc.a.example")
	wc := readyWC("a", map[string]string{"gpu": "gb300"})
	wc.Status.Conditions[0].Status = metav1.ConditionFalse
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc, wc)

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, res.RequeueAfter)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Equal(t, "a", p.Cluster)
	require.NotNil(t, cpStatusURL(t, cp))
	assert.Equal(t, "svc.a.example", cpStatusURL(t, cp).Host)
}

func TestReconcile_NoReadyClustersRefreshesStandingHome(t *testing.T) {
	s := testScheme(t)
	worker := workerWithReplicaCounts(t, s, "fresh.example.com", 4, 2)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced, Endpoint: apis.HTTPS("stale.example.com"),
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("stale.example.com"), AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	isvc.Status.URL = apis.HTTPS("stale.example.com")
	wc := readyWC("a", map[string]string{"gpu": "gb300"})
	wc.Status.Conditions[0].Status = metav1.ConditionFalse
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, isvc, wc)

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, int32(4), p.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(2), p.Candidates[0].ReadyReplicas)
	assert.Equal(t, "fresh.example.com", p.Candidates[0].Endpoint.Host)
	assert.Equal(t, "fresh.example.com", p.Endpoint.Host)
}

func TestReconcile_StandingHomeObservedWhenOnlyUnrelatedClusterIsReady(t *testing.T) {
	s := testScheme(t)
	worker := workerWithReplicaCounts(t, s, "fresh.example.com", 3, 2)
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	wcB := readyWC("b", map[string]string{"gpu": "gb300"})
	wcB.Status.Conditions[0].Status = metav1.ConditionFalse
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"b": workloadcluster.NewNeverCachingClient(worker),
	}}, isvc, readyWC("a", map[string]string{"gpu": "h100"}), wcB)

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Equal(t, "b", p.Cluster)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, int32(3), p.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(2), p.Candidates[0].ReadyReplicas)
	assert.Equal(t, "fresh.example.com", p.Candidates[0].Endpoint.Host)
}

func TestReconcile_MalformedSelectorHoldsActuationButRefreshesStandingHome(t *testing.T) {
	s := testScheme(t)
	worker := workerWithReplicaCounts(t, s, "fresh.example.com", 3, 2)
	isvc := srcISVC("!!!")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, isvc, readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "a", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, int32(3), p.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(2), p.Candidates[0].ReadyReplicas)
	assert.Equal(t, "fresh.example.com", p.Candidates[0].Endpoint.Host)
}

// Single-mode race: one candidate whose authoritative IR read fails must not
// abort the race — the healthy candidate still wins.
func TestReconcile_SingleModeRemoteIRFailureDoesNotBlockRace(t *testing.T) {
	s := testScheme(t)
	bad := irGetFailClient{WithWatch: workerWithAdmittedURL(t, s, "svc.a.example")}
	good := workerWithAdmittedURL(t, s, "svc.b.example")
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(bad),
		"b": workloadcluster.NewNeverCachingClient(good),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Equal(t, "b", p.Cluster)
}

// Index: isvcsForClusterChange enqueues an ISVC only when the changed
// cluster is (or could become) one of its candidates — its requirement selector
// matches the cluster's labels — and skips ISVCs the cluster cannot affect.
func TestIsvcsForClusterChange_EnqueuesOnlyMatchingCandidates(t *testing.T) {
	s := testScheme(t)
	// match: selector gpu=gb300 matches the changed cluster.
	match := srcISVCNamed("match", "gpu=gb300")
	// noMatch: requires a different accelerator; the changed cluster is not a candidate.
	noMatch := srcISVCNamed("nomatch", "gpu=h100")
	// nameMatch: selects the changed cluster through its immutable object name.
	nameMatch := srcISVCNamed("name-match")
	nameMatch.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: testAffinity("metadata.name=b")}
	// nameNoMatch: selects a different cluster name.
	nameNoMatch := srcISVCNamed("name-nomatch")
	nameNoMatch.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: testAffinity("metadata.name=c")}
	// noReq: declares no placement requirement; never fanned out, must be excluded
	// (and is not even in the index).
	noReq := srcISVCNamed("noreq")

	c := indexedPlacementClient(t, s, match, noMatch, nameMatch, nameNoMatch, noReq)
	r := &Reconciler{Client: c, Scheme: s, Log: log.Log}

	changed := readyWC("b", map[string]string{"gpu": "gb300", "metadata.name": "spoofed"})
	reqs := r.isvcsForClusterChange(context.Background(), changed)

	got := requestNames(reqs)
	assert.Contains(t, got, "match", "ISVC whose selector matches the changed cluster is enqueued")
	assert.NotContains(t, got, "nomatch", "ISVC whose selector does not match is skipped")
	assert.Contains(t, got, "name-match", "ISVC whose name selector matches the changed cluster is enqueued")
	assert.NotContains(t, got, "name-nomatch", "ISVC whose name selector does not match is skipped")
	assert.NotContains(t, got, "noreq", "ISVC with no placement requirement is skipped")
}

// Index: an ISVC whose status references the changed cluster (it is
// placed/racing there) is enqueued even when the post-change labels no longer
// match its selector — this is the "cluster leaving the candidate set" case the
// event's post-change object alone would miss.
func TestIsvcsForClusterChange_EnqueuesStatusReferencedClusterOnLabelLoss(t *testing.T) {
	s := testScheme(t)
	// Selector requires gpu=gb300, but the changed cluster has LOST that label.
	// The ISVC is currently placed on "b", so it must be re-evaluated.
	placed := srcISVCNamed("placed", "gpu=gb300")
	placed.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "b", Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}},
	}
	c := indexedPlacementClient(t, s, placed)
	r := &Reconciler{Client: c, Scheme: s, Log: log.Log}

	// Changed cluster "b" no longer carries gpu=gb300 (label removed).
	changed := readyWC("b", map[string]string{"gpu": "h100"})
	reqs := r.isvcsForClusterChange(context.Background(), changed)

	assert.Contains(t, requestNames(reqs), "placed",
		"ISVC placed on the cluster is re-enqueued even when the cluster's new labels no longer match its selector")
}

// Index: the field-index extractor marks exactly the requirement-
// declaring ISVCs (and leaves the rest unindexed).
func TestPlacementEligibleIndexExtractor(t *testing.T) {
	withAccel := srcISVCNamed("a", "gpu=gb300")
	withSelector := srcISVCNamed("b", "provider=cloud-a")
	withNone := srcISVCNamed("c")

	assert.Equal(t, []string{placementEligibleIndexValue}, placementEligibleIndexExtractor(withAccel))
	assert.Equal(t, []string{placementEligibleIndexValue}, placementEligibleIndexExtractor(withSelector))
	assert.Nil(t, placementEligibleIndexExtractor(withNone))
	// Wrong type yields no index entry.
	assert.Nil(t, placementEligibleIndexExtractor(&v1beta1.WorkloadCluster{}))
}

func TestModeOnlyPlacementClusterEvents(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy v1beta1.PlacementPolicy
		mode   v1beta1.PlacementMode
		want   bool
	}{
		{name: "single", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSingle, want: true},
		{name: "all", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeAll, want: true},
		{name: "split", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSplit, want: true},
		{name: "capacity", policy: v1beta1.PlacementPolicyClusterAffinity, mode: v1beta1.PlacementModeSplitByCapacity, want: true},
		{name: "retired policy requeues for validation", policy: v1beta1.PlacementPolicy("Legacy"), mode: v1beta1.PlacementModeAll, want: true},
		{name: "omitted policy requeues for validation", mode: v1beta1.PlacementModeSingle, want: true},
		{name: "local"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVCNamed("source")
			source.Namespace = "team-a"
			if tt.mode != "" {
				source.Spec.Placement = &v1beta1.PlacementSpec{Policy: tt.policy, Mode: tt.mode}
			}
			var wantIndex []string
			wantRequests := []ctrl.Request{}
			if source.Spec.Placement != nil {
				wantIndex = []string{placementEligibleIndexValue}
			}
			if tt.want {
				wantRequests = append(wantRequests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(source)})
			}
			if diff := cmp.Diff(wantIndex, placementEligibleIndexExtractor(source)); diff != "" {
				t.Fatalf("index (-want +got):\n%s", diff)
			}
			scheme := testScheme(t)
			r := &Reconciler{Client: indexedPlacementClient(t, scheme, source), Scheme: scheme, Log: log.Log}
			for _, cluster := range []*v1beta1.WorkloadCluster{
				readyWC("member-a", map[string]string{"region": "east"}),
				{ObjectMeta: metav1.ObjectMeta{Name: "member-b"}},
			} {
				t.Run(cluster.Name, func(t *testing.T) {
					got := r.isvcsForClusterChange(t.Context(), cluster)
					if diff := cmp.Diff(wantRequests, got); diff != "" {
						t.Fatalf("cluster event requests (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

// srcISVCNamed builds a named source; omitted selectors produce a local service.
func srcISVCNamed(name string, selectors ...string) *v1beta1.InferenceService {
	source := srcISVC("")
	source.Name = name
	source.UID = types.UID("uid-" + name)
	if len(selectors) == 0 {
		source.Spec.Placement = nil
	} else {
		source.Spec.Placement.ClusterAffinity = testAffinity(selectors...)
	}
	return source
}

// indexedPlacementClient builds a fake control-plane client with the placement-
// eligible field index registered (mirroring what cmd/manager installs on the
// manager cache), so isvcsForClusterChange's MatchingFields list resolves.
func indexedPlacementClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fakeclient.NewClientBuilder().WithScheme(s).
		WithIndex(&v1beta1.InferenceService{}, placementEligibleIndexField, placementEligibleIndexExtractor).
		WithObjects(objs...).
		Build()
}

func requestNames(reqs []ctrl.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Name)
	}
	return out
}

// hangingClient blocks every Create until the (per-cluster) context deadline
// fires, simulating a wedged remote apiserver that neither succeeds nor returns
// an error promptly.
type hangingClient struct {
	client.WithWatch
}

func (c hangingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	<-ctx.Done()
	return ctx.Err()
}

// Fault isolation: a wedged remote whose apply hangs is bounded by the
// per-cluster PlaceTimeout and skipped, so the healthy peer still gets the
// derived and the reconcile completes (Admitting) instead of stalling on the stuck
// cluster.
func TestReconcile_FanOutBoundsStuckClusterAndProceeds(t *testing.T) {
	s := testScheme(t)
	wa := hangingClient{WithWatch: fakeclient.NewClientBuilder().WithScheme(s).Build()} // wedged
	wb := emptyWorker(s)                                                                // healthy
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))
	r.PlaceTimeout = 50 * time.Millisecond // bound the wedged apply tightly for the test

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := r.Reconcile(context.Background(), req())
		assert.NoError(t, err, "a wedged cluster must not fail the whole reconcile")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile blocked on a stuck cluster; per-cluster timeout did not fire")
	}

	assert.True(t, hasDerived(t, wb), "healthy cluster b still got the derived despite the wedged peer")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
}

// srcISVCMode builds a source ISVC that drives placement through the structured
// spec.placement field with the given mode + requirement selector.
func srcISVCMode(mode v1beta1.PlacementMode, requirements string) *v1beta1.InferenceService {
	i := srcISVC("")
	i.Annotations = nil
	i.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: mode, ClusterAffinity: testAffinity(requirements)}
	return i
}

func placedAllISVC() *v1beta1.InferenceService {
	i := srcISVCMode(v1beta1.PlacementModeAll, "gpu=gb300")
	i.Finalizers = []string{PlacementFinalizer}
	i.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{
			{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("svc.a.example")},
			{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("svc.b.example")},
		},
	}
	return i
}

// workerWithGatedDerived returns a worker whose derived ISVC exists but whose
// engine IR reports no admitted instance — a home that fanned out but is still
// gated behind Kueue admission.
func workerWithGatedDerived(t *testing.T, s *runtime.Scheme) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent)
	derived.UID = "member-uid"
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	engineIR := irWithInstances(v1beta1.EngineComponent, false) // present, NOT admitted
	return fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(observedWorkerObjects(derived, engineIR)...).Build()
}

func candidatesByCluster(cs []v1beta1.CandidatePlacement) map[string]v1beta1.CandidatePlacement {
	m := map[string]v1beta1.CandidatePlacement{}
	for _, c := range cs {
		m[c.Cluster] = c
	}
	return m
}

// TestReconcile_AllModeKeepsEveryAdmittedHome: mode All places on every candidate
// that admits, keeps them all (no sweep, no single winner), and records each
// home's own endpoint.
func TestReconcile_AllModeKeepsEveryAdmittedHome(t *testing.T) {
	s := testScheme(t)
	wa := workerWithReadyURL(t, s, "svc.a.example", 2)
	wb := workerWithReadyURL(t, s, "svc.b.example", 1)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVCMode(v1beta1.PlacementModeAll, "gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.True(t, hasDerived(t, wa), "home a kept (no sweep)")
	assert.True(t, hasDerived(t, wb), "home b kept (no sweep)")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Empty(t, p.Cluster, "All has no single winner cluster")
	require.Len(t, p.Candidates, 2)
	by := candidatesByCluster(p.Candidates)
	require.Contains(t, by, "a")
	require.Contains(t, by, "b")
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, by["a"].Phase)
	assert.Equal(t, int32(2), by["a"].AdmittedReplicas)
	assert.Equal(t, int32(2), by["a"].ReadyReplicas)
	require.NotNil(t, by["a"].Endpoint)
	assert.Equal(t, "svc.a.example", by["a"].Endpoint.Host)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, by["b"].Phase)
	assert.Equal(t, int32(1), by["b"].AdmittedReplicas)
	assert.Equal(t, int32(1), by["b"].ReadyReplicas)
	require.NotNil(t, by["b"].Endpoint)
	assert.Equal(t, "svc.b.example", by["b"].Endpoint.Host)
}

// All mode keeps its published homes while the remote-client registry is
// rebuilding. WorkloadCluster readiness survives a controller restart, but the
// process-local clients do not, so an empty registry is not evidence that the
// serving homes disappeared.
func TestReconcile_AllModeDisconnectedStartupPreservesLastKnownHomes(t *testing.T) {
	s := testScheme(t)
	isvc := placedAllISVC()
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	require.Len(t, p.Candidates, 2)
	by := candidatesByCluster(p.Candidates)
	require.Contains(t, by, "a")
	require.Contains(t, by, "b")
	assert.Equal(t, "svc.a.example", by["a"].Endpoint.Host)
	assert.Equal(t, "svc.b.example", by["b"].Endpoint.Host)
}

func TestReconcile_AllModeNormalizesCarriedLegacyCandidatePhase(t *testing.T) {
	s := testScheme(t)
	isvc := placedAllISVC()
	isvc.Status.Placement.Candidates = []v1beta1.CandidatePlacement{{
		Cluster: "a", Phase: v1beta1.CandidatePhasePlaced, //nolint:staticcheck // Seed legacy status.
	}}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitting, p.Candidates[0].Phase)
}

func TestReconcile_AllModeDisconnectedNewSourceRetriesFast(t *testing.T) {
	s := testScheme(t)
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}},
		srcISVCMode(v1beta1.PlacementModeAll, "gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
	assert.Empty(t, p.Candidates)
}

// A disconnected home remains published while connected homes continue to be
// observed normally. One transport gap must not remove only that backend from
// an otherwise healthy All placement.
func TestReconcile_AllModePartialDisconnectPreservesLastKnownHome(t *testing.T) {
	s := testScheme(t)
	wa := workerWithAdmittedURL(t, s, "svc.a.example")
	isvc := placedAllISVC()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	require.Len(t, p.Candidates, 2)
	by := candidatesByCluster(p.Candidates)
	require.Contains(t, by, "a")
	require.Contains(t, by, "b")
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, by["a"].Phase)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, by["b"].Phase)
	assert.Equal(t, "svc.b.example", by["b"].Endpoint.Host)
}

func TestReconcile_AllModeObservesStandingHomeAcrossReadinessGate(t *testing.T) {
	s := testScheme(t)
	wa := workerWithReadyURL(t, s, "fresh.a.example", 2)
	wb := workerWithReadyURL(t, s, "fresh.b.example", 4)
	isvc := placedAllISVC()
	wcB := readyWC("b", map[string]string{"gpu": "gb300"})
	wcB.Status.Conditions[0].Status = metav1.ConditionFalse
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}, isvc, readyWC("a", map[string]string{"gpu": "gb300"}), wcB)

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	by := candidatesByCluster(p.Candidates)
	require.Contains(t, by, "a")
	require.Contains(t, by, "b")
	assert.Equal(t, int32(2), by["a"].ReadyReplicas)
	assert.Equal(t, int32(4), by["b"].ReadyReplicas)
	assert.Equal(t, "fresh.b.example", by["b"].Endpoint.Host)
}

// An aggregate Ready condition not written by this controller carries no
// per-candidate ingress attribution, so it must not be assigned to an
// unreadable home whose carried ReadyReplicas may have come from pods alone.
func TestReconcile_AllModeUnreadableHomeDoesNotBorrowLegacyReady(t *testing.T) {
	s := testScheme(t)
	wa := workerWithAdmittedURL(t, s, "svc.a.example")
	isvc := placedAllISVC()
	for i := range isvc.Status.Placement.Candidates {
		isvc.Status.Placement.Candidates[i].ReadyReplicas = 1
	}
	isvc.Status.SetCondition(apis.ConditionReady, &apis.Condition{
		Status: corev1.ConditionTrue,
		Reason: "LegacyAggregateReady",
	})
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	r, cp := newPlacer(s, clusters, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), req().NamespacedName, got))
	ready := got.Status.GetCondition(apis.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonUnknown, ready.Reason)
	by := candidatesByCluster(got.Status.Placement.Candidates)
	require.Contains(t, by, "b")
	assert.Zero(t, by["b"].ReadyReplicas,
		"an unproven carried count must not become routing weight")
}

func TestReconcile_AllModeBackendHoldRetainsOutgoingHome(t *testing.T) {
	s := testScheme(t)
	isvc := placedAllISVC()
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "h100"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	got := map[string]string{}
	for _, candidate := range p.Candidates {
		got[candidate.Cluster] = candidate.Endpoint.Host
	}
	if diff := cmp.Diff(map[string]string{"a": "svc.a.example", "b": "svc.b.example"}, got); diff != "" {
		t.Fatalf("held homes (-want +got):\n%s", diff)
	}
}

// TestReconcile_AllModePartialAdmitStaysPlaced: one home admitted, one still
// gated -> Placed (>=1 admitted), neither swept, gated home has no endpoint yet.
func TestReconcile_AllModePartialAdmitStaysPlaced(t *testing.T) {
	s := testScheme(t)
	wa := workerWithAdmittedURL(t, s, "svc.a.example") // admitted
	wb := workerWithGatedDerived(t, s)                 // present but gated
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVCMode(v1beta1.PlacementModeAll, "gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.True(t, hasDerived(t, wa))
	assert.True(t, hasDerived(t, wb), "gated home is NOT swept")
	// A home still gated must keep the placement re-polling (not fall to the long
	// steady-state backstop) so a home that admits later is observed promptly.
	assert.Positive(t, res.RequeueAfter, "re-polls while a home is still gated")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase, ">=1 admitted -> Placed")
	by := candidatesByCluster(p.Candidates)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, by["a"].Phase)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitting, by["b"].Phase)
	assert.Nil(t, by["b"].Endpoint, "gated home has no endpoint yet")
}

// All mode keeps every home independent. A failed IR read retains identity and
// the freshly observed endpoint, but publishes zero ready capacity.
func TestReconcile_AllModeRemoteIRFailureDoesNotHideHealthyHome(t *testing.T) {
	s := testScheme(t)
	bad := irGetFailClient{WithWatch: workerWithAdmittedURL(t, s, "svc.a.example")}
	good := workerWithAdmittedURL(t, s, "svc.b.example")
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(bad),
		"b": workloadcluster.NewNeverCachingClient(good),
	}}
	r, cp := newPlacer(s, clusters, srcISVCMode(v1beta1.PlacementModeAll, "gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	by := candidatesByCluster(p.Candidates)
	require.Contains(t, by, "a")
	assert.Zero(t, by["a"].ReadyReplicas)
	require.NotNil(t, by["a"].Endpoint)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, by["b"].Phase)
}

func TestReconcile_AllModeRepeatedIRFailureKeepsFreshObservation(t *testing.T) {
	s := testScheme(t)
	worker := &failIRGetAfterClient{
		WithWatch:        workerWithReplicaCounts(t, s, "fresh.a.example", 3, 2),
		successfulIRGets: 1,
	}
	src := srcISVCMode(v1beta1.PlacementModeAll, "gpu=gb300")
	src.Finalizers = []string{PlacementFinalizer}
	src.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("stale.a.example"), AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, src, readyWC("a", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter)
	assert.Greater(t, worker.irGets, worker.successfulIRGets, "the later authoritative inventory must fail")
	assert.Zero(t, worker.updates, "unknown physical inventory cannot authorize a member refresh")

	got := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), req().NamespacedName, got))
	p := got.Status.Placement
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	candidate := p.Candidates[0]
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, candidate.Phase)
	require.NotNil(t, candidate.Endpoint)
	assert.Equal(t, "fresh.a.example", candidate.Endpoint.Host)
	assert.Equal(t, int32(3), candidate.AdmittedReplicas)
	assert.Zero(t, candidate.ReadyReplicas, "an unknown post-apply read cannot publish ready capacity")
	ready := got.Status.GetCondition(apis.ConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, corev1.ConditionUnknown, ready.Status)
	assert.Equal(t, PlacementReadyReasonUnknown, ready.Reason)
}

// TestReconcile_AllModeAdmittingWhileAllGated: no home admitted yet -> Admitting, both
// kept, fast requeue.
func TestReconcile_AllModeAdmittingWhileAllGated(t *testing.T) {
	s := testScheme(t)
	wa, wb := emptyWorker(s), emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
		"b": workloadcluster.NewNeverCachingClient(wb),
	}}
	r, cp := newPlacer(s, clusters, srcISVCMode(v1beta1.PlacementModeAll, "gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("b", map[string]string{"gpu": "gb300"}))

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.True(t, hasDerived(t, wa))
	assert.True(t, hasDerived(t, wb))
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	assert.Empty(t, p.Cluster)
	assert.Len(t, p.Candidates, 2)
	assert.Positive(t, res.RequeueAfter, "admitting re-polls fast")
}

// A Split placement with no desired replicas remains Pending and does not fan out.
func TestReconcile_SplitWithNoDesiredReplicasHoldsPending(t *testing.T) {
	s := testScheme(t)
	wa := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(wa),
	}}
	r, cp := newPlacer(s, clusters, srcISVCMode(v1beta1.PlacementModeSplit, "gpu=gb300"),
		readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, wa), "a zero target fans out to nothing")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePending, p.Phase)
}

// srcISVCSplit builds a Split-mode source ISVC with the fleet-wide desired
// replica count set via spec.placement.split.replicas.
func srcISVCSplit(requirements string, replicas int32) *v1beta1.InferenceService {
	i := srcISVC("")
	i.Annotations = nil
	i.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity,
		Mode:            v1beta1.PlacementModeSplit,
		ClusterAffinity: testAffinity(requirements),
		Split:           &v1beta1.SplitSpec{Replicas: &replicas},
	}
	return i
}

// workerWithReplicas returns a worker whose derived engine IR reports `admitted`
// admitted instances and `ready` ready replicas, with an addressable status.URL.
// Models a home that Kueue admitted a fraction of the requested replicas.
func workerWithReplicas(t *testing.T, s *runtime.Scheme, host string, admitted, ready int32) client.WithWatch {
	t.Helper()
	derived := isvcWithInstances(v1beta1.EngineComponent)
	derived.UID = "member-uid"
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status.URL = &apis.URL{Scheme: "https", Host: host}
	if ready > 0 && host != "" {
		derived.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	}
	flags := make([]bool, admitted)
	for i := range flags {
		flags[i] = true
	}
	ir := irWithInstances(v1beta1.EngineComponent, flags...)
	ir.Status.ReadyReplicas = ready
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(observedWorkerObjects(derived, ir)...).Build()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if cur.Status.URL == nil {
		cur.Status = derived.Status
		require.NoError(t, w.Status().Update(context.Background(), cur))
	}
	return w
}

func TestSplitDesiredReplicas(t *testing.T) {
	// spec.placement.split.replicas wins.
	assert.Equal(t, int32(4), splitDesiredReplicas(srcISVCSplit("gpu=gb300", 4)))
	// Falls back to the engine floor when split.replicas is unset.
	i := srcISVCMode(v1beta1.PlacementModeSplit, "gpu=gb300")
	i.Spec.Engine.MinReplicas = ptr.To(3)
	assert.Equal(t, int32(3), splitDesiredReplicas(i))
	// Zero when neither is declared.
	assert.Equal(t, int32(0), splitDesiredReplicas(srcISVCMode(v1beta1.PlacementModeSplit, "gpu=gb300")))
}

// Split observes each home independently: one home's failed authoritative IR
// read must not abort the pass and strand the healthy home unobserved.
func TestReconcile_SplitRemoteIRFailureDoesNotAbortHealthyHome(t *testing.T) {
	s := testScheme(t)
	good := workerWithReplicas(t, s, "svc.a.example", 1, 1)
	bad := irGetFailClient{WithWatch: workerWithReplicas(t, s, "svc.z.example", 1, 1)}
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(good),
		"z": workloadcluster.NewNeverCachingClient(bad),
	}}
	r, cp := newPlacer(s, clusters, srcISVCSplit("gpu=gb300", 1),
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("z", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	by := candidatesByCluster(p.Candidates)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, by["a"].Phase)
}

// An unreadable home must not read as free capacity. Its last published
// admitted count carries into apportionment, so a transient read error cannot
// flip the pass out of its current regime and provision a duplicate elsewhere,
// and the home keeps its published candidate entry so the endpoint publisher
// does not drop its backend.
func TestReconcile_SplitUnreadableHomeKeepsItsShare(t *testing.T) {
	s := testScheme(t)
	good := workerWithReplicas(t, s, "svc.a.example", 1, 1)
	bad := irGetFailClient{WithWatch: workerWithReplicas(t, s, "svc.z.example", 1, 1)}
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(good),
		"z": workloadcluster.NewNeverCachingClient(bad),
	}}
	src := srcISVCSplit("gpu=gb300", 2)
	// z was admitted with one replica on a previous pass.
	src.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{
			{Cluster: "z", Phase: v1beta1.CandidatePhaseAdmitted, AdmittedReplicas: 1, ReadyReplicas: 1},
		},
	}
	r, cp := newPlacer(s, clusters, src,
		readyWC("a", map[string]string{"gpu": "gb300"}), readyWC("z", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	by := candidatesByCluster(p.Candidates)
	zc, ok := by["z"]
	require.True(t, ok, "unreadable home dropped from candidates; the publisher would delete its backend")
	assert.Equal(t, v1beta1.CandidatePhaseAdmitted, zc.Phase)
	assert.Equal(t, int32(1), zc.AdmittedReplicas, "unreadable home lost its published replica count")
	assert.Zero(t, zc.ReadyReplicas, "unknown health must not remain routable")
}

func TestReconcile_SplitTerminalHomeIsObservedWithoutReapply(t *testing.T) {
	s := testScheme(t)
	derived := isvcWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	derived.Namespace, derived.Name = "prod", "svc"
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	failedIR := irWithPhases(v1beta1.EngineComponent, v1beta1.OMENativeInstanceFailed)
	worker := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(derived, failedIR).Build()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, worker.Get(context.Background(), req().NamespacedName, cur))
	cur.Status = derived.Status
	require.NoError(t, worker.Status().Update(context.Background(), cur))
	mutations := 0
	counting := mutationCountingClient{WithWatch: worker, mutations: &mutations}

	src := srcISVCSplit("gpu=gb300", 1)
	src.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhasePlaced,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: apis.HTTPS("stale.a.example"), AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(counting),
	}}, src, readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Zero(t, mutations, "a terminal home must not be applied or swept")
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitting, p.Candidates[0].Phase)
	assert.Nil(t, p.Candidates[0].Endpoint)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)
}

func TestReconcile_SplitNormalizesCarriedLegacyCandidatePhase(t *testing.T) {
	s := testScheme(t)
	src := srcISVCSplit("gpu=gb300", 1)
	src.Status.Placement = &v1beta1.PlacementStatus{
		Phase: v1beta1.PlacementPhaseRacing, //nolint:staticcheck // Seed legacy status.
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhasePlaced, //nolint:staticcheck // Seed legacy status.
		}},
	}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, src,
		readyWC("a", map[string]string{"gpu": "gb300"}))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitting, p.Candidates[0].Phase)
}

func TestSplitScaleComponentsAndAccounting(t *testing.T) {
	// Engine-only ISVC -> just the engine.
	eng := srcISVCSplit("gpu=gb300", 3)
	assert.Equal(t, []v1beta1.ComponentType{v1beta1.EngineComponent}, placementScaleComponents(eng))

	// PD ISVC (engine + decoder) -> both are scaled components.
	pd := srcISVCSplit("gpu=gb300", 3)
	pd.Spec.Decoder = &v1beta1.DecoderSpec{}
	assert.Equal(t, []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}, placementScaleComponents(pd))

	// A PD replica is admitted only when BOTH components admit that instance, so
	// the count is the MIN across components: 3 engine + 2 decoder = 2 pairs.
	es := irWithInstances(v1beta1.EngineComponent, true, true, true).Status
	es.ReadyReplicas = 3
	ds := irWithInstances(v1beta1.DecoderComponent, true, true).Status
	ds.ReadyReplicas = 1
	statuses := map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus{
		v1beta1.EngineComponent:  &es,
		v1beta1.DecoderComponent: &ds,
	}
	comps := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	assert.Equal(t, int32(2), placementAdmittedReplicas(comps, statuses), "min admitted = complete pairs")
	assert.Equal(t, int32(1), placementReadyReplicas(comps, statuses), "min ready across components")

	// Engine-only accounting is just the engine's counts.
	assert.Equal(t, int32(3), placementAdmittedReplicas([]v1beta1.ComponentType{v1beta1.EngineComponent}, statuses))
}
