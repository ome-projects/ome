package placement

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// The placement pass reads one source and ends at the first step that decides
// for it: a source that is gone or local, a deleting source, the direct read,
// an outdated cleanup release request, invalid intent, a mode change against
// the accepted plan, the candidate match, the two policy preflights, the
// backend preflight, and the mode's own reconcile. One row per ending: the
// source and fleet it starts from, the result, and what the pass left behind.

// passRow is one shape of source and fleet and what the pass does with it.
type passRow struct {
	name string
	// source seeds the control-plane InferenceService; nil seeds none.
	source func() *v1beta1.InferenceService
	// clusters are the registered WorkloadClusters.
	clusters []*v1beta1.WorkloadCluster
	// objects are other control-plane objects: policies.
	objects []client.Object
	// workers are the connected member clusters by name.
	workers func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch
	// reader replaces the direct reader of the source when set.
	reader func(t *testing.T, s *runtime.Scheme) client.Reader
	// edit changes the reconciler before the pass.
	edit func(r *Reconciler)
	// wantErr is a fragment of the error the pass returns; empty means none.
	wantErr    string
	wantResult ctrl.Result
	// check reads what the pass left behind.
	check func(t *testing.T, p passOutcome)
}

// passOutcome is what a row can read after the pass.
type passOutcome struct {
	reconciler *Reconciler
	control    client.Client
	workers    map[string]client.WithWatch
}

// source reads the control-plane InferenceService after the pass.
func (p passOutcome) source(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	got := &v1beta1.InferenceService{}
	require.NoError(t, p.control.Get(context.Background(), req().NamespacedName, got))
	return got
}

// conditionReason is the reason of one condition of the source, or "" when
// the condition is not written.
func conditionReason(isvc *v1beta1.InferenceService, conditionType string) string {
	condition := isvc.Status.GetCondition(apis.ConditionType(conditionType))
	if condition == nil {
		return ""
	}
	return condition.Reason
}

// conditionMessage is the message of one condition of the source, or "".
func conditionMessage(isvc *v1beta1.InferenceService, conditionType string) string {
	condition := isvc.Status.GetCondition(apis.ConditionType(conditionType))
	if condition == nil {
		return ""
	}
	return condition.Message
}

// matchingCluster is a Ready cluster the test source's affinity matches.
func matchingCluster(name string, extra map[string]string) *v1beta1.WorkloadCluster {
	labels := map[string]string{"gpu": "gb300"}
	for key, value := range extra {
		labels[key] = value
	}
	return readyWC(name, labels)
}

// notReadyCluster is a matching cluster whose Ready condition is False.
func notReadyCluster(name string) *v1beta1.WorkloadCluster {
	cluster := matchingCluster(name, nil)
	cluster.Status.Conditions[0].Status = metav1.ConditionFalse
	cluster.Status.Conditions[0].Reason = "Unreachable"
	return cluster
}

// bothLabels carries the autoscaler and the rollout capability labels.
func bothLabels() map[string]string {
	labels := capabilityLabels()
	for key, value := range rolloutCapabilityLabels() {
		labels[key] = value
	}
	return labels
}

// withRolloutRef adds a ref-only rollout group to a source.
func withRolloutRef(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Spec.Rollout = srcISVCWithRolloutRef("gpu=gb300").Spec.Rollout
	return isvc
}

// deletingSource is the test source with a deletion timestamp and finalizers.
func deletingSource(finalizers ...string) *v1beta1.InferenceService {
	isvc := srcISVC("gpu=gb300")
	isvc.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	isvc.Finalizers = finalizers
	return isvc
}

// admittingSource is the test source recorded as racing on cluster "a".
func admittingSource() *v1beta1.InferenceService {
	return admittingSourceOn("a")()
}

// admittingSourceOn is the test source recorded as racing on one cluster.
func admittingSourceOn(cluster string) func() *v1beta1.InferenceService {
	return func() *v1beta1.InferenceService {
		isvc := srcISVC("gpu=gb300")
		isvc.Finalizers = []string{PlacementFinalizer}
		isvc.Status.Placement = &v1beta1.PlacementStatus{
			Phase:      v1beta1.PlacementPhaseAdmitting,
			Candidates: []v1beta1.CandidatePlacement{{Cluster: cluster, Phase: v1beta1.CandidatePhaseAdmitting}},
		}
		return isvc
	}
}

// singlePlanSource is the test source whose accepted plan is Single on "a"
// while its spec now asks for All.
func singlePlanSource() *v1beta1.InferenceService {
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	isvc.Spec.Placement.Mode = v1beta1.PlacementModeAll
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
		Plan: &v1beta1.PlacementPlanStatus{ID: "plan-1", Mode: v1beta1.PlacementModeSingle, Winner: "a", SourceUID: "uid-1", Revision: 1},
	}
	return isvc
}

// splitSourceWithRef is the autoscaler-referencing source in Split mode.
func splitSourceWithRef() *v1beta1.InferenceService {
	isvc := srcISVCWithRef("gpu=gb300")
	isvc.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	isvc.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
	return isvc
}

// emptyWorkers returns one empty member per name.
func emptyWorkers(names ...string) func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
	return func(_ *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
		workers := map[string]client.WithWatch{}
		for _, name := range names {
			workers[name] = emptyWorker(s)
		}
		return workers
	}
}

// workerWithPolicy is an empty member that carries the autoscaler policy copy.
func workerWithPolicy(t *testing.T, s *runtime.Scheme) client.WithWatch {
	t.Helper()
	worker := emptyWorker(s)
	require.NoError(t, worker.Create(context.Background(), hpaPolicy(testPolicyName)))
	return worker
}

// workerWithoutRuntime is a member with no ClusterServingRuntime.
func workerWithoutRuntime(s *runtime.Scheme) client.WithWatch {
	return fakeclient.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1beta1.InferenceService{}).Build()
}

// pollCadence is the result of a pass that wrote status: the poll cadence of
// the test reconciler.
var pollCadence = ctrl.Result{RequeueAfter: time.Second}

// placementPassRows are the rows of TestPlacementPass.
var placementPassRows = []passRow{
	{
		name: "a source that is gone ends the pass",
	},
	{
		name: "a source with no placement intent is left alone",
		source: func() *v1beta1.InferenceService {
			isvc := srcISVC("gpu=gb300")
			isvc.Spec.Placement = nil
			return isvc
		},
		clusters: []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:  emptyWorkers("a"),
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Empty(t, got.Finalizers)
			assert.Nil(t, got.Status.Placement)
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name: "a derived copy is left alone",
		source: func() *v1beta1.InferenceService {
			isvc := srcISVC("gpu=gb300")
			isvc.Labels = map[string]string{PlacementOriginLabel: "another-uid"}
			return isvc
		},
		clusters: []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:  emptyWorkers("a"),
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Empty(t, got.Finalizers)
			assert.Nil(t, got.Status.Placement)
		},
	},
	{
		name:   "a deleting source without the placement finalizer ends the pass",
		source: func() *v1beta1.InferenceService { return deletingSource("ome.io/another") },
		check: func(t *testing.T, p passOutcome) {
			assert.Nil(t, p.source(t).Status.Placement)
		},
	},
	{
		name:       "a deleting source waits for the endpoint finalizer",
		source:     func() *v1beta1.InferenceService { return deletingSource(PlacementFinalizer, EndpointFinalizer) },
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			assert.ElementsMatch(t, []string{PlacementFinalizer, EndpointFinalizer}, p.source(t).Finalizers)
		},
	},
	{
		name:    "the pass needs a direct reader",
		source:  func() *v1beta1.InferenceService { return srcISVC("gpu=gb300") },
		edit:    func(r *Reconciler) { r.APIReader = nil },
		wantErr: "placement requires a direct source reader",
	},
	{
		name:   "the direct read decides that the source is gone",
		source: func() *v1beta1.InferenceService { return srcISVC("gpu=gb300") },
		reader: func(_ *testing.T, s *runtime.Scheme) client.Reader {
			return fakeclient.NewClientBuilder().WithScheme(s).Build()
		},
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Empty(t, got.Finalizers)
			assert.Nil(t, got.Status.Placement)
		},
	},
	{
		name:   "the direct read decides that the intent is gone",
		source: func() *v1beta1.InferenceService { return srcISVC("gpu=gb300") },
		reader: func(_ *testing.T, s *runtime.Scheme) client.Reader {
			local := srcISVC("gpu=gb300")
			local.Spec.Placement = nil
			return fakeclient.NewClientBuilder().WithScheme(s).WithObjects(local).Build()
		},
		clusters: []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:  emptyWorkers("a"),
		check: func(t *testing.T, p passOutcome) {
			assert.Empty(t, p.source(t).Finalizers)
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name: "an outdated cleanup release request is removed and ends the pass",
		source: func() *v1beta1.InferenceService {
			isvc := srcISVC("gpu=gb300")
			isvc.Annotations[constants.PlacementReleaseCleanupWaitAnnotation] = "seven"
			return isvc
		},
		clusters:   []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:    emptyWorkers("a"),
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.NotContains(t, got.Annotations, constants.PlacementReleaseCleanupWaitAnnotation)
			assert.Empty(t, got.Finalizers)
			assert.Nil(t, got.Status.Placement)
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name: "invalid intent is written as Pending before any finalizer",
		source: func() *v1beta1.InferenceService {
			isvc := srcISVC("gpu=gb300")
			isvc.Annotations[ClusterSelectorAnnotation] = "gpu=gb300"
			return isvc
		},
		clusters:   []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:    emptyWorkers("a"),
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Empty(t, got.Finalizers)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhasePending, got.Status.Placement.Phase)
			assert.Equal(t, placementReadyReasonPending, conditionReason(got, string(apis.ConditionReady)))
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name:       "a mode change against the accepted plan holds",
		source:     singlePlanSource,
		clusters:   []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:    emptyWorkers("a"),
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Equal(t, "PlacementModeChangeBlocked", conditionReason(got, v1beta1.PlacementConverged))
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name:       "a source without a candidate gets the finalizer and Pending",
		source:     func() *v1beta1.InferenceService { return srcISVC("gpu=gb300") },
		clusters:   []*v1beta1.WorkloadCluster{readyWC("a", map[string]string{"gpu": "h100"})},
		workers:    emptyWorkers("a"),
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Contains(t, got.Finalizers, PlacementFinalizer)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhasePending, got.Status.Placement.Phase)
			assert.Empty(t, got.Status.Placement.Candidates)
		},
	},
	{
		name:       "one candidate is enough to fan out",
		source:     func() *v1beta1.InferenceService { return srcISVC("gpu=gb300") },
		clusters:   []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:    emptyWorkers("a"),
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhaseAdmitting, got.Status.Placement.Phase)
			assert.Len(t, got.Status.Placement.Candidates, 1)
			assert.True(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name:     "no Ready cluster keeps observing the standing member",
		source:   admittingSource,
		clusters: []*v1beta1.WorkloadCluster{notReadyCluster("a")},
		workers: func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
			return map[string]client.WithWatch{"a": workerWithAdmittedDerived(t, s)}
		},
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhasePlaced, got.Status.Placement.Phase)
			assert.Equal(t, "a", got.Status.Placement.Cluster)
		},
	},
	{
		name:       "the autoscaler preflight hold observes instead of fanning out",
		source:     func() *v1beta1.InferenceService { return srcISVCWithRef("gpu=gb300") },
		clusters:   []*v1beta1.WorkloadCluster{readyWC("a", capabilityLabels())},
		workers:    emptyWorkers("a"),
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhasePending, got.Status.Placement.Phase)
			assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPolicyMissing, conditionReason(got, v1beta1.PlacementPolicyPreflightCondition))
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name:       "while the autoscaler preflight holds the rollout preflight does not run",
		source:     func() *v1beta1.InferenceService { return withRolloutRef(srcISVCWithRef("gpu=gb300")) },
		clusters:   []*v1beta1.WorkloadCluster{readyWC("a", bothLabels())},
		workers:    emptyWorkers("a"),
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPolicyMissing, conditionReason(got, v1beta1.PlacementPolicyPreflightCondition))
			_, staged := p.reconciler.rolloutState().preflightFor(got.UID)
			assert.False(t, staged, "the rollout preflight staged a condition")
		},
	},
	{
		name:     "the rollout preflight gates the candidates the autoscaler preflight left",
		source:   func() *v1beta1.InferenceService { return withRolloutRef(srcISVCWithRef("gpu=gb300")) },
		clusters: []*v1beta1.WorkloadCluster{readyWC("a", bothLabels()), readyWC("b", rolloutCapabilityLabels())},
		objects:  []client.Object{hpaPolicy(testPolicyName), canaryRolloutPolicy(testRolloutPolicyName)},
		workers: func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
			return map[string]client.WithWatch{"a": workerWithPolicy(t, s), "b": emptyWorker(s)}
		},
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			message := conditionMessage(got, v1beta1.PlacementPolicyPreflightCondition)
			assert.Contains(t, message, "rollout policy: 1 of 1 candidates eligible")
			assert.True(t, hasDerived(t, p.workers["a"]))
			assert.False(t, hasDerived(t, p.workers["b"]))
		},
	},
	{
		name:     "a transient anchor read keeps the standing placement as it is",
		source:   func() *v1beta1.InferenceService { return placedSrcISVCWithRef("gpu=gb300") },
		clusters: []*v1beta1.WorkloadCluster{readyWC("a", capabilityLabels())},
		objects:  []client.Object{hpaPolicy(testPolicyName)},
		workers: func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
			return map[string]client.WithWatch{"a": workerWithAdmittedDerived(t, s)}
		},
		edit: func(r *Reconciler) {
			fail := true
			r.APIReader = flakyAnchorReader{Reader: r.APIReader, fail: &fail}
		},
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhasePlaced, got.Status.Placement.Phase)
			assert.Equal(t, "a", got.Status.Placement.Cluster)
			assert.Equal(t, "BackendNotObserved", conditionReason(got, v1beta1.PlacementBackendReady), "the pass went on to the backend preflight")
		},
	},
	{
		name:     "Split observes every matched member before the gates",
		source:   splitSourceWithRef,
		clusters: []*v1beta1.WorkloadCluster{readyWC("a", capabilityLabels())},
		workers: func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
			return map[string]client.WithWatch{"a": workerWithAdmittedDerived(t, s)}
		},
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPolicyMissing, conditionReason(got, v1beta1.PlacementPolicyPreflightCondition))
			require.Len(t, got.Status.Placement.Candidates, 1)
			assert.Equal(t, "a", got.Status.Placement.Candidates[0].Cluster)
			assert.Equal(t, v1beta1.PlacementPhasePlaced, got.Status.Placement.Phase)
		},
	},
	{
		name:     "a failed backend preflight holds and observes",
		source:   func() *v1beta1.InferenceService { return srcISVC("gpu=gb300") },
		clusters: []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers: func(_ *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
			return map[string]client.WithWatch{"a": workerWithoutRuntime(s)}
		},
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Equal(t, "BackendUnknown", conditionReason(got, v1beta1.PlacementBackendReady))
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhasePending, got.Status.Placement.Phase)
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name:     "a failed backend preflight keeps observing a standing member off the match",
		source:   admittingSourceOn("b"),
		clusters: []*v1beta1.WorkloadCluster{matchingCluster("a", nil), readyWC("b", map[string]string{"gpu": "h100"})},
		workers: func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
			return map[string]client.WithWatch{"a": workerWithoutRuntime(s), "b": workerWithAdmittedDerived(t, s)}
		},
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			assert.Equal(t, "BackendUnknown", conditionReason(got, v1beta1.PlacementBackendReady))
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhasePlaced, got.Status.Placement.Phase)
			assert.Equal(t, "b", got.Status.Placement.Cluster)
			assert.Nil(t, got.Status.Placement.Plan, "the pass went on to the mode's reconcile")
			assert.False(t, hasDerived(t, p.workers["a"]))
		},
	},
	{
		name:     "a transient member read re-checks an admission wait at the poll cadence",
		source:   func() *v1beta1.InferenceService { return srcISVCWithRef("gpu=gb300") },
		clusters: []*v1beta1.WorkloadCluster{readyWC("a", capabilityLabels()), readyWC("b", capabilityLabels())},
		objects:  []client.Object{hpaPolicy(testPolicyName)},
		workers: func(t *testing.T, s *runtime.Scheme) map[string]client.WithWatch {
			return map[string]client.WithWatch{"a": workerWithPolicy(t, s), "b": unreadablePolicyWorker(t, s)}
		},
		edit:       longBackstop,
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			got := p.source(t)
			require.NotNil(t, got.Status.Placement)
			assert.Equal(t, v1beta1.PlacementPhaseAdmitting, got.Status.Placement.Phase)
			assert.True(t, hasDerived(t, p.workers["a"]), "the readable member is a nominee")
			assert.False(t, hasDerived(t, p.workers["b"]), "the unreadable member waits for the next read")
		},
	},
	{
		name:       "any mode but split-by-capacity drops the pending hardware record",
		source:     func() *v1beta1.InferenceService { return srcISVC("gpu=gb300") },
		clusters:   []*v1beta1.WorkloadCluster{matchingCluster("a", nil)},
		workers:    emptyWorkers("a"),
		edit:       pendingHardware,
		wantResult: pollCadence,
		check: func(t *testing.T, p passOutcome) {
			assert.False(t, p.hardwarePending(), "the stability window of another mode is dropped")
		},
	},
}

// unreadablePolicyWorker is an empty member whose autoscaler policy reads fail
// with an error that can clear by itself.
func unreadablePolicyWorker(t *testing.T, s *runtime.Scheme) client.WithWatch {
	t.Helper()
	return fakeclient.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(backendTestRuntime()).
		WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1beta1.AutoscalerPolicy); ok {
				return errors.New("connection reset by peer")
			}
			return cl.Get(ctx, key, obj, opts...)
		}}).Build()
}

// longBackstop sets the steady-state re-read backstop well above the poll
// cadence, so a shortened re-check is visible in the result.
func longBackstop(r *Reconciler) {
	r.converge = &convergeConfig{safetyRequeue: 30 * time.Second}
}

// pendingHardware seeds a hardware change that waits for its stability window.
func pendingHardware(r *Reconciler) {
	r.capacityPending = map[types.NamespacedName]capacityPending{
		req().NamespacedName: {uid: "uid-1", digest: "digest-1", since: time.Now()},
	}
}

// hardwarePending reports that the source's hardware change still waits.
func (p passOutcome) hardwarePending() bool {
	_, pending := p.reconciler.capacityPending[req().NamespacedName]
	return pending
}

func TestPlacementPass(t *testing.T) {
	for _, row := range placementPassRows {
		t.Run(row.name, func(t *testing.T) {
			s := testScheme(t)
			workers := map[string]client.WithWatch{}
			if row.workers != nil {
				workers = row.workers(t, s)
			}
			connections := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}
			for name, worker := range workers {
				connections.m[name] = workloadcluster.NewNeverCachingClient(worker)
			}
			objects := append([]client.Object{}, row.objects...)
			if row.source != nil {
				objects = append(objects, row.source())
			}
			for _, cluster := range row.clusters {
				objects = append(objects, cluster)
			}
			reconciler, control := newPlacer(s, connections, objects...)
			if row.reader != nil {
				reconciler.APIReader = row.reader(t, s)
			}
			if row.edit != nil {
				row.edit(reconciler)
			}

			result, err := reconciler.reconcile(context.Background(), req())

			if row.wantErr != "" {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), row.wantErr), "error %q lacks %q", err, row.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, row.wantResult, result)
			if row.check != nil {
				row.check(t, passOutcome{reconciler: reconciler, control: control, workers: workers})
			}
		})
	}
}
