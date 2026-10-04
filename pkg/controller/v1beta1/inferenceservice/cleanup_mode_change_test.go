package inferenceservice

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

const (
	modeChangeISVC = "test-isvc"
	modeChangeNS   = "default"
	modeChangeUID  = "test-uid"
)

type modeMap = map[v1beta1.ComponentType]constants.DeploymentModeType

func modeChangeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, autoscalingv2.AddToScheme(scheme))
	require.NoError(t, lws.AddToScheme(scheme))
	return scheme
}

func modeChangeISVCObject() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: modeChangeISVC, Namespace: modeChangeNS, UID: modeChangeUID},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
}

// newModeChangeReconciler builds a reconciler over a fake client that holds
// isvc and objs, with a recorder whose events the test can drain.
func newModeChangeReconciler(t *testing.T, isvc *v1beta1.InferenceService, objs ...client.Object) (*InferenceServiceReconciler, client.Client, *record.FakeRecorder) {
	t.Helper()
	c := fakeclient.NewClientBuilder().
		WithScheme(modeChangeScheme(t)).
		WithObjects(append([]client.Object{isvc}, objs...)...).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		Build()
	rec := record.NewFakeRecorder(32)
	r := &InferenceServiceReconciler{
		Client:    c,
		APIReader: c,
		Clientset: fake.NewSimpleClientset(),
		Recorder:  rec,
	}
	return r, c, rec
}

func modeChangeContext() context.Context {
	return log.IntoContext(context.Background(), log.Log)
}

func drainEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case ev := <-rec.Events:
			events = append(events, ev)
		default:
			return events
		}
	}
}

func createPerRevisionService(name string, component v1beta1.ComponentType) *corev1.Service {
	svc := createService(name, modeChangeNS, modeChangeISVC, modeChangeUID, component)
	svc.Labels[query.LabelManagedBy] = query.ManagedByOMENative
	svc.Labels[query.LabelRevisionHash] = "abc123"
	return svc
}

func createLeaderWorkerSet(name string, component v1beta1.ComponentType) *lws.LeaderWorkerSet {
	return &lws.LeaderWorkerSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: modeChangeNS,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: modeChangeISVC,
				constants.OMEComponentLabel:           string(component),
			},
			OwnerReferences: []metav1.OwnerReference{{
				Kind:       "InferenceService",
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Name:       modeChangeISVC,
				UID:        types.UID(modeChangeUID),
			}},
		},
	}
}

func objectKey(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: modeChangeNS, Name: name}
}

func assertGone(t *testing.T, c client.Client, obj client.Object, name string) {
	t.Helper()
	err := c.Get(context.Background(), objectKey(name), obj)
	assert.True(t, apierrors.IsNotFound(err), "%T %s must be gone, got err=%v", obj, name, err)
}

func assertPresent(t *testing.T, c client.Client, obj client.Object, name string) {
	t.Helper()
	require.NoError(t, c.Get(context.Background(), objectKey(name), obj), "%T %s must still exist", obj, name)
	assert.Nil(t, obj.GetDeletionTimestamp(), "%T %s must not be terminating", obj, name)
}

// assertTerminating checks that deletion of a finalizer-bearing object was
// requested: the fake client keeps it with a deletion timestamp.
func assertTerminating(t *testing.T, c client.Client, obj client.Object, name string) {
	t.Helper()
	require.NoError(t, c.Get(context.Background(), objectKey(name), obj))
	assert.NotNil(t, obj.GetDeletionTimestamp(), "%T %s must be terminating", obj, name)
}

// A Component keeps only the workload object of the backend its deployment
// mode selects; the workload of any other backend, and the per-revision
// Services only OMENative projects, are deleted, recorded as an event, and
// reflected on the Component's ready condition.
func TestCleanupReplacedBackendsRemovesTheReplacedWorkload(t *testing.T) {
	const (
		engineName  = modeChangeISVC + "-engine"
		decoderName = modeChangeISVC + "-decoder"
		lwsName     = "lws-" + engineName
	)
	engineIR := func() *v1beta1.InferenceReplica {
		return createInferenceReplica(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent)
	}
	engineDeployment := func() *appsv1.Deployment {
		return createDeployment(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent)
	}

	cases := []struct {
		name      string
		modes     modeMap
		objects   []client.Object
		check     func(t *testing.T, c client.Client)
		wantEvent string
		wantCond  bool
	}{
		{
			name:  "RawDeployment to OMENative deletes the Deployment and keeps the InferenceReplica and the stable Service",
			modes: modeMap{v1beta1.EngineComponent: constants.OMENative},
			objects: []client.Object{
				engineDeployment(),
				engineIR(),
				createService(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent),
			},
			check: func(t *testing.T, c client.Client) {
				assertGone(t, c, &appsv1.Deployment{}, engineName)
				assertPresent(t, c, &v1beta1.InferenceReplica{}, engineName)
				assertPresent(t, c, &corev1.Service{}, engineName)
			},
			wantEvent: "Deployment " + engineName,
			wantCond:  true,
		},
		{
			name:  "OMENative to RawDeployment requests the InferenceReplica teardown and deletes the per-revision Services",
			modes: modeMap{v1beta1.EngineComponent: constants.RawDeployment},
			objects: []client.Object{
				engineIR(),
				engineDeployment(),
				createService(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent),
				createPerRevisionService(engineName+"-rev-abc123", v1beta1.EngineComponent),
				createPerRevisionService(engineName+"-rev-abc123-headless", v1beta1.EngineComponent),
			},
			check: func(t *testing.T, c client.Client) {
				assertTerminating(t, c, &v1beta1.InferenceReplica{}, engineName)
				assertPresent(t, c, &appsv1.Deployment{}, engineName)
				assertPresent(t, c, &corev1.Service{}, engineName)
				assertGone(t, c, &corev1.Service{}, engineName+"-rev-abc123")
				assertGone(t, c, &corev1.Service{}, engineName+"-rev-abc123-headless")
			},
			wantEvent: "InferenceReplica " + engineName,
			wantCond:  true,
		},
		{
			name:  "RawDeployment to MultiNode deletes the Deployment and keeps the LeaderWorkerSet",
			modes: modeMap{v1beta1.EngineComponent: constants.MultiNode},
			objects: []client.Object{
				engineDeployment(),
				createLeaderWorkerSet(lwsName, v1beta1.EngineComponent),
			},
			check: func(t *testing.T, c client.Client) {
				assertGone(t, c, &appsv1.Deployment{}, engineName)
				assertPresent(t, c, &lws.LeaderWorkerSet{}, lwsName)
			},
			wantEvent: "Deployment " + engineName,
			wantCond:  true,
		},
		{
			name:  "MultiNode to OMENative deletes the LeaderWorkerSet and keeps the InferenceReplica",
			modes: modeMap{v1beta1.EngineComponent: constants.OMENative},
			objects: []client.Object{
				createLeaderWorkerSet(lwsName, v1beta1.EngineComponent),
				engineIR(),
			},
			check: func(t *testing.T, c client.Client) {
				assertGone(t, c, &lws.LeaderWorkerSet{}, lwsName)
				assertPresent(t, c, &v1beta1.InferenceReplica{}, engineName)
			},
			wantEvent: "LeaderWorkerSet " + lwsName,
			wantCond:  true,
		},
		{
			name: "every declared Component is swept on its own mode",
			modes: modeMap{
				v1beta1.EngineComponent:  constants.OMENative,
				v1beta1.DecoderComponent: constants.OMENative,
			},
			objects: []client.Object{
				engineDeployment(),
				createDeployment(decoderName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.DecoderComponent),
				engineIR(),
				createInferenceReplica(decoderName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.DecoderComponent),
			},
			check: func(t *testing.T, c client.Client) {
				assertGone(t, c, &appsv1.Deployment{}, engineName)
				assertGone(t, c, &appsv1.Deployment{}, decoderName)
				assertPresent(t, c, &v1beta1.InferenceReplica{}, engineName)
				assertPresent(t, c, &v1beta1.InferenceReplica{}, decoderName)
			},
			wantEvent: "Deployment " + decoderName,
			wantCond:  true,
		},
		{
			name:  "a Component absent from the modes is not this sweep's to remove",
			modes: modeMap{v1beta1.EngineComponent: constants.RawDeployment},
			objects: []client.Object{
				engineDeployment(),
				createInferenceReplica(decoderName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.DecoderComponent),
			},
			check: func(t *testing.T, c client.Client) {
				assertPresent(t, c, &appsv1.Deployment{}, engineName)
				assertPresent(t, c, &v1beta1.InferenceReplica{}, decoderName)
			},
		},
		{
			name:  "a workload the InferenceService does not own is left alone",
			modes: modeMap{v1beta1.EngineComponent: constants.OMENative},
			objects: []client.Object{
				createDeploymentWithoutOwner(engineName, modeChangeNS, modeChangeISVC, v1beta1.EngineComponent),
				engineIR(),
			},
			check: func(t *testing.T, c client.Client) {
				assertPresent(t, c, &appsv1.Deployment{}, engineName)
			},
		},
		{
			name:  "a mode that projects no workload deletes nothing",
			modes: modeMap{v1beta1.EngineComponent: constants.PDDisaggregated},
			objects: []client.Object{
				engineDeployment(),
				engineIR(),
			},
			check: func(t *testing.T, c client.Client) {
				assertPresent(t, c, &appsv1.Deployment{}, engineName)
				assertPresent(t, c, &v1beta1.InferenceReplica{}, engineName)
			},
		},
		{
			name:  "a workload already being deleted is left to finish without a new record",
			modes: modeMap{v1beta1.EngineComponent: constants.RawDeployment},
			objects: []client.Object{
				func() *v1beta1.InferenceReplica {
					ir := engineIR()
					now := metav1.Now()
					ir.DeletionTimestamp = &now
					return ir
				}(),
				engineDeployment(),
			},
			check: func(t *testing.T, c client.Client) {
				assertTerminating(t, c, &v1beta1.InferenceReplica{}, engineName)
				assertPresent(t, c, &appsv1.Deployment{}, engineName)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := modeChangeISVCObject()
			r, c, rec := newModeChangeReconciler(t, isvc, tc.objects...)

			require.NoError(t, r.cleanupReplacedBackends(modeChangeContext(), isvc, tc.modes))
			tc.check(t, c)

			events := drainEvents(rec)
			if tc.wantEvent == "" {
				assert.Empty(t, events, "nothing was removed, so nothing is recorded")
			} else {
				assert.True(t, containsEvent(events, deploymentModeChangedReason, tc.wantEvent),
					"want a %s event mentioning %q, got %v", deploymentModeChangedReason, tc.wantEvent, events)
			}

			cond := isvc.Status.GetCondition(v1beta1.EngineReady)
			if tc.wantCond {
				require.NotNil(t, cond, "the engine ready condition records the mode change")
				assert.Equal(t, corev1.ConditionFalse, cond.Status)
				assert.Equal(t, deploymentModeChangedReason, cond.Reason)
			} else {
				assert.Nil(t, cond, "no removal, no condition")
			}
		})
	}
}

func containsEvent(events []string, reason, fragment string) bool {
	for _, ev := range events {
		if strings.Contains(ev, reason) && strings.Contains(ev, fragment) {
			return true
		}
	}
	return false
}

func omeNativeComponentStatus() v1beta1.ComponentStatusSpec {
	return v1beta1.ComponentStatusSpec{
		URL:                     &knapis.URL{Scheme: "http", Host: "engine.example.com"},
		Lifecycle:               &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 2},
		RolloutPhase:            v1beta1.RolloutPhaseStable,
		LatestReadyRevision:     modeChangeISVC + "-engine-rev-abc123",
		LatestRolledoutRevision: modeChangeISVC + "-engine-rev-abc123",
		Traffic:                 []v1beta1.ComponentTrafficTarget{{RevisionName: modeChangeISVC + "-engine-rev-abc123", Percent: 100}},
	}
}

// A Component that runs under a backend other than OMENative carries none of
// the status only OMENative writes, on the live object as well as in the
// pass's copy; the rest of its status is untouched.
func TestCleanupReplacedBackendsClearsTheOMENativeStatus(t *testing.T) {
	isvc := modeChangeISVCObject()
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: omeNativeComponentStatus(),
	}
	r, c, _ := newModeChangeReconciler(t, isvc)
	ctx := modeChangeContext()

	t.Run("a Component still on OMENative keeps it", func(t *testing.T) {
		require.NoError(t, r.cleanupReplacedBackends(ctx, isvc, modeMap{v1beta1.EngineComponent: constants.OMENative}))
		assert.NotNil(t, isvc.Status.Components[v1beta1.EngineComponent].Lifecycle)
	})

	t.Run("a Component on RawDeployment loses it live and in memory", func(t *testing.T) {
		require.NoError(t, r.cleanupReplacedBackends(ctx, isvc, modeMap{v1beta1.EngineComponent: constants.RawDeployment}))

		for name, cs := range map[string]v1beta1.ComponentStatusSpec{
			"in-memory": isvc.Status.Components[v1beta1.EngineComponent],
			"live": func() v1beta1.ComponentStatusSpec {
				live := &v1beta1.InferenceService{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(isvc), live))
				return live.Status.Components[v1beta1.EngineComponent]
			}(),
		} {
			assert.Nil(t, cs.Lifecycle, "%s lifecycle", name)
			assert.Empty(t, cs.RolloutPhase, "%s rollout phase", name)
			assert.Empty(t, cs.LatestReadyRevision, "%s latest ready revision", name)
			assert.Empty(t, cs.LatestRolledoutRevision, "%s latest rolled-out revision", name)
			assert.Empty(t, cs.Traffic, "%s traffic", name)
			require.NotNil(t, cs.URL, "%s url is not OMENative's to clear", name)
			assert.Equal(t, "engine.example.com", cs.URL.Host, "%s url", name)
		}
	})

	t.Run("a second pass has nothing to clear", func(t *testing.T) {
		before := &v1beta1.InferenceService{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(isvc), before))
		require.NoError(t, r.cleanupReplacedBackends(ctx, isvc, modeMap{v1beta1.EngineComponent: constants.RawDeployment}))
		after := &v1beta1.InferenceService{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(isvc), after))
		assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "no write when nothing changes")
	})
}

// A VirtualDeployment selects no backend: every object a backend projected
// is removed and recorded, and the OMENative status footprint is cleared.
func TestCleanupVirtualDeploymentRemovesEveryOwnedObject(t *testing.T) {
	const engineName = modeChangeISVC + "-engine"
	isvc := modeChangeISVCObject()
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: omeNativeComponentStatus(),
	}
	external := createService(modeChangeISVC, modeChangeNS, modeChangeISVC, modeChangeUID, "external-service")
	r, c, rec := newModeChangeReconciler(t, isvc,
		createDeployment(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent),
		createService(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent),
		createPerRevisionService(engineName+"-rev-abc123", v1beta1.EngineComponent),
		createHPA(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent),
		createConfigMap(engineName+"-config", modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent),
		createInferenceReplica(engineName, modeChangeNS, modeChangeISVC, modeChangeUID, v1beta1.EngineComponent),
		external,
	)
	ctx := modeChangeContext()

	require.NoError(t, r.cleanupVirtualDeployment(ctx, isvc))

	assertGone(t, c, &appsv1.Deployment{}, engineName)
	assertGone(t, c, &corev1.Service{}, engineName)
	assertGone(t, c, &corev1.Service{}, engineName+"-rev-abc123")
	assertGone(t, c, &corev1.Service{}, modeChangeISVC)
	assertGone(t, c, &autoscalingv2.HorizontalPodAutoscaler{}, engineName)
	assertGone(t, c, &corev1.ConfigMap{}, engineName+"-config")
	assertTerminating(t, c, &v1beta1.InferenceReplica{}, engineName)

	events := drainEvents(rec)
	assert.Len(t, events, 7, "one record per removed object, got %v", events)
	for _, ev := range events {
		assert.Contains(t, ev, deploymentModeChangedReason)
		assert.Contains(t, ev, "VirtualDeployment")
	}

	live := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(isvc), live))
	assert.Nil(t, live.Status.Components[v1beta1.EngineComponent].Lifecycle)
	assert.Nil(t, isvc.Status.Components[v1beta1.EngineComponent].Lifecycle)

	t.Run("a second pass finds nothing and records nothing", func(t *testing.T) {
		require.NoError(t, r.cleanupVirtualDeployment(ctx, isvc))
		assert.Empty(t, drainEvents(rec))
	})
}
