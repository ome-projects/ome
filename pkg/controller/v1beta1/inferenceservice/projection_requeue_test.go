package inferenceservice

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// TestReconcileProjectsIRStatusOnRequeuingPass pins the ordering contract
// that keeps an OMENative ISVC's component status alive during a rollout.
//
// AggregateIRStatus is the ONLY writer of
// status.components.<c>.lifecycle and of the top-level
// EngineReady / DecoderReady / RouterReady conditions. Reconcile
// short-circuits as soon as any Component (or the canary / coordination
// layer) asks to requeue — and a requeue is the steady state of an active
// rollout, not an exception. If the projector runs only on the no-requeue
// tail, the projection freezes for as long as anything requeues.
//
// That freeze is self-sustaining, which is what turns it from a latency
// bug into a wedge: the rollout engines gate on component readiness, so
// stale NotReady conditions hold the canary at its current step, the held
// canary keeps asking to requeue, and the requeue keeps the projection
// stale. The InferenceService then stays Ready=False/Initializing while
// its live InferenceReplica advances arbitrarily far and is fully Ready,
// with no reconcile error and no recovery across controller restarts.
//
// The requeue here comes from an unresolvable autoscalerPolicyRef: that
// hold recovers only via an operator-config edit (which emits no ISVC
// event), so the component carries a periodic requeue on EVERY pass —
// a permanently requeuing ISVC, exactly the shape that wedges.
func TestReconcileProjectsIRStatusOnRequeuingPass(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(appsv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(autoscalingv2.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(policyv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	const (
		ns       = "default"
		isvcName = "proj-isvc"
	)

	two := 2
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvcName,
			Namespace:  ns,
			UID:        types.UID(isvcName + "-uid"),
			Generation: 7,
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "proj-runtime", Kind: stringPtr("ServingRuntime")},
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: &two,
					MaxReplicas: 2,
					// A policyRef with no matching AutoscalerPolicy holds the
					// autoscaler at last-known-good and carries a periodic
					// requeue on every pass.
					AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: "missing-policy"},
				},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {},
			},
		},
	}

	rt := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-runtime", Namespace: ns},
		Spec: v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				Containers: []v1.Container{{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}},
			},
		},
	}

	// The live InferenceReplica the IR controller has already reconciled to
	// fully Ready. Its status is what the projection must pick up.
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvcName + "-engine",
			Namespace:  ns,
			Generation: 3,
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: "7",
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: isvcName},
			Replicas:  ptr.To(int32(2)),
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   3,
			Replicas:             2,
			ReadyReplicas:        2,
			ServingReplicas:      2,
			AvailableReplicas:    2,
			UpdatedReplicas:      2,
			UpdatedReadyReplicas: 2,
		},
	}

	c := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc, rt, ir).
		WithStatusSubresource(isvc, ir).
		Build()

	clientset := fake.NewClientset()
	_, err := clientset.CoreV1().ConfigMaps("ome").Create(context.TODO(), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-config", Namespace: "ome"},
		Data: map[string]string{
			"deploy": `{"defaultDeploymentMode": "RawDeployment"}`,
		},
	}, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	reconciler := &InferenceServiceReconciler{
		Client:                   c,
		APIReader:                c,
		ClientConfig:             &rest.Config{},
		Clientset:                clientset,
		Log:                      ctrl.Log.WithName("test"),
		Scheme:                   scheme,
		Recorder:                 record.NewFakeRecorder(64),
		RuntimeSelector:          runtimeselector.New(c),
		AcceleratorClassSelector: acceleratorclassselector.New(c),
		// Enables the policy layer so the missing AutoscalerPolicy resolves
		// to a hold rather than being ignored, and gives the hold a nonzero
		// periodic requeue.
		AutoscalerPolicyEnabled: true,
		ConfigCacheTTL:          30 * time.Second,
	}

	result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: isvcName, Namespace: ns},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Guard the premise: if this pass stopped requeueing, the test would
	// silently start exercising the no-requeue tail and stop covering the
	// regression.
	g.Expect(result.RequeueAfter).To(gomega.BeNumerically(">", 0),
		"premise: the autoscaler-policy hold must make this a requeueing pass")

	persisted := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.TODO(), types.NamespacedName{Name: isvcName, Namespace: ns}, persisted)).
		NotTo(gomega.HaveOccurred())

	lifecycle := persisted.Status.Components[v1beta1.EngineComponent].Lifecycle
	g.Expect(lifecycle).NotTo(gomega.BeNil(),
		"a requeueing pass must still project the live IR status onto the ISVC; "+
			"skipping it freezes status.components.engine.lifecycle for the whole rollout")
	g.Expect(lifecycle.ReadyReplicas).To(gomega.Equal(int32(2)),
		"the projection must carry the live IR counters, not a stale snapshot")
	g.Expect(lifecycle.Replicas).To(gomega.Equal(int32(2)))
	g.Expect(lifecycle.ObservedGeneration).To(gomega.Equal(int64(7)),
		"the projection must report the parent ISVC generation the IR has reconciled")

	engineReady := persisted.Status.GetCondition(v1beta1.EngineReady)
	g.Expect(engineReady).NotTo(gomega.BeNil(),
		"the top-level EngineReady condition rides the same write as the subtree; "+
			"without it the ISVC stays Initializing forever and the rollout can never start")
	g.Expect(engineReady.Status).To(gomega.Equal(v1.ConditionTrue),
		"EngineReady must reflect the live IR (2/2 Ready), got reason=%q message=%q",
		engineReady.Reason, engineReady.Message)
}

// TestReconcileProjectionConvergesAcrossRepeatedRequeues pins the
// convergence property: an ISVC that requeues on every pass must still
// track its InferenceReplica as the IR advances.
//
// A projection can fall many IR generations behind under uninterrupted
// reconciles, so one passing reconcile proves nothing: the projection
// must keep up while the requeue persists.
func TestReconcileProjectionConvergesAcrossRepeatedRequeues(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(appsv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(autoscalingv2.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(policyv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	const (
		ns       = "default"
		isvcName = "converge-isvc"
	)

	four := 4
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvcName,
			Namespace:  ns,
			UID:        types.UID(isvcName + "-uid"),
			Generation: 1,
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "proj-runtime", Kind: stringPtr("ServingRuntime")},
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:         &four,
					MaxReplicas:         4,
					AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: "missing-policy"},
				},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {},
			},
		},
	}

	rt := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-runtime", Namespace: ns},
		Spec: v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				Containers: []v1.Container{{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}},
			},
		},
	}

	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvcName + "-engine",
			Namespace:  ns,
			Generation: 1,
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: "1",
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))},
		},
		Spec:   v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: isvcName}, Replicas: ptr.To(int32(4))},
		Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 1},
	}

	c := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc, rt, ir).
		WithStatusSubresource(isvc, ir).
		Build()

	clientset := fake.NewClientset()
	_, err := clientset.CoreV1().ConfigMaps("ome").Create(context.TODO(), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-config", Namespace: "ome"},
		Data:       map[string]string{"deploy": `{"defaultDeploymentMode": "RawDeployment"}`},
	}, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	reconciler := &InferenceServiceReconciler{
		Client:                   c,
		APIReader:                c,
		ClientConfig:             &rest.Config{},
		Clientset:                clientset,
		Log:                      ctrl.Log.WithName("test"),
		Scheme:                   scheme,
		Recorder:                 record.NewFakeRecorder(256),
		RuntimeSelector:          runtimeselector.New(c),
		AcceleratorClassSelector: acceleratorclassselector.New(c),
		AutoscalerPolicyEnabled:  true,
		ConfigCacheTTL:           30 * time.Second,
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: isvcName, Namespace: ns}}
	key := types.NamespacedName{Name: isvcName, Namespace: ns}
	irKey := types.NamespacedName{Name: isvcName + "-engine", Namespace: ns}

	// Walk the IR forward one "generation" of readiness at a time, the way a
	// real rollout does, reconciling after each step. Every pass requeues.
	for ready := int32(1); ready <= 4; ready++ {
		live := &v1beta1.InferenceReplica{}
		g.Expect(c.Get(context.TODO(), irKey, live)).NotTo(gomega.HaveOccurred())
		live.Status.ObservedGeneration = live.Generation
		live.Status.Replicas = 4
		live.Status.ReadyReplicas = ready
		live.Status.ServingReplicas = ready
		live.Status.AvailableReplicas = ready
		g.Expect(c.Status().Update(context.TODO(), live)).NotTo(gomega.HaveOccurred())

		result, err := reconciler.Reconcile(context.TODO(), req)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(result.RequeueAfter).To(gomega.BeNumerically(">", 0),
			"premise: every pass must requeue (ready=%d)", ready)

		persisted := &v1beta1.InferenceService{}
		g.Expect(c.Get(context.TODO(), key, persisted)).NotTo(gomega.HaveOccurred())
		lifecycle := persisted.Status.Components[v1beta1.EngineComponent].Lifecycle
		g.Expect(lifecycle).NotTo(gomega.BeNil(),
			"projection missing while the IR is at readyReplicas=%d", ready)
		g.Expect(lifecycle.ReadyReplicas).To(gomega.Equal(ready),
			"projection must track the live IR, not lag it; IR=%d projected=%d",
			ready, lifecycle.ReadyReplicas)
	}

	// Fully rolled out: readiness must have flipped despite never having had
	// a single non-requeueing pass.
	persisted := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.TODO(), key, persisted)).NotTo(gomega.HaveOccurred())
	engineReady := persisted.Status.GetCondition(v1beta1.EngineReady)
	g.Expect(engineReady).NotTo(gomega.BeNil())
	g.Expect(engineReady.Status).To(gomega.Equal(v1.ConditionTrue),
		"EngineReady must flip once the IR reports 4/4 Ready, even though every pass requeued; "+
			"got reason=%q message=%q", engineReady.Reason, engineReady.Message)
}
