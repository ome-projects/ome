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
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// TestReconcileProgramsRouteWeightsOnRequeuingPass checks the trigger that
// makes a rollout's traffic targets reach the data plane.
//
// The rollout engines write status.components.<c>.traffic on requeueing
// passes, and a requeueing pass is the steady state of an active rollout:
// every step of a canary is entered, held and advanced on such a pass. If the
// routes were rendered only on the no-requeue tail, the HTTPRoutes would keep
// one unweighted backend on the aggregate Service for the whole rollout and
// the requested split would never be applied. The requeue here comes from an
// unresolvable autoscalerPolicyRef, which carries a periodic requeue on every
// pass; the targets are the ones a canary step publishes.
func TestReconcileProgramsRouteWeightsOnRequeuingPass(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(appsv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(autoscalingv2.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(policyv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(gatewayapiv1.Install(scheme)).NotTo(gomega.HaveOccurred())

	const (
		ns          = "default"
		isvcName    = "weights-isvc"
		stableRev   = isvcName + "-engine-rev-a1b2c3d4"
		canaryRev   = isvcName + "-engine-rev-e5f6a7b8"
		servingPort = int32(8080)
	)

	two := 2
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvcName,
			Namespace:  ns,
			UID:        types.UID(isvcName + "-uid"),
			Generation: 3,
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "weights-runtime", Kind: stringPtr("ServingRuntime")},
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{Container: v1.Container{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
				}},
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas:         &two,
					MaxReplicas:         2,
					AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: "missing-policy"},
				},
			},
		},
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				// The split a canary step publishes: the stable revision keeps
				// 90%, the canary revision receives 10%.
				v1beta1.EngineComponent: {Traffic: []v1beta1.ComponentTrafficTarget{
					{RevisionName: stableRev, Percent: 90, Tag: "stable"},
					{RevisionName: canaryRev, Percent: 10, Tag: "canary", LatestRevision: true},
				}},
			},
		},
	}

	rt := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "weights-runtime", Namespace: ns},
		Spec: v1beta1.ServingRuntimeSpec{
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				Containers: []v1.Container{{
					Name:  constants.MainContainerName,
					Image: "fake-serving:v1",
					Ports: []v1.ContainerPort{{Name: "http", ContainerPort: servingPort}},
				}},
			},
		},
	}

	// The live InferenceReplica is fully Ready, so the engine can back a route.
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvcName + "-engine",
			Namespace:  ns,
			Generation: 2,
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: "3",
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: isvcName},
			Replicas:  ptr.To(int32(2)),
		},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration:   2,
			Replicas:             2,
			ReadyReplicas:        2,
			ServingReplicas:      2,
			AvailableReplicas:    2,
			UpdatedReplicas:      2,
			UpdatedReadyReplicas: 2,
		},
	}

	// The per-revision Services the targets name, each on the serving port.
	perRevisionService := func(name string) *v1.Service {
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{
				Name: "http", Port: servingPort, TargetPort: intstr.FromInt32(servingPort), Protocol: v1.ProtocolTCP,
			}}},
		}
	}

	c := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc, rt, ir, perRevisionService(stableRev), perRevisionService(canaryRev)).
		WithStatusSubresource(isvc, ir).
		Build()

	clientset := fake.NewClientset()
	_, err := clientset.CoreV1().ConfigMaps("ome").Create(context.TODO(), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-config", Namespace: "ome"},
		Data: map[string]string{
			"deploy": `{"defaultDeploymentMode": "RawDeployment"}`,
			"ingress": `{"ingressGateway": "ome/gateway", "ingressService": "gateway",
				"omeIngressGateway": "ome/gateway", "ingressDomain": "example.com",
				"enableGatewayAPI": true}`,
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
		AutoscalerPolicyEnabled:  true,
		ConfigCacheTTL:           30 * time.Second,
	}

	result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: isvcName, Namespace: ns},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.RequeueAfter).To(gomega.BeNumerically(">", 0),
		"premise: the autoscaler-policy hold must make this a requeueing pass")

	persisted := &v1beta1.InferenceService{}
	g.Expect(c.Get(context.TODO(), types.NamespacedName{Name: isvcName, Namespace: ns}, persisted)).NotTo(gomega.HaveOccurred())
	g.Expect(persisted.Status.Components[v1beta1.EngineComponent].Traffic).To(gomega.HaveLen(2),
		"premise: the pass keeps the published targets")

	type backend struct {
		name   string
		weight int32
		port   int32
	}
	for _, routeName := range []string{constants.EngineServiceName(isvcName), isvcName} {
		route := &gatewayapiv1.HTTPRoute{}
		g.Expect(c.Get(context.TODO(), types.NamespacedName{Name: routeName, Namespace: ns}, route)).NotTo(gomega.HaveOccurred(),
			"a requeueing pass must render HTTPRoute %s; the whole rollout runs on such passes", routeName)
		g.Expect(route.Spec.Rules).To(gomega.HaveLen(1))
		backends := make([]backend, 0, len(route.Spec.Rules[0].BackendRefs))
		for _, ref := range route.Spec.Rules[0].BackendRefs {
			g.Expect(ref.Weight).NotTo(gomega.BeNil(), "route %s backend %s must carry the published weight", routeName, ref.Name)
			backends = append(backends, backend{name: string(ref.Name), weight: *ref.Weight, port: int32(*ref.Port)})
		}
		g.Expect(backends).To(gomega.Equal([]backend{
			{name: stableRev, weight: 90, port: servingPort},
			{name: canaryRev, weight: 10, port: servingPort},
		}), "route %s must split traffic exactly as status.components.engine.traffic says", routeName)
	}
}
