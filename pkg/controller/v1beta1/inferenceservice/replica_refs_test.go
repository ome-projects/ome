package inferenceservice

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	knapis "knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// referencingService is a service whose engine (and, when set, router) are
// standalone replicas.
func referencingService(engine, router string) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a", UID: types.UID("svc-uid"), Generation: 3},
		Spec:       v1beta1.InferenceServiceSpec{ReplicaRefs: &v1beta1.ReplicaRefs{Engine: []string{engine}}},
		Status:     v1beta1.InferenceServiceStatus{Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}},
	}
	if router != "" {
		isvc.Spec.ReplicaRefs.Router = []string{router}
	}
	return isvc
}

// runnersReplica is a standalone replica with one runner serving on port.
func runnersReplica(name string, c v1beta1.ComponentType, port int32) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: c,
			Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "server", Image: "example.com/serving:1.0", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: port}}}},
			}}}},
		},
	}
}

func newReferencedReconciler(t *testing.T, objs ...client.Object) (*InferenceServiceReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	recorder := record.NewFakeRecorder(10)
	return &InferenceServiceReconciler{Client: c, APIReader: c, Scheme: scheme, Log: ctrl.Log.WithName("test"), Recorder: recorder}, recorder
}

func TestFrontReferencedReplicaFromRunners(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := referencingService("pool-a", "router-a")
	r, _ := newReferencedReconciler(t, isvc, runnersReplica("pool-a", v1beta1.EngineComponent, 9000), runnersReplica("router-a", v1beta1.RouterComponent, 8000))
	cfg := &controllerconfig.InferenceServicesConfig{}
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent} {
		fronted, err := r.frontReferencedReplica(context.Background(), isvc, cfg, c)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(fronted).To(gomega.BeTrue())
	}
	svc := &corev1.Service{}
	g.Expect(r.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "svc-engine"}, svc)).To(gomega.Succeed())
	g.Expect(svc.Spec.Selector).To(gomega.Equal(map[string]string{
		constants.InferenceServicePodLabelKey: "pool-a",
		constants.OMEComponentLabel:           "engine",
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}))
	g.Expect(svc.Spec.Ports).To(gomega.HaveLen(1))
	g.Expect(svc.Spec.Ports[0].Port).To(gomega.Equal(int32(9000)))
	g.Expect(metav1.IsControlledBy(svc, isvc)).To(gomega.BeTrue())
	router := &corev1.Service{}
	g.Expect(r.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "svc-router"}, router)).To(gomega.Succeed())
	g.Expect(router.Spec.Selector[constants.InferenceServicePodLabelKey]).To(gomega.Equal("router-a"))
	g.Expect(router.Spec.Ports[0].Port).To(gomega.Equal(int32(8000)))
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].ScaleTargetRef.Name).To(gomega.Equal("pool-a"))

	// The service projects nothing and scales nothing: no replica of its own
	// name, no disruption budget, no autoscaler.
	err := r.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "svc-engine"}, &v1beta1.InferenceReplica{})
	g.Expect(client.IgnoreNotFound(err)).To(gomega.Succeed())
	g.Expect(err).To(gomega.HaveOccurred())
	pdbs := &policyv1.PodDisruptionBudgetList{}
	g.Expect(r.List(context.Background(), pdbs)).To(gomega.Succeed())
	g.Expect(pdbs.Items).To(gomega.BeEmpty())
	hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
	g.Expect(r.List(context.Background(), hpas)).To(gomega.Succeed())
	g.Expect(hpas.Items).To(gomega.BeEmpty())
}

func TestFrontReferencedReplicasRouterEngineDecoder(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := referencingService("pool-a", "router-a")
	isvc.Spec.ReplicaRefs.Decoder = []string{"pool-d"}
	r, _ := newReferencedReconciler(t, isvc,
		runnersReplica("pool-a", v1beta1.EngineComponent, 9000),
		runnersReplica("pool-d", v1beta1.DecoderComponent, 9001),
		runnersReplica("router-a", v1beta1.RouterComponent, 8000))
	replicas := map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "pool-a", v1beta1.DecoderComponent: "pool-d", v1beta1.RouterComponent: "router-a"}
	roles := irprojector.ReferencedRoles(isvc)
	g.Expect(roles).To(gomega.HaveLen(3))
	for _, c := range roles {
		fronted, err := r.frontReferencedReplica(context.Background(), isvc, &controllerconfig.InferenceServicesConfig{}, c)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(fronted).To(gomega.BeTrue())
		svc := &corev1.Service{}
		g.Expect(r.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "svc-" + string(c)}, svc)).To(gomega.Succeed())
		g.Expect(svc.Spec.Selector).To(gomega.Equal(map[string]string{
			constants.InferenceServicePodLabelKey: replicas[c],
			constants.OMEComponentLabel:           string(c),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		}), string(c))
		g.Expect(metav1.IsControlledBy(svc, isvc)).To(gomega.BeTrue())
	}
}

func TestFrontReferencedReplicaFromRevision(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := referencingService("pool-m", "")
	// A replica rendered from refs stores no runners; its current revision
	// carries the leader and worker templates.
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-m", Namespace: "team-a"},
		Spec:       v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent, ModelRef: &v1beta1.ModelRef{Name: "example-model"}},
		Status:     v1beta1.InferenceReplicaStatus{CurrentRevision: "pool-m-engine-0a1b2c3d"},
	}
	raw, err := json.Marshal(revision.DataPayload{
		PodSpec:       &corev1.PodSpec{Containers: []corev1.Container{{Name: "server", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 9100}}}}},
		WorkerPodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "server"}}},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	cr := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "pool-m-engine-0a1b2c3d", Namespace: "team-a"}, Data: runtime.RawExtension{Raw: raw}}
	r, _ := newReferencedReconciler(t, isvc, ir, cr)
	fronted, err := r.frontReferencedReplica(context.Background(), isvc, &controllerconfig.InferenceServicesConfig{}, v1beta1.EngineComponent)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(fronted).To(gomega.BeTrue())
	svc := &corev1.Service{}
	g.Expect(r.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "svc-engine"}, svc)).To(gomega.Succeed())
	g.Expect(svc.Spec.Ports[0].Port).To(gomega.Equal(int32(9100)))
	// A leader-fronted set selects the leader only.
	g.Expect(svc.Spec.Selector[query.LabelRunner]).To(gomega.Equal(string(v1beta1.RunnerNameLeader)))
	g.Expect(svc.Spec.Selector[query.LabelPodOrdinal]).To(gomega.Equal("0"))
}

func TestFrontReferencedReplicaKeepsServiceWhileRevisionUnread(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := referencingService("pool-m", "")
	// The replica names a revision the client does not hold; the Service
	// from an earlier pass keeps its ports instead of falling back to the
	// default port.
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-m", Namespace: "team-a"},
		Spec:       v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent, ModelRef: &v1beta1.ModelRef{Name: "example-model"}},
		Status:     v1beta1.InferenceReplicaStatus{CurrentRevision: "pool-m-engine-0a1b2c3d"},
	}
	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-engine", Namespace: "team-a"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 8080}}},
	}
	r, _ := newReferencedReconciler(t, isvc, ir, existing)
	fronted, err := r.frontReferencedReplica(context.Background(), isvc, &controllerconfig.InferenceServicesConfig{}, v1beta1.EngineComponent)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(fronted).To(gomega.BeTrue())
	svc := &corev1.Service{}
	g.Expect(r.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "svc-engine"}, svc)).To(gomega.Succeed())
	g.Expect(svc.Spec.Ports).To(gomega.HaveLen(1))
	g.Expect(svc.Spec.Ports[0].Port).To(gomega.Equal(int32(8080)))
	g.Expect(isvc.Status.Components[v1beta1.EngineComponent].ScaleTargetRef.Name).To(gomega.Equal("pool-m"))
}

func TestFrontReferencedReplicaMissingOrInvalid(t *testing.T) {
	g := gomega.NewWithT(t)
	isvc := referencingService("pool-a", "router-a")
	// The router reference names an engine replica.
	r, recorder := newReferencedReconciler(t, isvc, runnersReplica("router-a", v1beta1.EngineComponent, 8000))
	cfg := &controllerconfig.InferenceServicesConfig{}
	for _, tc := range []struct {
		c      v1beta1.ComponentType
		cond   knapis.ConditionType
		reason string
	}{
		{v1beta1.EngineComponent, v1beta1.EngineReady, ReasonReplicaRefMissing},
		{v1beta1.RouterComponent, v1beta1.RouterReady, ReasonReplicaRefInvalid},
	} {
		fronted, err := r.frontReferencedReplica(context.Background(), isvc, cfg, tc.c)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(fronted).To(gomega.BeFalse())
		cond := isvc.Status.GetCondition(tc.cond)
		g.Expect(cond).NotTo(gomega.BeNil())
		g.Expect(cond.Status).To(gomega.Equal(corev1.ConditionFalse))
		g.Expect(cond.Reason).To(gomega.Equal(tc.reason))
		g.Expect(<-recorder.Events).To(gomega.ContainSubstring(tc.reason))
	}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "svc-engine"}, &corev1.Service{})
	g.Expect(err).To(gomega.HaveOccurred(), "no Service is created for a role without a replica")
}

func TestServicesReferencingReplica(t *testing.T) {
	g := gomega.NewWithT(t)
	inline := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "inline", Namespace: "team-a"}, Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}
	r, _ := newReferencedReconciler(t, referencingService("pool-a", ""), inline)
	reqs := r.servicesReferencingReplica(context.Background(), runnersReplica("pool-a", v1beta1.EngineComponent, 8080))
	g.Expect(reqs).To(gomega.ConsistOf(ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: "svc"}}))
	g.Expect(r.servicesReferencingReplica(context.Background(), runnersReplica("pool-b", v1beta1.EngineComponent, 8080))).To(gomega.BeEmpty())
	projected := runnersReplica("pool-a", v1beta1.EngineComponent, 8080)
	projected.Spec.ParentRef = &v1beta1.ParentReference{Name: "other"}
	g.Expect(r.servicesReferencingReplica(context.Background(), projected)).To(gomega.BeEmpty())
}
