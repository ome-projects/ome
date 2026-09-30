package components

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/render"
)

func TestComponentReconcileChecksPlacementDemandBeforeWrites(t *testing.T) {
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		t.Run(string(component), func(t *testing.T) {
			for _, mode := range []constants.DeploymentModeType{constants.OMENative, constants.RawDeployment, constants.MultiNode} {
				t.Run(string(mode), func(t *testing.T) {
					for _, tt := range []struct {
						name       string
						wantErr    string
						allowWrite bool
					}{
						{name: "matching rendering", allowWrite: true},
						{name: "changed replica floor", allowWrite: true},
						{name: "changed primary resources", wantErr: "differs from the member rendering"},
						{name: "changed worker count", wantErr: "differs from the member rendering"},
						{name: "changed worker image", wantErr: "differs from the member rendering"},
						{name: "different rendering mode", wantErr: "differs from the member rendering"},
						{name: "incomplete gang", wantErr: "multi-pod replica"},
						{name: "missing runtime class", wantErr: "not found"},
						{name: "direct reader required", wantErr: "direct member reader"},
						{name: "invalid policy", wantErr: "invalid placement execution policy"},
						{name: "static member", allowWrite: true},
						{name: "local ignores demand", allowWrite: true},
						{name: "local ignores invalid policy", allowWrite: true},
					} {
						t.Run(tt.name, func(t *testing.T) {
							scheme := runtime.NewScheme()
							for _, add := range []func(*runtime.Scheme) error{v1beta1.AddToScheme, corev1.AddToScheme, nodev1.AddToScheme, appsv1.AddToScheme, autoscalingv2.AddToScheme, policyv1.AddToScheme, kedav1.AddToScheme, monitoringv1.AddToScheme, lws.AddToScheme} {
								if err := add(scheme); err != nil {
									t.Fatal(err)
								}
							}
							writeErr := errors.New("workload write reached")
							var writes []string
							reject := func(obj client.Object) error {
								writes = append(writes, fmt.Sprintf("%T", obj))
								return writeErr
							}
							cl := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
								Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
									return reject(obj)
								},
								Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
									return reject(obj)
								},
								Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
									return reject(obj)
								},
								Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteOption) error {
									return reject(obj)
								},
							}).Build()
							configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace}, Data: map[string]string{}}
							deps := &ComponentDeps{Client: cl, APIReader: cl, Clientset: kubefake.NewClientset(configMap), Scheme: scheme, Config: &controllerconfig.InferenceServicesConfig{}}
							in := ComponentInputs{DeploymentMode: mode}
							runner := &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1", Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{"example.com/gpu": resource.MustParse("2")}, Limits: corev1.ResourceList{"example.com/gpu": resource.MustParse("2")},
							}}}
							ext := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1)}
							leader := &v1beta1.LeaderSpec{Runner: runner.DeepCopy()}
							worker := &v1beta1.WorkerSpec{Runner: runner.DeepCopy(), Size: ptr.To(2)}
							engine := &v1beta1.EngineSpec{ComponentExtensionSpec: ext, Runner: runner, Leader: leader, Worker: worker}
							decoder := &v1beta1.DecoderSpec{ComponentExtensionSpec: ext, Runner: runner, Leader: leader, Worker: worker}
							service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "member-service", Namespace: "team-a", UID: "member-uid",
								Labels: map[string]string{constants.PlacementOrigin: "source-a"}, Annotations: map[string]string{},
							}}
							piece := &render.Piece{Client: cl, InferenceServiceConfig: deps.Config, DeploymentMode: mode}
							var rendered render.Rendered
							var err error
							if component == v1beta1.EngineComponent {
								service.Spec.Engine = engine
								rendered, err = render.RenderEngine(t.Context(), piece, service, engine)
							} else {
								service.Spec.Decoder = decoder
								rendered, err = render.RenderDecoder(t.Context(), piece, service, decoder)
							}
							if err != nil {
								t.Fatal(err)
							}
							sets, err := ReplicaTemplatesFrom(rendered.Templates).PodSets(mode, true, true)
							if err != nil {
								t.Fatal(err)
							}
							hashMode := mode
							if tt.name == "different rendering mode" {
								hashMode = constants.OMENative
								if mode == hashMode {
									hashMode = constants.MultiNode
								}
							}
							hash, err := capacity.ComponentFingerprint(t.Context(), cl, component, hashMode, sets)
							if err != nil {
								t.Fatal(err)
							}
							policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: "source-a", ClusterUID: "cluster-a", Demand: &v1beta1.PlacementDemandContract{
								Fingerprint: strings.Repeat("a", 64), Components: []v1beta1.PlacementComponentDemand{{Component: component, RenderingHash: hash}},
							}}
							allowWrite := tt.allowWrite
							switch tt.name {
							case "changed replica floor":
								engine.MinReplicas, decoder.MinReplicas = ptr.To(3), ptr.To(3)
							case "changed primary resources":
								leader.Runner.Resources.Requests["example.com/gpu"] = resource.MustParse("4")
								leader.Runner.Resources.Limits["example.com/gpu"] = resource.MustParse("4")
							case "changed worker count":
								worker.Size = ptr.To(3)
								allowWrite = mode == constants.RawDeployment
							case "changed worker image":
								worker.Runner.Image = "example.com/serving:v2"
								allowWrite = mode == constants.RawDeployment
							case "incomplete gang":
								engine.Worker, decoder.Worker = nil, nil
								allowWrite = mode == constants.RawDeployment
							case "missing runtime class":
								leader.RuntimeClassName = ptr.To("missing")
							case "direct reader required":
								deps.APIReader = nil
							case "static member":
								policy.Demand = nil
							case "local ignores demand", "local ignores invalid policy":
								service.Labels = nil
								deps.APIReader = nil
								policy.Demand.Components[0].RenderingHash = strings.Repeat("c", 64)
							}
							raw, err := protocol.Encode(policy)
							if err != nil {
								t.Fatal(err)
							}
							if strings.Contains(tt.name, "invalid policy") {
								raw = "{"
							}
							service.Annotations[constants.PlacementExecution] = raw
							var reconciler Component
							if component == v1beta1.EngineComponent {
								reconciler = NewEngine(deps, in, engine)
							} else {
								reconciler = NewDecoder(deps, in, decoder)
							}
							_, err = reconciler.Reconcile(t.Context(), service)
							var wantWrites []string
							if allowWrite {
								if !errors.Is(err, writeErr) {
									t.Fatalf("matching rendering did not reach workload projection: %v", err)
								}
								kind := map[constants.DeploymentModeType]string{constants.OMENative: "*v1beta1.InferenceReplica", constants.RawDeployment: "*v1.Deployment", constants.MultiNode: "*v1.LeaderWorkerSet"}[mode]
								wantWrites = []string{kind}
							} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
								t.Fatalf("error = %v, want %q", err, tt.wantErr)
							}
							if diff := cmp.Diff(wantWrites, writes); diff != "" {
								t.Fatalf("workload writes (-want +got):\n%s", diff)
							}
						})
					}
				})
			}
		})
	}
}

func TestReplicaPodSets(t *testing.T) {
	primary := &corev1.PodSpec{Containers: []corev1.Container{{Name: "primary"}}}
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
	for _, tt := range []struct {
		name                        string
		mode                        constants.DeploymentModeType
		leader, worker, missingSpec bool
		size                        int
		want                        []capacity.PodSet
		wantErr                     bool
	}{
		{name: "single native", mode: constants.OMENative, want: []capacity.PodSet{{Name: "primary", Count: 1, Spec: primary}}},
		{name: "raw excludes workers", mode: constants.RawDeployment, leader: true, worker: true, size: 2, want: []capacity.PodSet{{Name: "primary", Count: 1, Spec: primary}}},
		{name: "native gang", mode: constants.OMENative, leader: true, worker: true, size: 2, want: []capacity.PodSet{{Name: "primary", Count: 1, Spec: primary}, {Name: "workers", Count: 2, Spec: worker}}},
		{name: "multi node gang", mode: constants.MultiNode, leader: true, worker: true, size: 2, want: []capacity.PodSet{{Name: "primary", Count: 1, Spec: primary}, {Name: "workers", Count: 2, Spec: worker}}},
		{name: "orphan leader", mode: constants.OMENative, leader: true, wantErr: true},
		{name: "orphan worker", mode: constants.OMENative, worker: true, size: 2, wantErr: true},
		{name: "missing worker template", mode: constants.MultiNode, leader: true, worker: true, size: 2, missingSpec: true, wantErr: true},
		{name: "zero workers", mode: constants.MultiNode, leader: true, worker: true, wantErr: true},
		{name: "negative workers", mode: constants.OMENative, leader: true, worker: true, size: -1, wantErr: true},
		{name: "unsupported mode", mode: constants.VirtualDeployment, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			templates := ReplicaTemplates{Primary: primary, Worker: worker, WorkerSize: tt.size}
			if tt.missingSpec {
				templates.Worker = nil
			}
			got, err := templates.PodSets(tt.mode, tt.leader, tt.worker)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error:\n%s\n%v", diff, err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("replica shape (-want +got):\n%s", diff)
			}
		})
	}
}
