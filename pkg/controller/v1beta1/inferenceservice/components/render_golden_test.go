package components

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/specdefaults"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	v1beta1testing "sigs.k8s.io/ome/pkg/utils/testing/v1beta1"
)

var updateRenderGoldens = flag.Bool("update", false, "rewrite the render goldens from this run")

const (
	goldenNamespace       = "team-a"
	goldenGPU             = "example.com/gpu"
	goldenScopeUID        = types.UID("scope-uid")
	goldenAcceleratorName = "accelerator-a"
	goldenContainerName   = "ome-container"
	// excludedAnnotationKeysSeparator joins the inherited annotation keys the
	// projector records on the replica; the replica controller splits on it.
	excludedAnnotationKeysSeparator = ","
)

// renderGolden is the projector's output for one component: what an
// InferenceService writes onto its InferenceReplica, the hash inputs that
// live outside spec.runners (topology key, pairing protocol, excluded
// annotation keys), and the revision hash the replica controller derives
// from all of it.
type renderGolden struct {
	Runners                []v1beta1.Runner              `json:"runners"`
	TopologyKey            *string                       `json:"topologyKey,omitempty"`
	TopologySpread         *v1beta1.TopologySpreadPolicy `json:"topologySpread,omitempty"`
	TopologySpreadKey      *string                       `json:"topologySpreadKey,omitempty"`
	PairingProtocol        string                        `json:"pairingProtocol"`
	ExcludedAnnotationKeys []string                      `json:"excludedAnnotationKeys"`
	RevisionHash           string                        `json:"revisionHash"`
}

type goldenCase struct {
	name      string
	component v1beta1.ComponentType
	service   *v1beta1.InferenceService
	runtime   *v1beta1.ClusterServingRuntime
	model     *v1beta1.ClusterBaseModel
	extra     []client.Object
	// The guards below assert in code the rule a fixture exists for, so a
	// fixture that silently skips its path fails instead of pinning the skip.
	// wantEnv names env vars that must be present on the primary container
	// with the given values, wantWorkerEnv the same on the worker container;
	// wantNodeSelector is the primary's whole node selector; wantGPU is the
	// primary container's accelerator limit.
	wantEnv          map[string]string
	wantWorkerEnv    map[string]string
	wantNodeSelector map[string]string
	wantGPU          string
}

func goldenResources(quantity string) corev1.ResourceRequirements {
	values := corev1.ResourceList{goldenGPU: resource.MustParse(quantity)}
	return corev1.ResourceRequirements{Requests: values.DeepCopy(), Limits: values}
}

func goldenContainer(quantity string) corev1.Container {
	return corev1.Container{
		Name:      goldenContainerName,
		Image:     "example.com/serving:v1",
		Ports:     []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
		Resources: goldenResources(quantity),
	}
}

// requiredNodeAffinity is one required node-affinity term: key In [value].
func requiredNodeAffinity(key, value string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{value}}}}}}}}
}

func goldenModel() *v1beta1.ClusterBaseModel {
	return v1beta1testing.MakeClusterBaseModel("model-a").
		StorageURI("pvc://models/model-a").
		ModelFormat("safetensors").ModelType("llama").
		ModelArchitecture("LlamaForCausalLM").ModelFramework("Transformers").Obj()
}

// goldenRuntime carries a runtime-level node selector whose "pool" key collides
// with the accelerator class and the service, so the golden shows the
// runtime < class < service precedence rather than three disjoint keys.
func goldenRuntime() *v1beta1testing.ClusterServingRuntimeWrapper {
	rt := v1beta1testing.MakeClusterServingRuntime("runtime-a").
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM")
	rt.Spec.ServingRuntimePodSpec.NodeSelector = map[string]string{"pool": "runtime", "tier": "x"}
	return rt
}

func goldenService(component v1beta1.ComponentType) *v1beta1.InferenceService {
	svc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pool-a", Namespace: goldenNamespace, UID: "service-uid", Generation: 1,
			Labels:      map[string]string{"team": "a"},
			Annotations: map[string]string{"example.com/owner": "team-a"},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Model:          &v1beta1.ModelRef{Name: "model-a", Kind: ptr.To("ClusterBaseModel")},
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: ptr.To("ClusterServingRuntime")},
			DeploymentMode: ptr.To(constants.OMENative),
			Engine:         &v1beta1.EngineSpec{},
		},
	}
	switch component {
	case v1beta1.DecoderComponent:
		svc.Spec.Decoder = &v1beta1.DecoderSpec{}
	case v1beta1.RouterComponent:
		svc.Spec.Router = &v1beta1.RouterSpec{}
	}
	return svc
}

// goldenAccelerator declares a node selector (colliding with the runtime on
// "pool") and a required node-affinity term, the two discovery inputs the
// renderer merges into a pod.
func goldenAccelerator() *v1beta1.AcceleratorClass {
	return &v1beta1.AcceleratorClass{
		ObjectMeta: metav1.ObjectMeta{Name: goldenAcceleratorName},
		Spec: v1beta1.AcceleratorClassSpec{
			Resources: []v1beta1.AcceleratorResource{{Name: goldenGPU, Quantity: resource.MustParse("8")}},
			Discovery: v1beta1.AcceleratorDiscovery{
				NodeSelector: map[string]string{"pool": "class", "accelerator": "type-a"},
				Affinity:     requiredNodeAffinity("accelerator-zone", "zone-a"),
			},
		},
	}
}

// withAccelerator makes the runtime require the golden accelerator class and
// the service select it by name.
func withAccelerator(tc *goldenCase) {
	tc.extra = append(tc.extra, goldenAccelerator())
	tc.runtime.Spec.AcceleratorRequirements = &v1beta1.AcceleratorRequirements{AcceleratorClasses: []string{goldenAcceleratorName}}
	tc.service.Spec.AcceleratorSelector = &v1beta1.AcceleratorSelector{AcceleratorClass: ptr.To(goldenAcceleratorName)}
}

func goldenCases() []goldenCase {
	single := goldenCase{name: "engine-single-model-runtime-accelerator", component: v1beta1.EngineComponent,
		service: goldenService(v1beta1.EngineComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineRunner(goldenContainer("2")).Obj(),
		wantEnv: map[string]string{constants.ModelPathEnvVarKey: constants.ModelDefaultMountPath},
		// The class outranks the runtime on "pool"; the class affinity term
		// fills in because the service declares no affinity.
		wantNodeSelector: map[string]string{"pool": "class", "tier": "x", "accelerator": "type-a"}}
	withAccelerator(&single)

	gang := goldenCase{name: "engine-leader-worker", component: v1beta1.EngineComponent,
		service: goldenService(v1beta1.EngineComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineLeaderRunner(goldenContainer("2")).EngineWorkerRunner(goldenContainer("3")).Obj(),
		// Two accelerators per leader pod and three per worker pod, each
		// across one leader and two workers.
		wantEnv:       map[string]string{constants.ParallelismSizeEnvVarKey: "6"},
		wantWorkerEnv: map[string]string{constants.ParallelismSizeEnvVarKey: "9"}}
	gang.runtime.Spec.EngineConfig.Worker.Size = ptr.To(2)
	gang.service.Spec.Engine.TopologyKey = ptr.To("kubernetes.io/hostname")

	decoder := goldenCase{name: "decoder-single", component: v1beta1.DecoderComponent,
		service: goldenService(v1beta1.DecoderComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineRunner(goldenContainer("2")).DecoderRunner(goldenContainer("4")).Obj(),
		wantEnv: map[string]string{constants.ModelPathEnvVarKey: constants.ModelDefaultMountPath}}

	decoderGang := goldenCase{name: "decoder-leader-worker", component: v1beta1.DecoderComponent,
		service: goldenService(v1beta1.DecoderComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineRunner(goldenContainer("2")).Obj(),
		// The class's eight accelerators replace both runners' counts, so each
		// pod sees eight across one leader and two workers.
		wantGPU:          "8",
		wantEnv:          map[string]string{constants.ParallelismSizeEnvVarKey: "24"},
		wantWorkerEnv:    map[string]string{constants.ParallelismSizeEnvVarKey: "24"},
		wantNodeSelector: map[string]string{"pool": "class", "tier": "x", "accelerator": "type-a"}}
	withAccelerator(&decoderGang)
	decoderGang.runtime.Spec.DecoderConfig = &v1beta1.DecoderSpec{
		Leader: &v1beta1.LeaderSpec{Runner: &v1beta1.RunnerSpec{Container: goldenContainer("2")}},
		Worker: &v1beta1.WorkerSpec{Size: ptr.To(2), Runner: &v1beta1.RunnerSpec{Container: goldenContainer("3")}},
	}
	decoderGang.service.Spec.Decoder.TopologyKey = ptr.To("kubernetes.io/hostname")

	router := goldenCase{name: "router-config-env", component: v1beta1.RouterComponent,
		service: goldenService(v1beta1.RouterComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineRunner(goldenContainer("2")).RouterRunner(corev1.Container{Name: goldenContainerName, Image: "example.com/router:v1"}).Obj(),
		wantEnv: map[string]string{"ROUTER_TIMEOUT": "30"}}
	router.service.Spec.Router.Config = map[string]string{"ROUTER_TIMEOUT": "30", "ROUTER_BACKEND": "engine"}

	overlay := goldenCase{name: "engine-overlays", component: v1beta1.EngineComponent,
		service: goldenService(v1beta1.EngineComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineRunner(goldenContainer("2")).Obj(),
		extra:   []client.Object{v1beta1testing.MakeClusterBaseModel("overlay-a").StorageURI("pvc://models/overlay-a").ModelFormat("safetensors").Obj()},
		wantEnv: map[string]string{"OVERLAY_OVERLAY_A_MODEL_PATH": "/opt/ml/model-overlays/overlay-a"}}
	overlay.service.Spec.Model.Overlays = []v1beta1.ModelOverlayRef{{Name: "overlay-a", Kind: ptr.To("ClusterBaseModel")}}

	userAffinity := goldenCase{name: "engine-user-nodeselector-affinity", component: v1beta1.EngineComponent,
		service: goldenService(v1beta1.EngineComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineRunner(goldenContainer("2")).Obj(),
		// The service outranks both the class and the runtime on "pool", and
		// its own affinity keeps the class affinity term out.
		wantNodeSelector: map[string]string{"pool": "b", "tier": "x", "accelerator": "type-a"}}
	withAccelerator(&userAffinity)
	userAffinity.service.Spec.Engine.PodSpec = v1beta1.PodSpec{
		NodeSelector: map[string]string{"pool": "b"},
		Affinity:     requiredNodeAffinity("zone", "a"),
	}

	userResources := goldenCase{name: "engine-user-runner-resources", component: v1beta1.EngineComponent,
		service: goldenService(v1beta1.EngineComponent), model: goldenModel(),
		runtime: goldenRuntime().EngineRunner(goldenContainer("2")).Obj(),
		// Resources authored on the service's runner are kept as-is: neither
		// the runtime's 2 nor the class's 8 replaces them, and the parallelism
		// size follows the authored count.
		wantGPU: "1",
		wantEnv: map[string]string{constants.ParallelismSizeEnvVarKey: "1"}}
	withAccelerator(&userResources)
	userResources.service.Spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{Name: goldenContainerName, Resources: goldenResources("1")}}

	return []goldenCase{single, gang, decoder, decoderGang, router, overlay, userAffinity, userResources}
}

func goldenClient(t *testing.T, tc goldenCase) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: goldenNamespace},
		Data: map[string]string{controllerconfig.AcceleratorResourcesConfigName: fmt.Sprintf("[%q]", goldenGPU)}}
	objects := append([]client.Object{tc.runtime, cm}, tc.extra...)
	if tc.model != nil {
		objects = append(objects, tc.model)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

// renderThroughProjector runs the production render path for one component
// and returns the projector's stored runners, topology fields and hash inputs.
func renderThroughProjector(t *testing.T, ctx context.Context, c client.Client, tc goldenCase) renderGolden {
	t.Helper()
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: goldenNamespace, Name: constants.InferenceServiceConfigMapName}, cm); err != nil {
		t.Fatal(err)
	}
	cfg, deploy, err := controllerconfig.RenderingConfig(cm)
	if err != nil {
		t.Fatal(err)
	}
	service := tc.service.DeepCopy()
	model, modelMeta, _, err := isvcutils.ReconcileBaseModelWithStatus(c, service)
	if err != nil {
		t.Fatal(err)
	}
	selector := runtimeselector.New(c)
	rt, _, err := selector.GetRuntime(ctx, service.Spec.Runtime.Name, service.Namespace, runtimeselector.RefKind(service.Spec.Runtime))
	if err != nil {
		t.Fatal(err)
	}
	engine, decoder, router, err := isvcutils.MergeRuntimeSpecs(service, rt, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	engineMode, decoderMode, routerMode, err := isvcutils.DetermineDeploymentModes(engine, decoder, router, rt, service.Spec.DeploymentMode)
	if err != nil {
		t.Fatal(err)
	}
	specdefaults.Engine(engine, engineMode, deploy)
	specdefaults.Decoder(decoder, decoderMode, deploy)
	specdefaults.Router(router, routerMode, deploy)
	overlays, err := isvcutils.ResolveOverlays(c, service)
	if err != nil {
		t.Fatal(err)
	}
	piece := &render.Piece{Client: c, Log: logr.Discard(), InferenceServiceConfig: cfg, BaseModel: model, BaseModelMeta: modelMeta, Runtime: rt, RuntimeName: service.Spec.Runtime.Name, Overlays: overlays}
	// The router takes no model format and no accelerator class; the engine
	// and decoder resolve both per component.
	if tc.component != v1beta1.RouterComponent {
		ac, acName, err := acceleratorclassselector.New(c).GetAcceleratorClass(ctx, service, rt, tc.component)
		if err != nil {
			t.Fatal(err)
		}
		piece.SupportedModelFormat = selector.GetSupportedModelFormat(ctx, rt, model, true)
		piece.AcceleratorClassName = acName
		if ac != nil {
			piece.AcceleratorClass = &ac.Spec
		}
	}

	var rendered render.Rendered
	switch tc.component {
	case v1beta1.EngineComponent:
		piece.DeploymentMode = engineMode
		rendered, err = render.RenderEngine(ctx, piece, service, engine)
	case v1beta1.DecoderComponent:
		piece.DeploymentMode = decoderMode
		rendered, err = render.RenderDecoder(ctx, piece, service, decoder)
	case v1beta1.RouterComponent:
		piece.DeploymentMode = routerMode
		rendered, err = render.RenderRouter(ctx, piece, service, router)
	}
	if err != nil {
		t.Fatalf("render %s: %v", tc.component, err)
	}
	ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
		ISVC: service, Component: tc.component, ComponentExt: rendered.ComponentExt,
		ObjectMeta: rendered.ObjectMeta, PodSpec: rendered.Primary, WorkerPodSpec: rendered.Worker, WorkerSize: rendered.WorkerSize,
		MultiPod: rendered.MultiPod, TopologyKey: rendered.TopologyKey, TopologySpread: rendered.TopologySpread, TopologySpreadKey: rendered.TopologySpreadKey, Client: c,
	})
	if err != nil {
		t.Fatalf("project %s: %v", tc.component, err)
	}
	out := renderGolden{
		Runners: ir.Spec.Runners, TopologyKey: ir.Spec.TopologyKey, TopologySpread: ir.Spec.TopologySpread, TopologySpreadKey: ir.Spec.TopologySpreadKey,
		PairingProtocol: ptr.Deref(ir.Spec.PairingProtocol, ""), ExcludedAnnotationKeys: excludedAnnotationKeys(ir),
	}
	out.RevisionHash = goldenHash(t, ir, out.ExcludedAnnotationKeys)
	return out
}

// excludedAnnotationKeys reads the inherited annotation keys the projector
// records on the replica; the replica controller leaves them out of the hash.
func excludedAnnotationKeys(ir *v1beta1.InferenceReplica) []string {
	var keys []string
	for _, k := range strings.Split(ir.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey], excludedAnnotationKeysSeparator) {
		if k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

// goldenHash mints the revision hash the replica controller derives from the
// stored runners. Hashed: the primary template (default or leader runner) and
// the worker template, the primary's template metadata minus the excluded
// inherited annotation keys, the topology key and the pairing protocol, under
// a fixed scope UID and no collision count.
func goldenHash(t *testing.T, ir *v1beta1.InferenceReplica, excluded []string) string {
	t.Helper()
	var primary, worker *corev1.PodSpec
	var meta *metav1.ObjectMeta
	for i := range ir.Spec.Runners {
		r := &ir.Spec.Runners[i]
		switch r.Name {
		case v1beta1.RunnerNameDefault, v1beta1.RunnerNameLeader:
			spec, m := r.Template.Spec, r.Template.ObjectMeta
			m.Annotations = maps.Clone(m.Annotations)
			for _, k := range excluded {
				delete(m.Annotations, k)
			}
			primary, meta = &spec, &m
		case v1beta1.RunnerNameWorker:
			spec := r.Template.Spec
			worker = &spec
		}
	}
	hash, _, err := revision.HashWithWorkerTopologyAndPairing(primary, worker, meta, ptr.Deref(ir.Spec.TopologyKey, ""), ptr.Deref(ir.Spec.PairingProtocol, ""), nil, goldenScopeUID)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func workerRunner(runners []v1beta1.Runner) *v1beta1.Runner {
	for i := range runners {
		if runners[i].Name == v1beta1.RunnerNameWorker {
			return &runners[i]
		}
	}
	return nil
}

func envValue(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func TestRenderedRunnersMatchGolden(t *testing.T) {
	ctx := t.Context()
	for _, tc := range goldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := goldenClient(t, tc)
			got := renderThroughProjector(t, ctx, c, tc)
			primary := got.Runners[0].Template.Spec
			for name, value := range tc.wantEnv {
				if v, ok := envValue(primary.Containers[0].Env, name); !ok || v != value {
					t.Fatalf("primary container env %s = %q (present: %t), want %q; the fixture does not exercise the path it exists for", name, v, ok, value)
				}
			}
			if len(tc.wantWorkerEnv) > 0 {
				worker := workerRunner(got.Runners)
				if worker == nil {
					t.Fatalf("no worker runner; the fixture does not exercise the multi-pod path it exists for")
				}
				for name, value := range tc.wantWorkerEnv {
					if v, ok := envValue(worker.Template.Spec.Containers[0].Env, name); !ok || v != value {
						t.Fatalf("worker container env %s = %q (present: %t), want %q; the fixture does not exercise the path it exists for", name, v, ok, value)
					}
				}
			}
			if tc.wantNodeSelector != nil {
				if diff := cmp.Diff(tc.wantNodeSelector, primary.NodeSelector); diff != "" {
					t.Fatalf("primary node selector (-want +got):\n%s", diff)
				}
			}
			if tc.wantGPU != "" {
				if q := primary.Containers[0].Resources.Limits[goldenGPU]; q.String() != tc.wantGPU {
					t.Fatalf("primary container %s limit = %s, want %s; the fixture does not exercise the path it exists for", goldenGPU, q.String(), tc.wantGPU)
				}
			}
			data, err := json.MarshalIndent(got, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			data = append(data, '\n')
			path := filepath.Join("testdata", "render", tc.name+".json")
			if *updateRenderGoldens {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden %s (run with -update to create it): %v", path, err)
			}
			if !bytes.Equal(want, data) {
				t.Fatalf("rendered runners differ from golden %s (run with -update only for an intended rendering change):\n%s", path, cmp.Diff(string(want), string(data)))
			}
		})
	}
}
