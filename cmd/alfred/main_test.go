package main

import (
	"context"
	"flag"
	"os"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/engine"
	"sigs.k8s.io/ome/pkg/alfred/observer"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/constants"
)

func resetFlags(t *testing.T, args []string) {
	t.Helper()
	oldArgs := os.Args
	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	})
	flag.CommandLine = flag.NewFlagSet(args[0], flag.ExitOnError)
	os.Args = args
}

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts.simulationWorkers != "" || opts.simulationTimeout != 10*time.Second {
		t.Fatalf("simulation must be opt-in with a bounded timeout: %+v", opts)
	}
	if opts.metricsAddr != ":8080" || opts.probeAddr != ":8081" {
		t.Fatalf("default addresses: %+v", opts)
	}
	if !opts.enableLeaderElection {
		t.Fatal("leader election must default on")
	}
	if opts.configMapName != "alfred-config" || opts.configMapKey != "config.yaml" {
		t.Fatalf("default config source: %+v", opts)
	}
	if opts.namespace == "" {
		t.Fatal("namespace must have a default")
	}
}

func TestGetOptionsDefaults(t *testing.T) {
	resetFlags(t, []string{"alfred"})
	opts := GetOptions()
	if opts.metricsAddr != ":8080" || opts.probeAddr != ":8081" {
		t.Fatalf("defaults not applied: %+v", opts)
	}
	if !opts.enableLeaderElection || opts.configMapName != "alfred-config" || opts.configMapKey != "config.yaml" {
		t.Fatalf("defaults not applied: %+v", opts)
	}
}

func TestMigrationStartupDefaultsAndFlags(t *testing.T) {
	resetFlags(t, []string{"alfred"})
	opts := GetOptions()
	if opts.migrationAPIVersion != "" || opts.migrationServiceAccount != "" {
		t.Fatalf("migration execution must require explicit compatibility and identity configuration: %+v", opts)
	}
	if opts.migrationAckTimeout != 2*time.Minute || opts.migrationFailureBackoff != 5*time.Minute {
		t.Fatalf("migration acknowledgement timeout and failure backoff must be bounded: %+v", opts)
	}
	if err := validateOptions(opts); err != nil {
		t.Fatalf("recommend-only defaults rejected: %v", err)
	}
	resetFlags(t, []string{"alfred", "--migration-api-version=v1", "--migration-service-account=caretaker", "--migration-ack-timeout=3m", "--migration-failure-backoff=7m", "--simulation-workers=/etc/alfred/workers.json"})
	opts = GetOptions()
	if opts.migrationAPIVersion != "v1" || opts.migrationServiceAccount != "caretaker" || opts.migrationAckTimeout != 3*time.Minute || opts.migrationFailureBackoff != 7*time.Minute {
		t.Fatalf("migration startup flags not parsed: %+v", opts)
	}
	if err := validateOptions(opts); err != nil {
		t.Fatalf("valid migration startup rejected: %v", err)
	}
}

func TestMigrationStartupRejectsUnsafeOptions(t *testing.T) {
	valid := DefaultOptions()
	valid.migrationAPIVersion = "v1"
	valid.migrationServiceAccount = "ome-alfred"
	valid.simulationWorkers = "/etc/alfred/workers.json"
	for _, tc := range []struct {
		name   string
		change func(*Options)
	}{
		{"unsupported API", func(o *Options) { o.migrationAPIVersion = "v2" }},
		{"missing account", func(o *Options) { o.migrationServiceAccount = "" }},
		{"invalid account", func(o *Options) { o.migrationServiceAccount = "Bad_Account" }},
		{"leader election disabled", func(o *Options) { o.enableLeaderElection = false }},
		{"zero acknowledgement timeout", func(o *Options) { o.migrationAckTimeout = 0 }},
		{"negative acknowledgement timeout", func(o *Options) { o.migrationAckTimeout = -time.Second }},
		{"long acknowledgement timeout", func(o *Options) { o.migrationAckTimeout = time.Hour + time.Second }},
		{"zero failure backoff", func(o *Options) { o.migrationFailureBackoff = 0 }},
		{"negative failure backoff", func(o *Options) { o.migrationFailureBackoff = -time.Second }},
		{"long failure backoff", func(o *Options) { o.migrationFailureBackoff = time.Hour + time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := valid
			tc.change(&opts)
			if err := validateOptions(opts); err == nil {
				t.Fatalf("unsafe startup configuration accepted: %+v", opts)
			}
		})
	}
	valid.migrationAckTimeout, valid.migrationFailureBackoff = time.Hour, time.Hour
	if err := validateOptions(valid); err != nil {
		t.Fatalf("inclusive timeout limits rejected: %v", err)
	}
	valid.simulationWorkers = ""
	if err := validateOptions(valid); err != nil {
		t.Fatalf("dispatch without a simulation worker, for moves that need none, rejected: %v", err)
	}
}

type startupSimulator struct{}

func (startupSimulator) Evaluate(context.Context, scheduling.Request) (scheduling.Result, error) {
	return scheduling.Result{}, nil
}

func TestConfigureMigrationRequiresOptInAndLoadedWorker(t *testing.T) {
	opts := DefaultOptions()
	observations := &observer.Loop{}
	decisions := &engine.DecisionLoop{Store: config.NewStore()}
	if _, err := decisions.Store.Update([]byte("schemaVersion: 1\nmode: execute")); err != nil {
		t.Fatal(err)
	}
	if err := configureMigration(opts, nil, nil, observations, decisions); err != nil {
		t.Fatal(err)
	}
	if observations.OMENativeExecutor != nil || decisions.Dispatcher != nil {
		t.Fatal("execute configuration cannot bypass the startup compatibility opt-in")
	}
	opts.migrationAPIVersion, opts.migrationServiceAccount = "v1", "ome-alfred"
	opts.simulationWorkers = "/etc/alfred/workers.json"
	if err := configureMigration(opts, nil, nil, observations, decisions); err == nil {
		t.Fatal("opt-in without a successfully loaded worker must fail startup")
	}
	decisions.Predictions = &engine.PredictionStage{}
	if err := configureMigration(opts, nil, nil, observations, decisions); err == nil {
		t.Fatal("a prediction stage without its simulator must fail startup")
	}

	// Without configured workers, dispatch runs with no simulator: only moves
	// onto a new TPU slice can be submitted.
	opts.simulationWorkers = ""
	decisions.Predictions = nil
	if err := configureMigration(opts, nil, nil, observations, decisions); err != nil {
		t.Fatalf("dispatch without a simulation worker rejected: %v", err)
	}
	dispatcher, ok := decisions.Dispatcher.(*engine.Dispatcher)
	if !ok || dispatcher.Simulator != nil {
		t.Fatalf("dispatcher must have no simulator without simulation-workers: %+v", decisions.Dispatcher)
	}
}

func TestConfigureMigrationReusesWorkerAndChecksGuard(t *testing.T) {
	opts := DefaultOptions()
	opts.namespace = "caretaker"
	opts.migrationAPIVersion, opts.migrationServiceAccount = "v1", "custom-alfred"
	opts.simulationWorkers = "/etc/alfred/workers.json"
	opts.migrationAckTimeout, opts.migrationFailureBackoff = 3*time.Minute, 7*time.Minute
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).Build()
	observations := &observer.Loop{}
	decisions := &engine.DecisionLoop{Store: config.NewStore(), Predictions: &engine.PredictionStage{Reader: cl, Simulator: &startupSimulator{}}}
	if err := configureMigration(opts, direct, cl, observations, decisions); err != nil {
		t.Fatal(err)
	}
	compatibility := observations.OMENativeExecutor(context.Background())
	if !compatibility.Available || compatibility.WireVersion != "v1" || compatibility.Reason != "OperatorConfigured" || !compatibility.RenewTime.IsZero() {
		t.Fatalf("operator compatibility must not claim a liveness observation: %+v", compatibility)
	}
	dispatcher, ok := decisions.Dispatcher.(*engine.Dispatcher)
	if !ok || dispatcher.Reader != direct || dispatcher.Client != cl || dispatcher.Simulator != decisions.Predictions.Simulator || dispatcher.Store != decisions.Store {
		t.Fatalf("dispatcher did not reuse the direct reader, worker and configuration: %+v", decisions.Dispatcher)
	}
	if dispatcher.CacheReader != cl {
		t.Fatal("TPU slice moves must replay the policy through the cached client, not the direct reader")
	}
	if dispatcher.Namespace != "caretaker" || dispatcher.Options.APIVersion != "v1" || dispatcher.Options.AcknowledgementTimeout != 3*time.Minute || dispatcher.Options.FailureBackoff != 7*time.Minute {
		t.Fatalf("dispatcher startup bounds or identity namespace changed: %+v", dispatcher)
	}
	if dispatcher.Guard == nil || dispatcher.Guard(context.Background()) == nil {
		t.Fatal("missing admission objects must fail the wired dispatcher guard")
	}
}

func TestPredictionStageDisabledAndInvalidStartup(t *testing.T) {
	opts := DefaultOptions()
	stage, err := predictionStage(context.Background(), nil, opts)
	if err != nil || stage != nil {
		t.Fatalf("default should not create a worker: %v %v", stage, err)
	}
	for _, timeout := range []time.Duration{0, -time.Second, time.Minute + time.Second} {
		opts.simulationTimeout = timeout
		if _, err := predictionStage(context.Background(), nil, opts); err == nil {
			t.Fatalf("accepted invalid timeout %v", timeout)
		}
	}
	opts = DefaultOptions()
	opts.simulationWorkers = "/does-not-exist/alfred-workers.json"
	if _, err := predictionStage(context.Background(), nil, opts); err == nil {
		t.Fatal("configured missing registry must fail startup")
	}
}

func TestGetOptionsCustom(t *testing.T) {
	resetFlags(t, []string{
		"alfred",
		"--metrics-bind-address=:9090",
		"--health-probe-bind-address=:9091",
		"--leader-elect=false",
		"--namespace=caretaker",
		"--config-name=my-config",
		"--config-key=alfred.yaml",
		"--simulation-workers=/etc/alfred/workers.json",
		"--simulation-timeout=7s",
		"--enable-defragmentation-fence=true",
	})
	opts := GetOptions()
	if !opts.defragmentationFence {
		t.Fatal("--enable-defragmentation-fence not parsed")
	}
	if opts.simulationWorkers != "/etc/alfred/workers.json" || opts.simulationTimeout != 7*time.Second {
		t.Fatalf("simulation options not parsed: %+v", opts)
	}
	if opts.metricsAddr != ":9090" || opts.probeAddr != ":9091" {
		t.Fatalf("addresses not parsed: %+v", opts)
	}
	if opts.enableLeaderElection {
		t.Fatal("--leader-elect=false not parsed")
	}
	if opts.namespace != "caretaker" || opts.configMapName != "my-config" || opts.configMapKey != "alfred.yaml" {
		t.Fatalf("config source not parsed: %+v", opts)
	}
}

func TestPodCacheTransform(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "p",
			UID:         "source-pod-uid",
			Labels:      map[string]string{"keep": "me"},
			Annotations: map[string]string{"drop": "me"},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kubelet"},
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "noncontroller", UID: "ignored"},
				{
					APIVersion: "ome.io/v1beta1",
					Kind:       "InferenceReplica",
					Name:       "ir-0",
					UID:        "controller-uid",
					Controller: &controller,
				},
			},
		},
		Spec: corev1.PodSpec{
			NodeName:     "node1",
			NodeSelector: map[string]string{"pool": "h100"},
			Containers: []corev1.Container{{
				Name:    "main",
				Command: []string{"serve"},
				Env:     []corev1.EnvVar{{Name: "X", Value: "1"}},
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("2"),
				}},
				VolumeMounts: []corev1.VolumeMount{{Name: "v"}},
			}},
			InitContainers: []corev1.Container{{
				Name:          "sidecar",
				RestartPolicy: &always,
				Env:           []corev1.EnvVar{{Name: "Y", Value: "2"}},
			}},
			Volumes:     []corev1.Volume{{Name: "v"}},
			Tolerations: []corev1.Toleration{{Key: "gpu"}},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main"}},
		},
	}

	out, err := podCacheTransform(pod)
	if err != nil {
		t.Fatal(err)
	}
	got := out.(*corev1.Pod)

	// Stripped: everything the snapshot never reads.
	if got.Annotations != nil || got.ManagedFields != nil {
		t.Fatalf("metadata not stripped: %+v", got.ObjectMeta)
	}
	if len(got.OwnerReferences) != 1 {
		t.Fatalf("owner references = %+v, want only controller", got.OwnerReferences)
	}
	owner := got.OwnerReferences[0]
	if owner.APIVersion != "ome.io/v1beta1" || owner.Kind != "InferenceReplica" ||
		owner.Name != "ir-0" || owner.UID != "controller-uid" ||
		owner.Controller == nil || !*owner.Controller {
		t.Fatalf("controller owner not preserved exactly: %+v", owner)
	}
	if got.Spec.Volumes != nil || got.Spec.Tolerations != nil {
		t.Fatalf("spec not stripped: %+v", got.Spec)
	}
	if c := got.Spec.Containers[0]; c.Command != nil || c.Env != nil || c.VolumeMounts != nil {
		t.Fatalf("container not stripped: %+v", c)
	}
	if got.Spec.InitContainers[0].Env != nil {
		t.Fatalf("init container not stripped: %+v", got.Spec.InitContainers[0])
	}
	if got.Status.ContainerStatuses != nil {
		t.Fatalf("status not stripped: %+v", got.Status)
	}

	// Kept: everything the snapshot reads.
	if got.UID != "source-pod-uid" || got.Labels["keep"] != "me" || got.Spec.NodeName != "node1" || got.Spec.NodeSelector["pool"] != "h100" {
		t.Fatalf("needed fields lost: %+v", got)
	}
	if got.Spec.Containers[0].Resources.Limits.Name("nvidia.com/gpu", resource.DecimalSI).Value() != 2 {
		t.Fatalf("resources lost: %+v", got.Spec.Containers[0].Resources)
	}
	if got.Spec.InitContainers[0].RestartPolicy == nil {
		t.Fatal("sidecar restart policy lost (GPU accounting needs it)")
	}
	if got.Status.Phase != corev1.PodRunning {
		t.Fatalf("phase lost: %+v", got.Status)
	}

	// Non-pod objects pass through untouched.
	cm := &corev1.ConfigMap{}
	if out, err := podCacheTransform(cm); err != nil || out != cm {
		t.Fatalf("non-pod transform: %v %v", out, err)
	}
}

func TestPodCacheTransformStripsNonControllerOwners(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "p",
		OwnerReferences: []metav1.OwnerReference{
			{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "observer", UID: "observer-uid"},
		},
	}}

	out, err := podCacheTransform(pod)
	if err != nil {
		t.Fatal(err)
	}
	if owners := out.(*corev1.Pod).OwnerReferences; len(owners) != 0 {
		t.Fatalf("non-controller owners retained: %+v", owners)
	}
}

// tpuSlicePod is a slice-provisioned pod that requests resourceName, with an
// extra annotation and node affinity beyond what TPU slice moves read.
func tpuSlicePod(resourceName corev1.ResourceName) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod",
			Name:      "svc-engine-0",
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "svc",
				constants.OMEComponentLabel:           "engine",
			},
			Annotations: map[string]string{
				constants.TPUSliceProvisioningAnnotationKey: "true",
				"example.com/other":                         "x",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:     "tpu-1",
			NodeSelector: map[string]string{"cloud.google.com/gke-tpu-topology": "2x2x2"},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
							{Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"zone-a"}},
						}}},
					},
					PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 1}},
				},
				PodAntiAffinity: &corev1.PodAntiAffinity{},
			},
			Containers: []corev1.Container{{Name: "main", Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{resourceName: resource.MustParse("4")},
			}}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// TestPodCacheTransformKeepsTPUSliceFields checks a TPU pod keeps only the
// slice-provisioning annotation and the required node affinity, and other pods
// keep neither.
func TestPodCacheTransformKeepsTPUSliceFields(t *testing.T) {
	source := tpuSlicePod("google.com/tpu")
	out, err := podCacheTransform(source.DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	got := out.(*corev1.Pod)
	if want := map[string]string{constants.TPUSliceProvisioningAnnotationKey: "true"}; !reflect.DeepEqual(got.Annotations, want) {
		t.Fatalf("annotations = %v, want %v", got.Annotations, want)
	}
	want := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: source.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
	}}
	if !reflect.DeepEqual(got.Spec.Affinity, want) {
		t.Fatalf("affinity = %+v, want only the required node affinity", got.Spec.Affinity)
	}

	bare := tpuSlicePod("google.com/tpu")
	bare.Annotations, bare.Spec.Affinity = map[string]string{"example.com/other": "x"}, nil
	if out, _ := podCacheTransform(bare); out.(*corev1.Pod).Annotations != nil || out.(*corev1.Pod).Spec.Affinity != nil {
		t.Fatalf("a TPU pod without the fields must keep none: %+v", out)
	}
	if out, _ := podCacheTransform(tpuSlicePod("nvidia.com/gpu")); out.(*corev1.Pod).Annotations != nil || out.(*corev1.Pod).Spec.Affinity != nil {
		t.Fatalf("a non-TPU pod must keep no annotation or affinity: %+v", out)
	}
}

// TestCachedTPUPodStaysSliceProvisioned builds a snapshot from a pod after the
// cache transform, as the live informer does, and checks the TPU slice fields
// survive.
func TestCachedTPUPodStaysSliceProvisioned(t *testing.T) {
	source := tpuSlicePod("google.com/tpu")
	cached, err := podCacheTransform(source.DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "tpu-1"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, cached.(*corev1.Pod)).Build()
	snap, err := snapshot.Build(context.Background(), cl, snapshot.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var got *snapshot.PodInfo
	for i := range snap.Nodes["tpu-1"].OMEPods {
		if p := &snap.Nodes["tpu-1"].OMEPods[i]; p.Name == source.Name {
			got = p
		}
	}
	if got == nil || !got.TPUSliceProvisioned || got.NodeSelector["cloud.google.com/gke-tpu-topology"] != "2x2x2" ||
		!reflect.DeepEqual(got.RequiredNodeAffinity, source.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution) {
		t.Fatalf("cached TPU pod lost the fields TPU slice moves read: %+v", got)
	}
}

func TestPodIdentity(t *testing.T) {
	t.Setenv("POD_NAME", "alfred-0")
	if got := podIdentity(); got != "alfred-0" {
		t.Fatalf("podIdentity = %q, want alfred-0", got)
	}
	t.Setenv("POD_NAME", "")
	if got := podIdentity(); got == "" {
		t.Fatal("podIdentity must fall back to hostname")
	}
}

func TestPtr(t *testing.T) {
	if v := ptr(42); *v != 42 {
		t.Fatalf("ptr: %v", *v)
	}
}
