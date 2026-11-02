package components

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// singlePodRunnerRuntime models a ServingRuntime whose engineConfig declares
// only a top-level runner — the single-pod shape: EngineConfig.Leader and
// .Worker are both nil.
func singlePodRunnerRuntime() *v1beta1.ServingRuntimeSpec {
	return &v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				NodeSelector: map[string]string{"kubernetes.io/arch": "amd64"},
			},
			Runner: &v1beta1.RunnerSpec{
				Container: v1.Container{Name: "example-container", Image: "example.com/serving:1.0"},
			},
		},
	}
}

// multiNodeRunnerRuntime models a ServingRuntime whose engineConfig declares
// per-role leader and worker runners (the same runner container reused for
// both, as a shared runtime fixture would) instead of a top-level runner —
// the multi-node shape.
func multiNodeRunnerRuntime() *v1beta1.ServingRuntimeSpec {
	runner := &v1beta1.RunnerSpec{
		Container: v1.Container{Name: "example-container", Image: "example.com/serving:1.0"},
	}
	return &v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{
			PodSpec: v1beta1.PodSpec{
				NodeSelector: map[string]string{"kubernetes.io/arch": "amd64"},
			},
			Leader: &v1beta1.LeaderSpec{Runner: runner},
			Worker: &v1beta1.WorkerSpec{Runner: runner},
		},
	}
}

// newRunnerTestEngine constructs an Engine the same way engine_test.go's
// other pod-spec-reconcile tests do: a fake client/clientset is enough
// because reconcilePodSpec/reconcileWorkerPodSpec never touch the API
// server, only the merged spec passed in.
func newRunnerTestEngine(t *testing.T, mergedEngine *v1beta1.EngineSpec) *Engine {
	t.Helper()
	cl := fakeclient.New(t)
	return NewEngine(
		&ComponentDeps{
			Client:    cl,
			Clientset: fake.NewClientset(),
			Scheme:    cl.Scheme(),
			Config:    &controllerconfig.InferenceServicesConfig{},
		},
		ComponentInputs{DeploymentMode: constants.OMENative},
		mergedEngine,
	).(*Engine)
}

// TestMergeEngineSpec_RunnerResolvesForBothSuiteShapes pins the render
// contract an ISVC that leaves its own Leader/Worker PodSpec and Runner
// unset depends on entirely: MergeRuntimeSpecs (which calls MergeEngineSpec)
// must carry the runtime's per-role runner onto the merged Leader/Worker so
// the pod-spec reconcile path that follows has a container to render.
//
// A runtime whose engineConfig has no leader/worker block (the single-pod
// shape) leaves the merged Worker nil — nothing to inherit a runner from —
// so a multi-pod ISVC must reference a runtime that actually declares one,
// never the single-pod runtime. Getting this wrong renders an empty pod
// spec: ReconcileWorkerPodSpec / resolveContainer return "no containers
// found" and the component never reaches Ready.
func TestMergeEngineSpec_RunnerResolvesForBothSuiteShapes(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	objectMeta := &metav1.ObjectMeta{Name: "test-isvc-engine", Namespace: "default"}

	t.Run("single-pod shape: no worker, engine renders from the top-level runner", func(t *testing.T) {
		one := 1
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one, MaxReplicas: one},
				},
			},
		}

		specs, err := isvcutils.MergeRuntimeRoleSpecs(isvc.DeepCopy(), singlePodRunnerRuntime(), logr.Discard())
		mergedEngine := specs.Engine
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(mergedEngine.Worker).To(gomega.BeNil(),
			"a runtime with no worker block must not promote a single-pod ISVC to multi-pod")

		engine := newRunnerTestEngine(t, mergedEngine)

		podSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(podSpec.Containers).NotTo(gomega.BeEmpty(), "the engine pod spec must have a container from the runtime's runner")
		g.Expect(podSpec.Containers[0].Image).NotTo(gomega.BeEmpty())

		workerPodSpec, err := engine.reconcileWorkerPodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(workerPodSpec).To(gomega.BeNil(), "a single-pod engine must stay single-pod: no WorkerPodSpec")
	})

	t.Run("multi-node shape: leader and worker both resolve a container from the runtime's per-role runner", func(t *testing.T) {
		one := 1
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{
					ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &one, MaxReplicas: one},
					Leader:                 &v1beta1.LeaderSpec{},
					Worker:                 &v1beta1.WorkerSpec{Size: &one},
				},
			},
		}

		specs, err := isvcutils.MergeRuntimeRoleSpecs(isvc.DeepCopy(), multiNodeRunnerRuntime(), logr.Discard())
		mergedEngine := specs.Engine
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(mergedEngine.Leader).NotTo(gomega.BeNil())
		g.Expect(mergedEngine.Worker).NotTo(gomega.BeNil())

		engine := newRunnerTestEngine(t, mergedEngine)

		leaderPodSpec, err := engine.reconcilePodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(leaderPodSpec.Containers).NotTo(gomega.BeEmpty(), "the leader pod spec must have a container from the runtime's leader runner")
		g.Expect(leaderPodSpec.Containers[0].Image).NotTo(gomega.BeEmpty())

		workerPodSpec, err := engine.reconcileWorkerPodSpec(isvc, objectMeta)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(workerPodSpec.Containers).NotTo(gomega.BeEmpty(), "the worker pod spec must have a container from the runtime's worker runner")
		g.Expect(workerPodSpec.Containers[0].Image).NotTo(gomega.BeEmpty())
	})
}
