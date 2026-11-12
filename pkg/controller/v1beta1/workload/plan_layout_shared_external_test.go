package workload_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// restingComponentPodObjects is every pod of a Component at rest as the renderer
// labels it: each Instance's pods named and labelled by runner and ordinal,
// serving on the revision the Instance's row names, bound to one of the
// fleet's Nodes.
func restingComponentPodObjects(input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, running string) []client.Object {
	key := input.Key
	var pods []client.Object
	for _, inst := range plan.Instances {
		for _, runner := range inst.Runners {
			for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
				pod := testfixture.ServingPod(query.PodName(key.OwnerName, key.Component, inst.Index, runner.Name, ordinal))
				pod.Namespace = key.Namespace
				pod.Labels = testfixture.PodLabels(key.OwnerName, key.Component, inst.Index, runner.Name, inst.Incarnation, ordinal)
				pod.Labels[query.LabelRevisionHash] = query.RevisionFromName(running).Hash()
				pod.Spec.NodeName = fmt.Sprintf("node-%d", int(inst.Index)%restingFleetNodes)
				pod.Spec.Containers = input.DesiredSpec.PodSpec.DeepCopy().Containers
				pods = append(pods, pod)
			}
		}
	}
	return pods
}

// sharedLayoutShape is one Component at rest whose plan carries a layout
// with three spare slots behind its length.
type sharedLayoutShape struct {
	name            string
	instances, pods int
}

// sharedLayoutFleet is a Component at rest whose plan's layout, shared with
// every Instance's entry, is backed by an array with three spare slots
// behind its length, so a write past the end lands where the test can see
// it.
type sharedLayoutFleet struct {
	input    workloadtypes.ReconcileInput
	plan     workloadtypes.ComponentPlan
	target   *appsv1.ControllerRevision
	store    *testAtomicMutationStore
	client   client.Client
	backing  []workloadtypes.RunnerPlan
	pristine []workloadtypes.RunnerPlan
	observed []workloadtypes.InstanceStatus
}

func newSharedLayoutFleet(t *testing.T, shape sharedLayoutShape) *sharedLayoutFleet {
	t.Helper()
	const running = "svc-engine-rev-a"
	f := &sharedLayoutFleet{input: fleetInput(shape.instances, shape.instances, shape.pods, running)}
	if shape.pods > 1 {
		f.input.ObservedState.WorkerSizeByRevision = map[string]int32{running: int32(shape.pods - 1)}
	}
	f.store = installTestAtomicMutationStore(&f.input)
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, f.input.DesiredSpec, f.input.ObservedState)
	if err != nil {
		t.Fatal(err)
	}
	spare := []workloadtypes.RunnerPlan{{Name: "spare-0", Size: 70}, {Name: "spare-1", Size: 71}, {Name: "spare-2", Size: 72}}
	f.backing = append(append([]workloadtypes.RunnerPlan(nil), plan.Runners...), spare...)
	plan.Runners = f.backing[:len(plan.Runners)]
	for i := range plan.Instances {
		plan.Instances[i].Runners = plan.RunnersFor(plan.Instances[i], running)
	}
	f.plan = plan
	f.pristine = append([]workloadtypes.RunnerPlan(nil), f.backing...)
	f.observed = cloneTestInstanceStatuses(f.input.ObservedState.InstanceStatuses)
	f.target = revisionWithPodSpec(t, running, f.input.Key.Namespace, f.input.DesiredSpec.PodSpec)
	objects := []client.Object{f.target}
	for i := 0; i < restingFleetNodes; i++ {
		objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i)}})
	}
	objects = append(objects, restingComponentPodObjects(f.input, plan, running)...)
	f.client = fakeclient.New(t, fakeclient.WithObjects(objects...),
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor))
	return f
}

// pass runs one engine pass over the fleet.
func (f *sharedLayoutFleet) pass(ctx context.Context, expectations *workloadtypes.Expectations) error {
	cached := workloadtypes.NewCachedPodList(f.client)
	in := f.input
	in.CachedPods = cached
	_, err := workload.Reconcile(ctx, workloadtypes.Deps{Client: cached.Client(), Expectations: expectations}, in, f.plan, f.target)
	return err
}

// checkUntouched holds every slot of the layout's array, the plan's layout,
// every entry and the observation as they were, and the store unwritten.
func (f *sharedLayoutFleet) checkUntouched(t *testing.T, pass int) {
	t.Helper()
	if diff := cmp.Diff(f.pristine, f.backing); diff != "" {
		t.Fatalf("pass %d wrote into the shared layout or behind its length (-before +after):\n%s", pass, diff)
	}
	layout := f.pristine[:len(f.plan.Runners)]
	if diff := cmp.Diff(layout, f.plan.Runners); diff != "" {
		t.Fatalf("pass %d left the plan's layout changed (-before +after):\n%s", pass, diff)
	}
	for i := range f.plan.Instances {
		if diff := cmp.Diff(layout, f.plan.Instances[i].Runners); diff != "" {
			t.Fatalf("pass %d left Instance %d's entry changed (-before +after):\n%s", pass, f.plan.Instances[i].Index, diff)
		}
	}
	if diff := cmp.Diff(f.observed, f.input.ObservedState.InstanceStatuses); diff != "" {
		t.Fatalf("pass %d wrote into the observation the hold pass reads in place (-before +after):\n%s", pass, diff)
	}
	if f.store.writes != 0 {
		t.Fatalf("pass %d over a Component at rest wrote %d rows", pass, f.store.writes)
	}
}

// TestPlanLayoutSharedWithThePassIsNeverWritten: the plan's runner layout
// reaches every Instance's entry and the create pass as it is, and no
// reader writes into it. Three passes over a Component at rest (single-pod
// Instances, then gangs of a leader and two workers) leave every slot of
// the layout's array as it was, the spare slots behind its length
// included, with every entry still the layout and the observation the hold
// pass reads in place unchanged.
func TestPlanLayoutSharedWithThePassIsNeverWritten(t *testing.T) {
	ctx := logr.NewContext(context.Background(), logr.Discard())
	shapes := []sharedLayoutShape{{name: "3x1", instances: 3, pods: 1}, {name: "2x3", instances: 2, pods: 3}}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			f := newSharedLayoutFleet(t, shape)
			expectations := workloadtypes.NewExpectations()
			for pass := 1; pass <= 3; pass++ {
				if err := f.pass(ctx, expectations); err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
				f.checkUntouched(t, pass)
			}
		})
	}
}
