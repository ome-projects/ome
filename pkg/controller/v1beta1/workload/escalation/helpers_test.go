package escalation

import (
	"context"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Test-only seam for the external test package: the pass is engine-
// internal, and its inputs come from the reconcile's memoized pod read,
// so tests drive it over pre-bucketed pods instead of a client.

// EscalateFromEvidenceForTest invokes the hold pass and the
// terminal-failure escalation pass in the order a reconcile runs them,
// over the given pod buckets. target is the reconcile's target
// ControllerRevision (nil is valid — the Create-op fallback stays empty).
// A fixture that wires no reader reads the buckets back as the live pod
// set, so the one live read the pass makes sees what it observed.
func EscalateFromEvidenceForTest(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, byIdx map[int32][]*corev1.Pod) error {
	pods := func(context.Context) (map[int32][]*corev1.Pod, error) { return byIdx, nil }
	held, err := holds.Run(ctx, holds.PassInput{
		Deps:  deps,
		Input: input,
		Plan:  plan,
		Rows:  holds.RowsForPlan(plan, input.ObservedState.InstanceStatuses, nil),
		Pods:  pods,
	})
	if err != nil {
		return err
	}
	if deps.Client == nil && deps.APIReader == nil {
		deps.APIReader = observedPodReader(input, plan, byIdx)
	}
	return Run(ctx, PassInput{Deps: deps, Input: input, Plan: plan, Target: target, Pods: pods, Held: held})
}

// observedPodReader is a reader holding the bucketed pods under the
// labels the live lister selects by, so a pass that re-reads an
// Instance's pods finds the ones it observed.
func observedPodReader(input types.ReconcileInput, plan types.ComponentPlan, byIdx map[int32][]*corev1.Pod) client.Reader {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	var objs []client.Object
	seen := map[string]struct{}{}
	for idx, pods := range byIdx {
		for _, pod := range pods {
			if pod == nil {
				continue
			}
			labeled := pod.DeepCopy()
			if labeled.Namespace == "" {
				labeled.Namespace = input.Key.Namespace
			}
			if labeled.Labels == nil {
				labeled.Labels = map[string]string{}
			}
			labeled.Labels[constants.InferenceServicePodLabelKey] = input.Key.OwnerName
			labeled.Labels[constants.OMEComponentLabel] = string(plan.Component)
			labeled.Labels[query.LabelManagedBy] = query.ManagedByOMENative
			labeled.Labels[query.LabelInstanceIdx] = strconv.Itoa(int(idx))
			// The fake store refuses a deleting object nothing holds open.
			if labeled.DeletionTimestamp != nil && len(labeled.Finalizers) == 0 {
				labeled.Finalizers = []string{"example.com/termination"}
			}
			key := labeled.Namespace + "/" + labeled.Name
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			objs = append(objs, labeled)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}
