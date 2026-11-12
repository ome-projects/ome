package testfixture

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ReconcileInput is the pass input for one component of the service over
// the fake client: the rows it observes come from the stored replica, and
// its row writer writes the replica back, so a test reads the pass's
// writes from the client. The desired spec is one runner container on
// image test:v1 and the requeue cadence is the one a chart configures.
func ReconcileInput(isvc *v1beta1.InferenceService, c client.Client, component types.ComponentType) types.ReconcileInput {
	return types.ReconcileInput{
		OwnerObject: isvc,
		OwnerGVK:    v1beta1.SchemeGroupVersion.WithKind("InferenceService"),
		EventTarget: isvc,
		Key: types.Key{
			Namespace: isvc.Namespace,
			OwnerName: isvc.Name,
			Component: component,
		},
		DesiredSpec: types.WorkloadDesiredSpec{
			PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: ContainerName, Image: "test:v1"}}},
		},
		ObservedState: types.WorkloadObservedState{
			InstanceStatuses: InstanceStatuses(c, isvc, component),
		},
		MutateInstance: MutateInstanceOnReplica(c, isvc, component),
		Requeue:        types.RequeueIntervals{Operation: 5 * time.Second, Gate: 3 * time.Second},
	}
}

// Deps is the engine's dependencies over the client alone: no recorder,
// which every event helper accepts.
func Deps(c client.Client) types.Deps {
	return types.Deps{Client: c}
}

// SinglePodPlan is the plan of one engine Instance with one default runner
// under the given update strategy, with a thirty-minute ready timeout.
func SinglePodPlan(strategy types.UpdateStrategyType, inPlace *types.InPlaceUpdateStrategy) types.ComponentPlan {
	return types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  1,
		Instances: []types.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners:     []types.RunnerPlan{{Name: types.RunnerDefault, Size: 1}},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: types.UpdateStrategy{
			Type:                  strategy,
			InPlaceUpdateStrategy: inPlace,
		},
	}
}

// LeaderWorkerPlan is the plan of one engine Instance that is a gang of a
// leader and a worker under the given update strategy.
func LeaderWorkerPlan(strategy types.UpdateStrategyType) types.ComponentPlan {
	return types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  1,
		Instances: []types.InstancePlan{{
			Index:       0,
			Incarnation: 1,
			Runners: []types.RunnerPlan{
				{Name: types.RunnerLeader, Size: 1},
				{Name: types.RunnerWorker, Size: 1},
			},
		}},
		InstanceReadyTimeout: 30 * time.Minute,
		UpdateStrategy: types.UpdateStrategy{
			Type: strategy,
		},
	}
}

// ResetExpectations replaces the process-wide create expectations, so one
// test's pending creates are not seen by the next.
func ResetExpectations(t *testing.T) {
	t.Helper()
	types.DefaultExpectations = types.NewExpectations()
}
