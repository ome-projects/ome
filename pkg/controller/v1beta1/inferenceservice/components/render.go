package components

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/render"
)

// ReplicaTemplates are the component's rendered templates before workload
// projection and pod admission. Deployment mode determines whether workers run.
type ReplicaTemplates struct {
	ObjectMeta metav1.ObjectMeta
	Primary    *corev1.PodSpec
	Worker     *corev1.PodSpec
	WorkerSize int
}

// ReplicaTemplatesFrom carries the library's rendered templates into the
// shape PodSets measures.
func ReplicaTemplatesFrom(t render.Templates) ReplicaTemplates {
	return ReplicaTemplates{ObjectMeta: t.ObjectMeta, Primary: t.Primary, Worker: t.Worker, WorkerSize: t.WorkerSize}
}

// PodSets describes one complete replica under the component's resolved mode.
func (t ReplicaTemplates) PodSets(mode constants.DeploymentModeType, leader, worker bool) ([]capacity.PodSet, error) {
	sets := []capacity.PodSet{{Name: "primary", Count: 1, Spec: t.Primary}}
	switch mode {
	case constants.RawDeployment:
		return sets, nil
	case constants.OMENative:
		if !leader && !worker {
			return sets, nil
		}
	case constants.MultiNode:
	default:
		return nil, fmt.Errorf("deployment mode %q has no supported replica shape", mode)
	}
	if !leader || !worker || t.Worker == nil || t.WorkerSize <= 0 {
		return nil, fmt.Errorf("a multi-pod replica requires a leader and a positive resolved worker count")
	}
	return append(sets, capacity.PodSet{Name: "workers", Count: int64(t.WorkerSize), Spec: t.Worker}), nil
}
