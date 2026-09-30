package inferenceservice

import (
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func checkPlacementReplicaFloors(service *v1beta1.InferenceService, engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec) error {
	policy, err := protocol.FromDerived(service)
	if err != nil {
		return err
	}
	if policy == nil || len(policy.ReplicaFloors) == 0 {
		return nil
	}
	return protocol.CheckReplicaFloors(policy.ReplicaFloors, engine, decoder, router)
}

func clearPlacementReplicaFloorsHold(service *v1beta1.InferenceService) {
	if service.Status.GetCondition(v1beta1.PlacementReplicaFloorsReady) != nil {
		placementBackendConditions.Manage(&service.Status).SetCondition(apis.Condition{
			Type: v1beta1.PlacementReplicaFloorsReady, Status: corev1.ConditionTrue, Reason: "ReplicaFloorsAccepted",
		})
	}
}
