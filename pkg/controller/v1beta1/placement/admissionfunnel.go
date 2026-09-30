package placement

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// AdmissionFunnelConfigFor observes IR resource versions without retaining
// instance status payloads. Events are wakeups; placement still reads live state.
func AdmissionFunnelConfigFor(controlPlaneID string) workloadcluster.FunnelConfig {
	return workloadcluster.FunnelConfig{
		MetadataOnly: true,
		NewList: func() client.ObjectList {
			list := &metav1.PartialObjectMetadataList{}
			list.SetGroupVersionKind(v1beta1.SchemeGroupVersion.WithKind("InferenceReplicaList"))
			return list
		},
		NewObject: func() client.Object { return &v1beta1.InferenceService{} },
		Resolve: func(remote client.Object) (types.NamespacedName, bool) {
			if _, ok := localKeyForDerived(remote, controlPlaneID); !ok {
				return types.NamespacedName{}, false
			}
			owner := metav1.GetControllerOf(remote)
			if owner == nil || owner.Kind != "InferenceService" || owner.APIVersion != v1beta1.SchemeGroupVersion.String() || owner.UID == "" || owner.Name == "" || remote.GetNamespace() == "" {
				return types.NamespacedName{}, false
			}
			return types.NamespacedName{Namespace: remote.GetNamespace(), Name: owner.Name}, true
		},
		WatchSelector: originWatchSelector(controlPlaneID),
	}
}

// StatusFunnelConfigsFor includes member summaries and per-instance admission
// changes that do not alter those summaries.
func StatusFunnelConfigsFor(controlPlaneID string) []workloadcluster.FunnelConfig {
	return []workloadcluster.FunnelConfig{FunnelConfigFor(controlPlaneID), AdmissionFunnelConfigFor(controlPlaneID)}
}
