package placement

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// A missing service can leave component execution authority behind. Creation
// requires a fresh empty inventory from the direct member client.
func requireEmptyMemberInventory(ctx context.Context, direct client.Client, source *v1beta1.InferenceService) error {
	selector := client.MatchingLabels{constants.InferenceServicePodLabelKey: source.Name}
	for _, kind := range []schema.GroupVersionKind{v1beta1.SchemeGroupVersion.WithKind("InferenceReplicaList"), corev1.SchemeGroupVersion.WithKind("PodList")} {
		inventory := &metav1.PartialObjectMetadataList{}
		inventory.SetGroupVersionKind(kind)
		if err := direct.List(ctx, inventory, client.InNamespace(source.Namespace), selector); err != nil {
			return err
		}
		if len(inventory.Items) > 0 || inventory.Continue != "" {
			return fmt.Errorf("member service recreation awaits empty %s inventory", kind.Kind)
		}
	}
	return nil
}
