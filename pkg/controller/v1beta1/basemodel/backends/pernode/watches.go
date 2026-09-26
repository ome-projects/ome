package pernode

import (
	"context"
	"maps"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func CreateNodeDeletionPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool { return false },
		DeleteFunc: func(e event.DeleteEvent) bool { return true },
	}
}

// HandleNodeDeletion deletes the per-node model-status ConfigMap when
// the Node it was tracking goes away. No-op if no ConfigMap exists
// (nodes that never ran model-agent).
func HandleNodeDeletion(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}

	nodeName := node.GetName()
	log = log.WithValues("node", nodeName)

	configMap := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{
		Namespace: constants.OMENamespace,
		Name:      nodeName,
	}

	if err := kubeClient.Get(ctx, configMapKey, configMap); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		log.Error(err, "Failed to check ConfigMap for deleted node")
		return nil
	}

	if !IsModelStatusConfigMap(configMap) {
		return nil
	}

	log.Info("Node deleted, cleaning up associated model status ConfigMap")
	if err := kubeClient.Delete(ctx, configMap); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete ConfigMap for deleted node")
		}
		return nil
	}

	log.Info("Successfully deleted ConfigMap for deleted node")
	return nil
}

func CreateModelStatusConfigMapPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return IsModelStatusConfigMap(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return IsModelStatusConfigMap(e.ObjectOld) || IsModelStatusConfigMap(e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return IsModelStatusConfigMap(e.Object)
		},
	}
}

func IsModelStatusConfigMap(obj client.Object) bool {
	if obj == nil || obj.GetNamespace() != constants.OMENamespace {
		return false
	}
	labels := obj.GetLabels()
	if labels == nil {
		return false
	}
	return labels[constants.ModelStatusConfigMapLabel] == "true"
}

// MapConfigMapToModelRequests fans a per-node ConfigMap event out to
// reconcile requests for the models it tracks. isNamespaced selects
// BaseModel (namespaced) vs ClusterBaseModel (cluster-scoped).
func MapConfigMapToModelRequests(obj client.Object, log logr.Logger, isNamespaced bool) []reconcile.Request {
	var requests []reconcile.Request

	configMap, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return requests
	}

	for key := range configMap.Data {
		namespace, modelName, isClusterBaseModel, success := constants.ParseModelInfoFromConfigMapKey(key)
		if !success {
			continue
		}
		if (isNamespaced && isClusterBaseModel) || (!isNamespaced && !isClusterBaseModel) {
			continue
		}

		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: modelName}}
		if isNamespaced {
			req.Namespace = namespace
		}
		requests = append(requests, req)
	}

	return requests
}

// CreateNodePlacementPredicate is separate from orphan cleanup: membership
// changes must reconcile models even when no agent has created a ConfigMap.
func CreateNodePlacementPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { _, ok := e.Object.(*corev1.Node); return ok },
		DeleteFunc: func(e event.DeleteEvent) bool { _, ok := e.Object.(*corev1.Node); return ok },
		UpdateFunc: func(e event.UpdateEvent) bool {
			old, oldOK := e.ObjectOld.(*corev1.Node)
			current, currentOK := e.ObjectNew.(*corev1.Node)
			return oldOK && currentOK && (old.UID != current.UID || !maps.Equal(old.Labels, current.Labels))
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// MapNodeToRestorationRequests only enqueues work; it never deletes reports.
func MapNodeToRestorationRequests(ctx context.Context, c client.Client, log logr.Logger, obj client.Object, isNamespaced bool) []reconcile.Request {
	if _, ok := obj.(*corev1.Node); !ok {
		return nil
	}
	var objects []client.Object
	if isNamespaced {
		list := &v1beta1.BaseModelList{}
		if err := c.List(ctx, list); err != nil {
			log.Error(err, "List BaseModels for Node placement change")
			return nil
		}
		for i := range list.Items {
			objects = append(objects, &list.Items[i])
		}
	} else {
		list := &v1beta1.ClusterBaseModelList{}
		if err := c.List(ctx, list); err != nil {
			log.Error(err, "List ClusterBaseModels for Node placement change")
			return nil
		}
		for i := range list.Items {
			objects = append(objects, &list.Items[i])
		}
	}
	var requests []reconcile.Request
	for _, model := range objects {
		if model.GetAnnotations()[constants.ModelArtifactRehydrationIDAnnotation] != "" {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
		}
	}
	return requests
}
