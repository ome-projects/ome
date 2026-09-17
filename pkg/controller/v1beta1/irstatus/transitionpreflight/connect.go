package transitionpreflight

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

// ConnectWithKubeconfig opens the read-only clients for one cluster from the
// kubeconfig file and context the inventory names. Only the explicit path and
// context are used; the ambient kubeconfig and its current context never
// select a cluster. The typed clientsets read the API server directly, with
// no informer or cache in between.
func ConnectWithKubeconfig(_ context.Context, cluster Cluster) (*ClusterClients, error) {
	loading := &clientcmd.ClientConfigLoadingRules{ExplicitPath: cluster.Kubeconfig}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: cluster.Context}
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %s context %q: %w", cluster.Kubeconfig, cluster.Context, err)
	}
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("build Kubernetes client: %w", err)
	}
	ome, err := versioned.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("build OME client: %w", err)
	}
	return &ClusterClients{
		Kube:     kube,
		OpenAPI:  kube.Discovery().OpenAPIV3(),
		Replicas: ome.OmeV1beta1().InferenceReplicas(metav1.NamespaceAll),
	}, nil
}
