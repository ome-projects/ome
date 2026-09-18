package transport

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// GetInferenceReplica returns one raw InferenceReplica for callers that inspect
// only its spec, metadata, and top-level status, not per-instance rows.
func (c *Client) GetInferenceReplica(ctx context.Context, namespace, name string, options metav1.GetOptions) (*v1beta1.InferenceReplica, error) {
	result := &v1beta1.InferenceReplica{}
	err := c.rest.Get().
		Namespace(namespace).
		Resource("inferencereplicas").
		Name(name).
		VersionedParams(&options, transportParameterCodec).
		WarningHandlerWithContext(rest.NoWarnings{}).
		MaxRetries(0).
		Do(ctx).
		Into(result)
	return result, err
}
