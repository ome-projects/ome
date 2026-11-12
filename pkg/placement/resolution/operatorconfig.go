package resolution

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
)

// requireOperatorNamespace refuses a step that reads from the member
// operator's namespace while the resolver names none. purpose names the step
// in the error.
func requireOperatorNamespace(namespace, purpose string) error {
	if namespace == "" {
		return fmt.Errorf("%s requires the member operator namespace", purpose)
	}
	return nil
}

// operatorConfigMap reads the member operator's inferenceservice configuration
// through the dependency-recording reader, so the ConfigMap's identity enters
// the verified inputs.
func (r Resolver) operatorConfigMap(ctx context.Context, reads client.Reader) (*corev1.ConfigMap, error) {
	configMap := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: r.OperatorNamespace, Name: constants.InferenceServiceConfigMapName}
	if err := reads.Get(ctx, key, configMap); err != nil {
		return nil, err
	}
	return configMap, nil
}
