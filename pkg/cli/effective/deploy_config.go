package effective

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// LoadDeployConfig reads the operator's inferenceservice-config ConfigMap from
// omeNamespace and parses its deploy block with the controller's rules. A
// missing, unreadable, or invalid ConfigMap is an error, as it fails the
// reconcile; a ConfigMap without a deploy block yields empty defaults.
func LoadDeployConfig(ctx context.Context, client coreclient.ConfigMapsGetter, omeNamespace string) (*controllerconfig.DeployConfig, error) {
	if client == nil {
		return nil, errors.New("ConfigMap client must not be nil")
	}
	if omeNamespace == "" {
		return nil, errors.New("OME namespace must not be empty")
	}
	name := constants.InferenceServiceConfigMapName
	configMap, err := client.ConfigMaps(omeNamespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil, fmt.Errorf("deploy defaults: ConfigMap %s/%s not found", omeNamespace, name)
	case apierrors.IsForbidden(err):
		return nil, fmt.Errorf("deploy defaults: reading ConfigMap %s/%s is forbidden: %w", omeNamespace, name, err)
	case err != nil:
		return nil, fmt.Errorf("deploy defaults: read ConfigMap %s/%s: %w", omeNamespace, name, err)
	case configMap == nil || configMap.Namespace != omeNamespace || configMap.Name != name:
		return nil, fmt.Errorf("deploy defaults: API server returned a different object for ConfigMap %s/%s", omeNamespace, name)
	}
	deployConfig, err := controllerconfig.ParseDeployConfig(configMap)
	if err != nil {
		return nil, fmt.Errorf("deploy defaults: ConfigMap %s/%s: %w", omeNamespace, name, err)
	}
	return deployConfig, nil
}

// DecodeDeployConfig parses deploy defaults from a manifest holding exactly
// one inferenceservice-config ConfigMap, in YAML or JSON.
func DecodeDeployConfig(data []byte) (*controllerconfig.DeployConfig, error) {
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var found *corev1.ConfigMap
	for {
		var configMap corev1.ConfigMap
		if err := decoder.Decode(&configMap); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("deploy defaults: decode manifest: %w", err)
		}
		if configMap.APIVersion == "" && configMap.Kind == "" && configMap.Name == "" && configMap.Data == nil {
			continue
		}
		if found != nil {
			return nil, errors.New("deploy defaults: manifest must hold exactly one object")
		}
		found = &configMap
	}
	if found == nil {
		return nil, errors.New("deploy defaults: manifest is empty")
	}
	if found.APIVersion != "v1" || found.Kind != "ConfigMap" {
		return nil, fmt.Errorf("deploy defaults: manifest is %s %s, want v1 ConfigMap", found.APIVersion, found.Kind)
	}
	if found.Name != constants.InferenceServiceConfigMapName {
		return nil, fmt.Errorf("deploy defaults: ConfigMap is named %q, want %q", found.Name, constants.InferenceServiceConfigMapName)
	}
	deployConfig, err := controllerconfig.ParseDeployConfig(found)
	if err != nil {
		return nil, fmt.Errorf("deploy defaults: %w", err)
	}
	return deployConfig, nil
}
