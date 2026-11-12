package testing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// ShippedInferenceServiceConfigMap reads config/configmap/inferenceservice.yaml,
// the inferenceservice-config the kustomize profile installs and the one copy
// of the webhook tables outside the chart. The file is found by walking up
// from the working directory to the module root, so a test binary finds it
// from any package directory of a checkout.
func ShippedInferenceServiceConfigMap() (*corev1.ConfigMap, error) {
	root, err := moduleRoot()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(root, "config", "configmap", "inferenceservice.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read the shipped ConfigMap: %w", err)
	}
	configMap := &corev1.ConfigMap{}
	if err := yaml.Unmarshal(raw, configMap); err != nil {
		return nil, fmt.Errorf("parse the shipped ConfigMap: %w", err)
	}
	return configMap, nil
}

// ShippedChartValue decodes one block of charts/ome-resources/values.yaml,
// named by its dotted path (for example "ome.podMonitor"), into out, so a
// test can hold the chart to the values the code expects it to supply.
func ShippedChartValue(path string, out any) error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(root, "charts", "ome-resources", "values.yaml"))
	if err != nil {
		return fmt.Errorf("read the chart values: %w", err)
	}
	var node any
	if err := yaml.Unmarshal(raw, &node); err != nil {
		return fmt.Errorf("parse the chart values: %w", err)
	}
	for _, key := range strings.Split(path, ".") {
		block, ok := node.(map[string]any)
		if !ok {
			return fmt.Errorf("chart values %q: %q is not a map", path, key)
		}
		if node, ok = block[key]; !ok {
			return fmt.Errorf("chart values %q: no key %q", path, key)
		}
	}
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// moduleRoot is the nearest ancestor of the working directory whose go.mod
// declares this module.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if data, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil &&
			bytes.Contains(data, []byte("module sigs.k8s.io/ome")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod of this module above the working directory")
		}
		dir = parent
	}
}
