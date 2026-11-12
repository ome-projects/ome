package runtimepreset

import (
	"testing"

	v1 "k8s.io/api/core/v1"

	pkgtest "sigs.k8s.io/ome/pkg/utils/testing"
)

// shippedConfigMap is the inferenceservice-config the kustomize profile
// installs, the one copy of the preset table outside the chart.
func shippedConfigMap(t testing.TB) *v1.ConfigMap {
	t.Helper()
	configMap, err := pkgtest.ShippedInferenceServiceConfigMap()
	if err != nil {
		t.Fatal(err)
	}
	return configMap
}

// shippedPresets is the preset table the shipped ConfigMap carries.
func shippedPresets(t testing.TB) Presets {
	t.Helper()
	presets, err := parsePresets(shippedConfigMap(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(presets) == 0 {
		t.Fatal("the shipped ConfigMap names no preset")
	}
	return presets
}
