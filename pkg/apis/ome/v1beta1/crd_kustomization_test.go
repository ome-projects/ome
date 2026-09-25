package v1beta1

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestCoreCRDKustomizationsAreComplete(t *testing.T) {
	coreCRDs := []string{
		"ome.io_acceleratorclasses.yaml",
		"ome.io_acceleratorquotas.yaml",
		"ome.io_basemodels.yaml",
		"ome.io_benchmarkjobs.yaml",
		"ome.io_clusterbasemodels.yaml",
		"ome.io_clusterservingruntimes.yaml",
		"ome.io_finetunedweights.yaml",
		"ome.io_inferencereplicas.yaml",
		"ome.io_inferenceservices.yaml",
		"ome.io_servingruntimes.yaml",
		"ome.io_trafficmaps.yaml",
		"ome.io_workloadclusters.yaml",
	}

	root := filepath.Join("..", "..", "..", "..", "config", "crd")
	tests := []struct {
		path   string
		prefix string
	}{
		{path: "kustomization.yaml", prefix: "full"},
		{path: filepath.Join("full", "kustomization.yaml")},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			path := filepath.Join(root, tt.path)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var manifest struct {
				Resources []string `yaml:"resources"`
			}
			if err := yaml.Unmarshal(data, &manifest); err != nil {
				t.Fatalf("unmarshal %s: %v", path, err)
			}

			got := append([]string(nil), manifest.Resources...)
			want := make([]string, len(coreCRDs))
			for i, resource := range coreCRDs {
				want[i] = filepath.Join(tt.prefix, resource)
			}
			for i, resource := range manifest.Resources {
				got[i] = filepath.Clean(resource)
				resourcePath := filepath.Join(filepath.Dir(path), resource)
				info, err := os.Stat(resourcePath)
				if err != nil {
					t.Errorf("referenced CRD %s: %v", resourcePath, err)
				} else if info.IsDir() {
					t.Errorf("referenced CRD %s is a directory", resourcePath)
				}
			}
			sort.Strings(got)
			sort.Strings(want)
			if !slices.Equal(got, want) {
				t.Errorf("%s resources = %v, want core CRDs %v", path, got, want)
			}
		})
	}
}
