package config

import (
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestShippedDefaultConfigsAreValid is the golden check on every config.yaml
// this repo ships: each must pass Alfred's own schema validation and keep the
// safe default — recommend-only — so an install can never start acting
// because a default drifted.
func TestShippedDefaultConfigsAreValid(t *testing.T) {
	shipped := map[string]func(t *testing.T) []byte{
		"charts/ome-alfred values.alfredConfig": func(t *testing.T) []byte {
			raw, err := os.ReadFile("../../../charts/ome-alfred/values.yaml")
			if err != nil {
				t.Fatal(err)
			}
			var values struct {
				AlfredConfig map[string]interface{} `json:"alfredConfig"`
			}
			if err := yaml.Unmarshal(raw, &values); err != nil {
				t.Fatal(err)
			}
			doc, err := yaml.Marshal(values.AlfredConfig)
			if err != nil {
				t.Fatal(err)
			}
			return doc
		},
		"config/alfred configmap": func(t *testing.T) []byte {
			raw, err := os.ReadFile("../../../config/alfred/configmap.yaml")
			if err != nil {
				t.Fatal(err)
			}
			// The manifest is multi-document; alfred-config comes first.
			first := strings.SplitN(string(raw), "\n---", 2)[0]
			var cm struct {
				Data map[string]string `json:"data"`
			}
			if err := yaml.Unmarshal([]byte(first), &cm); err != nil {
				t.Fatal(err)
			}
			doc, ok := cm.Data["config.yaml"]
			if !ok {
				t.Fatal("config.yaml key missing from alfred-config manifest")
			}
			return []byte(doc)
		},
	}

	for name, extract := range shipped {
		t.Run(name, func(t *testing.T) {
			cfg, err := Load(extract(t))
			if err != nil {
				t.Fatalf("shipped default rejected by Alfred's own validation: %v", err)
			}
			if cfg.Mode != ModeRecommendOnly {
				t.Fatalf("shipped default mode = %q; the safe default is %q", cfg.Mode, ModeRecommendOnly)
			}
			if !*cfg.Policies.Defragmentation.Enabled {
				t.Fatal("shipped default should enable defragmentation (recommend-only makes it safe)")
			}
		})
	}
}
