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
			// Select the document by name, not position: a reordered manifest must
			// fail naming the real cause rather than a misleading missing key.
			for _, part := range strings.Split(string(raw), "\n---") {
				// Declared per document: unmarshalling into a reused struct keeps
				// fields absent from the next document, so a nameless one would
				// inherit the previous document's name.
				var cm struct {
					Metadata struct {
						Name string `json:"name"`
					} `json:"metadata"`
					Data map[string]string `json:"data"`
				}
				if err := yaml.Unmarshal([]byte(part), &cm); err != nil {
					t.Fatal(err)
				}
				if cm.Metadata.Name != "alfred-config" {
					continue
				}
				doc, ok := cm.Data["config.yaml"]
				if !ok {
					t.Fatal("config.yaml key missing from alfred-config manifest")
				}
				return []byte(doc)
			}
			t.Fatal("alfred-config ConfigMap not found in config/alfred/configmap.yaml")
			return nil
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
