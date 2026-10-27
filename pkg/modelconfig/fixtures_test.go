package modelconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAllFixturesParse walks testdata/ and asserts the Loose contract:
// every fixture parses without error, has non-empty model_type and
// architecture, and a positive context length. Parameter count and
// model size are NOT asserted — those are unreliable when derived
// from config.json alone (parameter math depends on architecture
// quirks the JSON doesn't always expose), which is exactly why the
// safetensors parser exists. Fixtures here are config-only, so any
// contract on params would just be reasserting the estimator's bias.
//
// This is the regression net for the upcoming refactor — it must
// keep passing as per-model parsers are deleted in favor of the
// generic loader.
func TestAllFixturesParse(t *testing.T) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	for _, entry := range entries {
		entry := entry
		path, ok := fixturePath(entry)
		if !ok {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			assertFixtureSane(t, path)
		})
	}
}

// fixturesSkipped is the set of testdata entries the Loose-contract
// walker excludes. Each entry has a stable reason captured here:
//
//   - clip_vision_model.json: CLIP vision-encoder sub-component, not a
//     complete model. It has no text context length (image_size /
//     patch_size only) so the Loose context > 0 assertion would fail
//     by design.
//   - llava_v1.5_7b.json: today's LLaVA per-model parser reads
//     c.TextConfig.MaxPositionEmbeddings, which is 0 for this
//     particular fixture (no text_config block — context lives at top
//     level as max_position_embeddings: 4096). The generic resolver
//     reads the top-level value correctly, so this entry should be
//     un-skipped after Task 5 of the modelconfig-unification plan
//     collapses LoadModelConfig onto GenericModelConfig.
var fixturesSkipped = map[string]string{
	"clip_vision_model.json": "vision-encoder sub-component, no text context length",

	// Genuinely sparse JSON — exotic per-model logic synthesized
	// data that isn't in the file. We agreed during brainstorming
	// to drop these formulas rather than carry them in generic.
	"janus_1.3b.json": "no architectures top-level or nested; per-model parser hardcodes 'JanusMultiModalityCausalLM'",
	"gemma3.json":     "context length only derivable as text_config.sliding_window * rope_factor; spec dropped this formula",
}

// fixturePath returns the path to the model config inside testdata/
// for a given entry, or false if the entry is not a fixture (auxiliary
// JSON, README, etc.) or is in the skip list.
func fixturePath(entry os.DirEntry) (string, bool) {
	name := entry.Name()
	if entry.IsDir() {
		// Directory fixtures (e.g. tiny-random-PhiModel) hold a
		// config.json next to safetensors siblings.
		path := filepath.Join("testdata", name, "config.json")
		if _, err := os.Stat(path); err != nil {
			return "", false
		}
		return path, true
	}
	if !strings.HasSuffix(name, ".json") {
		return "", false
	}
	if _, skip := fixturesSkipped[name]; skip {
		return "", false
	}
	return filepath.Join("testdata", name), true
}

func assertFixtureSane(t *testing.T, path string) {
	t.Helper()

	model, err := LoadModelConfig(path)
	if err != nil {
		t.Fatalf("LoadModelConfig(%s) returned error: %v", path, err)
	}

	if model.GetModelType() == "" {
		t.Errorf("%s: model_type is empty", path)
	}
	if model.GetArchitecture() == "" {
		t.Errorf("%s: architecture is empty", path)
	}
	if ctx := model.GetContextLength(); ctx <= 0 {
		t.Errorf("%s: context length must be positive, got %d", path, ctx)
	}
	if model.GetModelType() == "diffusers" {
		if _, ok := model.(HuggingFaceDiffusionModel); !ok {
			t.Errorf("%s: diffusion fixture does not satisfy HuggingFaceDiffusionModel", path)
		}
	}
}
