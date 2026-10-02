package modelparser

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestNemotronSuperVLMetadata(t *testing.T) {
	// Synthetic input pins the proposed rule contract, not checkpoint provenance.
	data := []byte(`{"model_type":"synthetic_super_vl_regression","architectures":["NemotronH_Omni_Reasoning_V3"],"vision_config":{},"sound_config":null,"llm_config":{"max_position_embeddings":1048576}}`)
	want := []string{"TEXT_TO_TEXT", "IMAGE_TEXT_TO_TEXT", "VIDEO_TEXT_TO_TEXT"}
	for _, loader := range []string{"files", "directory"} {
		for _, kind := range []string{"BaseModel", "ClusterBaseModel"} {
			for _, initial := range []struct {
				name string
				caps []string
			}{
				{name: "new"},
				{name: "stale_audio", caps: []string{"TEXT_TO_AUDIO", "IMAGE_TEXT_TO_AUDIO", "VIDEO_TEXT_TO_AUDIO", "AUDIO_TO_TEXT", "AUDIO_TO_AUDIO"}},
				{name: "operator_override", caps: []string{"TEXT_TO_TEXT"}},
			} {
				t.Run(loader+"/"+kind+"/"+initial.name, func(t *testing.T) {
					ctx := context.Background()
					bm := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "synthetic-super-vl", Namespace: "test"}}
					cbm := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "synthetic-super-vl"}}
					bm.Spec.ModelCapabilities = append([]string(nil), initial.caps...)
					cbm.Spec.ModelCapabilities = append([]string(nil), initial.caps...)
					client := fake.NewSimpleClientset(bm, cbm)
					parser := NewModelConfigParser(client, zap.NewNop().Sugar())
					if kind == "BaseModel" {
						cbm = nil
					} else {
						bm = nil
					}
					var metadata *ModelMetadata
					var err error
					if loader == "files" {
						metadata, err = parser.ParseModelConfigFromFiles(ctx, []ModelConfigFileInput{{Path: "config.json", Data: data}}, bm, cbm)
					} else {
						dir := t.TempDir()
						require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), data, 0600))
						metadata, err = parser.ParseModelConfig(dir, bm, cbm)
					}
					require.NoError(t, err)
					require.NotNil(t, metadata)
					require.Equal(t, want, metadata.ModelCapabilities)
					var spec v1beta1.BaseModelSpec
					if kind == "BaseModel" {
						persisted, err := client.OmeV1beta1().BaseModels("test").Get(ctx, "synthetic-super-vl", metav1.GetOptions{})
						require.NoError(t, err)
						spec = persisted.Spec
					} else {
						persisted, err := client.OmeV1beta1().ClusterBaseModels().Get(ctx, "synthetic-super-vl", metav1.GetOptions{})
						require.NoError(t, err)
						spec = persisted.Spec
					}
					expected := want
					if len(initial.caps) > 0 {
						expected = initial.caps
					}
					require.Equal(t, expected, spec.ModelCapabilities)
					require.NotNil(t, spec.MaxTokens)
					require.Equal(t, int32(1048576), *spec.MaxTokens)
				})
			}
		}
	}
}
