package render

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestProcessComponentAnnotationsBaseModelDecryption(t *testing.T) {
	modelAnnotations := map[string]string{
		constants.BaseModelDecryptionKeyName:    "embed",
		constants.BaseModelDecryptionSecretName: "embed-dek",
		"models.ome.io/internal":                "false",
	}
	decryptionAnnotations := map[string]string{
		constants.BaseModelDecryptionKeyName:    "embed",
		constants.BaseModelDecryptionSecretName: "embed-dek",
	}
	for _, tt := range []struct {
		name          string
		modelMeta     *metav1.ObjectMeta
		mergedWeights bool
		component     map[string]string
		want          map[string]string
	}{
		{
			name:      "cluster base model",
			modelMeta: &metav1.ObjectMeta{Name: "embed-v4", Annotations: modelAnnotations},
			want:      decryptionAnnotations,
		},
		{
			name:      "namespaced base model",
			modelMeta: &metav1.ObjectMeta{Name: "embed-v4", Namespace: "models", Annotations: modelAnnotations},
			want:      decryptionAnnotations,
		},
		{
			name:      "model values override component values",
			modelMeta: &metav1.ObjectMeta{Name: "embed-v4", Annotations: modelAnnotations},
			component: map[string]string{
				constants.BaseModelDecryptionKeyName:    "other-key",
				constants.BaseModelDecryptionSecretName: "other-secret",
			},
			want: decryptionAnnotations,
		},
		{
			name:      "key without secret",
			modelMeta: &metav1.ObjectMeta{Annotations: map[string]string{constants.BaseModelDecryptionKeyName: "embed"}},
			want:      map[string]string{constants.BaseModelDecryptionKeyName: "embed"},
		},
		{
			name:      "secret without key",
			modelMeta: &metav1.ObjectMeta{Annotations: map[string]string{constants.BaseModelDecryptionSecretName: "embed-dek"}},
			want:      map[string]string{constants.BaseModelDecryptionSecretName: "embed-dek"},
		},
		{
			name:      "model without decryption annotations",
			modelMeta: &metav1.ObjectMeta{Name: "unencrypted-model"},
		},
		{
			name: "no model metadata",
		},
		{
			name:          "merged weights do not inherit base model keys",
			modelMeta:     &metav1.ObjectMeta{Name: "embed-v4", Annotations: modelAnnotations},
			mergedWeights: true,
			want: map[string]string{
				constants.FTServingWithMergedWeightsAnnotationKey: "true",
				constants.ModelInitInjectionKey:                   "false",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			piece := &Piece{
				BaseModelMeta:                     tt.modelMeta,
				FineTunedServingWithMergedWeights: tt.mergedWeights,
				Log:                               logr.Discard(),
			}
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "service", Namespace: "default",
				Annotations: map[string]string{"example.com/keep": "value"},
			}}
			got, err := ProcessComponentAnnotations(piece, service, tt.component)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]string{"example.com/keep": "value"}
			for key, value := range tt.want {
				want[key] = value
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("component annotations (-want +got):\n%s", diff)
			}
		})
	}
}
