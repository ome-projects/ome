package modelagent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

func TestRestorationPreservesActualOCIDestination(t *testing.T) {
	for _, spelling := range []string{"nil", "empty", "spaces", "relative", "ancestor alias"} {
		for _, healthy := range []bool{false, true} {
			name := spelling + "/repair"
			if healthy {
				name = spelling + "/reuse"
			}
			t.Run(name, func(t *testing.T) {
				g, task, original := newDirectArtifactTestModel(t)
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
				task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore"}
				switch spelling {
				case "nil":
					task.BaseModel.Spec.Storage.Path = nil
				case "empty":
					task.BaseModel.Spec.Storage.Path = stringPtr("")
				case "spaces":
					task.BaseModel.Spec.Storage.Path = stringPtr(original + " ")
				case "relative":
					cwd, err := os.Getwd()
					require.NoError(t, err)
					relative, err := filepath.Rel(cwd, original)
					require.NoError(t, err)
					task.BaseModel.Spec.Storage.Path = &relative
				case "ancestor alias":
					alias := filepath.Join(t.TempDir(), "alias")
					require.NoError(t, os.Symlink(filepath.Dir(original), alias))
					task.BaseModel.Spec.Storage.Path = stringPtr(filepath.Join(alias, filepath.Base(original)))
				}
				destination := getDestPath(&task.BaseModel.Spec, g.modelRootDir)
				require.NoError(t, os.MkdirAll(destination, 0o700))
				contents := "CORRUPT"
				if healthy {
					contents = "weights"
				}
				require.NoError(t, os.WriteFile(filepath.Join(destination, "weights"), []byte(contents), 0o600))
				g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
				store, gets := restorationOCIStore(t, g, map[string]string{"model/weights": "weights"})
				g.ociStore = func(v1beta1.BaseModelSpec) (*ociobjectstore.OCIOSDataStore, error) { return store, nil }
				g.concurrency, g.multipartConcurrency = 1, 1
				g.metrics = NewMetrics(prometheus.NewRegistry())
				ctx, release := withDirectArtifactDownloadOperation(context.Background())
				defer release()
				unlock, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
				require.NoError(t, err)
				require.True(t, acquired)
				defer unlock()
				require.NoError(t, g.downloadModel(ctx, &ociobjectstore.ObjectURI{Namespace: "ns", BucketName: "bucket", Prefix: "model"}, destination, task))
				after, err := os.ReadFile(filepath.Join(destination, "weights"))
				require.NoError(t, err)
				require.Equal(t, "weights", string(after))
				if healthy {
					require.Zero(t, gets.Load())
				} else {
					require.EqualValues(t, 1, gets.Load())
				}
			})
		}
	}
}
