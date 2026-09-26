package modelagent

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

func TestRestorationHealthyOCIStaysReadOnlyAfterRemoteChange(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore"}
	borrower := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "borrower", Namespace: "default", UID: "borrower"},
		Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}},
	}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel, borrower)
	require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte("weights"), 0600))
	contents := map[string]string{"model/weights": "weights"}
	store, gets := restorationOCIStore(t, g, contents)
	original := store.Client.HTTPClient
	changed := false
	store.Client.HTTPClient = restorationOCITransport(func(r *http.Request) (*http.Response, error) {
		response, err := original.Do(r)
		if r.Method == http.MethodHead && !changed {
			changed = true
			// The all-object pre-inspection observes the original healthy copy.
			// BulkDownload's subsequent HEAD sees a newer remote version.
			contents["model/weights"] = "REVISED"
		}
		return response, err
	})
	g.ociStore = func(v1beta1.BaseModelSpec) (*ociobjectstore.OCIOSDataStore, error) { return store, nil }
	g.concurrency, g.multipartConcurrency = 1, 1
	g.metrics = NewMetrics(prometheus.NewRegistry())
	unlock, acquired, err := g.acquireDirectArtifactDownload(context.Background(), task)
	require.NoError(t, err)
	require.True(t, acquired)
	defer unlock()
	err = g.downloadModel(context.Background(), &ociobjectstore.ObjectURI{Namespace: "ns", BucketName: "bucket", Prefix: "model"}, path, task)
	require.True(t, changed)
	after, readErr := os.ReadFile(filepath.Join(path, "weights"))
	require.NoError(t, readErr)
	require.Equal(t, "weights", string(after), "a healthy restoration must not overwrite a borrower's bytes after metadata changes")
	require.Zero(t, gets.Load())
	require.Error(t, err, "final verification should reject the changed remote object without repairing it")
}
