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

func TestRestorationOCIInspectionMatchesDownloadThroughAliasTraversal(t *testing.T) {
	g, task, original := newDirectArtifactTestModel(t)
	// Raw physical traversal stays in a managed family, but OCI's existing
	// lexical join selects the external sibling. All guards must agree with it.
	physicalParent := filepath.Join(original, "physical-parent")
	require.NoError(t, os.MkdirAll(filepath.Join(physicalParent, "nested"), 0700))
	aliasRoot := t.TempDir()
	alias := filepath.Join(aliasRoot, "alias")
	require.NoError(t, os.Symlink(filepath.Join(physicalParent, "nested"), alias))
	destination := alias + "/../model"
	physicalDestination := filepath.Join(physicalParent, "model")
	actualDestination := filepath.Join(aliasRoot, "model")
	require.NoError(t, os.MkdirAll(physicalDestination, 0700))
	require.NoError(t, os.MkdirAll(actualDestination, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(physicalDestination, "weights"), []byte("weights"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(actualDestination, "weights"), []byte("CORRUPT"), 0600))
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
	task.BaseModel.Spec.Storage.Path = &destination
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	store, gets := restorationOCIStore(t, g, map[string]string{"model/weights": "weights"})
	g.ociStore = func(v1beta1.BaseModelSpec) (*ociobjectstore.OCIOSDataStore, error) { return store, nil }
	g.concurrency, g.multipartConcurrency = 1, 1
	g.metrics = NewMetrics(prometheus.NewRegistry())
	effective, err := g.artifactDestination(task)
	require.NoError(t, err)
	require.Equal(t, filepath.Clean(destination), effective)
	managed, err := g.directArtifactOperationPath(task, false)
	require.NoError(t, err)
	require.Empty(t, managed, "do not lock an unrelated managed family for an external legacy destination")
	ctx, release := withDirectArtifactDownloadOperation(context.Background())
	defer release()
	unlock, acquired, err := g.acquireDirectArtifactDownload(ctx, task)
	require.NoError(t, err)
	require.True(t, acquired)
	defer unlock()
	// Match the existing SDK's ComputeTargetFilePath semantics exactly.
	uri := ociobjectstore.ObjectURI{Namespace: "ns", BucketName: "bucket", Prefix: "model", ObjectName: "model/weights"}
	require.Equal(t, filepath.Join(actualDestination, "weights"), ociobjectstore.ComputeTargetFilePath(uri, destination, &ociobjectstore.DownloadOptions{StripPrefix: true, PrefixToStrip: "model"}))
	err = g.downloadModel(ctx, &uri, destination, task)
	require.NoError(t, err, "inspection must inspect and repair the same path that the OCI downloader writes")
	contents, err := os.ReadFile(filepath.Join(actualDestination, "weights"))
	require.NoError(t, err)
	require.Equal(t, "weights", string(contents))
	require.EqualValues(t, 1, gets.Load())
}
