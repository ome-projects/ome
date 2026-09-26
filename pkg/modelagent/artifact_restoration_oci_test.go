package modelagent

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/principals"
)

type restorationOCITransport func(*http.Request) (*http.Response, error)

func (f restorationOCITransport) Do(r *http.Request) (*http.Response, error) { return f(r) }

// The real OCI store and downloader run against an in-memory SDK transport.
// Authentication uses a throwaway local key; no cloud endpoint is contacted.
func restorationOCIStore(t *testing.T, g *Gopher, contents map[string]string) (*ociobjectstore.OCIOSDataStore, *atomic.Int32) {
	t.Helper()
	dir := t.TempDir()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPath := filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	configPath := filepath.Join(dir, "config")
	require.NoError(t, os.WriteFile(configPath, []byte(fmt.Sprintf("[DEFAULT]\nuser=test\ntenancy=test\nfingerprint=test\nregion=us-phoenix-1\nkey_file=%s\n", keyPath)), 0o600))
	t.Setenv("OCI_CONFIG_PATH", configPath)
	t.Setenv("PROFILE", "DEFAULT")
	t.Setenv("USE_SESSION_TOKEN", "false")
	auth := principals.UserPrincipal
	store, err := ociobjectstore.NewOCIOSDataStore(&ociobjectstore.Config{AuthType: &auth, AnotherLogger: logging.ForZap(g.logger.Desugar())})
	require.NoError(t, err)
	gets := new(atomic.Int32)
	store.Client.HTTPClient = restorationOCITransport(func(r *http.Request) (*http.Response, error) {
		header := http.Header{"Content-Type": []string{"application/json"}}
		body := ""
		if strings.HasSuffix(r.URL.Path, "/o") {
			objects := []map[string]any{}
			for name, content := range contents {
				if strings.HasPrefix(name, r.URL.Query().Get("prefix")) {
					objects = append(objects, map[string]any{"name": name, "size": len(content)})
				}
			}
			encoded, err := json.Marshal(map[string]any{"objects": objects})
			require.NoError(t, err)
			body = string(encoded)
		} else {
			_, name, ok := strings.Cut(r.URL.Path, "/o/")
			content, exists := contents[name]
			if !ok || !exists {
				return nil, fmt.Errorf("unexpected OCI request %s", r.URL)
			}
			sum := md5.Sum([]byte(content))
			header.Set("Content-Md5", base64.StdEncoding.EncodeToString(sum[:]))
			header.Set("Content-Length", fmt.Sprint(len(content)))
			if r.Method == http.MethodGet {
				gets.Add(1)
				body = content
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	return store, gets
}

func TestOCIArtifactInspectionChecksAfterCorruption(t *testing.T) {
	for _, laterError := range []bool{false, true} {
		t.Run(fmt.Sprint(laterError), func(t *testing.T) {
			path, err := filepath.EvalSymlinks(t.TempDir())
			require.NoError(t, err)
			uris := []ociobjectstore.ObjectURI{{Prefix: "model", ObjectName: "model/a"}, {Prefix: "model", ObjectName: "model/b"}}
			calls := 0
			inspectionError := errors.New("cannot inspect second object")
			valid, err := inspectOCIArtifactObjects(context.Background(), uris, path, func(uri ociobjectstore.ObjectURI, localPath string) (bool, error) {
				calls++
				require.Equal(t, filepath.Join(path, strings.TrimPrefix(uri.ObjectName, "model/")), localPath)
				if calls == 1 {
					return false, nil
				}
				if laterError {
					return false, inspectionError
				}
				return true, nil
			})
			require.Equal(t, 2, calls)
			require.False(t, valid)
			if laterError {
				require.ErrorIs(t, err, inspectionError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRestorationOCIValidatesInsteadOfMetadataReuse(t *testing.T) {
	for _, state := range []string{"healthy", "corrupt", "borrowed healthy", "borrowed corrupt"} {
		t.Run(state, func(t *testing.T) {
			g, task, path := newDirectArtifactTestModel(t)
			task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			if strings.HasPrefix(state, "borrowed") {
				require.NoError(t, g.modelClient.(*omefake.Clientset).Tracker().Add(&v1beta1.BaseModel{
					ObjectMeta: metav1.ObjectMeta{Name: "borrower", Namespace: "default", UID: "borrower"},
					Spec:       v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: stringPtr("local://" + path)}},
				}))
			}
			contents := "weights"
			if strings.HasSuffix(state, "corrupt") {
				contents = "CORRUPT"
			}
			require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte(contents), 0o600))
			store, gets := restorationOCIStore(t, g, map[string]string{"model/weights": "weights"})
			g.ociStore = func(v1beta1.BaseModelSpec) (*ociobjectstore.OCIOSDataStore, error) { return store, nil }
			g.concurrency, g.multipartConcurrency = 1, 1
			g.metrics = NewMetrics(prometheus.NewRegistry())
			err := g.downloadModel(context.Background(), &ociobjectstore.ObjectURI{Namespace: "ns", BucketName: "bucket", Prefix: "model"}, path, task)
			if state == "borrowed corrupt" {
				require.Error(t, err)
				require.Zero(t, gets.Load())
			} else {
				require.NoError(t, err)
				if contents == "weights" {
					require.Zero(t, gets.Load())
				} else {
					require.EqualValues(t, 1, gets.Load())
				}
			}
			require.False(t, shouldUseSamePathObjectStorageReuse(task), "a restoration request cannot trust Ready metadata")
		})
	}
}

func TestRestorationOCISelectedShapeVerification(t *testing.T) {
	g, task, path := newDirectArtifactTestModel(t)
	task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
	task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "request"}
	task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: string(constants.ServingBaseModel), ShapeAlias: "shape-a"}
	g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
	g.concurrency, g.multipartConcurrency = 1, 1
	g.metrics = NewMetrics(prometheus.NewRegistry())
	require.NoError(t, os.Mkdir(filepath.Join(path, "shape-a"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "shape-a", "weights"), []byte("weights"), 0o600))
	store, gets := restorationOCIStore(t, g, map[string]string{"model/shape-a/weights": "weights", "model/shape-b/weights": "unselected"})
	g.ociStore = func(v1beta1.BaseModelSpec) (*ociobjectstore.OCIOSDataStore, error) { return store, nil }
	uri := &ociobjectstore.ObjectURI{Namespace: "ns", BucketName: "bucket", Prefix: "model"}
	valid, err := g.validateHfOCIArtifact(context.Background(), task, uri, path)
	require.NoError(t, err)
	require.True(t, valid)
	require.NoError(t, g.downloadModel(context.Background(), uri, path, task))
	require.Zero(t, gets.Load())
	require.NoDirExists(t, filepath.Join(path, "shape-b"))
}

func TestRestorationOCIRepairRequiresWithdrawnReadiness(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy reuse", true: "repair"}[corrupt], func(t *testing.T) {
			ctx := context.Background()
			g, task, path := newDirectArtifactTestModel(t)
			task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
			task.BaseModel.Annotations = map[string]string{constants.ModelArtifactRehydrationIDAnnotation: "restore", ConfigParsingAnnotation: "true"}
			g.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			contents := "weights"
			if corrupt {
				contents = "CORRUPT"
			}
			require.NoError(t, os.WriteFile(filepath.Join(path, "weights"), []byte(contents), 0o600))
			store, gets := restorationOCIStore(t, g, map[string]string{"model/weights": "weights"})
			g.ociStore = func(v1beta1.BaseModelSpec) (*ociobjectstore.OCIOSDataStore, error) { return store, nil }
			g.concurrency, g.multipartConcurrency, g.downloadRetry = 1, 1, 1
			g.metrics = NewMetrics(prometheus.NewRegistry())
			g.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("label patch unavailable")
			})
			err := g.processTask(task)
			if corrupt {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			node, err := g.kubeClient.CoreV1().Nodes().Get(ctx, g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, string(Ready), node.Labels[constants.GetBaseModelLabel(task.BaseModel.Namespace, task.BaseModel.Name)])
			after, err := os.ReadFile(filepath.Join(path, "weights"))
			require.NoError(t, err)
			require.Equal(t, contents, string(after))
			require.Zero(t, gets.Load())
		})
	}
}
