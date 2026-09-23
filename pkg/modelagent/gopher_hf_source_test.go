package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/xet"
)

func newTestDirectHfSource(t *testing.T) (*Gopher, *GopherTask, hfArtifactTaskInput, directHfSource, *int) {
	t.Helper()
	s, task, input := newTestHfArtifactGopher(t)
	uri := "hf://" + input.Parent.Identity.ModelID + "@refs/pr/7"
	task.BaseModel.Spec.Storage.StorageUri = &uri
	// Source tests configure this task between calls; expose that configured
	// Model through the live API used by ordinary bounded writers as well.
	s.modelClient.(*omefake.Clientset).PrependReactor("get", "basemodels", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, task.BaseModel.DeepCopy(), nil
	})
	s.xetConfig = &xet.Config{Token: "fallback-token", Endpoint: "https://hf.example.test/hub"}
	contents := map[string]string{"config.json": "{}", "model.safetensors": "weights"}
	manifest := testHfSnapshotManifest(contents)
	manifest.SHA = input.Parent.Identity.CommitSHA
	downloads := new(int)
	source := directHfSource{
		resolve: func(_ context.Context, id, revision, token, endpoint string) (string, error) {
			assert.Equal(t, input.Parent.Identity.ModelID, id)
			assert.Equal(t, "refs/pr/7", revision)
			assert.Equal(t, "fallback-token", token)
			assert.Equal(t, s.xetConfig.Endpoint, endpoint)
			return strings.ToUpper(input.Parent.Identity.CommitSHA), nil
		},
		manifest: func(_ context.Context, id, revision, token, endpoint string) (hfSnapshotManifest, error) {
			assert.Equal(t, input.Parent.Identity.CommitSHA, revision)
			assert.Equal(t, "fallback-token", token)
			return manifest, nil
		},
		download: func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
			*downloads++
			assert.Equal(t, input.Parent.Identity.CommitSHA, config.Revision)
			require.NoError(t, os.MkdirAll(config.LocalDir, 0o755))
			for name, value := range contents {
				// Emulate Xet's same-size shortcut, including corrupt files.
				filename := filepath.Join(config.LocalDir, name)
				info, err := os.Stat(filename)
				if err == nil && info.Size() == int64(len(value)) {
					continue
				}
				require.NoError(t, os.WriteFile(filename, []byte(value), 0o600))
			}
			return nil
		},
	}
	return s, task, input, source, downloads
}

func TestDirectHfSourcePinsRevisionAcrossRequeue(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	resolves := 0
	resolve := source.resolve
	source.resolve = func(ctx context.Context, id, revision, token, endpoint string) (string, error) {
		resolves++
		return resolve(ctx, id, revision, token, endpoint)
	}
	waiting, err := source.process(context.Background(), s, task, task.BaseModel.Spec, false)
	require.NoError(t, err)
	require.True(t, waiting)
	assert.Equal(t, input.Parent.Identity.CommitSHA, task.HfResolvedRevision)
	assert.Zero(t, *downloads)
	waiting, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.False(t, waiting)
	assert.Equal(t, 1, resolves)
	assert.Equal(t, 1, *downloads)
	parent, found, err := s.sharedHfArtifactHandler().repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, input.Parent.Key, parent.Key)
	assert.Equal(t, input.Parent.LocalPath, parent.LocalPath)
}

func TestDirectHfSourceResumesOldDeletionBeforeResolution(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			s, task, input, source, downloads := newTestDirectHfSource(t)
			if !shared {
				task.BaseModel.Spec.Storage.DownloadPolicy = nil
			}
			handler := s.sharedHfArtifactHandler()
			old := input
			old.Parent.Identity.CommitSHA = strings.Repeat("b", 40)
			old.Parent.Key = hfArtifactConfigMapKey(old.Parent.Identity)
			old.ChildModelPath = filepath.Join(input.ModelStoreRoot, "old-child-path")
			old.Parent.LocalPath = canonicalHfArtifactPath(old.ChildModelPath, old.Parent.Identity)
			require.NoError(t, runTestHfArtifactDownload(handler, old))
			_, err := handler.repository.removeModelReference(context.Background(), old.Parent, old.ChildModelKey, old.ChildModelUID, old.ChildModelPath, true)
			require.NoError(t, err)
			resolves := 0
			resolve := source.resolve
			source.resolve = func(ctx context.Context, id, revision, token, endpoint string) (string, error) {
				resolves++
				assertChildPathMissing(t, old.ChildModelPath)
				assert.NoDirExists(t, old.Parent.LocalPath)
				pending, err := handler.repository.pendingDeletion(ctx, input.ChildModelKey)
				require.NoError(t, err)
				assert.Nil(t, pending, "old cleanup must finish before resolving the new source")
				return resolve(ctx, id, revision, token, endpoint)
			}
			waiting, err := source.process(context.Background(), s, task, task.BaseModel.Spec, false)
			require.NoError(t, err)
			require.True(t, waiting)
			assert.Zero(t, resolves)
			assert.Zero(t, *downloads)
			assert.DirExists(t, old.Parent.LocalPath)
			waiting, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			assert.False(t, waiting)
			assert.Equal(t, 1, resolves)
			assert.Equal(t, 1, *downloads)
		})
	}
}

func TestDirectHfSourceCleanupErrorPreventsResolution(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	handler := s.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	_, err := handler.repository.removeModelReference(context.Background(), input.Parent, input.ChildModelKey, input.ChildModelUID, input.ChildModelPath, true)
	require.NoError(t, err)
	unlock, acquired, err := handler.tryArtifactOperation(input)
	require.NoError(t, err)
	require.True(t, acquired)
	defer unlock()
	s.samePathWaitTimeout = time.Millisecond
	task.SamePathWaitStartedAt = time.Now().Add(-time.Second)
	resolves := 0
	source.resolve = func(context.Context, string, string, string, string) (string, error) {
		resolves++
		return input.Parent.Identity.CommitSHA, nil
	}
	_, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.ErrorContains(t, err, "retry budget exhausted")
	assert.Zero(t, resolves)
	assert.Zero(t, *downloads)
	assert.DirExists(t, input.Parent.LocalPath)
}

func TestDirectHfSourceHTTPProviderPinsMovingBranch(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	resolutions, manifests := 0, 0
	manifest := testHfSnapshotManifest(map[string]string{"config.json": "{}", "model.safetensors": "weights"})
	manifest.SHA = input.Parent.Identity.CommitSHA
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer fallback-token", r.Header.Get("Authorization"))
		switch r.URL.EscapedPath() {
		case "/hub/api/models/" + input.Parent.Identity.ModelID + "/revision/refs%2Fpr%2F7":
			resolutions++
			sha := manifest.SHA
			if resolutions > 1 {
				sha = strings.Repeat("a", 40)
			}
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"sha": sha}))
		case "/hub/api/models/" + input.Parent.Identity.ModelID + "/revision/" + manifest.SHA:
			manifests++
			assert.Equal(t, "true", r.URL.Query().Get("blobs"))
			require.NoError(t, json.NewEncoder(w).Encode(manifest))
		default:
			t.Errorf("unexpected provider path %s", r.URL.EscapedPath())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	s.xetConfig.Endpoint = server.URL + "/hub"
	source.resolve, source.manifest = resolveHfRevision, fetchHfSnapshotManifest
	waiting, err := source.process(context.Background(), s, task, task.BaseModel.Spec, false)
	require.NoError(t, err)
	require.True(t, waiting)
	waiting, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.False(t, waiting)
	assert.Equal(t, 1, resolutions)
	assert.Equal(t, 1, manifests)
	assert.Equal(t, 1, *downloads)
}

func TestDirectHfSourceHealthyReuseAndRepair(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	setTestRepairChildStatus(t, s.sharedHfArtifactHandler().repository, input.ChildModelKey, ModelStatusReady)
	task.TaskType = DownloadOverride
	_, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.Equal(t, 1, *downloads, "healthy override only validates")
	file := filepath.Join(input.Parent.LocalPath, "model.safetensors")
	require.NoError(t, os.WriteFile(file, []byte("corrupt"), 0o600))
	download := source.download
	source.download = func(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
		assert.NoFileExists(t, file, "same-size corruption removed before Xet can skip it")
		assertTestRepairChildStatuses(t, s.sharedHfArtifactHandler(), map[string]ModelStatus{input.ChildModelKey: ModelStatusFailed})
		return download(ctx, task, config)
	}
	_, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.Equal(t, 2, *downloads)
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	assert.Equal(t, "weights", string(data))
}

func TestDirectHfSourceCrossSourceIdentity(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	// The existing parent is registered by the same handler used by OCI.
	require.NoError(t, runTestHfArtifactDownload(s.sharedHfArtifactHandler(), input))
	require.NoError(t, os.WriteFile(filepath.Join(input.Parent.LocalPath, "model.safetensors"), []byte("weights"), 0o600))
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.Zero(t, *downloads)
	parent, found, err := s.sharedHfArtifactHandler().repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, input.Parent.Key, parent.Key)
}

func TestDirectHfSourcePolicyChangeDetachesBeforeWriting(t *testing.T) {
	s, task, input, source, _ := newTestDirectHfSource(t)
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	download := source.download
	source.download = func(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
		_, found, err := s.sharedHfArtifactHandler().repository.GetParentForChild(ctx, input.ChildModelKey)
		require.NoError(t, err)
		assert.False(t, found)
		assert.Equal(t, input.ChildModelPath, config.LocalDir)
		assert.False(t, isSharedHfArtifactSymlink(config.LocalDir))
		return download(ctx, task, config)
	}
	waiting, err := source.process(context.Background(), s, task, task.BaseModel.Spec, false)
	require.NoError(t, err)
	assert.True(t, waiting)
	_, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	info, err := os.Lstat(input.ChildModelPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestDirectHfSourceResolutionFailureUsesOrdinaryPath(t *testing.T) {
	s, task, input, source, _ := newTestDirectHfSource(t)
	source.resolve = func(context.Context, string, string, string, string) (string, error) {
		return "", errors.New("metadata unavailable")
	}
	source.manifest = func(context.Context, string, string, string, string) (hfSnapshotManifest, error) {
		t.Fatal("unresolved identity cannot enter shared validation")
		return hfSnapshotManifest{}, nil
	}
	source.download = func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
		assert.Equal(t, input.ChildModelPath, config.LocalDir)
		assert.Equal(t, "refs/pr/7", config.Revision)
		assert.Equal(t, "fallback-token", config.Token)
		return nil
	}
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.Empty(t, task.HfResolvedRevision)
	_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestDirectHfSourceOrdinaryDownloadWithMissingConfigMap(t *testing.T) {
	for _, policy := range []v1beta1.DownloadPolicy{"", v1beta1.AlwaysDownload} {
		name := string(policy)
		if policy == "" {
			name = "nil policy"
		}
		t.Run(name, func(t *testing.T) {
			s, task, input, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			if policy != "" {
				task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			}
			ctx := context.Background()
			require.NoError(t, s.configMapReconciler.ReconcileModelStatus(ctx, &ConfigMapStatusOp{
				BaseModel: task.BaseModel, ModelStatus: ModelStatusUpdating,
			}))
			require.NoError(t, s.configMapReconciler.kubeClient.CoreV1().ConfigMaps(s.configMapReconciler.namespace).
				Delete(ctx, s.configMapReconciler.nodeName, metav1.DeleteOptions{}))

			waiting, err := source.process(ctx, s, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			assert.False(t, waiting)
			assert.Equal(t, 1, *downloads)
			assert.FileExists(t, filepath.Join(input.ChildModelPath, "model.safetensors"))
			assert.NoDirExists(t, input.Parent.LocalPath)
		})
	}
}

func TestDirectHfDispatcherFailureMetrics(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		lookupErr  error
		rateLimits float64
	}{
		{name: "HTTP 429", status: http.StatusTooManyRequests, rateLimits: 1},
		{name: "rate limit string", lookupErr: errors.New("provider rate limit exceeded"), rateLimits: 1},
		{name: "HTTP 503", status: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, input, _, _ := newTestDirectHfSource(t)
			cm, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			s = newGopherForProcessTask(cm)
			s.modelRootDir = input.ModelStoreRoot
			s.modelClient = omefake.NewSimpleClientset(task.BaseModel)
			task.HfResolvedRevision = input.Parent.Identity.CommitSHA
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "true", r.URL.Query().Get("blobs"))
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			s.xetConfig = &xet.Config{Endpoint: server.URL}
			s.captureStartupReadyModels(context.Background())
			if tc.lookupErr != nil {
				s.configMapReconciler.kubeClient.(*fake.Clientset).PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.lookupErr
				})
			}
			labels := []string{"model_type", "namespace", "name"}
			s.metrics = &Metrics{
				modelDownloadsFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_download_failures"}, labels),
				rateLimitCounter:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_rate_limits"}, labels),
				rateLimitWaitDuration:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_rate_limit_wait"}, labels),
			}

			err = s.processTask(task)
			require.Error(t, err)
			if tc.lookupErr != nil {
				assert.ErrorContains(t, err, "rate limit")
			} else {
				assert.ErrorContains(t, err, fmt.Sprintf("HTTP %d", tc.status))
			}
			modelType, namespace, name := GetModelTypeNamespaceAndName(task)
			assert.Equal(t, 1.0, testutil.ToFloat64(s.metrics.modelDownloadsFailedTotal.WithLabelValues(modelType, namespace, name)))
			assert.Equal(t, tc.rateLimits, testutil.ToFloat64(s.metrics.rateLimitCounter.WithLabelValues(modelType, namespace, name)))
			metric := &dto.Metric{}
			require.NoError(t, s.metrics.rateLimitWaitDuration.WithLabelValues(modelType, namespace, name).(prometheus.Metric).Write(metric))
			assert.Equal(t, uint64(tc.rateLimits), metric.GetHistogram().GetSampleCount())
			assert.Equal(t, 30*tc.rateLimits, metric.GetHistogram().GetSampleSum())
		})
	}
}

func TestDirectHfSourceUnresolvedRevisionPreservesReadyLastChild(t *testing.T) {
	for _, scenario := range []string{"resolver 503", "empty revision", "retry budget exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			s, task, input, source, _ := newTestDirectHfSource(t)
			uri := "hf://" + input.Parent.Identity.ModelID + "@main"
			task.BaseModel.Spec.Storage.StorageUri = &uri
			source.resolve = func(_ context.Context, _, revision, _, _ string) (string, error) {
				assert.Equal(t, "main", revision)
				return input.Parent.Identity.CommitSHA, nil
			}
			_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			handler := s.sharedHfArtifactHandler()
			setTestRepairChildStatus(t, handler.repository, input.ChildModelKey, ModelStatusReady)
			before, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			parent, found, err := handler.repository.GetParentForChild(context.Background(), input.ChildModelKey)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, HfArtifactStatusReady, parent.Status)
			require.Len(t, parent.Children, 1)

			// A later task must resolve the unchanged branch again. Neither a Hub
			// outage nor a failed fallback downloader may discard the usable copy.
			task.HfResolvedRevision = ""
			s.samePathWaitDelay = time.Millisecond
			if scenario == "retry budget exhausted" {
				s.samePathWaitTimeout = time.Millisecond
				task.SamePathWaitStartedAt = time.Now().Add(-time.Second)
			}
			source.resolve = func(context.Context, string, string, string, string) (string, error) {
				if scenario == "empty revision" {
					return "", nil
				}
				return "", errors.New("HF revision metadata returned HTTP 503")
			}
			downloads := 0
			source.download = func(context.Context, *GopherTask, *xet.DownloadConfig) error {
				downloads++
				return errors.New("downloader unavailable")
			}
			waiting, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			assert.True(t, waiting)
			if scenario == "retry budget exhausted" {
				assert.ErrorContains(t, err, "retry budget exhausted")
				assert.ErrorContains(t, err, "HTTP 503")
			} else {
				assert.NoError(t, err)
			}
			assert.Zero(t, downloads, "unresolved ownership must not reach the fallback downloader")
			assert.Empty(t, task.HfResolvedRevision)
			after, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			assert.Equal(t, before.Data, after.Data, "parent, child reference, and Ready state must survive")
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
			assert.True(t, handler.files.ParentReadyMarkerExists(parent))
			data, err := os.ReadFile(filepath.Join(input.Parent.LocalPath, "model.safetensors"))
			assert.NoError(t, err)
			assert.Equal(t, "weights", string(data))
			if waiting && scenario != "retry budget exhausted" {
				select {
				case retry := <-s.gopherChan:
					assert.Same(t, task, retry)
				case <-time.After(time.Second):
					t.Fatal("unresolved shared child was not requeued")
				}
			}
		})
	}
}

func TestDirectHfSourceLegacyDirectoryAndDescendantsStayInPlace(t *testing.T) {
	s, task, input, source, _ := newTestDirectHfSource(t)
	require.NoError(t, os.MkdirAll(input.ChildModelPath, 0o755))
	file := filepath.Join(input.ChildModelPath, "legacy-sentinel")
	require.NoError(t, os.WriteFile(file, []byte("untouched"), 0o600))
	child := filepath.Join(input.ModelStoreRoot, "old-child")
	require.NoError(t, os.Symlink(input.ChildModelPath, child))
	source.download = func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
		assert.Equal(t, input.ChildModelPath, config.LocalDir)
		return nil
	}
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	info, err := os.Lstat(input.ChildModelPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	data, err := os.ReadFile(filepath.Join(child, "legacy-sentinel"))
	require.NoError(t, err)
	assert.Equal(t, "untouched", string(data))
	_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found, "old directories must not become canonical parents")
}

func TestDirectHfSourceLegacyMetadataPreservesChildren(t *testing.T) {
	s, task, input, _, _ := newTestDirectHfSource(t)
	entry := ModelEntry{Name: "model-1", Config: &ModelConfig{Artifact: Artifact{Sha: "old", ChildrenPaths: []string{"/old-child"}}}}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		cm.Data[input.ChildModelKey] = string(data)
		return true, nil
	}))
	artifact, err := s.directHfLegacyArtifact(context.Background(), task, input.ChildModelPath, testDirectHfSHA)
	require.NoError(t, err)
	assert.Equal(t, []string{"/old-child"}, artifact.ChildrenPaths)
	assert.Equal(t, map[string]string{input.ChildModelKey: input.ChildModelPath}, artifact.ParentPath)
}

func TestDirectHfSourceOrdinaryPathCompatibility(t *testing.T) {
	for _, pathKind := range []string{"nil", "empty", "relative", "space"} {
		t.Run(pathKind, func(t *testing.T) {
			s, task, _, source, _ := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			expected := s.modelRootDir + "/" + *task.BaseModel.Spec.Storage.StorageUri
			switch pathKind {
			case "nil":
				task.BaseModel.Spec.Storage.Path = nil
			case "empty":
				path := ""
				task.BaseModel.Spec.Storage.Path = &path
			case "relative":
				expected = "relative-model-path"
				task.BaseModel.Spec.Storage.Path = &expected
			case "space":
				expected = filepath.Join(s.modelRootDir, "model with space ")
				task.BaseModel.Spec.Storage.Path = &expected
			}
			source.download = func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
				assert.Equal(t, expected, config.LocalDir)
				return nil
			}
			_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
		})
	}
}

func TestDirectHfSourceLegacyDescendantsNeverSeeInPlaceReplacement(t *testing.T) {
	for _, scenario := range []string{"changed revision", "unresolved revision", "damaged same revision", "healthy same revision", "always download"} {
		t.Run(scenario, func(t *testing.T) {
			s, task, input, source, downloads := newTestDirectHfSource(t)
			require.NoError(t, os.MkdirAll(input.ChildModelPath, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "config.json"), []byte("{}"), 0o600))
			weights := "weights"
			if scenario == "damaged same revision" {
				weights = "corrupt"
			}
			require.NoError(t, os.WriteFile(filepath.Join(input.ChildModelPath, "model.safetensors"), []byte(weights), 0o600))
			child := filepath.Join(input.ModelStoreRoot, "legacy-descendant")
			require.NoError(t, os.Symlink(input.ChildModelPath, child))
			oldSHA := input.Parent.Identity.CommitSHA
			if scenario == "changed revision" {
				oldSHA = strings.Repeat("f", 40)
			}
			if scenario == "unresolved revision" {
				source.resolve = func(context.Context, string, string, string, string) (string, error) {
					return "", errors.New("unavailable")
				}
			}
			if scenario == "always download" {
				policy := v1beta1.AlwaysDownload
				task.BaseModel.Spec.Storage.DownloadPolicy = &policy
			}
			entry := ModelEntry{Name: "model-1", Config: &ModelConfig{Artifact: Artifact{Sha: oldSHA,
				ParentPath: map[string]string{input.ChildModelKey: input.ChildModelPath}, ChildrenPaths: []string{child}}}}
			require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
				return writeModelEntry(cm.Data, input.ChildModelKey, entry)
			}))
			_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			if scenario == "healthy same revision" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "legacy HF artifact has descendants")
			}
			assert.Zero(t, *downloads)
			data, err := os.ReadFile(filepath.Join(child, "model.safetensors"))
			require.NoError(t, err)
			assert.Equal(t, weights, string(data))
			_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
			require.NoError(t, err)
			assert.False(t, found)
		})
	}
}

func TestDirectHfSourceIneligibleReuseUsesOrdinaryDestination(t *testing.T) {
	for _, mode := range []string{"missing path", "empty path", "TensorRT serving base model"} {
		t.Run(mode, func(t *testing.T) {
			s, task, input, source, _ := newTestDirectHfSource(t)
			expectedPath := s.modelRootDir + "/" + *task.BaseModel.Spec.Storage.StorageUri
			switch mode {
			case "missing path":
				task.BaseModel.Spec.Storage.Path = nil
			case "empty path":
				task.BaseModel.Spec.Storage.Path = stringPtr("")
			case "TensorRT serving base model":
				task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: string(constants.ServingBaseModel)}
				expectedPath = input.ChildModelPath
			}
			before := task.BaseModel.DeepCopy()
			downloads := 0
			source.download = func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
				downloads++
				assert.Equal(t, expectedPath, config.LocalDir)
				return nil
			}
			source.manifest = func(context.Context, string, string, string, string) (hfSnapshotManifest, error) {
				t.Error("ordinary download must not validate a shared snapshot")
				return hfSnapshotManifest{}, errors.New("unexpected shared validation")
			}
			_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			assert.Equal(t, 1, downloads)
			assert.Equal(t, before.Spec, task.BaseModel.Spec, "adapter must not mutate the CR spec")
			_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
			require.NoError(t, err)
			assert.False(t, found)
		})
	}
}

func TestDirectHfSourcePreservesXetDownloadSettings(t *testing.T) {
	s, task, _, source, _ := newTestDirectHfSource(t)
	s.xetConfig.CacheDir = "/custom-cache"
	s.xetConfig.MaxConcurrentDownloads = 7
	s.xetConfig.EnableDedup = false
	s.xetConfig.EnableProgressReporting = false
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	source.download = func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
		assert.Equal(t, "/custom-cache", config.CacheDir)
		assert.Equal(t, 7, config.MaxWorkers)
		assert.Equal(t, xet.RepoTypeModel, config.RepoType)
		assert.Equal(t, s.xetConfig.Endpoint, config.Endpoint)
		assert.Equal(t, s.xetConfig.Token, config.Token)
		return nil
	}
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
}

func TestDirectHfSourceEffectiveCredentials(t *testing.T) {
	for _, credential := range []string{"fallback", "parameter", "secret"} {
		t.Run(credential, func(t *testing.T) {
			s, task, _, source, _ := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			expected := "fallback-token"
			if credential != "fallback" {
				parameters := map[string]string{"token": "parameter-token", "secretKey": "custom"}
				task.BaseModel.Spec.Storage.Parameters = &parameters
				expected = "parameter-token"
			}
			if credential == "secret" {
				key := "hf-token"
				task.BaseModel.Spec.Storage.StorageKey = &key
				s.kubeClient = fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key, Namespace: task.BaseModel.Namespace},
					Data: map[string][]byte{"custom": []byte("secret-token")}})
				expected = "secret-token"
			}
			source.resolve = func(_ context.Context, _, revision, token, endpoint string) (string, error) {
				assert.Equal(t, expected, token)
				assert.Equal(t, "refs/pr/7", revision)
				assert.Equal(t, s.xetConfig.Endpoint, endpoint)
				return testDirectHfSHA, nil
			}
			source.download = func(_ context.Context, _ *GopherTask, config *xet.DownloadConfig) error {
				assert.Equal(t, expected, config.Token)
				assert.Equal(t, s.xetConfig.Endpoint, config.Endpoint)
				assert.Equal(t, testDirectHfSHA, config.Revision)
				return nil
			}
			_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
		})
	}
}

func TestDirectHfSourceFailedVerificationNeverPublishesReady(t *testing.T) {
	s, task, input, source, _ := newTestDirectHfSource(t)
	download := source.download
	source.download = func(ctx context.Context, task *GopherTask, config *xet.DownloadConfig) error {
		require.NoError(t, download(ctx, task, config))
		return os.WriteFile(filepath.Join(config.LocalDir, "model.safetensors"), []byte("corrupt"), 0o600)
	}
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.ErrorContains(t, err, "failed content validation")
	parent, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, HfArtifactStatusFailed, parent.Status)
	assert.False(t, s.sharedHfArtifactHandler().files.ParentReadyMarkerExists(parent))
	assertChildPathMissing(t, input.ChildModelPath)
}

func TestDirectHfSourceManifestFailureDoesNotMutateSharedFiles(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	task.TaskType = DownloadOverride
	source.manifest = func(context.Context, string, string, string, string) (hfSnapshotManifest, error) {
		return hfSnapshotManifest{}, errors.New("manifest unavailable")
	}
	_, err = source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.ErrorContains(t, err, "manifest unavailable")
	assert.Equal(t, 1, *downloads)
	data, err := os.ReadFile(filepath.Join(input.ChildModelPath, "model.safetensors"))
	require.NoError(t, err)
	assert.Equal(t, "weights", string(data))
}

func TestDirectHfSourceCancellationNeverFallsBack(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		t.Run(fmt.Sprintf("late success=%t", lateSuccess), func(t *testing.T) {
			s, task, input, source, downloads := newTestDirectHfSource(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source.resolve = func(context.Context, string, string, string, string) (string, error) {
				cancel()
				if lateSuccess {
					return input.Parent.Identity.CommitSHA, nil
				}
				return "", context.Canceled
			}
			_, err := source.process(ctx, s, task, task.BaseModel.Spec, true)
			assert.ErrorIs(t, err, context.Canceled)
			assert.Zero(t, *downloads)
			assert.Empty(t, task.HfResolvedRevision)
		})
	}
}

func TestDirectHfSourceCanceledParentLookupDoesNotRequeue(t *testing.T) {
	for _, lookupErr := range []error{nil, errors.New("lookup failed")} {
		t.Run(fmt.Sprintf("lookup error=%v", lookupErr), func(t *testing.T) {
			s, task, input, source, downloads := newTestDirectHfSource(t)
			_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
			require.NoError(t, err)
			before, err := s.configMapReconciler.getConfigMap(context.Background())
			require.NoError(t, err)
			task.HfResolvedRevision = ""
			*downloads = 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source.resolve = func(context.Context, string, string, string, string) (string, error) {
				// Install only after the earlier pending-deletion lookup completes.
				s.configMapReconciler.kubeClient.(*fake.Clientset).PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					cancel()
					return lookupErr != nil, nil, lookupErr
				})
				return "", errors.New("revision resolution unavailable")
			}
			waiting, err := source.process(ctx, s, task, task.BaseModel.Spec, true)
			require.ErrorIs(t, err, context.Canceled)
			assert.False(t, waiting)
			assert.True(t, task.SamePathWaitStartedAt.IsZero(), "cancellation must not schedule a retry")
			assert.Zero(t, *downloads)
			after, err := s.configMapReconciler.kubeClient.(*fake.Clientset).Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), before.Namespace, before.Name)
			require.NoError(t, err)
			assert.Equal(t, before.Data, after.(*corev1.ConfigMap).Data)
			assertChildSymlinkTarget(t, input.ChildModelPath, input.Parent.LocalPath)
		})
	}
}

func TestDirectHfSourceDoesNotResolveOCI(t *testing.T) {
	s, task, _, source, downloads := newTestDirectHfSource(t)
	uri := "oci://n/namespace/b/models/o/Org/Model/" + testDirectHfSHA
	task.BaseModel.Spec.Storage.StorageUri = &uri
	source.resolve = func(context.Context, string, string, string, string) (string, error) {
		t.Fatal("OCI must not make HF API calls")
		return "", nil
	}
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.Error(t, err)
	assert.Zero(t, *downloads)
}

func TestDirectHfSourceRejectsSharedAncestorForOrdinaryWrites(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	task.BaseModel.Spec.Storage.DownloadPolicy = nil
	require.NoError(t, os.MkdirAll(input.Parent.LocalPath, 0o755))
	alias := filepath.Join(input.ModelStoreRoot, "alias")
	require.NoError(t, os.Symlink(input.Parent.LocalPath, alias))
	destination := filepath.Join(alias, "ordinary")
	task.BaseModel.Spec.Storage.Path = &destination
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.ErrorContains(t, err, "resolves inside a shared artifact")
	assert.Zero(t, *downloads)
}

func TestDirectHfSourceLegacySymlinkDoesNotAdoptItsParent(t *testing.T) {
	s, task, input, source, _ := newTestDirectHfSource(t)
	legacy := filepath.Join(input.ModelStoreRoot, "legacy-parent")
	require.NoError(t, os.MkdirAll(legacy, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "sentinel"), []byte("untouched"), 0o600))
	require.NoError(t, os.Symlink(legacy, input.ChildModelPath))
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	info, err := os.Lstat(input.ChildModelPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir(), "fallback downloads to the ordinary path")
	assert.FileExists(t, filepath.Join(legacy, "sentinel"))
	_, found, err := s.sharedHfArtifactHandler().repository.Get(context.Background(), input.Parent.Identity)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestDirectHfSourceAlwaysDownloadDoesNotUseCanonicalParent(t *testing.T) {
	s, task, input, source, _ := newTestDirectHfSource(t)
	policy := v1beta1.AlwaysDownload
	task.BaseModel.Spec.Storage.DownloadPolicy = &policy
	_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err)
	assert.DirExists(t, input.ChildModelPath)
	assert.NoDirExists(t, input.Parent.LocalPath)
}

func TestDownloadDirectHfSnapshotEscapesRevisionAtXetBoundary(t *testing.T) {
	config := &xet.DownloadConfig{Revision: "refs/pr/7#%"}
	err := downloadDirectHfSnapshotWithProgress(context.Background(), config,
		func(_ context.Context, config *xet.DownloadConfig, _ xet.ProgressHandler, _ time.Duration) (string, error) {
			assert.Equal(t, "refs%2Fpr%2F7%23%25", config.Revision)
			return "", nil
		}, func(context.Context, *DownloadProgress) {})
	require.NoError(t, err)
	assert.Equal(t, "refs/pr/7#%", config.Revision, "caller config remains unescaped")
}

func TestDownloadDirectHfSnapshotWaitsForFinalProgress(t *testing.T) {
	for _, downloadErr := range []error{nil, context.Canceled} {
		t.Run(fmt.Sprint(downloadErr), func(t *testing.T) {
			flushing, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				done <- downloadDirectHfSnapshotWithProgress(context.Background(), &xet.DownloadConfig{},
					func(_ context.Context, _ *xet.DownloadConfig, report xet.ProgressHandler, _ time.Duration) (string, error) {
						report(xet.ProgressUpdate{CompletedBytes: 10, TotalBytes: 10})
						return "", downloadErr
					}, func(ctx context.Context, progress *DownloadProgress) {
						assert.NoError(t, ctx.Err())
						assert.EqualValues(t, 10, progress.CompletedBytes)
						close(flushing)
						<-release
					})
			}()
			select {
			case <-flushing:
			case <-time.After(time.Second):
				t.Fatal("final flush never started")
			}
			select {
			case <-done:
				t.Fatal("download returned before final progress flush")
			default:
			}
			close(release)
			select {
			case err := <-done:
				assert.ErrorIs(t, err, downloadErr)
			case <-time.After(time.Second):
				t.Fatal("progress worker was not joined")
			}
		})
	}
}
