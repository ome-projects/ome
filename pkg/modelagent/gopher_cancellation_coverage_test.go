package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/modelparser"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/xet"
)

func newCancellationCoverageGopher(t *testing.T, uri string) (*Gopher, *GopherTask) {
	t.Helper()
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", UID: "current-uid"},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			StorageUri: &uri, Path: stringPtr(t.TempDir()),
		}},
	}
	g := newGopherForProcessTask(makeConfigMap("node-1", map[string]string{}), map[string]string{"kubernetes.io/hostname": "node-1"})
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.downloadRetry = 1
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{model}}
	g.clusterBaseModelLister = &mockClusterBaseModelLister{}
	return g, &GopherTask{TaskType: Download, BaseModel: model}
}

func assertCancellationCoverageStatus(t *testing.T, g *Gopher, task *GopherTask, status ModelStatus) {
	t.Helper()
	model := task.BaseModel
	key := constants.GetModelConfigMapKey(model.Namespace, model.Name, false)
	exists, data, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), key)
	require.NoError(t, err)
	require.True(t, exists)
	var entry ModelEntry
	require.NoError(t, json.Unmarshal([]byte(data), &entry))
	assert.Equal(t, status, entry.Status)
	node, err := g.nodeLabelReconciler.kubeClient.CoreV1().Nodes().Get(context.Background(), g.nodeLabelReconciler.nodeName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, string(status), node.Labels[constants.GetBaseModelLabel(model.Namespace, model.Name)])
}

func TestCancellationCoverageOrdinaryOCIFailureStillPublishesFailed(t *testing.T) {
	g, task := newCancellationCoverageGopher(t, "oci://n/ns/b/bucket/o/model")
	task.TaskType = DownloadOverride
	// Unsupported authentication fails client construction before credentials
	// or network access, exercising a real non-cancellation download failure.
	task.BaseModel.Spec.Storage.Parameters = &map[string]string{"auth": "unsupported-test-auth"}
	err := g.processTask(task)
	require.ErrorContains(t, err, "failed to create object storage client")
	assert.NotErrorIs(t, err, context.Canceled)
	assertCancellationCoverageStatus(t, g, task, ModelStatusFailed)
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)
	assert.Equal(t, float64(1), testutil.ToFloat64(g.metrics.modelDownloadsFailedTotal.WithLabelValues(modelType, namespace, name)))
	assert.Zero(t, testutil.ToFloat64(g.metrics.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name)))
}

func TestCancellationCoverageSharedOCIFailureStillPublishesFailed(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.downloadRetry = 1
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
	client := g.configMapReconciler.kubeClient
	_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, client, 1, g.logger)
	task.BaseModel.Spec.Storage.Parameters = &map[string]string{"auth": "unsupported-test-auth"}

	err = g.processTask(task)
	require.ErrorContains(t, err, "failed to create object storage client")
	assert.NotErrorIs(t, err, context.Canceled)
	assertCancellationCoverageStatus(t, g, task, ModelStatusFailed)
	assertChildPathMissing(t, input.ChildModelPath)
}

func TestCancellationCoverageSourceURIFailureHonorsContext(t *testing.T) {
	for _, tc := range []struct {
		name     string
		uri      string
		canceled bool
	}{
		{name: "active local task records failure", uri: "local://"},
		{name: "canceled local task cannot restore status", uri: "local://", canceled: true},
		{name: "active HF task records failure", uri: "hf://"},
		{name: "canceled HF task cannot restore status", uri: "hf://", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, tc.uri)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			var err error
			if tc.uri == "hf://" {
				err = g.processHuggingFaceModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
			} else {
				err = g.processLocalStorageModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
			}
			failed := testutil.ToFloat64(g.metrics.modelDownloadsFailedTotal.WithLabelValues("BaseModel", "default", "model"))
			if !tc.canceled {
				require.ErrorContains(t, err, "missing")
				assert.Equal(t, float64(1), failed)
				assertCancellationCoverageStatus(t, g, task, ModelStatusFailed)
				return
			}
			require.ErrorIs(t, err, context.Canceled)
			assert.Zero(t, failed)
			exists, _, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), constants.GetModelConfigMapKey("default", "model", false))
			require.NoError(t, err)
			assert.False(t, exists)
			node, err := g.nodeLabelReconciler.kubeClient.CoreV1().Nodes().Get(context.Background(), "node-1", metav1.GetOptions{})
			require.NoError(t, err)
			assert.NotContains(t, node.Labels, constants.GetBaseModelLabel("default", "model"))
		})
	}
}

func TestCancellationCoverageReadyAfterOptionalConfigParseError(t *testing.T) {
	for _, storageKind := range []string{"local", "ordinary OCI", "shared OCI"} {
		t.Run(storageKind, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "local:///model")
			modelPath := *task.BaseModel.Spec.Storage.Path
			switch storageKind {
			case "ordinary OCI":
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
				existing := task.BaseModel.DeepCopy()
				existing.Name = "already-downloaded"
				existing.UID = "existing-uid"
				g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, existing}}
				require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: existing, ModelStateOnNode: Ready}))
			case "shared OCI":
				var input hfArtifactTaskInput
				g, task, input = newTestHfArtifactGopher(t)
				g.metrics = NewMetrics(prometheus.NewRegistry())
				g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
				client := g.configMapReconciler.kubeClient
				_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName},
				}}, metav1.CreateOptions{})
				require.NoError(t, err)
				g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, client, 1, g.logger)
				require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
				modelPath = input.ChildModelPath
			}
			// Parsing is optional for these existing artifacts. A malformed config
			// must not prevent an active task from publishing its final Ready state.
			require.NoError(t, os.WriteFile(filepath.Join(modelPath, "config.json"), []byte("{invalid-json"), 0600))
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			require.NoError(t, g.processTask(task))
			assertCancellationCoverageStatus(t, g, task, ModelStatusReady)
			modelType, namespace, name := GetModelTypeNamespaceAndName(task)
			assert.Equal(t, float64(1), testutil.ToFloat64(g.metrics.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name)))
			assert.Zero(t, testutil.ToFloat64(g.metrics.modelDownloadsFailedTotal.WithLabelValues(modelType, namespace, name)))
		})
	}
}

func TestCancellationCoverageDuringFinalFileVerification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		canceled bool
	}{
		{name: "completed verification records success"},
		{name: "cancellation on final file records no result", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "oci://n/ns/b/bucket/o/model")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dispatcher := &cancelVerificationDispatcher{cancel: func() {}}
			if tc.canceled {
				dispatcher.cancel = cancel
			}
			store := &ociobjectstore.OCIOSDataStore{Client: &objectstorage.ObjectStorageClient{BaseClient: common.BaseClient{
				HTTPClient: dispatcher, Signer: verificationTestSigner{}, Host: "https://objectstorage.test", UserAgent: "test",
			}}}
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "last"), []byte("data"), 0600))
			verificationErrors := g.verifyDownloadedFiles(ctx, store, []ociobjectstore.ObjectURI{{Namespace: "ns", BucketName: "bucket", ObjectName: "last"}}, dir, task)
			assert.Equal(t, 1, dispatcher.calls)
			modelType, namespace, name := GetModelTypeNamespaceAndName(task)
			wantSuccess := float64(1)
			if tc.canceled {
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				wantSuccess = 0
			} else {
				require.NoError(t, ctx.Err())
				assert.Empty(t, verificationErrors)
			}
			assert.Equal(t, wantSuccess, testutil.ToFloat64(g.metrics.modelVerificationsTotal.WithLabelValues(modelType, namespace, name, "success")))
			assert.Zero(t, testutil.ToFloat64(g.metrics.modelVerificationsTotal.WithLabelValues(modelType, namespace, name, "failure")))
			assert.Zero(t, testutil.ToFloat64(g.metrics.mdChecksumsFailedTotal.WithLabelValues(modelType, namespace, name)))
		})
	}
}

func assertNoDownloadOutcome(t *testing.T, g *Gopher, task *GopherTask) {
	t.Helper()
	modelType, namespace, name := GetModelTypeNamespaceAndName(task)
	assert.Zero(t, testutil.ToFloat64(g.metrics.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name)))
	assert.Zero(t, testutil.ToFloat64(g.metrics.modelDownloadsFailedTotal.WithLabelValues(modelType, namespace, name)))
	var metric dto.Metric
	histogram := g.metrics.modelDownloadDuration.WithLabelValues(modelType, namespace, name).(prometheus.Metric)
	require.NoError(t, histogram.Write(&metric))
	assert.Zero(t, metric.GetHistogram().GetSampleCount())
}

func TestCanceledUpdatingStopsNonOCITasks(t *testing.T) {
	for _, taskType := range []GopherTaskType{Download, DownloadOverride} {
		for _, uri := range []string{"pvc://claim", "vendor://model", "local:///model", "unknown://model"} {
			t.Run(string(taskType)+"/"+uri, func(t *testing.T) {
				g, task := newCancellationCoverageGopher(t, uri)
				task.TaskType = taskType
				g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
				client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
				client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					g.taskTracker.cancelLegacyDownload(gopherTaskModelKey(task))
					return false, nil, nil
				})
				require.ErrorIs(t, g.processTask(task), context.Canceled)
				assertNoDownloadOutcome(t, g, task)
			})
		}
	}
}

// Install a local snapshot stub so cancellation and pending progress can be
// exercised without a network request or Rust transfer.
func stubHfSnapshot(t *testing.T, download func(context.Context, *xet.DownloadConfig, xet.ProgressHandler, time.Duration) (string, error)) {
	t.Helper()
	oldFetch, oldDownload := fetchAttributeFromHfModelMetaData, snapshotDownloadWithProgress
	fetchAttributeFromHfModelMetaData = func(context.Context, string, string) (interface{}, error) { return "sha", nil }
	snapshotDownloadWithProgress = download
	t.Cleanup(func() {
		fetchAttributeFromHfModelMetaData, snapshotDownloadWithProgress = oldFetch, oldDownload
	})
}

// Fake client reactors do not expose request contexts, so intercept Secret.Get
// at the typed client boundary to verify cancellation reaches the request.
type secretGetClient struct {
	kubernetes.Interface
	get func(context.Context, string, string) (*corev1.Secret, error)
}

func (c secretGetClient) CoreV1() typedcorev1.CoreV1Interface {
	return secretGetCore{c.Interface.CoreV1(), c.get}
}

type secretGetCore struct {
	typedcorev1.CoreV1Interface
	get func(context.Context, string, string) (*corev1.Secret, error)
}

func (c secretGetCore) Secrets(namespace string) typedcorev1.SecretInterface {
	return secretGetter{c.CoreV1Interface.Secrets(namespace), namespace, c.get}
}

type secretGetter struct {
	typedcorev1.SecretInterface
	namespace string
	get       func(context.Context, string, string) (*corev1.Secret, error)
}

func (c secretGetter) Get(ctx context.Context, name string, _ metav1.GetOptions) (*corev1.Secret, error) {
	return c.get(ctx, c.namespace, name)
}

func TestHfSecretLookupHonorsTaskCancellation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		canceled       bool
		secretReturned bool
		wantToken      string
	}{
		{name: "canceled request", canceled: true},
		{name: "canceled late response", canceled: true, secretReturned: true},
		{name: "active lookup failure uses parameter token", wantToken: "parameter-token"},
		{name: "active lookup success uses secret token", secretReturned: true, wantToken: "secret-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "hf://org/model")
			g.xetConfig = &xet.Config{}
			task.BaseModel.Spec.Storage.StorageKey = stringPtr("hf-token")
			task.BaseModel.Spec.Storage.Parameters = &map[string]string{"token": "parameter-token"}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lookupCalled := false
			g.kubeClient = secretGetClient{g.configMapReconciler.kubeClient, func(requestCtx context.Context, namespace, name string) (*corev1.Secret, error) {
				lookupCalled = true
				assert.Equal(t, ctx.Done(), requestCtx.Done(), "Secret.Get must receive the task cancellation signal")
				assert.Equal(t, "default", namespace)
				assert.Equal(t, "hf-token", name)
				if tc.canceled {
					cancel()
				}
				if tc.secretReturned {
					return &corev1.Secret{Data: map[string][]byte{"token": []byte("secret-token")}}, nil
				}
				if tc.canceled {
					select {
					case <-requestCtx.Done():
						return nil, requestCtx.Err()
					case <-time.After(time.Second):
						t.Error("Secret.Get did not observe task cancellation")
					}
				}
				return nil, errors.New("secret lookup failed")
			}}
			downloadCalled := false
			downloadErr := errors.New("stop at snapshot download")
			stubHfSnapshot(t, func(_ context.Context, config *xet.DownloadConfig, _ xet.ProgressHandler, _ time.Duration) (string, error) {
				downloadCalled = true
				if !tc.canceled {
					assert.Equal(t, tc.wantToken, config.Token)
				}
				return "", downloadErr
			})

			err := g.processHuggingFaceModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
			assert.True(t, lookupCalled)
			if tc.canceled {
				require.ErrorIs(t, err, context.Canceled)
				assert.False(t, downloadCalled, "canceled token lookup must not start a download")
				assertNoDownloadOutcome(t, g, task)
			} else {
				require.ErrorIs(t, err, downloadErr)
				assert.True(t, downloadCalled)
			}
		})
	}
}

func TestHfSnapshotCancellationSkipsFailureMetrics(t *testing.T) {
	for _, tc := range []struct {
		name     string
		canceled bool
		err      error
	}{
		{name: "active failure", err: errors.New("snapshot failed")},
		{name: "active rate limit", err: errors.New("429 rate limit")},
		{name: "canceled failure", canceled: true, err: context.Canceled},
		{name: "canceled late success", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "hf://org/model")
			g.xetConfig = &xet.Config{}
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stubHfSnapshot(t, func(_ context.Context, config *xet.DownloadConfig, progress xet.ProgressHandler, _ time.Duration) (string, error) {
				progress(xet.ProgressUpdate{TotalBytes: 100, CompletedBytes: 50})
				if tc.canceled {
					cancel()
				}
				return config.LocalDir, tc.err
			})
			err := g.processHuggingFaceModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
			if tc.canceled {
				require.ErrorIs(t, err, context.Canceled)
				assertNoDownloadOutcome(t, g, task)
			} else {
				require.ErrorIs(t, err, tc.err)
				assertCancellationCoverageStatus(t, g, task, ModelStatusFailed)
				exists, raw, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), constants.GetModelConfigMapKey("default", "model", false))
				require.NoError(t, err)
				require.True(t, exists)
				var entry ModelEntry
				require.NoError(t, json.Unmarshal([]byte(raw), &entry))
				assert.Nil(t, entry.Progress, "the final worker flush must not restore progress after Failed")
				assert.Equal(t, float64(1), testutil.ToFloat64(g.metrics.modelDownloadsFailedTotal.WithLabelValues("BaseModel", "default", "model")))
			}
		})
	}
}

func TestHfFinalProgressFlushHonorsTaskCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "active progress is published"
		if canceled {
			name = "canceled progress cannot restore entry"
		}
		t.Run(name, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "hf://org/model")
			g.xetConfig = &xet.Config{}
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
			stubHfSnapshot(t, func(_ context.Context, config *xet.DownloadConfig, progress xet.ProgressHandler, _ time.Duration) (string, error) {
				progress(xet.ProgressUpdate{TotalBytes: 100, CompletedBytes: 50})
				if canceled {
					cancel()
					// Affinity cleanup keeps the CR UID valid, so UID fencing alone
					// cannot stop a late progress writer from restoring the entry.
					require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted}))
					client.ClearActions()
					return "", context.Canceled
				}
				return config.LocalDir, nil
			})
			err := g.processHuggingFaceModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
				for _, action := range client.Actions() {
					assert.NotEqual(t, "update", action.GetVerb(), "late progress must not update ConfigMap")
					assert.NotEqual(t, "create", action.GetVerb(), "late progress must not create ConfigMap")
				}
			} else {
				require.NoError(t, err)
			}
			exists, raw, err := g.configMapReconciler.getDataEntryBasedOnModelKey(context.Background(), constants.GetModelConfigMapKey("default", "model", false))
			require.NoError(t, err)
			if canceled {
				assert.False(t, exists)
			} else {
				require.True(t, exists)
				var entry ModelEntry
				require.NoError(t, json.Unmarshal([]byte(raw), &entry))
				require.NotNil(t, entry.Progress)
				assert.EqualValues(t, 50, entry.Progress.CompletedBytes)
			}
		})
	}
}

// Block outside the fake client's own mutex so it cannot accidentally serialize
// progress with cleanup and hide a missing Gopher lock.
type progressBlockingClient struct {
	kubernetes.Interface
	beforeUpdate func(*corev1.ConfigMap)
}

func (c progressBlockingClient) CoreV1() typedcorev1.CoreV1Interface {
	return progressBlockingCore{c.Interface.CoreV1(), c.beforeUpdate}
}

type progressBlockingCore struct {
	typedcorev1.CoreV1Interface
	beforeUpdate func(*corev1.ConfigMap)
}

func (c progressBlockingCore) ConfigMaps(namespace string) typedcorev1.ConfigMapInterface {
	return progressBlockingConfigMaps{c.CoreV1Interface.ConfigMaps(namespace), c.beforeUpdate}
}

type progressBlockingConfigMaps struct {
	typedcorev1.ConfigMapInterface
	beforeUpdate func(*corev1.ConfigMap)
}

func (c progressBlockingConfigMaps) Update(ctx context.Context, cm *corev1.ConfigMap, opts metav1.UpdateOptions) (*corev1.ConfigMap, error) {
	c.beforeUpdate(cm)
	return c.ConfigMapInterface.Update(ctx, cm, opts)
}

// The progress timeout asks the task context for its deadline just before
// acquiring configMapMutex. This signals the flush without a scheduling sleep.
type progressFlushContext struct {
	context.Context
	started chan struct{}
	once    sync.Once
}

func (c *progressFlushContext) Deadline() (time.Time, bool) {
	c.once.Do(func() { close(c.started) })
	return c.Context.Deadline()
}

func waitForProgressEvent(t *testing.T, event <-chan struct{}) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for progress/cleanup synchronization")
	}
}

func TestHfProgressFlushSerializesWithAffinityCleanup(t *testing.T) {
	for _, cleanupFirst := range []bool{false, true} {
		name := "flush before cleanup"
		if cleanupFirst {
			name = "cleanup before flush"
		}
		t.Run(name, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "hf://org/model")
			g.xetConfig = &xet.Config{}
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Updating}))
			baseCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &progressFlushContext{Context: baseCtx, started: make(chan struct{})}
			key := constants.GetModelConfigMapKey("default", "model", false)
			updateStarted, releaseUpdate := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseUpdate) }) }
			t.Cleanup(release)
			client := g.configMapReconciler.kubeClient
			g.configMapReconciler.kubeClient = progressBlockingClient{client, func(cm *corev1.ConfigMap) {
				var entry ModelEntry
				if json.Unmarshal([]byte(cm.Data[key]), &entry) == nil && entry.Progress != nil {
					close(updateStarted)
					select {
					case <-releaseUpdate:
					case <-time.After(5 * time.Second):
						t.Error("timed out waiting to release progress update")
					}
				}
			}}
			stubHfSnapshot(t, func(_ context.Context, config *xet.DownloadConfig, progress xet.ProgressHandler, _ time.Duration) (string, error) {
				progress(xet.ProgressUpdate{TotalBytes: 100, CompletedBytes: 50})
				if cleanupFirst {
					g.configMapMutex.Lock()
					cancel()
					return "", context.Canceled
				}
				return config.LocalDir, nil
			})
			done := make(chan error, 1)
			go func() {
				done <- g.processHuggingFaceModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
			}()
			if cleanupFirst {
				waitForProgressEvent(t, ctx.started)
				// The snapshot stub holds the same lock as affinity cleanup while
				// the canceled flush is attempting to acquire it.
				err := g.configMapReconciler.DeleteModelFromConfigMap(context.Background(), task.BaseModel, nil)
				g.configMapMutex.Unlock()
				require.NoError(t, err)
			} else {
				waitForProgressEvent(t, updateStarted)
				locked := g.configMapMutex.TryLock()
				if locked {
					g.configMapMutex.Unlock()
				}
				assert.False(t, locked, "progress must hold the cleanup lock across its API update")
				cancel()
				cleanupStarted, cleanupDone := make(chan struct{}), make(chan error, 1)
				go func() {
					close(cleanupStarted)
					cleanupDone <- g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: task.BaseModel, ModelStateOnNode: Deleted})
				}()
				waitForProgressEvent(t, cleanupStarted)
				release()
				select {
				case err := <-cleanupDone:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("affinity cleanup did not finish")
				}
			}
			select {
			case err := <-done:
				if cleanupFirst {
					require.ErrorIs(t, err, context.Canceled)
				} else if err != nil {
					require.ErrorIs(t, err, context.Canceled)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("progress worker did not exit")
			}
			cm, err := client.CoreV1().ConfigMaps(g.configMapReconciler.namespace).Get(context.Background(), g.configMapReconciler.nodeName, metav1.GetOptions{})
			require.NoError(t, err)
			assert.NotContains(t, cm.Data, key)
			if cleanupFirst {
				select {
				case <-updateStarted:
					t.Fatal("canceled flush tried to update ConfigMap after cleanup")
				default:
				}
			}
		})
	}
}

func TestCanceledConfigParsingIsNotOptional(t *testing.T) {
	for _, source := range []string{"local", "HF", "shared OCI", "ordinary OCI"} {
		t.Run(source, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "local:///model")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var run func() error
			switch source {
			case "local":
				run = func() error {
					return g.processLocalStorageModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
				}
			case "HF":
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("hf://org/model")
				g.xetConfig = &xet.Config{}
				stubHfSnapshot(t, func(_ context.Context, config *xet.DownloadConfig, _ xet.ProgressHandler, _ time.Duration) (string, error) {
					return config.LocalDir, nil
				})
				run = func() error {
					return g.processHuggingFaceModel(ctx, task, task.BaseModel.Spec, "model", "BaseModel", "default", "model")
				}
			case "ordinary OCI":
				task.BaseModel.Spec.Storage.StorageUri = stringPtr("oci://n/ns/b/bucket/o/model")
				existing := task.BaseModel.DeepCopy()
				existing.Name, existing.UID = "existing", "existing-uid"
				g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel, existing}}
				require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{BaseModel: existing, ModelStateOnNode: Ready}))
				cancel = func() { g.taskTracker.cancelLegacyDownload(gopherTaskModelKey(task)) }
				run = func() error { return g.processTask(task) }
			case "shared OCI":
				var input hfArtifactTaskInput
				g, task, input = newTestHfArtifactGopher(t)
				g.metrics = NewMetrics(prometheus.NewRegistry())
				require.NoError(t, runTestHfArtifactDownload(g.sharedHfArtifactHandler(), input))
				// A fresh sibling forces the final reference write. Cancel in that
				// write's response, immediately before the optional parse call.
				input.ChildModelKey = "default.basemodel.model-2"
				input.ChildModelUID = "uid-2"
				input.ChildModelPath = filepath.Join(input.ModelStoreRoot, "model-2")
				seedTestChildModelEntry(t, g.sharedHfArtifactHandler().repository, input)
				task.BaseModel.Name, task.BaseModel.UID = "model-2", input.ChildModelUID
				task.BaseModel.Spec.Storage.Path = &input.ChildModelPath
				run = func() error {
					_, _, err := g.processHfOCIArtifact(ctx, task, task.BaseModel.Spec, true)
					return err
				}
			}
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			core, _ := observer.New(zap.DebugLevel)
			entered := false
			// Direct callers log immediately before parsing; shared OCI reaches
			// the parser after its final child reference update.
			if source == "shared OCI" {
				client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
				client.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
					if !entered {
						entered = true
						cancel()
					}
					return false, nil, nil
				})
			} else {
				g.logger = zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
					if strings.Contains(entry.Message, "for config parsing") {
						entered = true
						g.configMapMutex.Lock()
						go func() { cancel(); g.configMapMutex.Unlock() }()
					}
					return nil
				})).Sugar()
			}
			require.ErrorIs(t, run(), context.Canceled)
			assert.True(t, entered, "test must reach config parsing")
			assertNoDownloadOutcome(t, g, task)
		})
	}
}

func TestDownloadSuccessRequiresReadyPublication(t *testing.T) {
	for _, mode := range []string{"canceled", "publication failed", "published"} {
		t.Run(mode, func(t *testing.T) {
			g, task := newCancellationCoverageGopher(t, "local:///model")
			g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
			core, logs := observer.New(zap.DebugLevel)
			g.logger = zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
				if strings.HasPrefix(entry.Message, "Successfully processed local model") && mode == "canceled" {
					g.taskTracker.cancelLegacyDownload(gopherTaskModelKey(task))
				}
				return nil
			})).Sugar()
			if mode == "publication failed" {
				patches := 0
				g.configMapReconciler.kubeClient.(*k8sfake.Clientset).PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					patches++
					if patches > 1 {
						return true, nil, errors.New("Ready publication failed")
					}
					return false, nil, nil
				})
			}
			err := g.processTask(task)
			if mode == "published" {
				require.NoError(t, err)
				assertCancellationCoverageStatus(t, g, task, ModelStatusReady)
				modelType, namespace, name := GetModelTypeNamespaceAndName(task)
				assert.Equal(t, float64(1), testutil.ToFloat64(g.metrics.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name)))
			} else {
				if mode == "canceled" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorContains(t, err, "Ready publication failed")
				}
				assertNoDownloadOutcome(t, g, task)
				assert.Zero(t, logs.FilterMessageSnippet("Successfully downloaded BaseModel").Len())
			}
		})
	}
}

func TestSharedReadyRetryDoesNotRecordSuccess(t *testing.T) {
	g, task, input := newTestHfArtifactGopher(t)
	g.metrics = NewMetrics(prometheus.NewRegistry())
	g.baseModelLister = &mockBaseModelLister{models: []*v1beta1.BaseModel{task.BaseModel}}
	handler := g.sharedHfArtifactHandler()
	require.NoError(t, runTestHfArtifactDownload(handler, input))
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	_, err := client.CoreV1().Nodes().Create(context.Background(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: g.configMapReconciler.nodeName, Labels: map[string]string{"kubernetes.io/hostname": g.configMapReconciler.nodeName},
	}}, metav1.CreateOptions{})
	require.NoError(t, err)
	g.nodeLabelReconciler = NewNodeLabelReconciler(g.configMapReconciler.nodeName, client, 1, g.logger)
	g.modelConfigParser = modelparser.NewModelConfigParser(nil, g.logger)
	g.samePathWaitDelay = time.Millisecond
	core, _ := observer.New(zap.DebugLevel)
	var unlock func()
	var once sync.Once
	g.logger = zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
		if strings.HasPrefix(entry.Message, "Failed to parse shared artifact model configuration") {
			once.Do(func() {
				var acquired bool
				unlock, acquired = handler.tryParentOperation(input.Parent.Key)
				require.True(t, acquired)
			})
		}
		return nil
	})).Sugar()
	require.NoError(t, g.processTask(task))
	require.NotNil(t, unlock, "sibling must acquire the parent before Ready publication")
	assertNoDownloadOutcome(t, g, task)
	unlock()
	select {
	case retry := <-g.gopherChan:
		require.NoError(t, g.processTask(retry))
		modelType, namespace, name := GetModelTypeNamespaceAndName(task)
		assert.Equal(t, float64(1), testutil.ToFloat64(g.metrics.modelDownloadsSuccessTotal.WithLabelValues(modelType, namespace, name)))
	case <-time.After(time.Second):
		t.Fatal("Ready retry not queued")
	}
}

func TestChildStatusWithoutModelIsNoOp(t *testing.T) {
	g, _ := newCancellationCoverageGopher(t, "local:///model")
	client := g.configMapReconciler.kubeClient.(*k8sfake.Clientset)
	for _, state := range []ModelStateOnNode{Updating, Ready, Failed, Deleted} {
		require.NotPanics(t, func() {
			require.NoError(t, g.safeNodeLabelReconciliation(context.Background(), &NodeLabelOp{ModelStateOnNode: state}))
		})
	}
	assert.Empty(t, client.Actions())
}
