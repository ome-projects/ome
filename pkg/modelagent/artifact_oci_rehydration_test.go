package modelagent

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

func TestOCIRehydrationConsumerGuard(t *testing.T) {
	for _, tc := range []struct {
		name               string
		request            bool
		consumer           bool
		invalid            bool
		verificationError  bool
		podError           bool
		noClient           bool
		noNode             bool
		cancelBefore       bool
		cancelDuringList   bool
		cancelDuringVerify bool
		wantReuse          bool
		wantError          bool
		wantVerify         bool
	}{
		{name: "ordinary download unchanged", consumer: true, invalid: true, noClient: true},
		{name: "invalid unused copy permits repair", request: true, invalid: true},
		{name: "healthy consumed copy skips download", request: true, consumer: true, wantReuse: true, wantVerify: true},
		{name: "invalid consumed copy blocks repair", request: true, consumer: true, invalid: true, wantError: true, wantVerify: true},
		{name: "verification error blocks repair", request: true, consumer: true, verificationError: true, wantError: true, wantVerify: true},
		{name: "pod lookup error blocks repair", request: true, podError: true, wantError: true},
		{name: "missing pod client blocks repair", request: true, noClient: true, wantError: true},
		{name: "missing node identity blocks repair", request: true, noNode: true, wantError: true},
		{name: "canceled before lookup", request: true, cancelBefore: true, wantError: true},
		{name: "canceled during lookup", request: true, consumer: true, cancelDuringList: true, wantError: true},
		{name: "canceled verification cannot acknowledge", request: true, consumer: true, cancelDuringVerify: true, wantError: true, wantVerify: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dir := t.TempDir()
			path := filepath.Join(dir, "weights")
			contents := []byte("healthy")
			if tc.invalid {
				contents = []byte("damaged")
			}
			require.NoError(t, os.WriteFile(path, contents, 0600))
			client := k8sfake.NewSimpleClientset()
			if tc.consumer {
				_, err := client.CoreV1().Pods("serving").Create(ctx, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "consumer"},
					Spec: corev1.PodSpec{NodeName: "node1", Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{Path: dir},
					}}}},
				}, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			client.ClearActions()
			client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				if tc.cancelDuringList {
					cancel()
				}
				if tc.podError {
					return true, nil, errors.New("pod lookup unavailable")
				}
				return false, nil, nil
			})
			g := &Gopher{kubeClient: client, configMapReconciler: &ConfigMapReconciler{nodeName: "node1"}}
			if tc.noClient {
				g.kubeClient = nil
			}
			if tc.noNode {
				g.configMapReconciler = nil
			}
			task := &GopherTask{TaskType: Download, BaseModel: &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{
				Name: "model", Namespace: "default", UID: "model-uid", Annotations: map[string]string{},
			}}}
			if tc.request {
				task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-1"
			}
			if tc.cancelBefore {
				cancel()
			}
			verified := false
			reuse, err := g.reuseConsumedOCIArtifact(ctx, task, dir, func() map[string]error {
				verified = true
				if tc.cancelDuringVerify {
					cancel()
					return nil // The real verifier can return an empty map on cancellation.
				}
				if tc.verificationError {
					return map[string]error{path: errors.New("object metadata unavailable")}
				}
				data, readErr := os.ReadFile(path)
				if readErr != nil {
					return map[string]error{path: readErr}
				}
				if string(data) != "healthy" {
					return map[string]error{path: errors.New("checksum mismatch")}
				}
				return nil
			})
			require.Equal(t, tc.wantReuse, reuse)
			require.Equal(t, tc.wantVerify, verified)
			if tc.wantError {
				require.Error(t, err)
				if tc.cancelBefore || tc.cancelDuringList || tc.cancelDuringVerify {
					require.ErrorIs(t, err, context.Canceled)
				}
			} else {
				require.NoError(t, err)
			}
			if tc.cancelBefore || !tc.request {
				require.Empty(t, client.Actions())
			}
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, contents, data, "the guard must never mutate consumed bytes")
		})
	}
}

type ociRehydrationVerificationDispatcher struct {
	requests int
}

func (d *ociRehydrationVerificationDispatcher) Do(req *http.Request) (*http.Response, error) {
	d.requests++
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{
		"Content-Length": {"4"}, "Content-Md5": {"jXd/OF09/siBXSD3SWAm3A=="},
	}, Body: http.NoBody, Request: req}, nil
}

func TestOCIRehydrationConsumerGuardVerifiesEverySelectedObject(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "second object missing"}[missing], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "H100"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "H100", "config"), []byte("data"), 0600))
			if !missing {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "H100", "weights"), []byte("data"), 0600))
			}
			client := k8sfake.NewSimpleClientset(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "serving"},
				Spec: corev1.PodSpec{NodeName: "node1", Volumes: []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: dir},
				}}}},
			})
			g := &Gopher{kubeClient: client, configMapReconciler: &ConfigMapReconciler{nodeName: "node1"}, metrics: NewMetrics(prometheus.NewRegistry())}
			task := newOCIInputTestTask()
			task.BaseModel.Annotations[constants.ModelArtifactRehydrationIDAnnotation] = "restore-1"
			task.TensorRTLLMShapeFilter = &TensorRTLLMShapeFilter{IsTensorrtLLMModel: true, ModelType: string(constants.ServingBaseModel), ShapeAlias: "H100"}
			objects := filterObjectStorageObjectsForTask([]objectstorage.ObjectSummary{
				{Name: stringPtr("prefix/H100/config")}, {Name: stringPtr("prefix/H100/weights")}, {Name: stringPtr("prefix/A100/weights")},
			}, task)
			require.Len(t, objects, 2)
			var uris []ociobjectstore.ObjectURI
			for _, object := range objects {
				uris = append(uris, ociobjectstore.ObjectURI{Namespace: "ns", BucketName: "models", Prefix: "prefix/", ObjectName: *object.Name})
			}
			dispatcher := &ociRehydrationVerificationDispatcher{}
			store := &ociobjectstore.OCIOSDataStore{Client: &objectstorage.ObjectStorageClient{BaseClient: common.BaseClient{
				HTTPClient: dispatcher, Signer: verificationTestSigner{}, Host: "https://objectstorage.test", UserAgent: "test",
			}}}
			reuse, err := g.reuseConsumedOCIArtifact(ctx, task, dir, func() map[string]error {
				return g.verifyDownloadedFiles(ctx, store, uris, dir, task)
			})
			require.Equal(t, !missing, reuse)
			if missing {
				require.ErrorContains(t, err, "cannot repair OCI artifact")
				require.Equal(t, 1, dispatcher.requests)
				require.NoFileExists(t, filepath.Join(dir, "H100", "weights"))
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, dispatcher.requests)
			}
			require.NoFileExists(t, filepath.Join(dir, "A100", "weights"))
			data, err := os.ReadFile(filepath.Join(dir, "H100", "config"))
			require.NoError(t, err)
			require.Equal(t, "data", string(data))
		})
	}
}
