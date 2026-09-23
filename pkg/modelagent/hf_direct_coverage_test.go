package modelagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/modelparser"
)

func TestDirectHFManifestRejectsMalformedAndOversizedResponses(t *testing.T) {
	for _, scenario := range []string{"invalid endpoint", "invalid JSON", "oversized", "truncated response"} {
		t.Run(scenario, func(t *testing.T) {
			payload, err := json.Marshal(testHfSnapshotManifest(map[string]string{"config.json": "{}"}))
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch scenario {
				case "invalid JSON":
					_, _ = w.Write([]byte("{"))
				case "oversized":
					_, _ = w.Write(append(payload, []byte(strings.Repeat(" ", hfSnapshotMaxMetadataBytes))...))
				case "truncated response":
					w.Header().Set("Content-Length", fmt.Sprint(len(payload)+100))
					_, _ = w.Write(payload)
				default:
					t.Error("invalid endpoint must be rejected without requesting metadata")
				}
			}))
			defer server.Close()
			endpoint := server.URL
			if scenario == "invalid endpoint" {
				endpoint += "?token=secret"
			}
			_, err = fetchHfSnapshotManifest(context.Background(), "Org/Model", testDirectHfSHA, "", endpoint)
			require.Error(t, err)
			switch scenario {
			case "oversized":
				require.ErrorContains(t, err, "exceeds size limit")
			case "truncated response":
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			case "invalid JSON":
				require.ErrorContains(t, err, "decode HF snapshot manifest")
			case "invalid endpoint":
				require.ErrorContains(t, err, "invalid Hugging Face endpoint")
			}
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestDirectHFSnapshotFilesystemFailuresPreserveContents(t *testing.T) {
	for _, scenario := range []string{"file as root", "file as directory", "directory as file", "missing root", "size mismatch", "canceled reader"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			manifest := testHfSnapshotManifest(map[string]string{"nested/weights": "expected"})
			sentinel := filepath.Join(root, "sentinel")
			require.NoError(t, os.WriteFile(sentinel, []byte("preserve"), 0o600))
			directory := root
			switch scenario {
			case "file as root":
				directory = sentinel
			case "file as directory":
				require.NoError(t, os.WriteFile(filepath.Join(root, "nested"), []byte("preserve"), 0o600))
			case "directory as file":
				require.NoError(t, os.MkdirAll(filepath.Join(root, "nested", "weights"), 0o700))
			case "missing root":
				directory = filepath.Join(root, "missing")
			case "size mismatch":
				require.NoError(t, os.MkdirAll(filepath.Join(root, "nested"), 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(root, "nested", "weights"), []byte("short"), 0o600))
			case "canceled reader":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				reader := hfSnapshotContextReader{ctx: ctx, Reader: strings.NewReader("preserve")}
				n, err := reader.Read(make([]byte, 8))
				require.Zero(t, n)
				require.ErrorIs(t, err, context.Canceled)
				return
			}
			valid, err := manifest.validate(context.Background(), directory)
			require.False(t, valid)
			if scenario == "missing root" || scenario == "size mismatch" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Error(t, manifest.removeInvalidFiles(context.Background(), directory))
			}
			if scenario == "missing root" {
				require.Error(t, manifest.removeInvalidFiles(context.Background(), directory))
				require.NoDirExists(t, directory)
			}
			data, err := os.ReadFile(sentinel)
			require.NoError(t, err)
			require.Equal(t, "preserve", string(data))
		})
	}
}

func TestDirectHFManifestRejectsFileDirectoryCollision(t *testing.T) {
	manifest := testHfSnapshotManifest(map[string]string{"nested": "file", "nested/weights": "weights"})
	require.ErrorContains(t, manifest.check(testDirectHfSHA), "both file and directory")
	_, err := manifest.invalidFiles(context.Background(), t.TempDir())
	require.ErrorContains(t, err, "both file and directory")
}

func TestDirectHFLegacyMetadataErrorsPreventDownload(t *testing.T) {
	for _, scenario := range []string{"corrupt metadata", "read failure", "source failure"} {
		t.Run(scenario, func(t *testing.T) {
			s, task, input, source, downloads := newTestDirectHfSource(t)
			task.BaseModel.Spec.Storage.DownloadPolicy = nil
			failure := errors.New("injected failure")
			client := s.configMapReconciler.kubeClient.(*k8sfake.Clientset)
			switch scenario {
			case "corrupt metadata":
				require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
					cm.Data[input.ChildModelKey] = "{"
					return true, nil
				}))
				_, err := s.directHfLegacyArtifact(context.Background(), task, input.ChildModelPath, testDirectHfSHA)
				require.ErrorContains(t, err, "read legacy HF artifact")
			case "read failure":
				client.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, failure
				})
				_, err := s.directHfLegacyArtifact(context.Background(), task, input.ChildModelPath, testDirectHfSHA)
				require.ErrorContains(t, err, failure.Error())
			case "source failure":
				source.resolve = func(context.Context, string, string, string, string) (string, error) {
					return "not-an-immutable-revision", nil
				}
				_, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
				require.Error(t, err)
			}
			require.Zero(t, *downloads)
			require.NoDirExists(t, input.ChildModelPath)
		})
	}
}

func TestDirectHFLegacyLinkWithDescendantsCannotBeReplaced(t *testing.T) {
	s, task, input, _, _ := newTestDirectHfSource(t)
	realParent := t.TempDir()
	require.NoError(t, os.Symlink(realParent, input.ChildModelPath))
	require.NoError(t, s.configMapReconciler.mutateConfigMapWithRetry(context.Background(), func(cm *corev1.ConfigMap) (bool, error) {
		cm.Data[input.ChildModelKey] = fmt.Sprintf(`{"config":{"artifact":{"sha":%q,"childrenPaths":["/models/descendant"]}}}`, testDirectHfSHA)
		return true, nil
	}))
	_, err := s.directHfLegacyArtifact(context.Background(), task, input.ChildModelPath, testDirectHfSHA)
	require.ErrorContains(t, err, "refusing to replace its link")
	target, err := os.Readlink(input.ChildModelPath)
	require.NoError(t, err)
	require.Equal(t, realParent, target)
}

func TestDirectHFConfigParsingFailurePreservesVerifiedDownload(t *testing.T) {
	s, task, input, source, downloads := newTestDirectHfSource(t)
	s.modelConfigParser = modelparser.NewModelConfigParser(nil, s.logger)
	waiting, err := source.process(context.Background(), s, task, task.BaseModel.Spec, true)
	require.NoError(t, err, "the verified snapshot remains usable even when its config format is unsupported")
	require.False(t, waiting)
	require.Equal(t, 1, *downloads)
	require.FileExists(t, filepath.Join(input.Parent.LocalPath, "model.safetensors"))
	parent, found, err := s.sharedHfArtifactHandler().repository.GetParentForChild(context.Background(), input.ChildModelKey)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, HfArtifactStatusReady, parent.Status)
}
