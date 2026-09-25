package cli

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// The traffic target must not shadow kubectl's API-cluster override: both
// the safety read and server dry-run must use the selected REST endpoint.
func TestRootTrafficDrainPreservesKubernetesClusterSelection(t *testing.T) {
	for _, globalsFirst := range []bool{true, false} {
		name := "global flags after command"
		if globalsFirst {
			name = "global flags before command"
		}
		t.Run(name, func(t *testing.T) {
			var wrongRequests, gets, patches atomic.Int32
			wrongServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				wrongRequests.Add(1)
				http.Error(w, "wrong API cluster", http.StatusBadRequest)
			}))
			defer wrongServer.Close()
			service := &v1beta1.InferenceService{
				TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
				ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "selected-namespace", UID: "uid-chat", ResourceVersion: "42", Generation: 1},
				Spec:       v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{ClusterSelector: "region=west"}},
			}
			selectedServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/apis/ome.io/v1beta1/namespaces/selected-namespace/inferenceservices/chat", r.URL.Path)
				assert.Equal(t, "Bearer synthetic-selected-token", r.Header.Get("Authorization"))
				switch r.Method {
				case http.MethodGet:
					gets.Add(1)
				case http.MethodPatch:
					patches.Add(1)
					assert.Equal(t, "All", r.URL.Query().Get("dryRun"))
				default:
					t.Errorf("unexpected API method %s", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				assert.NoError(t, json.NewEncoder(w).Encode(service))
			}))
			defer selectedServer.Close()

			config := clientcmdapi.NewConfig()
			config.CurrentContext = "default-context"
			config.Contexts["default-context"] = &clientcmdapi.Context{Cluster: "context-api", AuthInfo: "default-user", Namespace: "wrong-namespace"}
			config.Contexts["selected-context"] = &clientcmdapi.Context{Cluster: "context-api", AuthInfo: "selected-user", Namespace: "selected-namespace"}
			config.AuthInfos["default-user"] = &clientcmdapi.AuthInfo{Token: "synthetic-default-token"}
			config.AuthInfos["selected-user"] = &clientcmdapi.AuthInfo{Token: "synthetic-selected-token"}
			config.Clusters["context-api"] = &clientcmdapi.Cluster{Server: wrongServer.URL}
			config.Clusters["override-api"] = &clientcmdapi.Cluster{
				Server: selectedServer.URL,
				CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{
					Type: "CERTIFICATE", Bytes: selectedServer.Certificate().Raw,
				}),
			}
			configPath := filepath.Join(t.TempDir(), "config")
			require.NoError(t, clientcmd.WriteToFile(*config, configPath))

			var out, stderr bytes.Buffer
			flags := genericclioptions.NewConfigFlags(true)
			f := factory.New(flags)
			root := newRootCmd(f, flags, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			globals := []string{"--kubeconfig=" + configPath, "--context=selected-context", "--cluster=override-api"}
			args := []string{"traffic", "drain", "chat", "--workload-cluster=worker-target", "--id=maintenance-a", "--reason=maintenance", "--dry-run=server", "--yes", "-o=json"}
			if globalsFirst {
				args = append(globals, args...)
			} else {
				args = append(args, globals...)
			}
			root.SetArgs(args)
			require.NoError(t, root.Execute())

			resolved, err := f.RESTConfig()
			require.NoError(t, err)
			assert.Equal(t, selectedServer.URL, resolved.Host)
			assert.Equal(t, "synthetic-selected-token", resolved.BearerToken)
			assert.Zero(t, wrongRequests.Load())
			assert.Equal(t, int32(1), gets.Load())
			assert.Equal(t, int32(1), patches.Load())
			var result reportv1alpha1.ActionResult
			require.NoError(t, json.Unmarshal(out.Bytes(), &result))
			require.NotNil(t, result.Traffic)
			assert.Equal(t, "worker-target", result.Traffic.Cluster)
			assert.True(t, result.Accepted)
			assert.False(t, result.Applied)
		})
	}
}

func TestRootTrafficDrainRequiresWorkloadClusterBeforeAcquisition(t *testing.T) {
	for _, clusterFlag := range []string{"", "--cluster=legacy-workload-target"} {
		t.Run(clusterFlag, func(t *testing.T) {
			var out, stderr bytes.Buffer
			f := &waitFlagFactory{}
			root := NewRootCmdWithFactory(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			args := []string{"traffic", "drain", "chat", "--id=maintenance-a", "--reason=maintenance", "--yes", "--dry-run=client"}
			if clusterFlag != "" {
				args = append(args, clusterFlag)
			}
			root.SetArgs(args)
			err := root.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "requires --workload-cluster")
			assert.Contains(t, err.Error(), "--cluster selects the Kubernetes API cluster")
			assert.Zero(t, f.calls, "missing target must refuse before namespace/config/client acquisition")
			assert.Empty(t, out.String())
			assert.Empty(t, stderr.String())
		})
	}
}
