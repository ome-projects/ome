package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

const renderFileDeployConfig = `apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: control-plane
data:
  deploy: |
    {"defaultDeploymentMode": "RawDeployment", "terminationGracePeriodSeconds": 45,
     "replicas": {"defaultMinReplicas": 2, "defaultMaxReplicas": {"engine": 6}}}
`

const renderFileService = `apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: service
spec:
  model:
    name: llama
  runtime:
    name: vllm
    kind: ServingRuntime
  engine:
    minReplicas: 3
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
data:
  deploy: '{"defaultDeploymentMode": "RawDeployment", "terminationGracePeriodSeconds": 999}'
`

const renderFileRuntimes = `apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: vllm
spec:
  engineConfig:
    runner:
      name: runner
      image: runtime:v1
      args: ["--port=8080"]
---
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama
spec:
  modelFormat:
    name: safetensors
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: unrelated
`

func renderFiles(extra map[string]string) map[string]string {
	files := map[string]string{
		"deploy.yaml":   renderFileDeployConfig,
		"service.yaml":  renderFileService,
		"runtimes.yaml": renderFileRuntimes,
	}
	for path, data := range extra {
		files[path] = data
	}
	return files
}

var renderFileArgs = []string{"-f", "service.yaml", "-f", "runtimes.yaml", "--deploy-config", "deploy.yaml"}

func TestRenderFilesGoldenOutput(t *testing.T) {
	f := renderFactory(t, false)

	out, errOut, err := executeRenderStreams(t, f, renderFiles(nil), renderFileArgs...)

	require.NoError(t, err)
	assertRenderGolden(t, "render_files.golden.yaml", out)
	assert.Equal(t,
		"ignoring v1 ConfigMap from service.yaml: -f reads only InferenceServices, runtimes, and models\n"+
			"ignoring apps/v1 Deployment from runtimes.yaml: -f reads only InferenceServices, runtimes, and models\n",
		errOut)
	assert.Zero(t, f.omeGet+f.kubeGet+f.runtimeGet, "file mode must not construct API clients")
}

// TestRenderFilesMatchesClusterMode renders the healthy fixture from the
// cluster and from its manifests; only the source origins may differ.
func TestRenderFilesMatchesClusterMode(t *testing.T) {
	f := renderFactory(t, false)
	isvc, err := f.ome.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
	require.NoError(t, err)
	isvc.TypeMeta = metav1.TypeMeta{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService"}
	runtimeObject := &v1beta1.ClusterServingRuntime{}
	require.NoError(t, f.runtime.Get(context.Background(), ctrlclient.ObjectKey{Name: "cluster-runtime"}, runtimeObject))
	runtimeObject.TypeMeta = metav1.TypeMeta{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: runtimeselector.KindClusterServingRuntime}
	var manifests []string
	for _, object := range []any{isvc, runtimeObject} {
		data, err := yaml.Marshal(object)
		require.NoError(t, err)
		manifests = append(manifests, string(data))
	}
	files := map[string]string{"all.yaml": strings.Join(manifests, "---\n"), "deploy.yaml": renderFileDeployConfig}

	fromCluster, err := executeRender(t, renderFactory(t, false), nil, "-o", "json")
	require.NoError(t, err)
	fromFiles, err := executeRender(t, f, files, "-f", "all.yaml", "--deploy-config", "deploy.yaml", "-o", "json")
	require.NoError(t, err)

	withoutSources := func(out string) map[string]any {
		var rendered map[string]any
		require.NoError(t, json.Unmarshal([]byte(out), &rendered))
		delete(rendered, "sources")
		return rendered
	}
	assert.Equal(t, withoutSources(fromCluster), withoutSources(fromFiles))
	assert.Contains(t, fromFiles, `"origin": "File"`)
	assert.NotContains(t, fromFiles, `"origin": "Cluster"`)
}

// TestRenderFilesUsesNamespaceFlag checks that -n selects the service and
// that the runtime without a namespace lands in the same namespace.
func TestRenderFilesUsesNamespaceFlag(t *testing.T) {
	service := strings.Replace(renderFileService, "name: service\n", "name: service\n  namespace: prod\n", 1)
	f := renderFactory(t, false)
	f.namespace = "prod"

	out, _, err := executeRenderStreams(t, f, renderFiles(map[string]string{"service.yaml": service}), renderFileArgs...)

	require.NoError(t, err)
	assert.Contains(t, out, "name: prod/vllm\n")
}

// renderFileProdService is a second service and runtime in another
// namespace, for rendering every service.
const renderFileProdService = `apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  model:
    name: llama
  runtime:
    name: vllm
    kind: ServingRuntime
  engine:
    minReplicas: 1
---
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: vllm
  namespace: prod
spec:
  engineConfig:
    runner:
      name: runner
      image: runtime:v2
`

var renderAllArgs = append([]string{"-f", "prod.yaml"}, renderFileArgs...)

func TestRenderFilesAllServices(t *testing.T) {
	// Pin the team-a namespaces so -n can select either service below.
	files := renderFiles(map[string]string{
		"prod.yaml":     renderFileProdService,
		"service.yaml":  strings.Replace(renderFileService, "name: service\n", "name: service\n  namespace: team-a\n", 1),
		"runtimes.yaml": strings.Replace(renderFileRuntimes, "metadata:\n  name: vllm\n", "metadata:\n  name: vllm\n  namespace: team-a\n", 1),
	})
	f := renderFactory(t, false)

	out, _, err := executeRenderArgs(t, f, files, renderAllArgs...)

	require.NoError(t, err)
	assertRenderGolden(t, "render_files_all.golden.yaml", out)
	assert.Zero(t, f.omeGet+f.kubeGet+f.runtimeGet, "file mode must not construct API clients")

	// Each item equals rendering that service by name.
	var list struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(out), &list))
	require.Len(t, list.Items, 2)
	for i, service := range []struct{ namespace, name string }{{"prod", "chat"}, {"team-a", "service"}} {
		f := renderFactory(t, false)
		f.namespace = service.namespace
		single, _, err := executeRenderArgs(t, f, files, append([]string{service.name}, renderAllArgs...)...)
		require.NoError(t, err)
		var want map[string]any
		require.NoError(t, yaml.Unmarshal([]byte(single), &want))
		assert.Equal(t, want, list.Items[i], service.name)
	}
}

func TestRenderFilesAllServicesFailsTogether(t *testing.T) {
	// Both services lose their runtime; every failure is reported and
	// nothing is printed.
	files := renderFiles(map[string]string{
		"prod.yaml":     strings.SplitN(renderFileProdService, "---\n", 2)[0],
		"runtimes.yaml": strings.SplitN(renderFileRuntimes, "---\n", 2)[1],
	})

	out, _, err := executeRenderArgs(t, renderFactory(t, false), files, renderAllArgs...)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 of 2 InferenceServices failed to render")
	assert.Contains(t, err.Error(), "InferenceService prod/chat: render live view: ")
	assert.Contains(t, err.Error(), "InferenceService team-a/service: render live view: ")
	assert.Empty(t, out)
}

func TestRenderFilesFailures(t *testing.T) {
	for _, test := range []struct {
		name     string
		files    map[string]string
		args     []string
		contains string
		noName   bool
	}{
		{
			name:     "missing deploy-config",
			args:     []string{"-f", "service.yaml"},
			contains: "-f requires --deploy-config",
		},
		{
			name:     "active view",
			args:     append([]string{"--view", "active"}, renderFileArgs...),
			contains: "--view active needs the cluster and cannot be used with -f",
		},
		{
			name: "duplicate runtime across files",
			files: map[string]string{"copy.yaml": "apiVersion: ome.io/v1beta1\nkind: ServingRuntime\nmetadata:\n" +
				"  name: vllm\n  namespace: team-a\n"},
			args:     append([]string{"-f", "copy.yaml"}, renderFileArgs...),
			contains: "ambiguous file input: ServingRuntime team-a/vllm is defined in copy.yaml and runtimes.yaml",
		},
		{
			name:     "service not in files",
			args:     []string{"-f", "runtimes.yaml", "--deploy-config", "deploy.yaml"},
			contains: "InferenceService team-a/service not found in -f files",
		},
		{
			name:     "runtime not in files",
			args:     []string{"-f", "service.yaml", "--deploy-config", "deploy.yaml"},
			contains: "render live view: ",
		},
		{
			name:     "duplicate key",
			files:    map[string]string{"service.yaml": strings.Replace(renderFileService, "minReplicas: 3", "minReplicas: 3\n    minReplicas: 4", 1)},
			args:     renderFileArgs,
			contains: `key "minReplicas" already set in map`,
		},
		{
			name:     "unknown field",
			files:    map[string]string{"service.yaml": strings.Replace(renderFileService, "minReplicas: 3", "minReplica: 3", 1)},
			args:     renderFileArgs,
			contains: `-f service.yaml: strict decoding error: unknown field "spec.engine.minReplica"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := renderFactory(t, false)
			args := test.args
			if !test.noName {
				args = append([]string{"service"}, args...)
			}
			out, _, err := executeRenderArgs(t, f, renderFiles(test.files), args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.contains)
			assert.Empty(t, out)
			assert.Zero(t, f.omeGet+f.kubeGet+f.runtimeGet, "file mode must not construct API clients")
		})
	}
}
