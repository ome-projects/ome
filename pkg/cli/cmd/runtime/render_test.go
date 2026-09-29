package runtime

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

var updateGolden = flag.Bool("update", false, "rewrite golden files")

const renderDeployBlock = `{
	"defaultDeploymentMode": "RawDeployment",
	"terminationGracePeriodSeconds": 45,
	"replicas": {"defaultMinReplicas": 2, "defaultMaxReplicas": {"engine": 6}}
}`

func renderConfigMap(namespace, deploy string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: namespace},
		Data:       map[string]string{controllerconfig.DeployConfigName: deploy},
	}
}

// renderFactory is the healthy pinned fixture plus the live deploy defaults.
func renderFactory(t *testing.T, reversed bool) *acquisitionFactory {
	t.Helper()
	f, _ := healthyPinnedFactoryVariant(t, reversed)
	require.NoError(t, f.kube.(*k8sfake.Clientset).Tracker().Add(renderConfigMap("control-plane", renderDeployBlock)))
	return f
}

func executeRender(t *testing.T, f *acquisitionFactory, files map[string]string, args ...string) (string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	o := &renderOptions{
		IOStreams:        genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut},
		namespaceOptions: namespace.NewOptions(),
		limits:           fixedEffectiveDependencies().limits,
		readFile: func(path string) ([]byte, error) {
			data, ok := files[path]
			if !ok {
				return nil, os.ErrNotExist
			}
			return []byte(data), nil
		},
	}
	cmd := newRenderCmdWithOptions(f, o)
	cmd.SetArgs(append([]string{"service", "--ome-namespace", "control-plane"}, args...))
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	require.Empty(t, errOut.String())
	return out.String(), err
}

func assertRenderGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, string(want), got)
}

func TestRenderGoldenOutputs(t *testing.T) {
	for _, test := range []struct {
		golden string
		args   []string
	}{
		{golden: "render_live.golden.yaml"},
		{golden: "render_active.golden.json", args: []string{"--view", "active", "-o", "json"}},
	} {
		t.Run(test.golden, func(t *testing.T) {
			out, err := executeRender(t, renderFactory(t, false), nil, test.args...)
			require.NoError(t, err)
			assertRenderGolden(t, test.golden, out)

			again, err := executeRender(t, renderFactory(t, true), nil, test.args...)
			require.NoError(t, err)
			assert.Equal(t, out, again, "equal inputs must render byte-identical output")
		})
	}
}

func TestRenderDeployConfigFileOverridesCluster(t *testing.T) {
	f, _ := healthyPinnedFactory(t)
	manifest := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: inferenceservice-config\ndata:\n  deploy: |\n" +
		"    {\"defaultDeploymentMode\": \"RawDeployment\", \"terminationGracePeriodSeconds\": 90}\n"

	out, err := executeRender(t, f, map[string]string{"proposed.yaml": manifest}, "--deploy-config", "proposed.yaml")

	require.NoError(t, err)
	assert.Contains(t, out, "terminationGracePeriodSeconds: 90\n")
	assert.Contains(t, out, "name: proposed.yaml\n  origin: File\n")
	for _, action := range f.kube.(*k8sfake.Clientset).Actions() {
		assert.NotEqual(t, "configmaps", action.GetResource().Resource, "--deploy-config must not read the cluster ConfigMap")
	}
}

// TestRenderServiceVirtualDeploymentIsInspectionOnly covers the command
// output; both service-level triggers are covered in pkg/cli/effective.
func TestRenderServiceVirtualDeploymentIsInspectionOnly(t *testing.T) {
	f := renderFactory(t, false)
	isvcs := f.ome.OmeV1beta1().InferenceServices("team-a")
	isvc, err := isvcs.Get(context.Background(), "service", metav1.GetOptions{})
	require.NoError(t, err)
	mode := constants.VirtualDeployment
	isvc.Spec.DeploymentMode = &mode
	_, err = isvcs.Update(context.Background(), isvc, metav1.UpdateOptions{})
	require.NoError(t, err)

	out, err := executeRender(t, f, nil)

	require.NoError(t, err)
	assert.Contains(t, out, "deployDefaults: NotApplicable\n")
	assert.Contains(t, out, "deploymentMode: VirtualDeployment\n")
	assert.NotContains(t, out, "terminationGracePeriodSeconds")
	assert.NotContains(t, out, "minReplicas")
}

func TestRenderFailures(t *testing.T) {
	forbidden := func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, constants.InferenceServiceConfigMapName, errors.New("denied"))
	}
	for _, test := range []struct {
		name     string
		setup    func(*testing.T) *acquisitionFactory
		files    map[string]string
		args     []string
		contains string
	}{
		{
			name: "InferenceService missing",
			setup: func(t *testing.T) *acquisitionFactory {
				f := renderFactory(t, false)
				require.NoError(t, f.ome.OmeV1beta1().InferenceServices("team-a").Delete(context.Background(), "service", metav1.DeleteOptions{}))
				return f
			},
			contains: "get InferenceService",
		},
		{
			name:     "ConfigMap missing",
			setup:    func(t *testing.T) *acquisitionFactory { f, _ := healthyPinnedFactory(t); return f },
			contains: "deploy defaults: ConfigMap control-plane/inferenceservice-config not found",
		},
		{
			name: "ConfigMap forbidden",
			setup: func(t *testing.T) *acquisitionFactory {
				f := renderFactory(t, false)
				f.kube.(*k8sfake.Clientset).PrependReactor("get", "configmaps", forbidden)
				return f
			},
			contains: "is forbidden",
		},
		{
			name: "ConfigMap invalid",
			setup: func(t *testing.T) *acquisitionFactory {
				f, _ := healthyPinnedFactory(t)
				require.NoError(t, f.kube.(*k8sfake.Clientset).Tracker().Add(renderConfigMap("control-plane", `{"replicas": {"defaultMinReplicas": 0}}`)))
				return f
			},
			contains: "deploy defaults: ConfigMap control-plane/inferenceservice-config:",
		},
		{
			name: "live runtime missing",
			setup: func(t *testing.T) *acquisitionFactory {
				f := renderFactory(t, false)
				f.runtime = ctrlfake.NewClientBuilder().WithScheme(scheme(t)).Build()
				return f
			},
			contains: "render live view: live runtime was not found",
		},
		{
			name: "pinned revision missing",
			setup: func(t *testing.T) *acquisitionFactory {
				f, _ := healthyPinnedFactory(t)
				f.kube = k8sfake.NewSimpleClientset(renderConfigMap("control-plane", renderDeployBlock))
				return f
			},
			args:     []string{"--view", "active"},
			contains: "render active view (pin state RevisionMissing)",
		},
		{
			name:     "deploy-config file missing",
			setup:    func(t *testing.T) *acquisitionFactory { f, _ := healthyPinnedFactory(t); return f },
			args:     []string{"--deploy-config", "absent.yaml"},
			contains: "read --deploy-config",
		},
		{
			name:     "deploy-config file invalid",
			setup:    func(t *testing.T) *acquisitionFactory { f, _ := healthyPinnedFactory(t); return f },
			files:    map[string]string{"bad.yaml": "apiVersion: v1\nkind: Secret\nmetadata:\n  name: inferenceservice-config\n"},
			args:     []string{"--deploy-config", "bad.yaml"},
			contains: "want v1 ConfigMap",
		},
		{
			name:     "unknown view",
			setup:    func(t *testing.T) *acquisitionFactory { return renderFactory(t, false) },
			args:     []string{"--view", "desired"},
			contains: `unsupported view "desired"`,
		},
		{
			name:     "table output",
			setup:    func(t *testing.T) *acquisitionFactory { return renderFactory(t, false) },
			args:     []string{"-o", "table"},
			contains: `unsupported output format "table"`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			out, err := executeRender(t, test.setup(t), test.files, test.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.contains)
			assert.Empty(t, out)
		})
	}
}
