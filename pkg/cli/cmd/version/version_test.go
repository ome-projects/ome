package version

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	omeversion "sigs.k8s.io/ome/pkg/version"
)

func run(t *testing.T, f factory.Factory, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &out}
	cmd := NewCmd(f, streams)
	cmd.SetArgs(args)
	require.NoError(t, cmd.Execute())
	return out.String()
}

func TestOperatorVersionFromDeployment(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: "ome"},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "manager", Image: "ghcr.io/x/ome-manager:v0.9.1"}},
		}}},
	}
	out := run(t, factory.Static{Kube: kubefake.NewSimpleClientset(dep)})
	assert.Contains(t, out, "Client Version:")
	assert.Contains(t, out, "Operator Version: v0.9.1")
}

func TestOperatorVersionUnknownWhenMissing(t *testing.T) {
	out := run(t, factory.Static{Kube: kubefake.NewSimpleClientset()})
	assert.Contains(t, out, "Operator Version: unknown")
}

func TestOMENamespaceFlag(t *testing.T) {
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: "infra"},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Image: "ome-manager:v0.9.2"}},
		}}},
	}
	out := run(t, factory.Static{Kube: kubefake.NewSimpleClientset(dep)}, "--ome-namespace", "infra")
	assert.Contains(t, out, "Operator Version: v0.9.2")
}

func TestOperatorVersionSelectsManagerContainer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		containers []corev1.Container
		want       string
	}{
		{
			name: "manager after sidecar",
			containers: []corev1.Container{
				{Name: "proxy", Image: "proxy:v9.9.9"},
				{Name: "manager", Image: "ghcr.io/x/ome-manager:v1.2.3"},
			},
			want: "v1.2.3",
		},
		{
			name:       "single legacy container",
			containers: []corev1.Container{{Name: "legacy", Image: "ome-manager:v1.2.4"}},
			want:       "v1.2.4",
		},
		{
			name: "multiple unnamed candidates",
			containers: []corev1.Container{
				{Name: "proxy", Image: "proxy:v9.9.9"},
				{Name: "other", Image: "other:v8.8.8"},
			},
			want: "unknown",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := run(t, factory.Static{Kube: kubefake.NewSimpleClientset(deployment(tc.containers...))})
			assert.Contains(t, out, "Operator Version: "+tc.want)
			assert.NotContains(t, out, "v9.9.9")
			assert.NotContains(t, out, "v8.8.8")
		})
	}
}

func TestOperatorVersionPreservesImageIdentity(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, tc := range []struct{ name, image, want string }{
		{"tag with registry port", "registry.example:5000/team/manager:v1.2.3", "v1.2.3"},
		{"digest only", "registry.example:5000/team/manager@" + digest, digest},
		{"tag and digest", "registry.example:5000/team/manager:v1.2.3@" + digest, "v1.2.3@" + digest},
		{"registry port is not tag", "registry.example:5000/team/manager", "unknown (image has no tag or digest)"},
		{"untagged repository", "ghcr.io/x/ome-manager", "unknown (image has no tag or digest)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := run(t, factory.Static{Kube: kubefake.NewSimpleClientset(deployment(corev1.Container{Name: "manager", Image: tc.image}))})
			assert.Contains(t, out, "Operator Version: "+tc.want+"\n")
		})
	}
}

type failingKubeFactory struct {
	factory.Static
	err error
}

func (f failingKubeFactory) KubeClient() (kubernetes.Interface, error) {
	return nil, f.err
}

func TestVersionDoesNotReflectRawLookupErrors(t *testing.T) {
	const private = "https://user:password@private.example?token=private-value\n\x1b[31m"
	t.Run("client configuration", func(t *testing.T) {
		out := run(t, failingKubeFactory{err: errors.New(private)})
		assert.Contains(t, out, "Operator Version: unknown")
		assert.NotContains(t, out, "private")
		assert.NotContains(t, out, "password")
		assert.NotContains(t, out, "\x1b")
		assert.Equal(t, 2, strings.Count(out, "\n"))
	})
	t.Run("API forbidden", func(t *testing.T) {
		kube := kubefake.NewSimpleClientset()
		kube.PrependReactor("get", "deployments", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "ome-controller-manager", errors.New(private))
		})
		out := run(t, factory.Static{Kube: kube}, "--ome-namespace", "private-token=private-value")
		assert.Contains(t, out, "Operator Version: unknown")
		assert.Contains(t, out, "Forbidden")
		assert.NotContains(t, out, "private")
		assert.NotContains(t, out, "password")
		assert.NotContains(t, out, "\x1b")
		assert.Equal(t, 2, strings.Count(out, "\n"))
	})
}

func TestVersionBoundsAndSanitizesDisplayValues(t *testing.T) {
	const credential = "ghp_abcdefghijklmnopqrstuvwxyz123456"
	for _, tc := range []struct{ name, value string }{
		{"credential", credential},
		{"terminal controls", "v1.2.3\nforged-output\x1b[31m\r"},
		{"oversized", strings.Repeat("v", 10000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previousVersion, previousCommit := omeversion.GitVersion, omeversion.GitCommit
			omeversion.GitVersion, omeversion.GitCommit = tc.value, tc.value
			t.Cleanup(func() { omeversion.GitVersion, omeversion.GitCommit = previousVersion, previousCommit })
			out := run(t, factory.Static{Kube: kubefake.NewSimpleClientset(deployment(corev1.Container{Name: "manager", Image: "manager:" + tc.value}))})
			assert.NotContains(t, out, credential)
			assert.NotContains(t, out, "\x1b")
			assert.NotContains(t, out, "\r")
			assert.Equal(t, 2, strings.Count(out, "\n"))
			assert.Less(t, len(out), 1024)
		})
	}
}

type failingWriter struct {
	allowed int
	writes  int
	width   int
}

func (w *failingWriter) TerminalWidth() (int, bool) { return w.width, w.width > 0 }

func (w *failingWriter) Write(data []byte) (int, error) {
	if w.writes >= w.allowed {
		return 0, errors.New("private-output-location")
	}
	w.writes++
	return len(data), nil
}

func TestVersionReturnsOutputFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		allowed int
		width   int
	}{
		{name: "first redirected line"},
		{name: "second redirected line", allowed: 1},
		{name: "terminal output", width: 80},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := &failingWriter{allowed: tc.allowed, width: tc.width}
			cmd := NewCmd(factory.Static{Kube: kubefake.NewSimpleClientset()}, genericiooptions.IOStreams{Out: out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs(nil)
			err := cmd.Execute()
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "private-output-location")
		})
	}
}

type terminalBuffer struct {
	bytes.Buffer
	width int
}

func (w *terminalBuffer) TerminalWidth() (int, bool) { return w.width, true }

func TestVersionFitsTerminalWithoutDroppingImageDigest(t *testing.T) {
	const identity = "dev-8d9f609-amd64@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, width := range []int{80, 60} {
		out := &terminalBuffer{width: width}
		kube := kubefake.NewSimpleClientset(deployment(corev1.Container{Name: "manager", Image: "ghcr.io/x/ome-manager:" + identity}))
		cmd := NewCmd(factory.Static{Kube: kube}, genericiooptions.IOStreams{Out: out})
		cmd.SetArgs(nil)
		require.NoError(t, cmd.Execute())
		for _, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
			assert.LessOrEqual(t, printers.CellDisplayWidth(line), width, "physical line exceeds terminal: %q", line)
		}
		assert.Contains(t, strings.Join(strings.Fields(out.String()), ""), identity)
	}
}

func deployment(containers ...corev1.Container) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: "ome"},
		Spec:       appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: containers}}},
	}
}
