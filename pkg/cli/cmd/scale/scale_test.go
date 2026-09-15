package scale

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type spyFactory struct{ calls int }

func (f *spyFactory) hit() error                                { f.calls++; return errors.New("private-factory-sentinel") }
func (f *spyFactory) Namespace() (string, bool, error)          { return "", false, f.hit() }
func (f *spyFactory) ContextName() (string, error)              { return "", f.hit() }
func (f *spyFactory) RESTConfig() (*rest.Config, error)         { return nil, f.hit() }
func (f *spyFactory) OMEClient() (versioned.Interface, error)   { return nil, f.hit() }
func (f *spyFactory) KubeClient() (kubernetes.Interface, error) { return nil, f.hit() }
func (f *spyFactory) RuntimeClient() (ctrlclient.Client, error) { return nil, f.hit() }

func executeSpy(t *testing.T, args []string) (*spyFactory, string, string, error) {
	t.Helper()
	f := &spyFactory{}
	var out, stderr bytes.Buffer
	root := &cobra.Command{Use: "ome", SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().String("namespace", "prod", "namespace")
	root.PersistentFlags().String("context", "test", "context")
	root.PersistentFlags().String("request-timeout", "0", "timeout")
	root.AddCommand(NewCmd(f, genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &out, ErrOut: &stderr}))
	root.SetArgs(append([]string{"scale"}, args...))
	err := root.Execute()
	return f, out.String(), stderr.String(), err
}

func TestScaleCallerOwnsConfigBeforeFirstCopyAndPreservesTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second, 20 * time.Second} {
		original := &runtime.Unknown{Raw: []byte(`{"private":"sentinel"}`)}
		provider := &clientcmdapi.ExecConfig{Config: original}
		config := &rest.Config{Host: "http://localhost", Timeout: timeout, ExecProvider: provider}
		copy, err := actionConfig(config)
		require.NoError(t, err)
		require.Same(t, provider, config.ExecProvider)
		require.Same(t, original, provider.Config)
		require.NotSame(t, provider, copy.ExecProvider)
		require.Equal(t, timeout, config.Timeout)
		expected := 10 * time.Second
		if timeout > 0 && timeout < expected {
			expected = timeout
		}
		require.Equal(t, expected, copy.Timeout)
	}
	_, err := actionConfig(nil)
	require.Error(t, err)
}

func TestScaleRejectsInvalidInheritedFlagsBeforeAcquisition(t *testing.T) {
	for _, flag := range []string{"namespace", "context", "request-timeout"} {
		t.Run(flag, func(t *testing.T) {
			f, out, stderr, err := executeSpy(t, []string{"chat", "--component=engine", "--replicas=3", "--override-autoscaler", "--yes", "--" + flag + "=sensitive@invalid value"})
			require.ErrorIs(t, err, ErrArguments)
			require.Zero(t, f.calls)
			require.Empty(t, out)
			require.Empty(t, stderr)
			require.NotContains(t, err.Error(), "sensitive")
		})
	}
}

func TestScaleLocalInvalidMatrixAcquiresNothing(t *testing.T) {
	base := []string{"chat", "--component=engine", "--replicas=3", "--override-autoscaler", "--yes"}
	cases := [][]string{{}, {"chat"}, {"chat", "extra"}, {"chat", "--component=engine", "--yes"}, {"chat", "--component=engine", "--replicas=1", "--override-autoscaler"}}
	for _, value := range []string{"", "0", "-1", "2147483648", "1.5", "1e2", "0x10", "+3", " 3", "private@sentinel"} {
		cases = append(cases, []string{"chat", "--component=engine", "--replicas=" + value, "--yes"})
	}
	for _, suffix := range []string{"--component=unknown", "--output=private@sentinel", "--dry-run=bad", "--ome-namespace=Bad@sentinel", "--force", "--yes=bad"} {
		cases = append(cases, append(append([]string(nil), base...), suffix))
	}
	for _, args := range cases {
		f, out, stderr, err := executeSpy(t, args)
		require.ErrorIs(t, err, ErrArguments, "%v", args)
		require.Zero(t, f.calls, "%v", args)
		require.Empty(t, out)
		require.Empty(t, stderr)
		require.NotContains(t, err.Error(), "sentinel")
	}
}
