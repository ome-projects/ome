package migration

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

type actionReadStartFactory struct {
	startFactory
	actionOME           versioned.Interface
	actionKube          kubernetes.Interface
	actionFailure       string
	omeCalls, kubeCalls int
}

func (f *actionReadStartFactory) OMEClient() (versioned.Interface, error) {
	return nil, errors.New("PRIVATE_CACHED_CLIENT")
}

func (f *actionReadStartFactory) KubeClient() (kubernetes.Interface, error) {
	return nil, errors.New("PRIVATE_CACHED_CLIENT")
}

func (f *actionReadStartFactory) OMEClientForAction(ctx context.Context) (versioned.Interface, error) {
	f.omeCalls++
	if f.actionFailure == "OME" {
		return nil, errors.New("PRIVATE_ACTION_CLIENT")
	}
	if f.actionFailure == "nil OME" {
		return nil, nil
	}
	return f.actionOME, ctx.Err()
}

func (f *actionReadStartFactory) KubeClientForAction(ctx context.Context) (kubernetes.Interface, error) {
	f.kubeCalls++
	if f.actionFailure == "Kube" {
		return nil, errors.New("PRIVATE_ACTION_CLIENT")
	}
	if f.actionFailure == "nil Kube" {
		return nil, nil
	}
	return f.actionKube, ctx.Err()
}

func TestStartPrefersActionOwnedReadClientsAndFailsClosed(t *testing.T) {
	for _, failure := range []string{"", "OME", "Kube", "nil OME", "nil Kube"} {
		t.Run(failure, func(t *testing.T) {
			fixture := newStartFixture()
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox"}}}}}
			f := &actionReadStartFactory{
				startFactory: startFactory{Static: factory.Static{Runtime: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(), NS: "prod", Context: "synthetic"}},
				actionOME:    omefake.NewSimpleClientset(fixture.parent, fixture.ir), actionKube: kubefake.NewClientset(fixture.cr, &fixture.pods[0]), actionFailure: failure,
			}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"start", "chat", "--component=engine", "--instance=3", "--yes", "--dry-run=client", "-o=json"})
			err := cmd.Execute()
			if failure == "" {
				require.NoError(t, err)
				require.Contains(t, out.String(), `"accepted": false`)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
				require.Empty(t, out.String())
			}
			require.Equal(t, 1, f.omeCalls)
			wantKube := 1
			if failure == "OME" || failure == "nil OME" {
				wantKube = 0
			}
			require.Equal(t, wantKube, f.kubeCalls)
			require.NotContains(t, out.String()+stderr.String(), "PRIVATE")
		})
	}
}
