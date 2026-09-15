package instance

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"

	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type heldActionReadFactory struct {
	heldActionFactory
	failure             string
	omeCalls, kubeCalls int
}

func (*heldActionReadFactory) OMEClient() (versioned.Interface, error) {
	return nil, errors.New("PRIVATE_CACHED_CLIENT")
}

func (*heldActionReadFactory) KubeClient() (kubernetes.Interface, error) {
	return nil, errors.New("PRIVATE_CACHED_CLIENT")
}

func (f *heldActionReadFactory) OMEClientForAction(ctx context.Context) (versioned.Interface, error) {
	f.omeCalls++
	if f.failure == "OME" {
		return nil, errors.New("PRIVATE_ACTION_CLIENT")
	}
	if f.failure == "nil OME" {
		return nil, nil
	}
	return f.Static.OME, ctx.Err()
}

func (f *heldActionReadFactory) KubeClientForAction(ctx context.Context) (kubernetes.Interface, error) {
	f.kubeCalls++
	if f.failure == "Kube" {
		return nil, errors.New("PRIVATE_ACTION_CLIENT")
	}
	if f.failure == "nil Kube" {
		return nil, nil
	}
	return f.Static.Kube, ctx.Err()
}

func TestReleaseHeldPrefersActionOwnedReadClientsAndFailsClosed(t *testing.T) {
	for _, failure := range []string{"", "OME", "Kube", "nil OME", "nil Kube"} {
		t.Run(failure, func(t *testing.T) {
			h := newHeldWireHarness(t)
			f := &heldActionReadFactory{heldActionFactory: h.f, failure: failure}
			var out, stderr bytes.Buffer
			cmd := newReleaseHeldCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, reportv1alpha1.SystemClock{})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "--dry-run=client", "-o=json"})
			err := cmd.Execute()
			if failure == "" {
				require.NoError(t, err)
				require.Contains(t, out.String(), `"accepted": false`)
				require.Equal(t, 2, h.parentReads)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
				require.Empty(t, out.String())
				require.Zero(t, h.parentReads)
			}
			require.Equal(t, 1, f.omeCalls)
			wantKube := 1
			if failure == "OME" || failure == "nil OME" {
				wantKube = 0
			}
			require.Equal(t, wantKube, f.kubeCalls)
			require.Zero(t, h.patches)
			require.NotContains(t, out.String()+stderr.String(), "PRIVATE")
		})
	}
}
