package scale

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type failureStageFactory struct {
	factory.Factory
	stage string
}

func (f *failureStageFactory) Namespace() (string, bool, error) {
	if f.stage == "namespace error" {
		return "", false, errors.New(privateSentinel)
	}
	if f.stage == "invalid namespace" {
		return "Invalid", false, nil
	}
	return f.Factory.Namespace()
}
func (f *failureStageFactory) ContextName() (string, error) {
	if f.stage == "context error" {
		return "", errors.New(privateSentinel)
	}
	if f.stage == "unsafe context" {
		return "sensitive invalid context", nil
	}
	return f.Factory.(factory.ContextResolver).ContextName()
}
func (f *failureStageFactory) OMEClientForAction(ctx context.Context) (versioned.Interface, error) {
	if f.stage == "ome nil" {
		return nil, nil
	}
	if f.stage == "ome error" {
		return nil, errors.New(privateSentinel)
	}
	return f.Factory.(factory.ActionReadClientsResolver).OMEClientForAction(ctx)
}
func (f *failureStageFactory) KubeClientForAction(ctx context.Context) (kubernetes.Interface, error) {
	if f.stage == "kube nil" {
		return nil, nil
	}
	if f.stage == "kube error" {
		return nil, errors.New(privateSentinel)
	}
	return f.Factory.(factory.ActionReadClientsResolver).KubeClientForAction(ctx)
}
func (f *failureStageFactory) RuntimeClientForAction(ctx context.Context) (ctrlclient.Client, error) {
	if f.stage == "runtime nil" {
		return nil, nil
	}
	if f.stage == "runtime error" {
		return nil, errors.New(privateSentinel)
	}
	return f.Factory.(factory.ActionRuntimeResolver).RuntimeClientForAction(ctx)
}
func (f *failureStageFactory) RESTConfig() (*rest.Config, error) {
	if f.stage == "rest error" {
		return nil, errors.New(privateSentinel)
	}
	if f.stage == "rest nil" {
		return nil, nil
	}
	if f.stage == "rest invalid" {
		return &rest.Config{Host: ":"}, nil
	}
	return f.Factory.RESTConfig()
}

func TestScaleAcquisitionFailuresArePrivateAndNeverPatch(t *testing.T) {
	for _, stage := range []string{"namespace error", "invalid namespace", "context error", "unsafe context", "ome nil", "ome error", "kube nil", "kube error", "runtime nil", "runtime error", "rest error", "rest nil", "rest invalid", "wrong parent", "parent deletion", "parent missing", "runtime missing", "wrong replica", "replica missing"} {
		t.Run(stage, func(t *testing.T) {
			a := nativeFixture()
			switch stage {
			case "wrong parent":
				a.parent.Name = "other"
			case "parent deletion":
				a.parent.Generation = 0
			case "parent missing":
				a.parent = nil
			case "runtime missing":
				a.runtime = nil
			case "wrong replica":
				a.replica.Name = "other"
			case "replica missing":
				a.replica = nil
			}
			server := httptest.NewServer(http.HandlerFunc(a.handler))
			defer server.Close()
			flags := genericclioptions.NewConfigFlags(true)
			config := nativeConfig(t, server.URL)
			flags.KubeConfig = &config
			f := &failureStageFactory{Factory: factory.New(flags), stage: stage}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &out, ErrOut: &stderr})
			cmd.SetArgs([]string{"chat", "--component=engine", "--replicas=3", "--override-autoscaler", "--yes", "-o=json"})
			err := cmd.Execute()
			require.Error(t, err)
			require.Empty(t, out.String())
			require.NotContains(t, stderr.String()+err.Error(), privateSentinel)
			require.Zero(t, a.patches)
		})
	}
}
