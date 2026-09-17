package runtime

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

var errSyncAcquisitionPrivate = errors.New("PRIVATE_ACQUISITION")

// Only Factory methods are promoted here, deliberately excluding optional seams.
type syncStageFactory struct {
	factory.Factory
	stage string
	calls []string
}

func (f *syncStageFactory) Namespace() (string, bool, error) {
	f.calls = append(f.calls, "namespace")
	if f.stage == "namespace error" {
		return "", false, errSyncAcquisitionPrivate
	}
	if f.stage == "invalid workload" {
		return "Bad_PRIVATE", false, nil
	}
	return f.Factory.Namespace()
}
func (f *syncStageFactory) RESTConfig() (*rest.Config, error) {
	f.calls = append(f.calls, "config")
	if f.stage == "config error" {
		return nil, errSyncAcquisitionPrivate
	}
	if f.stage == "config nil" {
		return nil, nil
	}
	return f.Factory.RESTConfig()
}
func (f *syncStageFactory) OMEClient() (versioned.Interface, error) {
	f.calls = append(f.calls, "ordinary ome")
	return f.ome()
}
func (f *syncStageFactory) ome() (versioned.Interface, error) {
	if f.stage == "ome error" {
		return nil, errSyncAcquisitionPrivate
	}
	if f.stage == "ome nil" {
		return nil, nil
	}
	return f.Factory.OMEClient()
}
func (f *syncStageFactory) KubeClient() (kubernetes.Interface, error) {
	f.calls = append(f.calls, "ordinary kube")
	return f.kube()
}
func (f *syncStageFactory) kube() (kubernetes.Interface, error) {
	if f.stage == "kube error" {
		return nil, errSyncAcquisitionPrivate
	}
	if f.stage == "kube nil" {
		return nil, nil
	}
	return f.Factory.KubeClient()
}
func (f *syncStageFactory) RuntimeClient() (ctrlclient.Client, error) {
	f.calls = append(f.calls, "ordinary runtime")
	return nil, errSyncAcquisitionPrivate
}

type syncContextStageFactory struct{ *syncStageFactory }

func (f *syncContextStageFactory) ContextName() (string, error) {
	f.calls = append(f.calls, "context")
	if f.stage == "context error" {
		return "", errSyncAcquisitionPrivate
	}
	if f.stage == "unsafe context" {
		return "PRIVATE\nCONTEXT", nil
	}
	return "dev-fra", nil
}

type syncActionStageFactory struct{ *syncContextStageFactory }

func (f *syncActionStageFactory) RuntimeClientForAction(ctx context.Context) (ctrlclient.Client, error) {
	f.calls = append(f.calls, "runtime")
	if f.stage == "runtime error" {
		return nil, errSyncAcquisitionPrivate
	}
	if f.stage == "runtime nil" {
		return nil, nil
	}
	return f.Factory.RuntimeClient()
}

type syncOwnedStageFactory struct{ *syncActionStageFactory }

func (f *syncOwnedStageFactory) OMEClientForAction(context.Context) (versioned.Interface, error) {
	f.calls = append(f.calls, "ome")
	return f.ome()
}
func (f *syncOwnedStageFactory) KubeClientForAction(context.Context) (kubernetes.Interface, error) {
	f.calls = append(f.calls, "kube")
	return f.kube()
}

// Acquiring later clients after a known refusal, silently accepting nil clients,
// or using an ordinary cached client instead of an available owned seam fails.
func TestRuntimeSyncAcquisitionRefusalsStopAtExactBoundary(t *testing.T) {
	for _, tc := range []struct {
		stage    string
		ordinary bool
		want     []string
	}{
		{"namespace error", false, []string{"namespace"}},
		{"invalid workload", false, []string{"namespace"}},
		{"no context", false, []string{"namespace"}},
		{"context error", false, []string{"namespace", "context"}},
		{"unsafe context", false, []string{"namespace", "context"}},
		{"ome error", false, []string{"namespace", "context", "ome"}},
		{"ome nil", false, []string{"namespace", "context", "ome"}},
		{"ome error", true, []string{"namespace", "context", "ordinary ome"}},
		{"ome nil", true, []string{"namespace", "context", "ordinary ome"}},
		{"parent error", false, []string{"namespace", "context", "ome", "parent"}},
		{"parent cancel", false, []string{"namespace", "context", "ome", "parent"}},
		{"parent UID", false, []string{"namespace", "context", "ome", "parent"}},
		{"parent name", false, []string{"namespace", "context", "ome", "parent"}},
		{"parent namespace", false, []string{"namespace", "context", "ome", "parent"}},
		{"kube error", false, []string{"namespace", "context", "ome", "parent", "kube"}},
		{"kube nil", false, []string{"namespace", "context", "ome", "parent", "kube"}},
		{"no action runtime", true, []string{"namespace", "context", "ordinary ome", "parent", "ordinary kube"}},
		{"runtime error", false, []string{"namespace", "context", "ome", "parent", "kube", "runtime"}},
		{"runtime nil", false, []string{"namespace", "context", "ome", "parent", "kube", "runtime"}},
		{"config error", false, []string{"namespace", "context", "ome", "parent", "kube", "runtime", "config"}},
		{"config nil", false, []string{"namespace", "context", "ome", "parent", "kube", "runtime", "config"}},
	} {
		t.Run(tc.stage+map[bool]string{true: "/ordinary", false: "/owned"}[tc.ordinary], func(t *testing.T) {
			seed, parent := syncFixture(t)
			core := &syncStageFactory{Factory: seed, stage: tc.stage}
			contexts := &syncContextStageFactory{core}
			actions := &syncActionStageFactory{contexts}
			var selected factory.Factory = &syncOwnedStageFactory{actions}
			if tc.ordinary {
				selected = actions
			}
			if tc.stage == "no context" {
				selected = core
			}
			if tc.stage == "no action runtime" {
				selected = contexts
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seed.OME.(*omefake.Clientset).PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				core.calls = append(core.calls, "parent")
				got := parent.DeepCopy()
				switch tc.stage {
				case "parent error":
					return true, nil, errSyncAcquisitionPrivate
				case "parent cancel":
					cancel()
				case "parent UID":
					got.UID = ""
				case "parent name":
					got.Name = "other"
				case "parent namespace":
					got.Namespace = "other"
				}
				return true, got, nil
			})
			var out, stderr bytes.Buffer
			command := NewCmd(selected, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &stderr})
			command.SilenceErrors, command.SilenceUsage = true, true
			command.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o=json"})
			err := command.ExecuteContext(ctx)
			require.Error(t, err)
			require.Equal(t, tc.want, core.calls)
			require.Empty(t, out.String())
			require.Empty(t, stderr.String(), "refused acquisition must precede preview")
			require.NotContains(t, err.Error(), "PRIVATE")
			if tc.stage == "parent cancel" {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

type syncFailingRuntimeClient struct {
	ctrlclient.Client
	beforeGet func() error
}

func (c *syncFailingRuntimeClient) Get(ctx context.Context, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	if err := c.beforeGet(); err != nil {
		return err
	}
	return c.Client.Get(ctx, key, object, opts...)
}

// Required source failures before preview remain ordinary; fresh failure is a
// stale snapshot, except action cancellation must retain its canonical cause.
func TestRuntimeSyncSourceFailureAndFreshCancellationNeverSend(t *testing.T) {
	for _, tc := range []struct {
		name            string
		fresh, canceled bool
		code            int
	}{
		{"initial canceled", false, true, 1}, {"fresh unavailable", true, false, 3}, {"fresh canceled", true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, parent := syncFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			parentReads := 0
			f.OME.(*omefake.Clientset).PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				parentReads++
				return true, parent.DeepCopy(), nil
			})
			f.Runtime = &syncFailingRuntimeClient{Client: f.Runtime, beforeGet: func() error {
				if !tc.fresh || parentReads == 2 {
					if tc.canceled {
						cancel()
						return context.Canceled
					}
					return errSyncAcquisitionPrivate
				}
				return nil
			}}
			var out, stderr bytes.Buffer
			command := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &stderr})
			command.SilenceErrors, command.SilenceUsage = true, true
			command.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o=json"})
			err := command.ExecuteContext(ctx)
			require.Error(t, err)
			require.Equal(t, tc.code, exitcode.FromError(err))
			require.Equal(t, map[bool]int{true: 2, false: 1}[tc.fresh], parentReads)
			require.Empty(t, out.String())
			require.NotContains(t, err.Error()+stderr.String(), "PRIVATE")
			if tc.canceled {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

// A failed fresh parent read cannot authorize send or mask action cancellation.
func TestRuntimeSyncFreshParentFailureAndCancellationNeverSend(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{true: "canceled", false: "unavailable"}[canceled], func(t *testing.T) {
			f, parent := syncFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			f.OME.(*omefake.Clientset).PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == 2 {
					if canceled {
						cancel()
					}
					return true, nil, errSyncAcquisitionPrivate
				}
				return true, parent.DeepCopy(), nil
			})
			var out, stderr bytes.Buffer
			command := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &stderr})
			command.SilenceErrors, command.SilenceUsage = true, true
			command.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client"})
			err := command.ExecuteContext(ctx)
			require.Error(t, err)
			require.Equal(t, 2, reads)
			require.Empty(t, out.String())
			require.NotContains(t, err.Error()+stderr.String(), "PRIVATE")
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.Equal(t, 3, exitcode.FromError(err))
			}
		})
	}
}

var _ factory.Factory = (*syncStageFactory)(nil)
var _ factory.ActionReadClientsResolver = (*syncOwnedStageFactory)(nil)
