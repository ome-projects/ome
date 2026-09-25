package factory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
)

func TestNamespaceDefaultsWhenUnset(t *testing.T) {
	flags := genericclioptions.NewConfigFlags(true)
	// Point at a kubeconfig that doesn't exist so only client defaulting applies.
	empty := ""
	flags.KubeConfig = &empty
	f := New(flags)
	ns, explicit, err := f.Namespace()
	require.NoError(t, err)
	assert.Equal(t, "default", ns)
	assert.False(t, explicit)
}

func TestNamespaceExplicitFlagWins(t *testing.T) {
	flags := genericclioptions.NewConfigFlags(true)
	team := "team-a"
	flags.Namespace = &team
	f := New(flags)
	ns, explicit, err := f.Namespace()
	require.NoError(t, err)
	assert.Equal(t, "team-a", ns)
	assert.True(t, explicit)
}

// TestRESTConfigBurstMatchesKubectlParity pins the client-side rate limits
// the CLI bumped to kubectl's own defaults: status/get fan out many small
// reads (pods, events, per-object queries) and must not trip client-side
// throttling on a cluster with lots of resources.
func TestRESTConfigBurstMatchesKubectlParity(t *testing.T) {
	flags := genericclioptions.NewConfigFlags(true)
	empty := ""
	flags.KubeConfig = &empty
	f := New(flags)
	cfg, err := f.RESTConfig()
	require.NoError(t, err)
	assert.Equal(t, float32(50), cfg.QPS)
	assert.Equal(t, 300, cfg.Burst)
}

// TestProtobufConfigNegotiatesProtobufWithoutMutatingInput exercises the
// pure copy-and-negotiate helper KubeClient() uses to get kubectl-style
// protobuf negotiation for core-type traffic, in isolation from any real
// REST config loading.
func TestProtobufConfigNegotiatesProtobufWithoutMutatingInput(t *testing.T) {
	base := &rest.Config{Host: "https://cluster.example:6443", Burst: 300, UserAgent: "operations-console/2 kubectl-ome/v1.2.3"}

	got := protobufConfig(base)

	assert.Equal(t, "application/vnd.kubernetes.protobuf,application/json", got.AcceptContentTypes)
	assert.Equal(t, "application/vnd.kubernetes.protobuf", got.ContentType)
	// Same server, same rate limits -- only content negotiation changed.
	assert.Equal(t, base.Host, got.Host)
	assert.Equal(t, base.Burst, got.Burst)
	assert.Equal(t, base.UserAgent, got.UserAgent)
	// The input must come back untouched: OMEClient/RuntimeClient share the
	// same underlying *rest.Config and must keep negotiating JSON (CRDs
	// don't serve protobuf).
	assert.Empty(t, base.AcceptContentTypes)
	assert.Empty(t, base.ContentType)
	assert.Equal(t, "operations-console/2 kubectl-ome/v1.2.3", base.UserAgent)
}

// TestKubeClientDoesNotLeakProtobufIntoSharedConfig is the end-to-end
// regression for protobufConfig's COPY requirement: building the real
// core-type client must not mutate the *rest.Config that OMEClient and
// RuntimeClient go on to share.
func TestKubeClientDoesNotLeakProtobufIntoSharedConfig(t *testing.T) {
	flags := genericclioptions.NewConfigFlags(true)
	empty := ""
	flags.KubeConfig = &empty
	f := New(flags)

	_, err := f.KubeClient()
	require.NoError(t, err)

	cfg, err := f.RESTConfig()
	require.NoError(t, err)
	assert.Empty(t, cfg.ContentType, "shared RESTConfig must stay JSON for OME/runtime clients")
	assert.Empty(t, cfg.AcceptContentTypes)
}

// TestOMEClientKeepsJSONConfig pins the requirement that the OME typed
// clientset never negotiates protobuf (CRDs don't serve it).
func TestOMEClientKeepsJSONConfig(t *testing.T) {
	flags := genericclioptions.NewConfigFlags(true)
	empty := ""
	flags.KubeConfig = &empty
	f := New(flags)

	_, err := f.OMEClient()
	require.NoError(t, err)

	cfg, err := f.RESTConfig()
	require.NoError(t, err)
	assert.Empty(t, cfg.ContentType)
	assert.Empty(t, cfg.AcceptContentTypes)
}
