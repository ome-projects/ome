package factory

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestActionContextUsesSelectedOverrideWithoutRESTHost(t *testing.T) {
	flags := genericclioptions.NewConfigFlags(true)
	selected := "moirai-selected"
	flags.Context = &selected
	resolver, ok := New(flags).(interface{ ContextName() (string, error) })
	require.True(t, ok, "actions need the selected context, not a REST host")
	name, err := resolver.ContextName()
	require.NoError(t, err)
	require.Equal(t, "moirai-selected", name)
}

func TestActionContextRawLoaderIsLocalAndFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	config := clientcmdapi.NewConfig()
	config.CurrentContext = "synthetic-current"
	config.Contexts[config.CurrentContext] = &clientcmdapi.Context{Cluster: "local", AuthInfo: "none", Namespace: "prod"}
	config.Clusters["local"] = &clientcmdapi.Cluster{Server: "https://SECRET_PRIVATE_HOST.invalid"}
	require.NoError(t, clientcmd.WriteToFile(*config, path))
	flags := genericclioptions.NewConfigFlags(true)
	flags.KubeConfig = &path
	resolver := New(flags).(ContextResolver)
	name, err := resolver.ContextName()
	require.NoError(t, err)
	require.Equal(t, "synthetic-current", name)
	missing := filepath.Join(t.TempDir(), "missing")
	flags = genericclioptions.NewConfigFlags(true)
	flags.KubeConfig = &missing
	_, err = New(flags).(ContextResolver).ContextName()
	require.Error(t, err)
	require.NotContains(t, err.Error(), missing)
	_, err = (&defaultFactory{}).ContextName()
	require.Error(t, err)
	var absent *defaultFactory
	_, err = absent.ContextName()
	require.Error(t, err)
	_, err = (Static{}).ContextName()
	require.Error(t, err)
	name, err = (Static{Context: "synthetic-static"}).ContextName()
	require.NoError(t, err)
	require.Equal(t, "synthetic-static", name)
}
