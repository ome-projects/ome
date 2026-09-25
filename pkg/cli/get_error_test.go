package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestRootGetRendersOneSafeAPIError(t *testing.T) {
	secret := strings.Join([]string{"xoxb", "123456789012", "123456789012", "abcdefghijklmnopqrstuvwxyz"}, "-")
	raw := kerrors.NewForbidden(
		schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
		"chat",
		errors.New("admission webhook returned "+secret),
	)
	client := omefake.NewSimpleClientset()
	client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, raw
	})

	var stdout, stderr bytes.Buffer
	streams := genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr}
	root := NewRootCmdWithFactory(factory.Static{OME: client, NS: "team-a"}, streams)
	root.SetArgs([]string{"get", "isvc", "chat", "-o", "json"})

	code := ExecuteCommand(root, &stderr)
	require.Equal(t, exitcode.GeneralError, code)
	assert.Empty(t, stdout.String(), "failed structured reads must be all-or-nothing")
	assert.Equal(t, "error: get inferenceservices team-a/chat: Forbidden\n", stderr.String())
	assert.NotContains(t, stderr.String(), secret)
	require.Len(t, client.Actions(), 1)
}
