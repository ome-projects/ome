package runtime

import (
	"bytes"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

func TestHistoryCommandContract(t *testing.T) {
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}}
	parent := NewCmd(factory.Static{}, streams)

	cmd, _, err := parent.Find([]string{"history"})
	require.NoError(t, err)
	require.NotSame(t, parent, cmd)
	assert.Equal(t, "history INFERENCESERVICE", cmd.Use)
	assert.Equal(t, "Show bounded runtime revision history for an InferenceService", cmd.Short)
	assert.Equal(t, `Shows allowlisted, retention-bounded ControllerRevision history for an
InferenceService's resolved runtime.

The command reads at most two pages and 1,000 revisions, and reports whether
the observed window is complete or truncated. Raw runtime specs,
ControllerRevision data, status messages, resource versions, and
synchronization tokens are never printed.

The compact table keeps each line within 80 columns. WINDOW is
OBS/BOUND/SEEN/ASKED: C=complete, P=partial, U=unavailable, N=not requested;
B=retention-bounded and I=incomplete. ROLES uses A=active, Q=requested,
R=reported, and H=history. CHECK uses OK, BAD, or ?; LIVE uses MATCH, DIFF,
AMB, or ?. ISSUES uses R/G for revision/global report counts. Long revision
names use PREFIX#DIGEST, with an eight-hex display-only digest of the complete
revision name. It is not a Kubernetes identity or runtime hash. Use -o wide
for full revision names, timestamps, hashes, sources, and exact issue codes.`, cmd.Long)

	output := cmd.Flags().Lookup("output")
	require.NotNil(t, output)
	assert.Equal(t, "o", output.Shorthand)
	assert.Equal(t, "table", output.DefValue)
	assert.Equal(t, "Output format: table, wide, json or yaml", output.Usage)
	omeNamespace := cmd.Flags().Lookup("ome-namespace")
	require.NotNil(t, omeNamespace)
	assert.Equal(t, "ome", omeNamespace.DefValue)
	assert.Equal(t, "Namespace where the OME control plane is installed", omeNamespace.Usage)
	assert.Nil(t, cmd.Flags().Lookup("namespace"), "namespace must remain inherited from the root")

	wantLocalFlags := map[string]bool{"ome-namespace": true, "output": true}
	cmd.LocalNonPersistentFlags().VisitAll(func(flag *pflag.Flag) {
		assert.Truef(t, wantLocalFlags[flag.Name], "unexpected local flag %q", flag.Name)
		delete(wantLocalFlags, flag.Name)
	})
	assert.Empty(t, wantLocalFlags)
}

func TestHistoryHelpOutput(t *testing.T) {
	var out bytes.Buffer
	parent := NewCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &out, ErrOut: &out,
	})
	parent.SetOut(&out)
	parent.SetErr(&out)
	parent.SetArgs([]string{"history", "--help"})
	require.NoError(t, parent.Execute())
	assert.Equal(t, `Shows allowlisted, retention-bounded ControllerRevision history for an
InferenceService's resolved runtime.

The command reads at most two pages and 1,000 revisions, and reports whether
the observed window is complete or truncated. Raw runtime specs,
ControllerRevision data, status messages, resource versions, and
synchronization tokens are never printed.

The compact table keeps each line within 80 columns. WINDOW is
OBS/BOUND/SEEN/ASKED: C=complete, P=partial, U=unavailable, N=not requested;
B=retention-bounded and I=incomplete. ROLES uses A=active, Q=requested,
R=reported, and H=history. CHECK uses OK, BAD, or ?; LIVE uses MATCH, DIFF,
AMB, or ?. ISSUES uses R/G for revision/global report counts. Long revision
names use PREFIX#DIGEST, with an eight-hex display-only digest of the complete
revision name. It is not a Kubernetes identity or runtime hash. Use -o wide
for full revision names, timestamps, hashes, sources, and exact issue codes.

Usage:
  runtime history INFERENCESERVICE [flags]

Flags:
  -h, --help                   help for history
      --ome-namespace string   Namespace where the OME control plane is installed (default "ome")
  -o, --output string          Output format: table, wide, json or yaml (default "table")
`, out.String())
}
