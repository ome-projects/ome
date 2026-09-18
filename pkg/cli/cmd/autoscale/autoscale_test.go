package autoscale

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

func TestNewCmdRegistersAutoscaleSubcommands(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
	})

	assert.Equal(t, "autoscale", cmd.Use)
	found, args, err := cmd.Find([]string{"status"})
	require.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "autoscale status", found.CommandPath())
	assert.Equal(t, "status INFERENCESERVICE", found.Use)

	found, args, err = cmd.Find([]string{"explain"})
	require.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "autoscale explain", found.CommandPath())
	assert.Equal(t, "explain INFERENCESERVICE", found.Use)
}

func TestAutoscaleHelpExplainsConfigurationAndEvidence(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--help"})

	require.NoError(t, cmd.Execute())
	assert.Equal(t, `Inspect autoscaling configuration and evidence

Usage:
  autoscale [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  explain     Explain effective and reported autoscaling
  help        Help about any command
  status      Show controller-reported autoscaling status

Flags:
  -h, --help   help for autoscale

Use "autoscale [command] --help" for more information about a command.
`, output.String())
}

func TestExplainHelpDefinesItsEvidenceBoundary(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"explain", "--help"})

	require.NoError(t, cmd.Execute())
	assert.Equal(t, `Explains declared autoscaling from the controller-selected active runtime configuration
and compares it with evidence reported on the InferenceService parent.

The command does not query child autoscaler or workload objects. A matching
report describes one bound API snapshot; it does not prove rollout convergence
or wall-clock freshness. Raw runtime specifications, autoscaler payloads,
resource versions, status messages, and synchronization tokens are never printed.

Usage:
  autoscale explain INFERENCESERVICE [flags]

Flags:
  -h, --help                   help for explain
      --ome-namespace string   Namespace where the OME control plane is installed (default "ome")
  -o, --output string          Output format: table, json or yaml (default "table")
`, output.String())
}

func TestStatusHelpDefinesTheReportedEvidenceBoundary(t *testing.T) {
	var output bytes.Buffer
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	})
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"status", "--help"})

	require.NoError(t, cmd.Execute())
	assert.Equal(t, `Show autoscaling evidence reported on the InferenceService parent.
By default, this performs no HPA, KEDA, Deployment, or InferenceReplica reads.
--live-scale adds exact parent-selected InferenceReplica and /scale reads only.
--live-scaler adds exact HPA or KEDA ScaledObject reads for OME-managed scalers;
IR-backed targets first require a verified exact InferenceReplica read.
KEDA's generated HPA and reconciliation freshness are not observed by this flag.
SCALER-GEN=Matched means only HPA observedGeneration equals HPA generation.
STATE remains parent-reported; SCALER-EVIDENCE is separate live evidence.
Count equality is not proof of ongoing freshness or scaler health.
The compact table abbreviates InferenceReplica as IR and formats LAST-SCALE
as UTC MonDD HH:MMZ. ISSUES uses compact aliases:
  UnknownComp=UnknownComponentStatus
  NoAutoscaler=AutoscalerNotReported
  NoTarget=ScaleTargetNotReported
  BadClass=ClassInvalid
  BadManager=ManagedByInvalid
  OwnerMismatch=OwnershipMismatch
  BadSpecSource=SpecSourceInvalid
  UnexpectedEv=UnexpectedScalerEvidence
  ReplicaAmbig=ReplicaEvidenceAmbiguous
  BadReplica=ReplicaEvidenceInvalid
  BadTarget=ScaleTargetInvalid
  BadCondition=ConditionInvalid
  CondConflict=ConditionConflict
Unknown or future issue codes use X# followed by a stable 10-digit hex digest.
Use -o wide for exact issue codes, complete identities, and timestamps.

Usage:
  autoscale status INFERENCESERVICE [flags]

Flags:
  -h, --help            help for status
      --live-scale      Compare parent counts with exact selected InferenceReplica and /scale reads
      --live-scaler     Inspect exact selected HPA or KEDA ScaledObject (additional read permission)
  -o, --output string   Output format: table, wide, json or yaml (default "table")
`, output.String())
}
