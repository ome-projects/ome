package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// testAgent records that it ran and returns the error it was given.
type testAgent struct {
	ran bool
	err error
}

func (a *testAgent) Start() error {
	a.ran = true
	return a.err
}

// testRow is a command that runs a, with no fx log output and no logger of its own.
func testRow(a *testAgent, modules ...fx.Option) command {
	return command{
		name:    "mock-agent",
		short:   "Mock Agent Short Description",
		long:    "Mock Agent Long Description",
		modules: append([]fx.Option{fx.NopLogger, fx.Provide(zap.NewNop), fx.Supply(a)}, modules...),
		runs:    fxBuilt[*testAgent](),
	}
}

// execute runs the row's cobra command with args and returns its error and
// everything cobra printed.
func execute(t *testing.T, row command, args ...string) (error, string) {
	t.Helper()
	t.Cleanup(func() { configFilePath = "" })
	var out bytes.Buffer
	cmd := row.cobraCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	return cmd.Execute(), out.String()
}

// run parses args on the row's cobra command and runs the agent the way the
// command's Run does, returning the error instead of ending the process.
func run(t *testing.T, row command, args ...string) error {
	t.Helper()
	t.Cleanup(func() { configFilePath = "" })
	cmd := row.cobraCommand()
	require.NoError(t, cmd.ParseFlags(args))
	return runAgentCommand(cmd, row)
}

// configFile writes a minimal config file and returns its path.
func configFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("debug: false\n"), 0o600))
	return path
}

func TestCobraCommandCarriesTheRowWordsAndFlags(t *testing.T) {
	cmd := testRow(&testAgent{}).cobraCommand()

	assert.Equal(t, "mock-agent", cmd.Use)
	assert.Equal(t, "Mock Agent Short Description", cmd.Short)
	assert.Equal(t, "Mock Agent Long Description", cmd.Long)
	assert.NotNil(t, cmd.Run)

	configFlag := cmd.PersistentFlags().Lookup("config")
	require.NotNil(t, configFlag)
	assert.Equal(t, "c", configFlag.Shorthand)

	debugFlag := cmd.PersistentFlags().Lookup("debug")
	require.NotNil(t, debugFlag)
	assert.Equal(t, "d", debugFlag.Shorthand)
}

func TestCobraCommandAddsTheRowFlags(t *testing.T) {
	row := testRow(&testAgent{})
	row.flags = func(cmd *cobra.Command) {
		cmd.Flags().String("extra", "", "a flag of the row's own")
		cmd.AddCommand(&cobra.Command{
			Use:   "subcommand",
			Short: "Subcommand Short Description",
			Run:   func(*cobra.Command, []string) {},
		})
	}

	cmd := row.cobraCommand()

	assert.NotNil(t, cmd.Flags().Lookup("extra"))
	require.Len(t, cmd.Commands(), 1)
	assert.Equal(t, "subcommand", cmd.Commands()[0].Use)
	assert.Equal(t, "Subcommand Short Description", cmd.Commands()[0].Short)
}

func TestRunAgentCommandReturnsTheBuildError(t *testing.T) {
	a := &testAgent{}
	// The agent's constructor needs the config, and no config file is given.
	row := testRow(a, fx.Provide(func(*viper.Viper) struct{} { return struct{}{} }), fx.Invoke(func(struct{}) {}))

	err := run(t, row)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no config file provided")
	assert.False(t, a.ran)
}

func TestRunAgentCommandReturnsTheAgentError(t *testing.T) {
	a := &testAgent{err: errors.New("start error")}

	err := run(t, testRow(a), "--config", configFile(t))

	assert.True(t, a.ran)
	assert.EqualError(t, err, "start error")
}

func TestRunAgentCommandReturnsNilWhenTheAgentFinishes(t *testing.T) {
	a := &testAgent{}

	err := run(t, testRow(a), "--config", configFile(t))

	assert.True(t, a.ran)
	assert.NoError(t, err)
}

func TestBadFlagStillPrintsTheUsage(t *testing.T) {
	a := &testAgent{}

	err, printed := execute(t, testRow(a), "--no-such-flag")

	assert.EqualError(t, err, "unknown flag: --no-such-flag")
	assert.False(t, a.ran)
	assert.Contains(t, printed, "Error: unknown flag: --no-such-flag")
	assert.Contains(t, printed, "Usage:")
}
