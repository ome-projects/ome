package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Exercise the real Fx shutdown path in a subprocess because Run can call os.Exit.
func TestRunAgentCommandExit(t *testing.T) {
	const testCaseEnv = "OME_AGENT_EXIT_TEST_CASE"
	if testCase := os.Getenv(testCaseEnv); testCase != "" {
		module := &exitTestAgentModule{name: "replica"}
		if strings.HasPrefix(testCase, "other-agent-") {
			module.name = "serving-agent"
		}
		if testCase == "completed-with-stop-error" {
			module.options = []fx.Option{fx.Invoke(func(lc fx.Lifecycle) {
				lc.Append(fx.Hook{OnStop: func(context.Context) error {
					return errors.New("cleanup failed")
				}})
			})}
		}
		runAgentCommand(&cobra.Command{}, module, func() error {
			switch testCase {
			case "completed", "completed-with-stop-error":
				return nil
			case "failed", "other-agent-failed":
				return errors.New("replication failed")
			default:
				// Request the same zero-code shutdown as SIGTERM while the
				// action is unfinished, without racing OS signal registration.
				if err := module.shutdown(); err != nil {
					return err
				}
				select {}
			}
		})
		return
	}

	for _, tt := range []struct {
		name     string
		wantCode int
	}{
		{name: "completed", wantCode: 0},
		{name: "failed", wantCode: 1},
		{name: "replica-interrupted", wantCode: 1},
		{name: "other-agent-interrupted", wantCode: 0},
		{name: "other-agent-failed", wantCode: 1},
		{name: "completed-with-stop-error", wantCode: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRunAgentCommandExit$")
			cmd.Env = append(os.Environ(), testCaseEnv+"="+tt.name)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("agent did not exit: %v\n%s", ctx.Err(), output)
			}
			if err != nil && cmd.ProcessState == nil {
				t.Fatal(err)
			}
			if got := cmd.ProcessState.ExitCode(); got != tt.wantCode {
				t.Fatalf("exit code = %d, want %d\n%s", got, tt.wantCode, output)
			}
		})
	}
}

func TestRunAgentCommandCompletesBeforeStopping(t *testing.T) {
	for _, name := range []string{"replica", "serving-agent"} {
		t.Run(name, func(t *testing.T) {
			var actionCalls, stopCalls atomic.Int32
			module := &exitTestAgentModule{name: name}
			module.options = []fx.Option{fx.Invoke(func(lc fx.Lifecycle) {
				lc.Append(fx.Hook{OnStop: func(context.Context) error {
					assert.Equal(t, int32(1), actionCalls.Load(), "action must finish before cleanup starts")
					stopCalls.Add(1)
					return nil
				}})
			})}

			runAgentCommand(&cobra.Command{}, module, func() error {
				actionCalls.Add(1)
				return nil
			})

			assert.Equal(t, int32(1), actionCalls.Load(), "action must execute exactly once")
			assert.Equal(t, int32(1), stopCalls.Load(), "command must wait for cleanup before returning")
		})
	}
}

type exitTestAgentModule struct {
	name     string
	shutdown func() error
	options  []fx.Option
}

func (m *exitTestAgentModule) Name() string                    { return m.name }
func (m *exitTestAgentModule) ShortDescription() string        { return "" }
func (m *exitTestAgentModule) LongDescription() string         { return "" }
func (m *exitTestAgentModule) ConfigureCommand(*cobra.Command) {}
func (m *exitTestAgentModule) Start() error                    { return nil }
func (m *exitTestAgentModule) FxModules() []fx.Option {
	return append([]fx.Option{
		fx.Provide(zap.NewNop),
		fx.NopLogger,
		fx.Invoke(func(sh fx.Shutdowner) { m.shutdown = func() error { return sh.Shutdown() } }),
	}, m.options...)
}
