package main

import (
	"context"
	"errors"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var configFilePath string
var debug bool

// command is one subcommand of the agent binary: its name and help texts, the
// flags of its own, the fx options that build what it runs, and the agent it runs.
type command struct {
	name    string
	short   string
	long    string
	flags   func(*cobra.Command)
	modules []fx.Option
	runs    agent
}

// agent is what a command runs once fx has built it.
type agent struct {
	populate fx.Option    // fills the value from the fx graph
	start    func() error // runs the value to completion
}

// fxBuilt is the agent of type T that fx builds from the command's modules.
func fxBuilt[T interface{ Start() error }]() agent {
	var a T
	return agent{
		populate: fx.Populate(&a),
		start:    func() error { return a.Start() },
	}
}

// cobraCommand builds the cobra command of the row: the shared config and debug
// flags, the row's own flags, and a run that ends the process when the agent fails.
func (c command) cobraCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   c.name,
		Short: c.short,
		Long:  c.long,
	}
	// Persistent, so a subcommand a row adds sees them too.
	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", "", "path to config file")
	cmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debug mode")
	if c.flags != nil {
		c.flags(cmd)
	}
	cmd.Run = func(cmd *cobra.Command, _ []string) {
		if err := runAgentCommand(cmd, c); err != nil {
			exitWithError(err)
		}
	}
	return cmd
}

// runAgentCommand builds the row's fx application, runs its agent to completion
// and returns the error that stopped it: fx could not build the graph, or the
// agent failed. The application stops in order before the error is returned.
func runAgentCommand(cmd *cobra.Command, c command) error {
	done := make(chan error, 1)
	options := []fx.Option{configProvider(cmd)}
	options = append(options, c.modules...)
	options = append(options, c.runs.populate, fx.Invoke(func(lc fx.Lifecycle, l *zap.Logger, sh fx.Shutdowner) {
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error {
				go func() {
					err := c.runs.start()
					if err != nil {
						l.Error(c.name+" encountered an error during execution", zap.Error(err))
					}
					done <- err
					if err := sh.Shutdown(); err != nil {
						l.Error("Failed to shutdown "+c.name, zap.Error(err))
					}
				}()
				return nil
			},
		})
	}))

	app := fx.New(options...)
	if err := app.Err(); err != nil {
		return err
	}
	app.Run()
	select {
	case err := <-done:
		return err
	default:
		// A signal stopped the application before the agent finished. An
		// interrupted replica must not let its Job report completion.
		if c.name == "replica" {
			return errors.New("replication interrupted before completion")
		}
		return nil
	}
}
