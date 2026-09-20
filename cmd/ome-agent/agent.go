package main

import (
	"context"
	"errors"
	"os"
	"sync/atomic"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var configFilePath string
var debug bool

// AgentModule represents a module that can be run by the agent framework
type AgentModule interface {
	Name() string
	ShortDescription() string
	LongDescription() string
	FxModules() []fx.Option

	// ConfigureCommand Allow agents to configure their commands (add subcommands, custom flags, etc.)
	ConfigureCommand(*cobra.Command)

	// Start is the default action when no subcommand is specified
	Start() error
}

// CreateAgentCommand creates a cobra command for an agent module
func CreateAgentCommand(module AgentModule) *cobra.Command {
	cmd := &cobra.Command{
		Use:   module.Name(),
		Short: module.ShortDescription(),
		Long:  module.LongDescription(),
		// We don't set Run here - let the module decide if it wants a default action
	}

	// Add common flags to persistent flags so they're available to subcommands
	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", "", "path to config file")
	cmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debug mode")

	// Let the module configure its command (add subcommands, set Run function, etc.)
	module.ConfigureCommand(cmd)

	return cmd
}

// runAgentCommand runs a specific command action for an agent
func runAgentCommand(cmd *cobra.Command, module AgentModule, action func() error) {
	options := []fx.Option{
		// Set up all config variables to viper
		configProvider(cmd, module),
	}

	// Add module-specific options
	options = append(options, module.FxModules()...)

	// Add lifecycle hooks
	options = append(options, fx.Invoke(func(lc fx.Lifecycle, l *zap.Logger, sh fx.Shutdowner) {
		var completed atomic.Bool
		lc.Append(
			fx.Hook{
				OnStart: func(context.Context) error {
					go func() {
						if err := action(); err != nil {
							l.Error(module.Name()+" encountered an error during execution", zap.Error(err))
							os.Exit(1)
						}
						completed.Store(true)
						if err := sh.Shutdown(); err != nil {
							l.Error("Failed to shutdown "+module.Name(), zap.Error(err))
						}
					}()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					// Fx treats SIGTERM as a successful shutdown by default. An
					// interrupted replica must not make its Job report completion.
					if module.Name() == "replica" && !completed.Load() {
						return errors.New("replication interrupted before completion")
					}
					return nil
				},
			})
	}))

	app := fx.New(fx.Options(options...))
	app.Run()
	err := app.Stop(context.Background())
	if err != nil {
		return
	}
}
