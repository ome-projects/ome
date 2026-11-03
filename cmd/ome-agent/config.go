package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/pkg/configutils"
	"sigs.k8s.io/ome/pkg/constants"
)

func configProvider(cli *cobra.Command) fx.Option {
	return fx.Provide(func() (*viper.Viper, error) {
		v := viper.GetViper()

		v.SetDefault("OME_AGENT", constants.AgentAppName)
		v.SetEnvPrefix(constants.AgentAppName)
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		v.AutomaticEnv()

		if err := v.BindPFlag("debug", cli.Flags().Lookup("debug")); err != nil {
			panic(err)
		}
		if configFilePath == "" {
			return nil, errors.New("no config file provided")
		}

		if err := configutils.ResolveAndMergeFile(v, configFilePath); err != nil {
			return nil, fmt.Errorf("cannot read config file: %w", err)
		}

		// Fix the issue where viper.UnmarshalKey only uses read config, neglects environment variables
		for _, key := range v.AllKeys() {
			v.Set(key, v.Get(key))
		}
		return v, nil
	})
}
