package main

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/internal/ome-agent/replica"
	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

// The replica command's storage providers: the source and target object stores
// and PVC file systems, each built only when its side is enabled in the config.

type OCIOSDataStoreConfigWrapper struct {
	fx.Out

	OCIOSDataStoreConfig *ociobjectstore.Config `group:"OCIOSDataStoreConfigs"`
}

func OCIOSDataStoreListProvider() fx.Option {
	return fx.Provide(
		provideSourceOCIOSDataSourceConfig,
		provideTargetOCIOSDataStoreConfig,
		ociobjectstore.ProvideListOfOCIOSDataStoreWithAppParams,
	)
}

func provideSourceOCIOSDataSourceConfig(
	logger logging.Interface,
	v *viper.Viper) (OCIOSDataStoreConfigWrapper, error) {
	sourceOCIEnabled := v.GetBool("source.oci.enabled")
	if !sourceOCIEnabled {
		return OCIOSDataStoreConfigWrapper{}, nil
	}

	sourceOCIOSDataStoreConfig := &ociobjectstore.Config{}
	if err := v.UnmarshalKey("source.oci", sourceOCIOSDataStoreConfig); err != nil {
		return OCIOSDataStoreConfigWrapper{}, fmt.Errorf("error occurred when unmarshalling key source: %+v", err)
	}
	sourceOCIOSDataStoreConfig.AnotherLogger = logger
	sourceOCIOSDataStoreConfig.Name = replica.SourceStorageConfigKeyName

	return OCIOSDataStoreConfigWrapper{
		OCIOSDataStoreConfig: sourceOCIOSDataStoreConfig,
	}, nil
}

func provideTargetOCIOSDataStoreConfig(logger logging.Interface, v *viper.Viper) (OCIOSDataStoreConfigWrapper, error) {
	targetOCIEnabled := v.GetBool("target.oci.enabled")
	if !targetOCIEnabled {
		return OCIOSDataStoreConfigWrapper{}, nil
	}

	targetOCIOSDataStoreConfig := &ociobjectstore.Config{}
	if err := v.UnmarshalKey("target.oci", targetOCIOSDataStoreConfig); err != nil {
		return OCIOSDataStoreConfigWrapper{}, fmt.Errorf("error occurred when unmarshalling key target: %+v", err)
	}

	targetOCIOSDataStoreConfig.AnotherLogger = logger
	targetOCIOSDataStoreConfig.Name = replica.TargetStorageConfigKeyName

	return OCIOSDataStoreConfigWrapper{
		OCIOSDataStoreConfig: targetOCIOSDataStoreConfig,
	}, nil
}

func PVCFileSystemProviders() fx.Option {
	return fx.Provide(
		fx.Annotate(
			provideSourcePVCFileSystem,
			fx.ResultTags(`name:"source_pvc_fs"`),
		),
		fx.Annotate(
			provideTargetPVCFileSystem,
			fx.ResultTags(`name:"target_pvc_fs"`),
		),
	)
}

func provideSourcePVCFileSystem(v *viper.Viper) *afero.OsFs {
	sourcePVCEnabled := v.GetBool("source.pvc.enabled")
	if !sourcePVCEnabled {
		return nil
	}
	return afero.NewOsFs().(*afero.OsFs)
}

func provideTargetPVCFileSystem(v *viper.Viper) *afero.OsFs {
	targetPVCEnabled := v.GetBool("target.pvc.enabled")
	if !targetPVCEnabled {
		return nil
	}
	return afero.NewOsFs().(*afero.OsFs)
}
