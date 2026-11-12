package replica

import (
	"fmt"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/internal/ome-agent/replica/replicator"
	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/utils/storage"
)

// storagePair is the storage kinds of a replication's source and target.
type storagePair struct {
	source, target storage.StorageType
}

// replicators holds the replicator of every supported storage pair.
var replicators = map[storagePair]func(logging.Interface, replicator.Dependencies, common.ReplicationInput) replicator.Replicator{
	{storage.StorageTypeHuggingFace, storage.StorageTypeOCI}: func(logger logging.Interface, dependencies replicator.Dependencies, input common.ReplicationInput) replicator.Replicator {
		return &replicator.HFToOCIReplicator{Logger: logger, Dependencies: dependencies, ReplicationInput: input}
	},
	{storage.StorageTypeOCI, storage.StorageTypeOCI}: func(logger logging.Interface, dependencies replicator.Dependencies, input common.ReplicationInput) replicator.Replicator {
		return &replicator.OCIToOCIReplicator{Logger: logger, Dependencies: dependencies, ReplicationInput: input}
	},
	{storage.StorageTypePVC, storage.StorageTypeOCI}: func(logger logging.Interface, dependencies replicator.Dependencies, input common.ReplicationInput) replicator.Replicator {
		return &replicator.PVCToOCIReplicator{Logger: logger, Dependencies: dependencies, ReplicationInput: input}
	},
	{storage.StorageTypeHuggingFace, storage.StorageTypePVC}: func(logger logging.Interface, dependencies replicator.Dependencies, input common.ReplicationInput) replicator.Replicator {
		return &replicator.HFToPVCReplicator{Logger: logger, Dependencies: dependencies, ReplicationInput: input}
	},
	{storage.StorageTypeOCI, storage.StorageTypePVC}: func(logger logging.Interface, dependencies replicator.Dependencies, input common.ReplicationInput) replicator.Replicator {
		return &replicator.OCIToPVCReplicator{Logger: logger, Dependencies: dependencies, ReplicationInput: input}
	},
	{storage.StorageTypePVC, storage.StorageTypePVC}: func(logger logging.Interface, dependencies replicator.Dependencies, input common.ReplicationInput) replicator.Replicator {
		return &replicator.PVCToPVCReplicator{Logger: logger, Dependencies: dependencies, ReplicationInput: input}
	},
}

// NewReplicator picks the replicator of the agent's storage pair and hands it the agent's
// dependencies.
func NewReplicator(r *ReplicaAgent) (replicator.Replicator, error) {
	pair := storagePair{r.ReplicationInput.SourceStorageType, r.ReplicationInput.TargetStorageType}
	build, ok := replicators[pair]
	if !ok {
		return nil, fmt.Errorf("unsupported replication: %s → %s", pair.source, pair.target)
	}
	return build(r.Logger, dependenciesFrom(r.Config), r.ReplicationInput), nil
}

// dependenciesFrom collects what the replicators need from the agent's config. A hub client
// that is not configured stays nil rather than becoming a non-nil interface around a nil pointer.
func dependenciesFrom(config Config) replicator.Dependencies {
	dependencies := replicator.Dependencies{
		LocalPath:            config.LocalPath,
		Connections:          config.NumConnections,
		Checksum:             config.Target.ChecksumConfig,
		DownloadTimeout:      config.HFDownloadTimeout,
		StaleProgressTimeout: config.HFDownloadStaleProgressTimeout,
		SourceStore:          config.Source.OCIOSDataStore,
		TargetStore:          config.Target.OCIOSDataStore,
		SourceFiles:          config.Source.PVCFileSystem,
		TargetFiles:          config.Target.PVCFileSystem,
		Transfer:             replicator.MultipartTransfer{},
	}
	if config.Source.HubClient != nil {
		dependencies.HubClient = config.Source.HubClient
	}
	return dependencies
}
