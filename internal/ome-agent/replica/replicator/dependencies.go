package replicator

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/ome/pkg/xet"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/afero"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

// Dependencies is everything a replicator needs besides its two endpoints, built once from the
// agent's config. A replicator reads the fields its storage pair uses and ignores the rest.
type Dependencies struct {
	// LocalPath is the directory the agent works in: the PVC mount for a PVC endpoint,
	// scratch space otherwise.
	LocalPath string
	// Connections is how many objects move at the same time.
	Connections int
	// Checksum says whether an uploaded object carries its checksum as metadata, and with
	// which algorithm.
	Checksum *common.ChecksumConfig

	// HubClient downloads HuggingFace snapshots. DownloadTimeout bounds a whole download and
	// StaleProgressTimeout one that stops making progress; zero leaves that guard off.
	HubClient            HubClient
	DownloadTimeout      time.Duration
	StaleProgressTimeout time.Duration

	// SourceStore and TargetStore are the object stores of the endpoints that are OCI buckets.
	SourceStore *ociobjectstore.OCIOSDataStore
	TargetStore *ociobjectstore.OCIOSDataStore
	// SourceFiles and TargetFiles are the file systems of the endpoints that are PVCs.
	SourceFiles *afero.OsFs
	TargetFiles *afero.OsFs

	// Transfer moves one object between an object store and the local disk.
	Transfer ObjectTransfer
}

// HubClient is what a HuggingFace download needs from the hub client: the snapshot download
// and its progress callbacks. *xet.Client is the production one.
type HubClient interface {
	DownloadSnapshotWithContext(ctx context.Context, req *xet.SnapshotRequest) (string, error)
	SetProgressHandler(handler xet.ProgressHandler, throttle time.Duration) error
	DisableProgress() error
}

// ObjectTransfer moves one object between an OCI object store and the local disk.
// MultipartTransfer is the production one; tests record the calls instead.
type ObjectTransfer interface {
	// Download copies the object into the directory, under its object name.
	Download(store *ociobjectstore.OCIOSDataStore, object ociobjectstore.ObjectURI, directory string) error
	// Upload copies the file to the object.
	Upload(store *ociobjectstore.OCIOSDataStore, object ociobjectstore.ObjectURI, filePath string) error
}

// MultipartTransfer moves objects with the store's multipart download and upload.
type MultipartTransfer struct{}

func (MultipartTransfer) Download(store *ociobjectstore.OCIOSDataStore, object ociobjectstore.ObjectURI, directory string) error {
	if store == nil {
		return fmt.Errorf("source ociOSDataStore is nil")
	}

	err := store.MultipartDownload(object, directory,
		ociobjectstore.WithChunkSize(DefaultDownloadChunkSizeInMB),
		ociobjectstore.WithThreads(DefaultDownloadThreads))
	if err != nil {
		store.Config.AnotherLogger.Errorf("Failed to download object %s: %+v", object.ObjectName, err)
		return err
	}
	return nil
}

func (MultipartTransfer) Upload(store *ociobjectstore.OCIOSDataStore, object ociobjectstore.ObjectURI, filePath string) error {
	if store == nil {
		return fmt.Errorf("target ociOSDataStore is nil")
	}

	err := store.MultipartFileUpload(filePath, object, DefaultUploadChunkSizeInMB, DefaultUploadThreads)
	if err != nil {
		store.Config.AnotherLogger.Errorf("Failed to upload %s: %+v", object.ObjectName, err)
		return err
	}
	return nil
}
