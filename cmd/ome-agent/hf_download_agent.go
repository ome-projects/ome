package main

import (
	"context"
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/xet"
)

// hfDownload downloads one model snapshot from the HuggingFace Hub with the
// settings the config file names. fx fills the fields.
type hfDownload struct {
	fx.In
	Client *xet.Client
	Viper  *viper.Viper
	Logger logging.Interface `name:"another_log"`
}

// Start downloads the configured model and returns when it is on disk.
func (h hfDownload) Start() error {
	modelName := h.Viper.GetString("model_name")
	localPath := h.Viper.GetString("local_path")
	revision := h.Viper.GetString("revision")
	repoType := h.Viper.GetString("repo_type")

	if h.Logger != nil {
		h.Logger.Infof("🤗 Starting HuggingFace model download")
		h.Logger.Infof("   Model: %s", modelName)
		h.Logger.Infof("   Revision: %s (defaults to 'main' if empty)", revision)
		h.Logger.Infof("   Target: %s", localPath)
		h.Logger.Infof("   Repository Type: %s (defaults to 'model' if empty)", repoType)
	}

	if repoType == "" {
		repoType = xet.RepoTypeModel
	}
	if revision == "" {
		revision = "main"
	}

	req := &xet.SnapshotRequest{
		RepoID:   modelName,
		RepoType: repoType,
		Revision: revision,
		LocalDir: localPath,
	}

	downloadPath, err := h.Client.DownloadSnapshotWithContext(context.Background(), req)
	if err != nil {
		if h.Logger != nil {
			h.Logger.Errorf("Failed to download model %s: %v", modelName, err)
		}
		return fmt.Errorf("model download failed: %w", err)
	}

	if h.Logger != nil {
		h.Logger.Infof("Successfully downloaded model %s", modelName)
		h.Logger.Infof("Downloaded to: %s", downloadPath)
	}

	return nil
}
