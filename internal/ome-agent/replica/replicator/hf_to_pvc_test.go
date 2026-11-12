package replicator

import (
	"context"
	"fmt"
	"testing"

	"sigs.k8s.io/ome/pkg/xet"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	testingPkg "sigs.k8s.io/ome/pkg/testing"
)

func TestHFToPVCReplicator_Replicate_Success(t *testing.T) {
	hub := &fakeHubClient{download: func(_ context.Context, req *xet.SnapshotRequest) (string, error) {
		if req.LocalDir != "/mnt/data/meta/lama-Guard-4-12B" {
			t.Errorf("unexpected path: got %s, want /mnt/data/meta/lama-Guard-4-12B", req.LocalDir)
		}
		return "/mock/downloaded_path", nil
	}}

	replicator := &HFToPVCReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath: "/mnt/data/",
			HubClient: hub,
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{
				Namespace:  "huggingface",
				BucketName: "meta-llama/Llama-Guard-4-12B",
				Prefix:     "main",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "amaaaaaax7756raaolxvbyk7toite23tbfkarxhiipv6jdy3tgwjjq4l6zma",
				BucketName: "model-pvc",
				Prefix:     "meta/lama-Guard-4-12B",
			},
		},
	}

	err := replicator.Replicate([]common.ReplicationObject{})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if hub.downloads != 1 {
		t.Errorf("expected one snapshot download, got %d", hub.downloads)
	}
}

func TestHFToPVCReplicator_Replicate_Failure(t *testing.T) {
	hub := &fakeHubClient{download: func(context.Context, *xet.SnapshotRequest) (string, error) {
		return "", fmt.Errorf("mock error")
	}}

	replicator := &HFToPVCReplicator{
		Logger: testingPkg.SetupMockLogger(),
		Dependencies: Dependencies{
			LocalPath: t.TempDir(),
			HubClient: hub,
		},
		ReplicationInput: common.ReplicationInput{
			Source: ociobjectstore.ObjectURI{
				Namespace:  "huggingface",
				BucketName: "fake-hf-model",
				Prefix:     "main",
			},
			Target: ociobjectstore.ObjectURI{
				Namespace:  "amaaaaaax7756raaolxvbyk7toite23tbfkarxhiipv6jdy3tgwjjq4l6zma",
				BucketName: "model-pvc",
				Prefix:     "models",
			},
		},
	}

	err := replicator.Replicate([]common.ReplicationObject{})
	if err == nil {
		t.Error("expected error, got nil")
	}
}
