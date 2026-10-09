package artifactinventory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ReplicationJob is an optional external CRD, not part of OME's API scheme.
// Decode only the fields needed for this adapter; do not install or own its CRD.
type replicationJob struct {
	metav1.ObjectMeta `json:"metadata"`
	Spec              struct {
		Destination *v1beta1.StorageSpec `json:"destination"`
	} `json:"spec"`
	Status struct {
		Status   string `json:"status"`
		Observed struct {
			SourceArtifactSizeBytes *int64 `json:"sourceArtifactSizeBytes"`
		} `json:"observed"`
	} `json:"status"`
}

func (b *Builder) readJob(ctx context.Context, name string) (*replicationJob, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "ome.io", Version: "v1beta1", Kind: "ReplicationJob"})
	key := client.ObjectKey{Namespace: "default", Name: name}
	if err := b.reader.Get(ctx, key, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read ReplicationJob %s: %w", key, err)
	}
	// JSON decoding keeps byte counts as int64, including values larger than
	// a float64 can represent exactly.
	raw, err := obj.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("encode ReplicationJob %s: %w", key, err)
	}
	var job replicationJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, fmt.Errorf("decode ReplicationJob %s: %w", key, err)
	}
	return &job, nil
}

// estimateSize validates every CR's source even when they share a Job. The
// cache avoids duplicate GETs; it does not let one CR inherit another's match.
func (b *Builder) estimateSize(ctx context.Context, group *artifactGroup, cache map[string]*replicationJob) error {
	jobNames := make(map[string]bool)
	var size *int64
	for _, item := range group.models {
		m := item.model
		job, fetched := cache[m.Name]
		if !fetched {
			var err error
			job, err = b.readJob(ctx, m.Name)
			if err != nil {
				return err
			}
			cache[m.Name] = job
		}
		jobNames[m.Name] = true
		issue := checkSize(m, job)
		if issue != nil {
			issue.ModelKey = item.key
			group.artifact.Issues = append(group.artifact.Issues, *issue)
			log.FromContext(ctx).V(1).Info("Artifact size unavailable", "modelKey", item.key,
				"job", "default/"+m.Name, "reason", issue.Code)
			continue
		}
		size = job.Status.Observed.SourceArtifactSizeBytes
	}

	// A shared directory is counted once, never once per CR. Distinct Jobs do
	// not supply a file-set manifest: equal sizes or revisions alone cannot
	// establish that their copy configurations produced identical contents.
	if len(jobNames) > 1 {
		group.artifact.Issues = append(group.artifact.Issues, Issue{
			Code: "multipleJobs", Message: "Different ReplicationJobs do not establish a common file set",
		})
	}
	if len(group.artifact.Issues) == 0 {
		group.artifact.EstimatedArtifactBytes = size
	}
	return nil
}

func checkSize(m *v1beta1.BaseModel, job *replicationJob) *Issue {
	if strings.EqualFold(m.Spec.ModelFormat.Name, "tensorrtllm") {
		return &Issue{Code: "filteredFiles", Message: "Job size covers all source files, not a GPU-specific subset"}
	}
	if job == nil {
		return &Issue{Code: "jobMissing", Message: "No same-name ReplicationJob in default namespace"}
	}
	if job.UID == "" || job.DeletionTimestamp != nil {
		return &Issue{Code: "jobUnavailable", Message: "ReplicationJob is deleting or has no UID"}
	}
	if job.Status.Status != "Completed" {
		return &Issue{Code: "jobNotCompleted", Message: "ReplicationJob is not Completed"}
	}
	storage, destination := m.Spec.Storage, job.Spec.Destination
	if storageURI(storage) == "" || storageURI(storage) != storageURI(destination) {
		return &Issue{Code: "destinationMismatch", Message: "BaseModel source and Job destination URI differ"}
	}
	if parameter(storage, "region") == "" || parameter(storage, "region") != parameter(destination, "region") {
		return &Issue{Code: "regionMismatch", Message: "Source region is missing or differs from Job destination"}
	}
	if parameter(storage, "revision") != parameter(destination, "revision") {
		return &Issue{Code: "revisionMismatch", Message: "BaseModel source and Job destination revision differ"}
	}
	if size := job.Status.Observed.SourceArtifactSizeBytes; size == nil || *size <= 0 {
		return &Issue{Code: "sizeUnavailable", Message: "ReplicationJob has no positive sourceArtifactSizeBytes"}
	}
	return nil
}

func storagePath(s *v1beta1.StorageSpec) string {
	if s == nil || s.Path == nil {
		return ""
	}
	return *s.Path
}

func storageURI(s *v1beta1.StorageSpec) string {
	if s == nil || s.StorageUri == nil {
		return ""
	}
	return *s.StorageUri
}

func parameter(s *v1beta1.StorageSpec, key string) string {
	if s == nil || s.Parameters == nil {
		return ""
	}
	return (*s.Parameters)[key]
}
