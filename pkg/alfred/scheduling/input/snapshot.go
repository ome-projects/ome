// Package input builds Alfred's predictive scheduler inputs from public API
// objects. It neither renders workloads nor performs live migration actions.
package input

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Snapshot is a lossless, content-addressed observation. Treat it as immutable;
// Validate detects accidental mutation before request construction. The lists
// are not an atomic API-server transaction: freshness is measured from the
// first read, and a prediction never reserves capacity or authorizes migration.
type Snapshot struct {
	ID                   string                     `json:"id"`
	StartedAt            time.Time                  `json:"startedAt"`
	CompletedAt          time.Time                  `json:"completedAt"`
	Objects              []runtime.RawExtension     `json:"objects"`
	InferenceServices    []v1beta1.InferenceService `json:"inferenceServices"`
	InferenceReplicas    []v1beta1.InferenceReplica `json:"inferenceReplicas"`
	ListResourceVersions map[string]string          `json:"listResourceVersions"`
}

// Capture lists the worker-supported scheduling objects plus the public OME
// owners used to identify a migration candidate. reader MUST be lossless (for
// example manager.GetAPIReader()); Alfred's transformed Pod cache is not a
// valid source. Supply a context deadline to bound API reads. A missing CRD,
// permission error, partial list, or other read error returns no snapshot.
// Pending and terminating Pods are retained so the model cannot silently omit
// transient demand. BuildRequest rejects pending non-request Pods until the
// worker can model that demand. Terminal Pods do not occupy scheduler capacity.
func Capture(ctx context.Context, reader client.Reader, now func() time.Time) (*Snapshot, error) {
	if reader == nil || now == nil {
		return nil, fmt.Errorf("capture requires a reader and clock")
	}
	s := &Snapshot{StartedAt: now(), ListResourceVersions: make(map[string]string)}
	group := schema.GroupVersion{Group: "scheduling.x-k8s.io", Version: "v1alpha1"}
	groups := &unstructured.UnstructuredList{}
	groups.SetGroupVersionKind(group.WithKind("PodGroupList"))
	lists := []struct {
		list client.ObjectList
		kind schema.GroupVersionKind
	}{
		{&corev1.NodeList{}, corev1.SchemeGroupVersion.WithKind("Node")},
		{&corev1.PodList{}, corev1.SchemeGroupVersion.WithKind("Pod")},
		{&corev1.NamespaceList{}, corev1.SchemeGroupVersion.WithKind("Namespace")},
		{&corev1.ServiceList{}, corev1.SchemeGroupVersion.WithKind("Service")},
		{&corev1.ReplicationControllerList{}, corev1.SchemeGroupVersion.WithKind("ReplicationController")},
		{&appsv1.ReplicaSetList{}, appsv1.SchemeGroupVersion.WithKind("ReplicaSet")},
		{&appsv1.StatefulSetList{}, appsv1.SchemeGroupVersion.WithKind("StatefulSet")},
		{groups, group.WithKind("PodGroup")},
		{&v1beta1.InferenceServiceList{}, v1beta1.SchemeGroupVersion.WithKind("InferenceService")},
		{&v1beta1.InferenceReplicaList{}, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica")},
	}
	seen := make(map[string]bool)
	for _, entry := range lists {
		if err := reader.List(ctx, entry.list); err != nil {
			return nil, fmt.Errorf("capture %s: %w", entry.kind.Kind, err)
		}
		if entry.list.GetContinue() != "" {
			return nil, fmt.Errorf("capture %s returned a partial list", entry.kind.Kind)
		}
		s.ListResourceVersions[entry.kind.String()] = entry.list.GetResourceVersion()
		objects, err := apiMeta.ExtractList(entry.list)
		if err != nil {
			return nil, fmt.Errorf("capture %s objects: %w", entry.kind.Kind, err)
		}
		for _, item := range objects {
			object := item.DeepCopyObject()
			object.GetObjectKind().SetGroupVersionKind(entry.kind)
			meta, err := apiMeta.Accessor(object)
			if err != nil {
				return nil, fmt.Errorf("capture %s metadata: %w", entry.kind.Kind, err)
			}
			key := entry.kind.String() + "/" + meta.GetNamespace() + "/" + meta.GetName()
			if meta.GetName() == "" || seen[key] {
				return nil, fmt.Errorf("capture has empty or duplicate object identity %q", key)
			}
			seen[key] = true
			switch typed := object.(type) {
			case *v1beta1.InferenceService:
				s.InferenceServices = append(s.InferenceServices, *typed)
				continue
			case *v1beta1.InferenceReplica:
				s.InferenceReplicas = append(s.InferenceReplicas, *typed)
				continue
			case *corev1.Pod:
				if typed.Status.Phase == corev1.PodSucceeded || typed.Status.Phase == corev1.PodFailed {
					continue
				}
			}
			raw, err := json.Marshal(object)
			if err != nil {
				return nil, fmt.Errorf("capture %s JSON: %w", key, err)
			}
			s.Objects = append(s.Objects, runtime.RawExtension{Raw: raw})
		}
	}
	// JSON is deterministic for the typed objects and string-keyed maps above.
	// Sorting the serialized objects also makes input-list ordering irrelevant.
	sort.Slice(s.Objects, func(i, j int) bool { return string(s.Objects[i].Raw) < string(s.Objects[j].Raw) })
	sort.Slice(s.InferenceServices, func(i, j int) bool {
		a, b := s.InferenceServices[i], s.InferenceServices[j]
		return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
	})
	sort.Slice(s.InferenceReplicas, func(i, j int) bool {
		a, b := s.InferenceReplicas[i], s.InferenceReplicas[j]
		return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
	})
	s.CompletedAt = now()
	if s.StartedAt.IsZero() || s.CompletedAt.Before(s.StartedAt) {
		return nil, fmt.Errorf("capture clock is zero or moved backwards")
	}
	var err error
	s.ID, err = s.contentID()
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Validate checks observation freshness and content identity. It does not
// claim cross-resource atomicity or verify that live objects remain unchanged.
func (s *Snapshot) Validate(now time.Time, maxAge time.Duration) error {
	if s == nil || maxAge <= 0 || now.IsZero() {
		return fmt.Errorf("snapshot, current time and positive maximum age are required")
	}
	if s.StartedAt.IsZero() || s.CompletedAt.IsZero() || s.CompletedAt.Before(s.StartedAt) || s.CompletedAt.After(now) {
		return fmt.Errorf("snapshot has invalid or future capture times")
	}
	if now.Sub(s.StartedAt) > maxAge {
		return fmt.Errorf("snapshot is stale")
	}
	id, err := s.contentID()
	if err != nil {
		return err
	}
	if s.ID == "" || s.ID != id {
		return fmt.Errorf("snapshot content identity changed")
	}
	return nil
}

func (s *Snapshot) contentID() (string, error) {
	copy := *s
	copy.ID = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return "", fmt.Errorf("snapshot digest: %w", err)
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}
