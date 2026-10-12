package modelusage

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const modelReferenceIndex = "ome.io/model-usage-reference"

// This cannot collide with a Kubernetes model name.
const unresolvedReference = "<unresolved>"

// SourceOptions must include every workload kind enabled in the deployment.
// The other six sources are mandatory; LWS is an optional third-party CRD.
type SourceOptions struct {
	LeaderWorkerSets bool
}

// ReferenceQuery uses informer-cache indexes, not a separate usage graph. Reader
// must be the indexed cache and hasSynced must cover all configured informers.
// Cache snapshots across resource kinds are eventually consistent, not atomic.
type ReferenceQuery struct {
	reader    client.Reader
	hasSynced func() bool
	options   SourceOptions
}

func NewReferenceQuery(reader client.Reader, hasSynced func() bool, options SourceOptions) *ReferenceQuery {
	return &ReferenceQuery{reader: reader, hasSynced: hasSynced, options: options}
}

type referenceSource struct {
	object client.Object
	list   client.ObjectList
}

func (q *ReferenceQuery) sources() []referenceSource {
	sources := []referenceSource{
		{&v1beta1.InferenceService{}, &v1beta1.InferenceServiceList{}},
		{&corev1.Pod{}, &corev1.PodList{}},
		{&appsv1.Deployment{}, &appsv1.DeploymentList{}},
		{&appsv1.ReplicaSet{}, &appsv1.ReplicaSetList{}},
		{&appsv1.StatefulSet{}, &appsv1.StatefulSetList{}},
		{&v1beta1.InferenceReplica{}, &v1beta1.InferenceReplicaList{}},
	}
	if q.options.LeaderWorkerSets {
		sources = append(sources, referenceSource{&lws.LeaderWorkerSet{}, &lws.LeaderWorkerSetList{}})
	}
	return sources
}

// RegisterIndexes registers precisely the source set this query will read. Call
// it before starting the cache, then wait for every configured informer to sync.
func (q *ReferenceQuery) RegisterIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	for _, source := range q.sources() {
		err := indexer.IndexField(ctx, source.object, modelReferenceIndex, func(object client.Object) []string {
			refs, err := References(object)
			if err != nil {
				return []string{unresolvedReference}
			}
			names := make([]string, 0, len(refs.Models)+1)
			for _, key := range refs.Models {
				names = append(names, key.Name)
			}
			if refs.Unresolved {
				names = append(names, unresolvedReference)
			}
			return names
		})
		if err != nil {
			return fmt.Errorf("index references for %T: %w", source.object, err)
		}
	}
	return nil
}

// HasReferences checks current sources in the model's namespace. A missing
// index, failed query, unsynchronized cache, or unresolved workload is an error
// and must protect the model. It never falls back to a full-cluster list.
func (q *ReferenceQuery) HasReferences(ctx context.Context, key types.NamespacedName) (bool, error) {
	if q.hasSynced == nil || !q.hasSynced() {
		return false, fmt.Errorf("reference cache is not synchronized")
	}
	if key.Namespace == "" || key.Name == "" {
		return false, fmt.Errorf("reference query requires model namespace and name")
	}
	for _, source := range q.sources() {
		for _, name := range []string{unresolvedReference, key.Name} {
			if err := q.reader.List(ctx, source.list, client.InNamespace(key.Namespace), client.MatchingFields{modelReferenceIndex: name}); err != nil {
				return false, fmt.Errorf("query references in %T: %w", source.list, err)
			}
			if meta.LenList(source.list) > 0 {
				if name == unresolvedReference {
					return false, fmt.Errorf("unresolved OME workload in namespace %s", key.Namespace)
				}
				return true, nil
			}
		}
	}
	return false, nil
}
