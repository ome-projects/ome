package runtimetreeprojection_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/runtimegraph"
	"sigs.k8s.io/ome/pkg/cli/runtimetreeprojection"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestProjectPreservesThreeContextsAndAttachesOnlyToExactHeads(t *testing.T) {
	projection := graphProjection(t, threeContextSnapshot(), runtimegraph.Target{
		Kind: runtimegraph.KindClusterServingRuntime, Name: "root",
	})
	input := runtimetreeprojection.Input{
		Projection: projection,
		Snapshot:   completeSnapshotObservation(2, 2, 4),
		Dependents: []runtimetreeprojection.DependentLeaf{
			{Runtime: clusterIdentity("root"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "direct"},
			{Runtime: clusterIdentity("cluster-child"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "cluster-user"},
			{Runtime: namespacedIdentity("team-a", "local-a"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "team-a", Name: "chat-a"},
			{Runtime: namespacedIdentity("team-b", "local-b"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "team-b", Name: "chat-b"},
		},
	}

	got, err := runtimetreeprojection.Project(input, fixedProjectionClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Contexts, 3)
	assert.Equal(t, []reportv1alpha1.RuntimeTreeResolutionContext{
		{Mode: reportv1alpha1.RuntimeTreeResolutionModeCluster},
		{Mode: reportv1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: "team-a"},
		{Mode: reportv1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: "team-b"},
	}, reportContexts(got.Content))
	assert.Equal(t, []string{"root", "cluster-child"}, reportHeads(got.Content.Contexts[0]))
	assert.Equal(t, []string{"local-a"}, reportHeads(got.Content.Contexts[1]))
	assert.Equal(t, []string{"local-b"}, reportHeads(got.Content.Contexts[2]))
	assert.Equal(t, 1, countDependent(got.Content, "ops", "direct"),
		"a direct ClusterServingRuntime user must not be repeated on ancestor occurrences")
	assert.Equal(t, 1, countDependent(got.Content, "team-a", "chat-a"))

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, got))
	assert.Equal(t, "RUNTIME TREE\n"+
		"Target: ClusterServingRuntime/root\n"+
		"Context: Cluster (resolution: Complete)\n"+
		"Head: ClusterServingRuntime/root\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- InferenceService/ops/direct\n"+
		"Head: ClusterServingRuntime/cluster-child\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ClusterServingRuntime/cluster-child\n"+
		"    `-- InferenceService/ops/cluster-user\n"+
		"Context: Namespaced/team-a (resolution: Complete)\n"+
		"Head: ServingRuntime/local-a\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ServingRuntime/local-a\n"+
		"    `-- InferenceService/chat-a\n"+
		"Context: Namespaced/team-b (resolution: Complete)\n"+
		"Head: ServingRuntime/local-b\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ServingRuntime/local-b\n"+
		"    `-- InferenceService/chat-b\n"+
		"Snapshot: Complete\n"+
		"Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=2\n"+
		"Collection: ServingRuntime scope=AllNamespaces status=Complete pages=1 items=2\n"+
		"Collection: InferenceService scope=AllNamespaces status=Complete pages=1 items=4\n",
		output.String())
}

func TestProjectKeepsSameContextMaxDepthPathsSeparate(t *testing.T) {
	projection := graphProjection(t, runtimegraph.Snapshot{ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{
		clusterRuntime("level-1", ""),
		clusterRuntime("level-2", "level-1"),
		clusterRuntime("target", "level-2"),
		clusterRuntime("level-4", "target"),
		clusterRuntime("level-5", "level-4"),
		clusterRuntime("level-6", "level-5"),
	}}, runtimegraph.Target{Kind: runtimegraph.KindClusterServingRuntime, Name: "target"})

	got, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection, Snapshot: completeSnapshotObservation(6, 0, 0),
	}, fixedProjectionClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Contexts, 1)
	paths := got.Content.Contexts[0].Paths
	require.Len(t, paths, 4)
	assert.Equal(t, []string{"target", "level-4", "level-5", "level-6"}, reportHeads(got.Content.Contexts[0]))
	assert.Equal(t, []string{"level-1", "level-2", "target"}, runtimeNames(paths[0]))
	assert.Equal(t, []string{"level-1", "level-2", "target", "level-4"}, runtimeNames(paths[1]))
	assert.Equal(t, []string{"level-1", "level-2", "target", "level-4", "level-5"}, runtimeNames(paths[2]))
	assert.Equal(t, []string{"level-2", "target", "level-4", "level-5", "level-6"}, runtimeNames(paths[3]))
	require.NotNil(t, paths[3].Issue)
	assert.Equal(t, reportv1alpha1.RuntimeTreeIssueMaxDepthExceeded, paths[3].Issue.Code)
	assert.Equal(t, "level-1", paths[3].Issue.ParentName)
	for i := 0; i < 3; i++ {
		assert.Nil(t, paths[i].Issue)
	}

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, got))
	assert.Equal(t, "RUNTIME TREE\n"+
		"Target: ClusterServingRuntime/target\n"+
		"Context: Cluster (resolution: Complete)\n"+
		"Head: ClusterServingRuntime/target\n"+
		"ClusterServingRuntime/level-1\n"+
		"`-- ClusterServingRuntime/level-2\n"+
		"    `-- ClusterServingRuntime/target [selected]\n"+
		"Head: ClusterServingRuntime/level-4\n"+
		"ClusterServingRuntime/level-1\n"+
		"`-- ClusterServingRuntime/level-2\n"+
		"    `-- ClusterServingRuntime/target [selected]\n"+
		"        `-- ClusterServingRuntime/level-4\n"+
		"Head: ClusterServingRuntime/level-5\n"+
		"ClusterServingRuntime/level-1\n"+
		"`-- ClusterServingRuntime/level-2\n"+
		"    `-- ClusterServingRuntime/target [selected]\n"+
		"        `-- ClusterServingRuntime/level-4\n"+
		"            `-- ClusterServingRuntime/level-5\n"+
		"Head: ClusterServingRuntime/level-6\n"+
		"ClusterServingRuntime/level-2\n"+
		"`-- ClusterServingRuntime/target [selected]\n"+
		"    `-- ClusterServingRuntime/level-4\n"+
		"        `-- ClusterServingRuntime/level-5\n"+
		"            `-- ClusterServingRuntime/level-6\n"+
		"Issue: MaxDepthExceeded subject=ClusterServingRuntime/level-6 parent=level-1\n"+
		"Issue path: ClusterServingRuntime/level-6\n"+
		"  -> ClusterServingRuntime/level-5\n"+
		"  -> ClusterServingRuntime/level-4\n"+
		"  -> ClusterServingRuntime/target\n"+
		"  -> ClusterServingRuntime/level-2\n"+
		"Snapshot: Complete\n"+
		"Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=6\n"+
		"Collection: ServingRuntime scope=AllNamespaces status=Complete pages=1 items=0\n"+
		"Collection: InferenceService scope=AllNamespaces status=Complete pages=1 items=0\n",
		output.String())
}

func TestProjectDoesNotInventShadowedNamespacedDescendants(t *testing.T) {
	projection := graphProjection(t, runtimegraph.Snapshot{
		ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{
			clusterRuntime("root", ""), clusterRuntime("cluster-child", "root"),
		},
		ServingRuntimes: []omev1beta1.ServingRuntime{
			namespacedRuntime("team-a", "root", ""),
			namespacedRuntime("team-a", "local-child", "root"),
		},
	}, runtimegraph.Target{Kind: runtimegraph.KindClusterServingRuntime, Name: "root"})

	got, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection, Snapshot: completeSnapshotObservation(2, 2, 0),
	}, fixedProjectionClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Contexts, 1)
	assert.Equal(t, reportv1alpha1.RuntimeTreeResolutionModeCluster, got.Content.Contexts[0].Context.Mode)
	assert.Equal(t, []string{"root", "cluster-child"}, reportHeads(got.Content.Contexts[0]))
}

func TestProjectDerivesSnapshotContextCompletenessAndWarnings(t *testing.T) {
	projection := graphProjection(t, threeContextSnapshot(), runtimegraph.Target{
		Kind: runtimegraph.KindClusterServingRuntime, Name: "root",
	})

	got, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection,
		Snapshot: runtimetreeprojection.SnapshotObservation{Collections: []runtimetreeprojection.CollectionObservation{
			{Kind: reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime, Scope: reportv1alpha1.RuntimeTreeCollectionScopeCluster, Status: reportv1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: 2},
			{Kind: reportv1alpha1.RuntimeTreeCollectionServingRuntime, Scope: reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: reportv1alpha1.RuntimeTreeCollectionStatusUnavailable, ObservedPages: 1, ObservedItems: 2},
			{Kind: reportv1alpha1.RuntimeTreeCollectionInferenceService, Scope: reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: reportv1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: 0},
		}},
	}, fixedProjectionClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotPartial, got.Content.Snapshot.Completeness)
	assert.Equal(t, []reportv1alpha1.RuntimeWarning{
		{Code: reportv1alpha1.WarningPartialData},
		{Code: reportv1alpha1.WarningSourceUnavailable},
	}, got.Warnings)
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotComplete, got.Content.Contexts[0].ResolutionCompleteness,
		"cluster resolution does not consume the ServingRuntime collection")
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotPartial, got.Content.Contexts[1].ResolutionCompleteness)
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotPartial, got.Content.Contexts[2].ResolutionCompleteness)
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, got))
	assert.Equal(t, "RUNTIME TREE\n"+
		"Target: ClusterServingRuntime/root\n"+
		"Context: Cluster (resolution: Complete)\n"+
		"Head: ClusterServingRuntime/root\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"Head: ClusterServingRuntime/cluster-child\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ClusterServingRuntime/cluster-child\n"+
		"Context: Namespaced/team-a (resolution: Partial)\n"+
		"Head: ServingRuntime/local-a\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ServingRuntime/local-a\n"+
		"Context: Namespaced/team-b (resolution: Partial)\n"+
		"Head: ServingRuntime/local-b\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ServingRuntime/local-b\n"+
		"Snapshot: Partial\n"+
		"Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=2\n"+
		"Collection: ServingRuntime scope=AllNamespaces\n"+
		"  status=Unavailable pages=1 items=2\n"+
		"Collection: InferenceService scope=AllNamespaces status=Complete pages=1 items=0\n"+
		"Warning: PartialData\n"+
		"Warning: SourceUnavailable\n",
		output.String())

	got, err = runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection,
		Snapshot: runtimetreeprojection.SnapshotObservation{Collections: []runtimetreeprojection.CollectionObservation{
			{Kind: reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime, Scope: reportv1alpha1.RuntimeTreeCollectionScopeCluster, Status: reportv1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: 2},
			{Kind: reportv1alpha1.RuntimeTreeCollectionServingRuntime, Scope: reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: reportv1alpha1.RuntimeTreeCollectionStatusTruncated, ObservedPages: 1, ObservedItems: 2},
			{Kind: reportv1alpha1.RuntimeTreeCollectionInferenceService, Scope: reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: reportv1alpha1.RuntimeTreeCollectionStatusUnavailable},
		}},
	}, fixedProjectionClock())
	require.NoError(t, err)
	assert.Equal(t, []reportv1alpha1.RuntimeWarning{
		{Code: reportv1alpha1.WarningPartialData},
		{Code: reportv1alpha1.WarningSourceUnavailable},
		{Code: reportv1alpha1.WarningTruncated},
	}, got.Warnings)
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotComplete, got.Content.Contexts[0].ResolutionCompleteness,
		"InferenceService availability does not change cluster runtime resolution")
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotPartial, got.Content.Contexts[1].ResolutionCompleteness)
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotPartial, got.Content.Contexts[2].ResolutionCompleteness)
}

func TestProjectMapsEachGraphIssueOntoItsPath(t *testing.T) {
	tests := []struct {
		name     string
		snapshot runtimegraph.Snapshot
		target   runtimegraph.Target
		code     reportv1alpha1.RuntimeTreeIssueCode
		count    int
	}{
		{
			name: "missing parent",
			snapshot: runtimegraph.Snapshot{ServingRuntimes: []omev1beta1.ServingRuntime{
				namespacedRuntime("team-a", "orphan", "missing"),
			}},
			target: runtimegraph.Target{Kind: runtimegraph.KindServingRuntime, Namespace: "team-a", Name: "orphan"},
			code:   reportv1alpha1.RuntimeTreeIssueParentMissing,
			count:  1,
		},
		{
			name: "cycle",
			snapshot: runtimegraph.Snapshot{ServingRuntimes: []omev1beta1.ServingRuntime{
				namespacedRuntime("team-a", "a", "b"), namespacedRuntime("team-a", "b", "a"),
			}},
			target: runtimegraph.Target{Kind: runtimegraph.KindServingRuntime, Namespace: "team-a", Name: "a"},
			code:   reportv1alpha1.RuntimeTreeIssueCycleDetected,
			count:  2,
		},
		{
			name: "max depth",
			snapshot: runtimegraph.Snapshot{ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{
				clusterRuntime("one", ""), clusterRuntime("two", "one"), clusterRuntime("three", "two"),
				clusterRuntime("four", "three"), clusterRuntime("five", "four"), clusterRuntime("six", "five"),
			}},
			target: runtimegraph.Target{Kind: runtimegraph.KindClusterServingRuntime, Name: "six"},
			code:   reportv1alpha1.RuntimeTreeIssueMaxDepthExceeded,
			count:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
				Projection: graphProjection(t, test.snapshot, test.target),
				Snapshot: completeSnapshotObservation(
					len(test.snapshot.ClusterServingRuntimes), len(test.snapshot.ServingRuntimes), 0,
				),
			}, fixedProjectionClock())
			require.NoError(t, err)
			issues := collectPathIssues(got.Content)
			require.Len(t, issues, test.count)
			for _, issue := range issues {
				assert.Equal(t, test.code, issue.Code)
			}
		})
	}
}

func TestProjectRejectsInvalidDuplicateAmbiguousAndNonHeadDependents(t *testing.T) {
	projection := graphProjection(t, runtimegraph.Snapshot{ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{
		clusterRuntime("ancestor", ""), clusterRuntime("target", "ancestor"), clusterRuntime("head", "target"),
	}}, runtimegraph.Target{Kind: runtimegraph.KindClusterServingRuntime, Name: "target"})
	valid := runtimetreeprojection.DependentLeaf{
		Runtime: clusterIdentity("head"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService,
		Namespace: "ops", Name: "chat", UID: "uid-1",
	}
	tests := []struct {
		name       string
		dependents []runtimetreeprojection.DependentLeaf
		want       error
	}{
		{name: "ancestor occurrence is not a head", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: clusterIdentity("ancestor"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService,
			Namespace: "ops", Name: "chat",
		}}, want: runtimetreeprojection.ErrDependentRuntimeNotVisible},
		{name: "missing head", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: clusterIdentity("absent"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService,
			Namespace: "ops", Name: "chat",
		}}, want: runtimetreeprojection.ErrDependentRuntimeNotVisible},
		{name: "duplicate", dependents: []runtimetreeprojection.DependentLeaf{valid, valid}, want: runtimetreeprojection.ErrInvalidDependent},
		{name: "ambiguous identity", dependents: []runtimetreeprojection.DependentLeaf{valid, {
			Runtime: clusterIdentity("target"), Kind: valid.Kind, Namespace: valid.Namespace, Name: valid.Name, UID: "uid-2",
		}}, want: runtimetreeprojection.ErrInvalidDependent},
		{name: "missing namespace", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: clusterIdentity("head"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService, Name: "chat",
		}}, want: runtimetreeprojection.ErrInvalidDependent},
		{name: "future dependent kind", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: clusterIdentity("head"), Kind: reportv1alpha1.RuntimeTreeDependentKind("Pod"), Namespace: "ops", Name: "chat",
		}}, want: runtimetreeprojection.ErrInvalidDependent},
		{name: "future runtime kind", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: runtimegraph.Identity{Kind: runtimegraph.Kind("FutureRuntime"), Name: "head"},
			Kind:    reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "chat",
		}}, want: runtimetreeprojection.ErrInvalidDependent},
		{name: "cluster runtime namespace", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Namespace: "team-a", Name: "head"},
			Kind:    reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "chat",
		}}, want: runtimetreeprojection.ErrInvalidDependent},
		{name: "ServingRuntime cross-namespace user", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: namespacedIdentity("team-a", "head"),
			Kind:    reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "team-b", Name: "chat",
		}}, want: runtimetreeprojection.ErrInvalidDependent},
		{name: "terminal control in name", dependents: []runtimetreeprojection.DependentLeaf{{
			Runtime: clusterIdentity("head"),
			Kind:    reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "bad\nname",
		}}, want: runtimetreeprojection.ErrInvalidDependent},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
				Projection: projection, Snapshot: completeSnapshotObservation(3, 0, 2), Dependents: test.dependents,
			}, fixedProjectionClock())
			assert.ErrorIs(t, err, test.want)
		})
	}
}

func TestProjectRejectsMalformedSnapshotAndFutureCollectionEnums(t *testing.T) {
	projection := validDirectProjection()
	tests := []struct {
		name        string
		collections []runtimetreeprojection.CollectionObservation
	}{
		{name: "missing kind", collections: completeCollections(1, 0, 0)[:2]},
		{name: "duplicate kind", collections: append(completeCollections(1, 0, 0), completeCollections(1, 0, 0)[0])},
		{name: "negative pages", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionServingRuntime, func(value *runtimetreeprojection.CollectionObservation) { value.ObservedPages = -1 })},
		{name: "future kind", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
			value.Kind = reportv1alpha1.RuntimeTreeCollectionKind("Future")
		})},
		{name: "future status", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
			value.Status = reportv1alpha1.RuntimeTreeCollectionStatus("Future")
		})},
		{name: "cluster runtime with namespace scope", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
			value.Scope = reportv1alpha1.RuntimeTreeCollectionScopeNamespace
			value.Namespace = "team-a"
		})},
		{name: "namespaced scope without namespace", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
			value.Scope = reportv1alpha1.RuntimeTreeCollectionScopeNamespace
			value.Namespace = ""
		})},
		{name: "all namespaces scope with namespace", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionInferenceService, func(value *runtimetreeprojection.CollectionObservation) {
			value.Scope = reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces
			value.Namespace = "team-a"
		})},
		{name: "future scope", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
			value.Scope = reportv1alpha1.RuntimeTreeCollectionScope("Future")
		})},
		{name: "invalid namespace scope", collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionInferenceService, func(value *runtimetreeprojection.CollectionObservation) {
			value.Scope = reportv1alpha1.RuntimeTreeCollectionScopeNamespace
			value.Namespace = "Bad_NS"
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
				Projection: projection,
				Snapshot:   runtimetreeprojection.SnapshotObservation{Collections: test.collections},
			}, fixedProjectionClock())
			assert.ErrorIs(t, err, runtimetreeprojection.ErrInvalidSnapshot)
		})
	}
}

func TestProjectRejectsCollectionPageAndVisibleCountContradictions(t *testing.T) {
	clusterProjection := validDirectProjection()
	namespacedProjection := graphProjection(t, runtimegraph.Snapshot{ServingRuntimes: []omev1beta1.ServingRuntime{
		namespacedRuntime("team-a", "target", ""),
	}}, runtimegraph.Target{Kind: runtimegraph.KindServingRuntime, Namespace: "team-a", Name: "target"})
	directDependent := runtimetreeprojection.DependentLeaf{
		Runtime: clusterIdentity("root"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService,
		Namespace: "ops", Name: "chat",
	}
	tests := []struct {
		name        string
		projection  runtimegraph.Projection
		collections []runtimetreeprojection.CollectionObservation
		dependents  []runtimetreeprojection.DependentLeaf
	}{
		{
			name: "complete collection has no observed page", projection: clusterProjection,
			collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
				value.ObservedPages = 0
			}),
		},
		{
			name: "truncated collection has no observed page", projection: clusterProjection,
			collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
				value.Status = reportv1alpha1.RuntimeTreeCollectionStatusTruncated
				value.ObservedPages = 0
			}),
		},
		{
			name: "unavailable collection has items but no observed page", projection: clusterProjection,
			collections: replaceCollection(completeCollections(1, 0, 0), reportv1alpha1.RuntimeTreeCollectionServingRuntime, func(value *runtimetreeprojection.CollectionObservation) {
				value.Status = reportv1alpha1.RuntimeTreeCollectionStatusUnavailable
				value.ObservedPages = 0
				value.ObservedItems = 1
			}),
		},
		{
			name: "cluster runtime count is below visible identities", projection: clusterProjection,
			collections: completeCollections(0, 0, 0),
		},
		{
			name: "namespaced runtime count is below visible identities", projection: namespacedProjection,
			collections: completeCollections(0, 0, 0),
		},
		{
			name: "inference service count is below visible dependents", projection: clusterProjection,
			collections: completeCollections(1, 0, 0), dependents: []runtimetreeprojection.DependentLeaf{directDependent},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
				Projection: test.projection,
				Snapshot:   runtimetreeprojection.SnapshotObservation{Collections: test.collections},
				Dependents: test.dependents,
			}, fixedProjectionClock())
			assert.ErrorIs(t, err, runtimetreeprojection.ErrInvalidSnapshot)
		})
	}
}

func TestProjectAllowsUnavailableEmptyCollectionWithNoVisibleObjects(t *testing.T) {
	collections := replaceCollection(
		completeCollections(1, 0, 0),
		reportv1alpha1.RuntimeTreeCollectionInferenceService,
		func(value *runtimetreeprojection.CollectionObservation) {
			value.Status = reportv1alpha1.RuntimeTreeCollectionStatusUnavailable
			value.ObservedPages = 0
		},
	)

	got, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: validDirectProjection(),
		Snapshot:   runtimetreeprojection.SnapshotObservation{Collections: collections},
	}, fixedProjectionClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotPartial, got.Content.Snapshot.Completeness)
	assert.Equal(t, reportv1alpha1.RuntimeTreeSnapshotComplete, got.Content.Contexts[0].ResolutionCompleteness)
}

func TestProjectRejectsCrossPathDeclaredParentConflict(t *testing.T) {
	baseA := runtimegraph.Runtime{Identity: clusterIdentity("base-a")}
	baseB := runtimegraph.Runtime{Identity: clusterIdentity("base-b")}
	targetIdentity := clusterIdentity("target")
	targetViaA := runtimegraph.Runtime{
		Identity: targetIdentity, ParentName: "base-a", ResolvedParent: graphIdentityPointer(baseA.Identity),
	}
	targetViaB := runtimegraph.Runtime{
		Identity: targetIdentity, ParentName: "base-b", ResolvedParent: graphIdentityPointer(baseB.Identity),
	}
	child := runtimegraph.Runtime{
		Identity: clusterIdentity("child"), ParentName: "target", ResolvedParent: graphIdentityPointer(targetIdentity),
	}
	projection := runtimegraph.Projection{
		Target: targetIdentity,
		Contexts: []runtimegraph.ContextProjection{{
			Context: runtimegraph.ResolutionContext{Mode: runtimegraph.ResolutionModeCluster},
			Paths: []runtimegraph.ResolutionPath{
				{Subject: targetIdentity, Runtimes: []runtimegraph.Runtime{baseA, targetViaA}},
				{Subject: child.Identity, Runtimes: []runtimegraph.Runtime{baseB, targetViaB, child}},
			},
		}},
	}

	_, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection, Snapshot: completeSnapshotObservation(4, 0, 0),
	}, fixedProjectionClock())
	assert.ErrorIs(t, err, runtimetreeprojection.ErrInvalidProjection)
}

func TestProjectRejectsCrossPathResolvedParentConflict(t *testing.T) {
	clusterBase := runtimegraph.Runtime{Identity: clusterIdentity("base")}
	localBase := runtimegraph.Runtime{Identity: namespacedIdentity("team-a", "base")}
	targetIdentity := namespacedIdentity("team-a", "target")
	targetViaCluster := runtimegraph.Runtime{
		Identity: targetIdentity, ParentName: "base", ResolvedParent: graphIdentityPointer(clusterBase.Identity),
	}
	targetViaLocal := runtimegraph.Runtime{
		Identity: targetIdentity, ParentName: "base", ResolvedParent: graphIdentityPointer(localBase.Identity),
	}
	child := runtimegraph.Runtime{
		Identity: namespacedIdentity("team-a", "child"), ParentName: "target", ResolvedParent: graphIdentityPointer(targetIdentity),
	}
	projection := runtimegraph.Projection{
		Target: targetIdentity,
		Contexts: []runtimegraph.ContextProjection{{
			Context: runtimegraph.ResolutionContext{Mode: runtimegraph.ResolutionModeNamespaced, Namespace: "team-a"},
			Paths: []runtimegraph.ResolutionPath{
				{Subject: targetIdentity, Runtimes: []runtimegraph.Runtime{localBase, targetViaLocal}},
				{Subject: child.Identity, Runtimes: []runtimegraph.Runtime{clusterBase, targetViaCluster, child}},
			},
		}},
	}

	_, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection, Snapshot: completeSnapshotObservation(1, 3, 0),
	}, fixedProjectionClock())
	assert.ErrorIs(t, err, runtimetreeprojection.ErrInvalidProjection)
}

func TestProjectRejectsCrossPathMissingVersusResolvedParent(t *testing.T) {
	base := runtimegraph.Runtime{Identity: namespacedIdentity("team-a", "base")}
	targetIdentity := namespacedIdentity("team-a", "target")
	missingTarget := runtimegraph.Runtime{Identity: targetIdentity, ParentName: "base"}
	resolvedTarget := runtimegraph.Runtime{
		Identity: targetIdentity, ParentName: "base", ResolvedParent: graphIdentityPointer(base.Identity),
	}
	child := runtimegraph.Runtime{
		Identity: namespacedIdentity("team-a", "child"), ParentName: "target", ResolvedParent: graphIdentityPointer(targetIdentity),
	}
	projection := runtimegraph.Projection{
		Target: targetIdentity,
		Contexts: []runtimegraph.ContextProjection{{
			Context: runtimegraph.ResolutionContext{Mode: runtimegraph.ResolutionModeNamespaced, Namespace: "team-a"},
			Paths: []runtimegraph.ResolutionPath{
				{
					Subject:  targetIdentity,
					Runtimes: []runtimegraph.Runtime{missingTarget},
					Issue: &runtimegraph.Issue{
						Code: runtimegraph.IssueParentMissing, Subject: targetIdentity,
						ParentName: "base", Path: []runtimegraph.Identity{targetIdentity},
					},
				},
				{Subject: child.Identity, Runtimes: []runtimegraph.Runtime{base, resolvedTarget, child}},
			},
		}},
	}

	_, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
		Projection: projection, Snapshot: completeSnapshotObservation(0, 3, 0),
	}, fixedProjectionClock())
	assert.ErrorIs(t, err, runtimetreeprojection.ErrInvalidProjection)
}

func TestProjectRejectsMalformedPathsAndFutureGraphEnums(t *testing.T) {
	root := runtimegraph.Runtime{Identity: clusterIdentity("root")}
	child := runtimegraph.Runtime{
		Identity: clusterIdentity("child"), ParentName: "root", ResolvedParent: graphIdentityPointer(clusterIdentity("root")),
	}
	base := validDirectProjection()
	tests := []struct {
		name   string
		mutate func(*runtimegraph.Projection)
	}{
		{name: "no contexts", mutate: func(value *runtimegraph.Projection) { value.Contexts = nil }},
		{name: "empty paths", mutate: func(value *runtimegraph.Projection) { value.Contexts[0].Paths = nil }},
		{name: "empty runtime path", mutate: func(value *runtimegraph.Projection) { value.Contexts[0].Paths[0].Runtimes = nil }},
		{name: "head differs from final runtime", mutate: func(value *runtimegraph.Projection) { value.Contexts[0].Paths[0].Subject = clusterIdentity("other") }},
		{name: "target absent", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths[0] = runtimegraph.ResolutionPath{Subject: clusterIdentity("child"), Runtimes: []runtimegraph.Runtime{child}}
		}},
		{name: "future resolution mode", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Context.Mode = runtimegraph.ResolutionMode("Future")
		}},
		{name: "cluster context namespace", mutate: func(value *runtimegraph.Projection) { value.Contexts[0].Context.Namespace = "team-a" }},
		{name: "namespaced head in cluster context", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths[0] = runtimegraph.ResolutionPath{Subject: namespacedIdentity("team-a", "root"), Runtimes: []runtimegraph.Runtime{{Identity: namespacedIdentity("team-a", "root")}}}
		}},
		{name: "bad structural edge", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths = append(value.Contexts[0].Paths, runtimegraph.ResolutionPath{Subject: child.Identity, Runtimes: []runtimegraph.Runtime{root, {Identity: child.Identity, ParentName: "wrong", ResolvedParent: child.ResolvedParent}}})
		}},
		{name: "future target kind", mutate: func(value *runtimegraph.Projection) { value.Target.Kind = runtimegraph.Kind("Future") }},
		{name: "future runtime kind", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths[0].Runtimes[0].Identity.Kind = runtimegraph.Kind("Future")
		}},
		{name: "future issue code", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths[0].Runtimes[0].ParentName = "missing"
			value.Contexts[0].Paths[0].Issue = &runtimegraph.Issue{Code: runtimegraph.IssueCode("Future"), Subject: clusterIdentity("root"), ParentName: "missing", Path: []runtimegraph.Identity{clusterIdentity("root")}}
		}},
		{name: "issue path mismatch", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths[0].Runtimes[0].ParentName = "missing"
			value.Contexts[0].Paths[0].Issue = &runtimegraph.Issue{Code: runtimegraph.IssueParentMissing, Subject: clusterIdentity("root"), ParentName: "missing", Path: []runtimegraph.Identity{clusterIdentity("other")}}
		}},
		{name: "max depth issue below controller bound", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths[0].Runtimes[0].ParentName = "missing"
			value.Contexts[0].Paths[0].Issue = &runtimegraph.Issue{
				Code: runtimegraph.IssueMaxDepthExceeded, Subject: clusterIdentity("root"),
				ParentName: "missing", Path: []runtimegraph.Identity{clusterIdentity("root")},
			}
		}},
		{name: "terminal control in runtime name", mutate: func(value *runtimegraph.Projection) {
			value.Target.Name = "bad\nname"
		}},
		{name: "duplicate context", mutate: func(value *runtimegraph.Projection) { value.Contexts = append(value.Contexts, value.Contexts[0]) }},
		{name: "duplicate head", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths = append(value.Contexts[0].Paths, value.Contexts[0].Paths[0])
		}},
		{name: "missing direct target head", mutate: func(value *runtimegraph.Projection) {
			value.Contexts[0].Paths = []runtimegraph.ResolutionPath{{Subject: child.Identity, Runtimes: []runtimegraph.Runtime{root, child}}}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projection := cloneGraphProjection(t, base)
			test.mutate(&projection)
			_, err := runtimetreeprojection.Project(runtimetreeprojection.Input{
				Projection: projection, Snapshot: completeSnapshotObservation(2, 0, 0),
			}, fixedProjectionClock())
			assert.ErrorIs(t, err, runtimetreeprojection.ErrInvalidProjection)
		})
	}
}

func TestProjectIsDeterministicAndDoesNotMutateInput(t *testing.T) {
	input := runtimetreeprojection.Input{
		Projection: graphProjection(t, threeContextSnapshot(), runtimegraph.Target{
			Kind: runtimegraph.KindClusterServingRuntime, Name: "root",
		}),
		Snapshot: completeSnapshotObservation(2, 2, 2),
		Dependents: []runtimetreeprojection.DependentLeaf{
			{Runtime: namespacedIdentity("team-b", "local-b"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "team-b", Name: "z"},
			{Runtime: clusterIdentity("root"), Kind: reportv1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "a"},
		},
	}
	before := cloneProjectionInput(t, input)
	reversed := cloneProjectionInput(t, input)
	reverse(reversed.Projection.Contexts)
	for i := range reversed.Projection.Contexts {
		reverse(reversed.Projection.Contexts[i].Paths)
	}
	reverse(reversed.Snapshot.Collections)
	reverse(reversed.Dependents)

	first, err := runtimetreeprojection.Project(input, fixedProjectionClock())
	require.NoError(t, err)
	second, err := runtimetreeprojection.Project(reversed, fixedProjectionClock())
	require.NoError(t, err)

	assert.Equal(t, first, second)
	assert.Equal(t, before, input, "projection mutated caller-owned evidence")
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		assert.Equal(t, renderProjectionReport(t, first, format), renderProjectionReport(t, second, format))
	}
}

func threeContextSnapshot() runtimegraph.Snapshot {
	return runtimegraph.Snapshot{
		ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{
			clusterRuntime("root", ""), clusterRuntime("cluster-child", "root"),
		},
		ServingRuntimes: []omev1beta1.ServingRuntime{
			namespacedRuntime("team-a", "local-a", "root"),
			namespacedRuntime("team-b", "local-b", "root"),
		},
	}
}

func validDirectProjection() runtimegraph.Projection {
	root := runtimegraph.Runtime{Identity: clusterIdentity("root")}
	return runtimegraph.Projection{
		Target: root.Identity,
		Contexts: []runtimegraph.ContextProjection{{
			Context: runtimegraph.ResolutionContext{Mode: runtimegraph.ResolutionModeCluster},
			Paths:   []runtimegraph.ResolutionPath{{Subject: root.Identity, Runtimes: []runtimegraph.Runtime{root}}},
		}},
	}
}

func completeSnapshotObservation(clusterRuntimes, namespacedRuntimes, inferenceServices int) runtimetreeprojection.SnapshotObservation {
	return runtimetreeprojection.SnapshotObservation{Collections: completeCollections(
		clusterRuntimes, namespacedRuntimes, inferenceServices,
	)}
}

func completeCollections(clusterRuntimes, namespacedRuntimes, inferenceServices int) []runtimetreeprojection.CollectionObservation {
	return []runtimetreeprojection.CollectionObservation{
		{Kind: reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime, Scope: reportv1alpha1.RuntimeTreeCollectionScopeCluster, Status: reportv1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: clusterRuntimes},
		{Kind: reportv1alpha1.RuntimeTreeCollectionServingRuntime, Scope: reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: reportv1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: namespacedRuntimes},
		{Kind: reportv1alpha1.RuntimeTreeCollectionInferenceService, Scope: reportv1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: reportv1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: inferenceServices},
	}
}

func replaceCollection(values []runtimetreeprojection.CollectionObservation, kind reportv1alpha1.RuntimeTreeCollectionKind, change func(*runtimetreeprojection.CollectionObservation)) []runtimetreeprojection.CollectionObservation {
	result := append([]runtimetreeprojection.CollectionObservation{}, values...)
	for i := range result {
		if result[i].Kind == kind {
			change(&result[i])
			return result
		}
	}
	return result
}

func graphProjection(t *testing.T, snapshot runtimegraph.Snapshot, target runtimegraph.Target) runtimegraph.Projection {
	t.Helper()
	graph, err := runtimegraph.Build(snapshot)
	require.NoError(t, err)
	projection, err := graph.Project(target)
	require.NoError(t, err)
	return projection
}

func clusterRuntime(name, parent string) omev1beta1.ClusterServingRuntime {
	annotations := map[string]string{}
	if parent != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = parent
	}
	return omev1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}}
}

func namespacedRuntime(namespace, name, parent string) omev1beta1.ServingRuntime {
	annotations := map[string]string{}
	if parent != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = parent
	}
	return omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Annotations: annotations}}
}

func clusterIdentity(name string) runtimegraph.Identity {
	return runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: name}
}

func namespacedIdentity(namespace, name string) runtimegraph.Identity {
	return runtimegraph.Identity{Kind: runtimegraph.KindServingRuntime, Namespace: namespace, Name: name}
}

func graphIdentityPointer(identity runtimegraph.Identity) *runtimegraph.Identity {
	return &identity
}

func reportContexts(content reportv1alpha1.RuntimeTreeContent) []reportv1alpha1.RuntimeTreeResolutionContext {
	result := make([]reportv1alpha1.RuntimeTreeResolutionContext, len(content.Contexts))
	for i := range content.Contexts {
		result[i] = content.Contexts[i].Context
	}
	return result
}

func reportHeads(context reportv1alpha1.RuntimeTreeContext) []string {
	result := make([]string, len(context.Paths))
	for i := range context.Paths {
		result[i] = context.Paths[i].Head.Name
	}
	return result
}

func runtimeNames(path reportv1alpha1.RuntimeTreePath) []string {
	result := make([]string, len(path.Runtimes))
	for i := range path.Runtimes {
		result[i] = path.Runtimes[i].Identity.Name
	}
	return result
}

func countDependent(content reportv1alpha1.RuntimeTreeContent, namespace, name string) int {
	result := 0
	for _, context := range content.Contexts {
		for _, path := range context.Paths {
			for _, dependent := range path.Dependents {
				if dependent.Namespace == namespace && dependent.Name == name {
					result++
				}
			}
		}
	}
	return result
}

func collectPathIssues(content reportv1alpha1.RuntimeTreeContent) []reportv1alpha1.RuntimeTreeIssue {
	result := []reportv1alpha1.RuntimeTreeIssue{}
	for _, context := range content.Contexts {
		for _, path := range context.Paths {
			if path.Issue != nil {
				result = append(result, *path.Issue)
			}
		}
	}
	return result
}

func fixedProjectionClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, time.September, 7, 18, 30, 0, 0, time.UTC)
	})
}

func cloneGraphProjection(t *testing.T, input runtimegraph.Projection) runtimegraph.Projection {
	t.Helper()
	data, err := json.Marshal(input)
	require.NoError(t, err)
	var result runtimegraph.Projection
	require.NoError(t, json.Unmarshal(data, &result))
	return result
}

func cloneProjectionInput(t *testing.T, input runtimetreeprojection.Input) runtimetreeprojection.Input {
	t.Helper()
	data, err := json.Marshal(input)
	require.NoError(t, err)
	var result runtimetreeprojection.Input
	require.NoError(t, json.Unmarshal(data, &result))
	return result
}

func reverse[T any](values []T) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func renderProjectionReport(t *testing.T, value reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent], format report.Format) string {
	t.Helper()
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, format, value))
	return output.String()
}
