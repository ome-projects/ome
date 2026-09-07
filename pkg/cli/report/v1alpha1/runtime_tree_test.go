package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRuntimeTreeTablePreservesContextAndHeadPaths(t *testing.T) {
	target := treeIdentity(v1alpha1.RuntimeKindClusterServingRuntime, "", "root")
	content := v1alpha1.RuntimeTreeContent{
		Target: target,
		Snapshot: v1alpha1.RuntimeTreeSnapshot{
			Completeness: v1alpha1.RuntimeTreeSnapshotPartial,
			Collections: []v1alpha1.RuntimeTreeCollection{
				{Kind: v1alpha1.RuntimeTreeCollectionServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: v1alpha1.RuntimeTreeCollectionStatusTruncated, ObservedPages: 1, ObservedItems: 2},
				{Kind: v1alpha1.RuntimeTreeCollectionInferenceService, Scope: v1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: v1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: 3},
				{Kind: v1alpha1.RuntimeTreeCollectionClusterServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeCluster, Status: v1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: 2},
			},
		},
		Contexts: []v1alpha1.RuntimeTreeContext{
			{
				Context:                v1alpha1.RuntimeTreeResolutionContext{Mode: v1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: "team-b"},
				ResolutionCompleteness: v1alpha1.RuntimeTreeSnapshotPartial,
				Paths: []v1alpha1.RuntimeTreePath{{
					Head: treeIdentity(v1alpha1.RuntimeKindServingRuntime, "team-b", "orphan"),
					Runtimes: []v1alpha1.RuntimeTreeRuntime{
						treeRuntime(v1alpha1.RuntimeKindClusterServingRuntime, "", "root", "", nil),
						treeRuntime(v1alpha1.RuntimeKindServingRuntime, "team-b", "orphan", "root", treeIdentityPointer(v1alpha1.RuntimeKindClusterServingRuntime, "", "root")),
					},
					Dependents: []v1alpha1.RuntimeTreeDependent{},
					Issue: &v1alpha1.RuntimeTreeIssue{
						Code:       v1alpha1.RuntimeTreeIssueParentMissing,
						Subject:    treeIdentity(v1alpha1.RuntimeKindServingRuntime, "team-b", "orphan"),
						ParentName: "missing",
						Path: []v1alpha1.RuntimeTreeIdentity{
							treeIdentity(v1alpha1.RuntimeKindServingRuntime, "team-b", "orphan"),
							treeIdentity(v1alpha1.RuntimeKindClusterServingRuntime, "", "root"),
						},
					},
				}},
			},
			{
				Context:                v1alpha1.RuntimeTreeResolutionContext{Mode: v1alpha1.RuntimeTreeResolutionModeCluster},
				ResolutionCompleteness: v1alpha1.RuntimeTreeSnapshotComplete,
				Paths: []v1alpha1.RuntimeTreePath{
					{
						Head:       treeIdentity(v1alpha1.RuntimeKindClusterServingRuntime, "", "worker"),
						Runtimes:   []v1alpha1.RuntimeTreeRuntime{treeRuntime(v1alpha1.RuntimeKindClusterServingRuntime, "", "root", "", nil), treeRuntime(v1alpha1.RuntimeKindClusterServingRuntime, "", "worker", "root", treeIdentityPointer(v1alpha1.RuntimeKindClusterServingRuntime, "", "root"))},
						Dependents: []v1alpha1.RuntimeTreeDependent{{Kind: v1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "worker-user"}},
					},
					{
						Head:       target,
						Runtimes:   []v1alpha1.RuntimeTreeRuntime{treeRuntime(v1alpha1.RuntimeKindClusterServingRuntime, "", "root", "", nil)},
						Dependents: []v1alpha1.RuntimeTreeDependent{{Kind: v1alpha1.RuntimeTreeDependentInferenceService, Namespace: "ops", Name: "direct"}},
					},
				},
			},
			{
				Context:                v1alpha1.RuntimeTreeResolutionContext{Mode: v1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: "team-a"},
				ResolutionCompleteness: v1alpha1.RuntimeTreeSnapshotPartial,
				Paths: []v1alpha1.RuntimeTreePath{{
					Head: treeIdentity(v1alpha1.RuntimeKindServingRuntime, "team-a", "local"),
					Runtimes: []v1alpha1.RuntimeTreeRuntime{
						treeRuntime(v1alpha1.RuntimeKindClusterServingRuntime, "", "root", "", nil),
						treeRuntime(v1alpha1.RuntimeKindServingRuntime, "team-a", "local", "root", treeIdentityPointer(v1alpha1.RuntimeKindClusterServingRuntime, "", "root")),
					},
					Dependents: []v1alpha1.RuntimeTreeDependent{{Kind: v1alpha1.RuntimeTreeDependentInferenceService, Namespace: "team-a", Name: "chat"}},
				}},
			},
		},
	}
	reportValue := v1alpha1.NewRuntimeTreeReport(v1alpha1.Metadata{Name: "root"}, content, treeClock())
	reportValue.Warnings = []v1alpha1.RuntimeWarning{{Code: v1alpha1.WarningTruncated}, {Code: v1alpha1.WarningPartialData}}

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))
	assert.Equal(t, "RUNTIME TREE\n"+
		"Target: ClusterServingRuntime/root\n"+
		"Context: Cluster (resolution: Complete)\n"+
		"Head: ClusterServingRuntime/root\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- InferenceService/ops/direct\n"+
		"Head: ClusterServingRuntime/worker\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ClusterServingRuntime/worker\n"+
		"    `-- InferenceService/ops/worker-user\n"+
		"Context: Namespaced/team-a (resolution: Partial)\n"+
		"Head: ServingRuntime/local\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ServingRuntime/local\n"+
		"    `-- InferenceService/chat\n"+
		"Context: Namespaced/team-b (resolution: Partial)\n"+
		"Head: ServingRuntime/orphan\n"+
		"ClusterServingRuntime/root [selected]\n"+
		"`-- ServingRuntime/orphan\n"+
		"Issue: ParentMissing subject=ServingRuntime/orphan parent=missing\n"+
		"Issue path: ServingRuntime/orphan -> ClusterServingRuntime/root\n"+
		"Snapshot: Partial\n"+
		"Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=2\n"+
		"Collection: ServingRuntime scope=AllNamespaces status=Truncated pages=1 items=2\n"+
		"Collection: InferenceService scope=AllNamespaces status=Complete pages=1 items=3\n"+
		"Warning: PartialData\n"+
		"Warning: Truncated\n",
		output.String())
}

func TestRuntimeTreeTableMakesEveryCollectionScopeExplicit(t *testing.T) {
	content := v1alpha1.RuntimeTreeContent{Snapshot: v1alpha1.RuntimeTreeSnapshot{
		Collections: []v1alpha1.RuntimeTreeCollection{
			{
				Kind:  v1alpha1.RuntimeTreeCollectionClusterServingRuntime,
				Scope: v1alpha1.RuntimeTreeCollectionScopeCluster,
			},
			{
				Kind:  v1alpha1.RuntimeTreeCollectionServingRuntime,
				Scope: v1alpha1.RuntimeTreeCollectionScopeAllNamespaces,
			},
			{
				Kind:      v1alpha1.RuntimeTreeCollectionInferenceService,
				Scope:     v1alpha1.RuntimeTreeCollectionScopeNamespace,
				Namespace: "team-a",
			},
		},
	}}

	table := content.Table()

	require.Len(t, table.Rows, 5)
	assert.Equal(t, []string{"Collection: ClusterServingRuntime scope=Cluster status= pages=0 items=0"}, table.Rows[2])
	assert.Equal(t, []string{"Collection: ServingRuntime scope=AllNamespaces status= pages=0 items=0"}, table.Rows[3])
	assert.Equal(t, []string{"Collection: InferenceService scope=Namespace/team-a status= pages=0 items=0"}, table.Rows[4])
}

func TestRuntimeTreeTableShowsCycleClosingEdge(t *testing.T) {
	target := treeIdentity(v1alpha1.RuntimeKindServingRuntime, "team-a", "a")
	parent := treeIdentity(v1alpha1.RuntimeKindServingRuntime, "team-a", "b")
	reportValue := v1alpha1.NewRuntimeTreeReport(
		v1alpha1.Metadata{Namespace: "team-a", Name: "a"},
		v1alpha1.RuntimeTreeContent{
			Target: target,
			Snapshot: v1alpha1.RuntimeTreeSnapshot{
				Completeness: v1alpha1.RuntimeTreeSnapshotComplete,
				Collections: []v1alpha1.RuntimeTreeCollection{
					{Kind: v1alpha1.RuntimeTreeCollectionClusterServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeCluster, Status: v1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1},
					{Kind: v1alpha1.RuntimeTreeCollectionServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeNamespace, Namespace: "team-a", Status: v1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1, ObservedItems: 2},
					{Kind: v1alpha1.RuntimeTreeCollectionInferenceService, Scope: v1alpha1.RuntimeTreeCollectionScopeNamespace, Namespace: "team-a", Status: v1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1},
				},
			},
			Contexts: []v1alpha1.RuntimeTreeContext{{
				Context:                v1alpha1.RuntimeTreeResolutionContext{Mode: v1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: "team-a"},
				ResolutionCompleteness: v1alpha1.RuntimeTreeSnapshotComplete,
				Paths: []v1alpha1.RuntimeTreePath{{
					Head: target,
					Runtimes: []v1alpha1.RuntimeTreeRuntime{
						{Identity: parent, ParentName: "a", ResolvedParent: &target},
						{Identity: target, ParentName: "b", ResolvedParent: &parent},
					},
					Dependents: []v1alpha1.RuntimeTreeDependent{},
					Issue: &v1alpha1.RuntimeTreeIssue{
						Code: v1alpha1.RuntimeTreeIssueCycleDetected, Subject: target, ParentName: "a",
						Path: []v1alpha1.RuntimeTreeIdentity{target, parent, target},
					},
				}},
			}},
		},
		treeClock(),
	)

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))
	assert.Equal(t, "RUNTIME TREE\n"+
		"Target: ServingRuntime/team-a/a\n"+
		"Context: Namespaced/team-a (resolution: Complete)\n"+
		"Head: ServingRuntime/a\n"+
		"ServingRuntime/b\n"+
		"`-- ServingRuntime/a [selected]\n"+
		"Issue: CycleDetected subject=ServingRuntime/a parent=a\n"+
		"Issue path: ServingRuntime/a -> ServingRuntime/b -> ServingRuntime/a\n"+
		"Snapshot: Complete\n"+
		"Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=0\n"+
		"Collection: ServingRuntime scope=Namespace/team-a status=Complete pages=1 items=2\n"+
		"Collection: InferenceService scope=Namespace/team-a status=Complete pages=1 items=0\n",
		output.String())
}

func TestRuntimeTreeCanonicalIsDeterministicImmutableAndNonNil(t *testing.T) {
	target := treeIdentity(v1alpha1.RuntimeKindClusterServingRuntime, "", "root")
	parent := treeIdentity(v1alpha1.RuntimeKindClusterServingRuntime, "", "root")
	content := v1alpha1.RuntimeTreeContent{
		Target: target,
		Snapshot: v1alpha1.RuntimeTreeSnapshot{Collections: []v1alpha1.RuntimeTreeCollection{
			{Kind: v1alpha1.RuntimeTreeCollectionServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeAllNamespaces, Status: v1alpha1.RuntimeTreeCollectionStatusComplete},
			{Kind: v1alpha1.RuntimeTreeCollectionClusterServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeCluster, Status: v1alpha1.RuntimeTreeCollectionStatusComplete},
		}},
		Contexts: []v1alpha1.RuntimeTreeContext{
			{
				Context: v1alpha1.RuntimeTreeResolutionContext{Mode: v1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: "z-team"},
				Paths: []v1alpha1.RuntimeTreePath{{
					Head: treeIdentity(v1alpha1.RuntimeKindServingRuntime, "z-team", "z"),
					Runtimes: []v1alpha1.RuntimeTreeRuntime{{
						Identity: treeIdentity(v1alpha1.RuntimeKindServingRuntime, "z-team", "z"), ResolvedParent: &parent,
					}},
					Issue: &v1alpha1.RuntimeTreeIssue{Code: v1alpha1.RuntimeTreeIssueCycleDetected, Path: []v1alpha1.RuntimeTreeIdentity{target}},
				}},
			},
			{
				Context: v1alpha1.RuntimeTreeResolutionContext{Mode: v1alpha1.RuntimeTreeResolutionModeCluster},
				Paths: []v1alpha1.RuntimeTreePath{
					{Head: treeIdentity(v1alpha1.RuntimeKindClusterServingRuntime, "", "z"), Dependents: []v1alpha1.RuntimeTreeDependent{{Kind: v1alpha1.RuntimeTreeDependentInferenceService, Namespace: "z", Name: "z"}}},
					{Head: target, Dependents: []v1alpha1.RuntimeTreeDependent{{Kind: v1alpha1.RuntimeTreeDependentInferenceService, Namespace: "a", Name: "a"}}},
				},
			},
		},
	}
	before := cloneTreeContent(t, content)

	first := content.Canonical()
	second := content.Canonical()

	assert.Equal(t, first, second)
	assert.Equal(t, before, content, "canonicalization mutated caller-owned content")
	require.NotNil(t, first.Snapshot.Collections)
	require.NotNil(t, first.Contexts)
	require.NotNil(t, first.Contexts[0].Paths)
	require.NotNil(t, first.Contexts[0].Paths[0].Runtimes)
	require.NotNil(t, first.Contexts[0].Paths[0].Dependents)
	assert.Equal(t, v1alpha1.RuntimeTreeResolutionModeCluster, first.Contexts[0].Context.Mode)
	assert.Equal(t, target, first.Contexts[0].Paths[0].Head, "selected target head sorts first")
	assert.Equal(t, "a", first.Contexts[0].Paths[0].Dependents[0].Namespace)
	assert.Equal(t, v1alpha1.RuntimeTreeCollectionClusterServingRuntime, first.Snapshot.Collections[0].Kind)

	first.Contexts[1].Paths[0].Runtimes[0].ResolvedParent.Name = "changed"
	first.Contexts[1].Paths[0].Issue.Path[0].Name = "changed"
	assert.Equal(t, before, content, "canonical result aliases caller-owned pointers or slices")
}

func TestRuntimeTreeMachineFormatsAreStableAndKeepPerPathEvidence(t *testing.T) {
	target := treeIdentity(v1alpha1.RuntimeKindServingRuntime, "team-a", "leaf")
	reportValue := v1alpha1.NewRuntimeTreeReport(
		v1alpha1.Metadata{Namespace: "team-a", Name: "leaf"},
		v1alpha1.RuntimeTreeContent{
			Target: target,
			Snapshot: v1alpha1.RuntimeTreeSnapshot{
				Completeness: v1alpha1.RuntimeTreeSnapshotPartial,
				Collections: []v1alpha1.RuntimeTreeCollection{
					{
						Kind: v1alpha1.RuntimeTreeCollectionClusterServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeCluster,
						Status: v1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1,
					},
					{
						Kind: v1alpha1.RuntimeTreeCollectionServingRuntime, Scope: v1alpha1.RuntimeTreeCollectionScopeNamespace,
						Namespace: "team-a", Status: v1alpha1.RuntimeTreeCollectionStatusUnavailable,
					},
					{
						Kind: v1alpha1.RuntimeTreeCollectionInferenceService, Scope: v1alpha1.RuntimeTreeCollectionScopeNamespace,
						Namespace: "team-a", Status: v1alpha1.RuntimeTreeCollectionStatusComplete, ObservedPages: 1,
					},
				},
			},
			Contexts: []v1alpha1.RuntimeTreeContext{{
				Context:                v1alpha1.RuntimeTreeResolutionContext{Mode: v1alpha1.RuntimeTreeResolutionModeNamespaced, Namespace: "team-a"},
				ResolutionCompleteness: v1alpha1.RuntimeTreeSnapshotPartial,
				Paths: []v1alpha1.RuntimeTreePath{{
					Head:       target,
					Runtimes:   []v1alpha1.RuntimeTreeRuntime{{Identity: target, ParentName: "missing"}},
					Dependents: []v1alpha1.RuntimeTreeDependent{},
					Issue: &v1alpha1.RuntimeTreeIssue{
						Code: v1alpha1.RuntimeTreeIssueParentMissing, Subject: target, ParentName: "missing",
						Path: []v1alpha1.RuntimeTreeIdentity{target},
					},
				}},
			}},
		},
		treeClock(),
	)
	reportValue.Warnings = []v1alpha1.RuntimeWarning{{Code: v1alpha1.WarningSourceUnavailable}, {Code: v1alpha1.WarningPartialData}}

	tests := []struct {
		format report.Format
		want   string
	}{
		{format: report.FormatJSON, want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RuntimeTreeReport",
  "metadata": {
    "namespace": "team-a",
    "name": "leaf"
  },
  "collectedAt": "2026-09-07T18:30:00Z",
  "sources": [],
  "content": {
    "target": {
      "kind": "ServingRuntime",
      "namespace": "team-a",
      "name": "leaf"
    },
    "snapshot": {
      "completeness": "Partial",
      "collections": [
        {
          "kind": "ClusterServingRuntime",
          "scope": "Cluster",
          "status": "Complete",
          "observedPages": 1,
          "observedItems": 0
        },
        {
          "kind": "ServingRuntime",
          "scope": "Namespace",
          "namespace": "team-a",
          "status": "Unavailable",
          "observedPages": 0,
          "observedItems": 0
        },
        {
          "kind": "InferenceService",
          "scope": "Namespace",
          "namespace": "team-a",
          "status": "Complete",
          "observedPages": 1,
          "observedItems": 0
        }
      ]
    },
    "contexts": [
      {
        "context": {
          "mode": "Namespaced",
          "namespace": "team-a"
        },
        "resolutionCompleteness": "Partial",
        "paths": [
          {
            "head": {
              "kind": "ServingRuntime",
              "namespace": "team-a",
              "name": "leaf"
            },
            "runtimes": [
              {
                "identity": {
                  "kind": "ServingRuntime",
                  "namespace": "team-a",
                  "name": "leaf"
                },
                "parentName": "missing"
              }
            ],
            "dependents": [],
            "issue": {
              "code": "ParentMissing",
              "subject": {
                "kind": "ServingRuntime",
                "namespace": "team-a",
                "name": "leaf"
              },
              "parentName": "missing",
              "path": [
                {
                  "kind": "ServingRuntime",
                  "namespace": "team-a",
                  "name": "leaf"
                }
              ]
            }
          }
        ]
      }
    ]
  },
  "warnings": [
    {
      "code": "PartialData"
    },
    {
      "code": "SourceUnavailable"
    }
  ]
}
`},
		{format: report.FormatYAML, want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-09-07T18:30:00Z"
content:
  contexts:
  - context:
      mode: Namespaced
      namespace: team-a
    paths:
    - dependents: []
      head:
        kind: ServingRuntime
        name: leaf
        namespace: team-a
      issue:
        code: ParentMissing
        parentName: missing
        path:
        - kind: ServingRuntime
          name: leaf
          namespace: team-a
        subject:
          kind: ServingRuntime
          name: leaf
          namespace: team-a
      runtimes:
      - identity:
          kind: ServingRuntime
          name: leaf
          namespace: team-a
        parentName: missing
    resolutionCompleteness: Partial
  snapshot:
    collections:
    - kind: ClusterServingRuntime
      observedItems: 0
      observedPages: 1
      scope: Cluster
      status: Complete
    - kind: ServingRuntime
      namespace: team-a
      observedItems: 0
      observedPages: 0
      scope: Namespace
      status: Unavailable
    - kind: InferenceService
      namespace: team-a
      observedItems: 0
      observedPages: 1
      scope: Namespace
      status: Complete
    completeness: Partial
  target:
    kind: ServingRuntime
    name: leaf
    namespace: team-a
kind: RuntimeTreeReport
metadata:
  name: leaf
  namespace: team-a
sources: []
warnings:
- code: PartialData
- code: SourceUnavailable
`},
	}

	for _, test := range tests {
		t.Run(string(test.format), func(t *testing.T) {
			first := renderTreeReport(t, reportValue, test.format)
			second := renderTreeReport(t, reportValue, test.format)
			assert.Equal(t, test.want, first)
			assert.Equal(t, first, second)
		})
	}
}

func TestRuntimeTreeWriteReturnsShortWritesForEveryFormat(t *testing.T) {
	target := treeIdentity(v1alpha1.RuntimeKindClusterServingRuntime, "", "root")
	reportValue := v1alpha1.NewRuntimeTreeReport(
		v1alpha1.Metadata{Name: "root"},
		v1alpha1.RuntimeTreeContent{Target: target, Contexts: []v1alpha1.RuntimeTreeContext{}},
		treeClock(),
	)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		t.Run(string(format), func(t *testing.T) {
			err := report.Write(treeShortWriter{}, format, reportValue)
			require.Error(t, err)
			assert.ErrorIs(t, err, io.ErrShortWrite)
		})
	}
}

func TestRuntimeTreeSchemaIsStrictlyAllowlisted(t *testing.T) {
	assertRuntimeReportSchema(t, reflect.TypeOf(v1alpha1.RuntimeEnvelope[v1alpha1.RuntimeTreeContent]{}), map[reflect.Type]bool{})
}

func treeIdentity(kind v1alpha1.RuntimeKind, namespace, name string) v1alpha1.RuntimeTreeIdentity {
	return v1alpha1.RuntimeTreeIdentity{Kind: kind, Namespace: namespace, Name: name}
}

func treeIdentityPointer(kind v1alpha1.RuntimeKind, namespace, name string) *v1alpha1.RuntimeTreeIdentity {
	identity := treeIdentity(kind, namespace, name)
	return &identity
}

func treeRuntime(kind v1alpha1.RuntimeKind, namespace, name, parentName string, resolvedParent *v1alpha1.RuntimeTreeIdentity) v1alpha1.RuntimeTreeRuntime {
	return v1alpha1.RuntimeTreeRuntime{Identity: treeIdentity(kind, namespace, name), ParentName: parentName, ResolvedParent: resolvedParent}
}

func treeClock() fixedClock {
	return fixedClock{now: time.Date(2026, time.September, 7, 18, 30, 0, 0, time.UTC)}
}

func cloneTreeContent(t *testing.T, content v1alpha1.RuntimeTreeContent) v1alpha1.RuntimeTreeContent {
	t.Helper()
	data, err := json.Marshal(content)
	require.NoError(t, err)
	var result v1alpha1.RuntimeTreeContent
	require.NoError(t, json.Unmarshal(data, &result))
	return result
}

func renderTreeReport(t *testing.T, reportValue v1alpha1.RuntimeEnvelope[v1alpha1.RuntimeTreeContent], format report.Format) string {
	t.Helper()
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, format, reportValue))
	return output.String()
}

type treeShortWriter struct{}

func (treeShortWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	return len(data) - 1, nil
}
