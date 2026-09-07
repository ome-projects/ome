package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/runtimegraph"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	treeFixtureNamespace = "ome-cli-tree-b4d9098"
	treeFixturePrefix    = "kome-tree-b4d9098-"
	treeSecretCanary     = "runtime-tree-secret-canary"
)

func executeTree(
	t *testing.T,
	f factory.Factory,
	out io.Writer,
	args ...string,
) (string, error) {
	t.Helper()
	var errOut bytes.Buffer
	cmd := newTreeCmd(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: out, ErrOut: &errOut,
	})
	cmd.SetArgs(args)
	return errOut.String(), cmd.Execute()
}

func executeTreeWithDependencies(
	t *testing.T,
	f factory.Factory,
	dependencies treeCommandDependencies,
	out io.Writer,
	args ...string,
) (string, error) {
	t.Helper()
	var errOut bytes.Buffer
	cmd := newTreeCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: out, ErrOut: &errOut,
	}, dependencies)
	cmd.SetArgs(args)
	return errOut.String(), cmd.Execute()
}

func fixedTreeDependencies() treeCommandDependencies {
	return treeCommandDependencies{
		clock: reportv1alpha1.ClockFunc(func() time.Time {
			return time.Date(2026, 9, 7, 12, 34, 56, 0, time.FixedZone("test", -7*60*60))
		}),
		limits: paging.Limits{
			PageSize: 500, MaxItems: 1000, MaxPages: 2, RequestTimeout: 10 * time.Second,
		},
	}
}

// TestTreeRendersTheMoiraiFixtureTopology catches namespace shadowing,
// omitted ancestors, detached dependents, or unstable path ordering.
func TestTreeRendersTheMoiraiFixtureTopology(t *testing.T) {
	client := omefake.NewSimpleClientset(treeFixtureObjects()...)
	var out bytes.Buffer

	errOut, err := executeTree(
		t,
		factory.Static{OME: client, NS: treeFixtureNamespace},
		&out,
		treeFixturePrefix+"bridge",
		"--kind", "ServingRuntime",
	)

	require.NoError(t, err)
	assert.Empty(t, errOut)
	require.Len(t, client.Actions(), 3)
	resources := []string{"clusterservingruntimes", "servingruntimes", "inferenceservices"}
	namespaces := []string{"", treeFixtureNamespace, treeFixtureNamespace}
	for index, resource := range resources {
		action := client.Actions()[index]
		assert.Equal(t, "list", action.GetVerb())
		assert.Equal(t, resource, action.GetResource().Resource)
		assert.Equal(t, namespaces[index], action.GetNamespace())
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Equal(t, int64(500), options.Limit)
		assert.Empty(t, options.Continue)
	}
	assert.Equal(t, `RUNTIME TREE
Target: ServingRuntime/ome-cli-tree-b4d9098/kome-tree-b4d9098-bridge
Context: Namespaced/ome-cli-tree-b4d9098 (resolution: Complete)
Head: ServingRuntime/kome-tree-b4d9098-bridge
ClusterServingRuntime/kome-tree-b4d9098-root
`+"`"+`-- ClusterServingRuntime/kome-tree-b4d9098-cluster-base
    `+"`"+`-- ServingRuntime/kome-tree-b4d9098-bridge [selected]
Head: ServingRuntime/kome-tree-b4d9098-branch-a
ClusterServingRuntime/kome-tree-b4d9098-root
`+"`"+`-- ClusterServingRuntime/kome-tree-b4d9098-cluster-base
    `+"`"+`-- ServingRuntime/kome-tree-b4d9098-bridge [selected]
        `+"`"+`-- ServingRuntime/kome-tree-b4d9098-branch-a
Head: ServingRuntime/kome-tree-b4d9098-collision
ClusterServingRuntime/kome-tree-b4d9098-root
`+"`"+`-- ClusterServingRuntime/kome-tree-b4d9098-cluster-base
    `+"`"+`-- ServingRuntime/kome-tree-b4d9098-bridge [selected]
        `+"`"+`-- ServingRuntime/kome-tree-b4d9098-collision
            `+"`"+`-- InferenceService/kome-tree-b4d9098-isvc-local-collision
Head: ServingRuntime/kome-tree-b4d9098-leaf-a
ClusterServingRuntime/kome-tree-b4d9098-root
`+"`"+`-- ClusterServingRuntime/kome-tree-b4d9098-cluster-base
    `+"`"+`-- ServingRuntime/kome-tree-b4d9098-bridge [selected]
        `+"`"+`-- ServingRuntime/kome-tree-b4d9098-branch-a
            `+"`"+`-- ServingRuntime/kome-tree-b4d9098-leaf-a
                `+"`"+`-- InferenceService/kome-tree-b4d9098-isvc-leaf-a
Head: ServingRuntime/kome-tree-b4d9098-leaf-b
ClusterServingRuntime/kome-tree-b4d9098-root
`+"`"+`-- ClusterServingRuntime/kome-tree-b4d9098-cluster-base
    `+"`"+`-- ServingRuntime/kome-tree-b4d9098-bridge [selected]
        `+"`"+`-- ServingRuntime/kome-tree-b4d9098-collision
            `+"`"+`-- ServingRuntime/kome-tree-b4d9098-leaf-b
                `+"`"+`-- InferenceService/kome-tree-b4d9098-isvc-leaf-b
Snapshot: Complete
Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=3
Collection: ServingRuntime scope=Namespace/ome-cli-tree-b4d9098 status=Complete pages=1 items=5
Collection: InferenceService scope=Namespace/ome-cli-tree-b4d9098 status=Complete pages=1 items=5
`, out.String())
	assert.NotContains(t, out.String(), treeSecretCanary)
}

// TestTreeExplainsHowToResolveKindCollisions catches an implicit target
// silently choosing either the namespaced or cluster-scoped object.
func TestTreeExplainsHowToResolveKindCollisions(t *testing.T) {
	client := omefake.NewSimpleClientset(treeFixtureObjects()...)
	var out bytes.Buffer

	errOut, err := executeTree(
		t,
		factory.Static{OME: client, NS: treeFixtureNamespace},
		&out,
		treeFixturePrefix+"collision",
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, runtimegraph.ErrTargetAmbiguous)
	assert.Contains(t, err.Error(), "pass --kind ServingRuntime or --kind ClusterServingRuntime")
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
	for _, action := range client.Actions() {
		assert.NotEqual(t, "inferenceservices", action.GetResource().Resource,
			"ambiguous targets must fail before dependency collection")
	}
}

// TestTreeReportsUnavailableInferenceServiceEvidence catches an optional
// dependency-list failure aborting an otherwise usable runtime tree or
// leaking the API server's error detail into the report.
func TestTreeReportsUnavailableInferenceServiceEvidence(t *testing.T) {
	client := omefake.NewSimpleClientset(treeFixtureObjects()...)
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
		"",
		errors.New("private authorization detail"),
	)
	client.PrependReactor("list", "inferenceservices", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, forbidden
	})
	var out bytes.Buffer

	errOut, err := executeTree(
		t,
		factory.Static{OME: client, NS: treeFixtureNamespace},
		&out,
		treeFixturePrefix+"root",
		"--kind", "ClusterServingRuntime",
	)

	require.NoError(t, err)
	assert.Empty(t, errOut)
	assert.Contains(t, out.String(), "Snapshot: Partial\n")
	assert.Contains(t, out.String(), "Collection: InferenceService scope=AllNamespaces status=Unavailable pages=0 items=0\n")
	assert.Contains(t, out.String(), "Warning: PartialData\n")
	assert.Contains(t, out.String(), "Warning: SourceUnavailable\n")
	assert.NotContains(t, out.String(), "private authorization detail")
	assert.NotContains(t, out.String(), treeSecretCanary)
}

// TestTreeRejectsUnavailableClusterRuntimeAfterBoundedProgress catches a
// later-page failure being treated as authoritative merely because the target
// appeared in the bounded prefix.
func TestTreeRejectsUnavailableClusterRuntimeAfterBoundedProgress(t *testing.T) {
	client := omefake.NewSimpleClientset(treeFixtureObjects()...)
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Group: "ome.io", Resource: "clusterservingruntimes"},
		"",
		errors.New("private cluster runtime authorization detail"),
	)
	clusterRequests := 0
	client.PrependReactor("list", "clusterservingruntimes", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		clusterRequests++
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		if options.Continue == "" {
			return true, &omev1beta1.ClusterServingRuntimeList{
				Items:    []omev1beta1.ClusterServingRuntime{*treeClusterRuntime("root", "")},
				ListMeta: metav1.ListMeta{Continue: "second-cluster-page"},
			}, nil
		}
		assert.Equal(t, "second-cluster-page", options.Continue)
		return true, nil, forbidden
	})
	var out bytes.Buffer

	errOut, err := executeTree(
		t,
		factory.Static{OME: client, NS: treeFixtureNamespace},
		&out,
		treeFixturePrefix+"root",
		"--kind", "ClusterServingRuntime",
	)

	require.Error(t, err)
	assert.Empty(t, errOut)
	assert.Equal(t, 2, clusterRequests)
	assert.Contains(t, err.Error(), "ClusterServingRuntime collection is unavailable")
	assert.Contains(t, err.Error(), "pages=1 items=1")
	assert.NotContains(t, err.Error(), "private cluster runtime authorization detail")
	assert.Empty(t, out.String())
	assertNoInferenceServiceList(t, client.Actions())
}

// TestTreeRejectsUnavailableNamespacedRuntimeEvidence catches a missing
// descendant collection being ignored because the selected cluster head was
// still observed.
func TestTreeRejectsUnavailableNamespacedRuntimeEvidence(t *testing.T) {
	client := omefake.NewSimpleClientset(treeFixtureObjects()...)
	client.PrependReactor("list", "servingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "ome.io", Resource: "servingruntimes"},
			"",
			errors.New("private namespaced runtime authorization detail"),
		)
	})
	var out bytes.Buffer

	errOut, err := executeTree(
		t,
		factory.Static{OME: client, NS: treeFixtureNamespace},
		&out,
		treeFixturePrefix+"root", "--kind", "ClusterServingRuntime",
	)

	require.Error(t, err)
	assert.Empty(t, errOut)
	assert.Contains(t, err.Error(), "ServingRuntime collection is unavailable")
	assert.Contains(t, err.Error(), "pages=0 items=0")
	assert.NotContains(t, err.Error(), "private namespaced runtime authorization detail")
	assert.Empty(t, out.String())
	assertNoInferenceServiceList(t, client.Actions())
}

// TestTreeReportsTruncatedInferenceServiceCollection proves dependency-list
// truncation remains reportable because it cannot change resolved runtime
// inheritance edges.
func TestTreeReportsTruncatedInferenceServiceCollection(t *testing.T) {
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items: []omev1beta1.ClusterServingRuntime{*clusterRuntimeWithExactName("root", "")},
		}, nil
	})
	client.PrependReactor("list", "servingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ServingRuntimeList{}, nil
	})
	runtimeKind := "ClusterServingRuntime"
	client.PrependReactor("list", "inferenceservices", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.InferenceServiceList{
			Items: []omev1beta1.InferenceService{{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "predict"},
				Spec: omev1beta1.InferenceServiceSpec{Runtime: &omev1beta1.ServingRuntimeRef{
					Name: "root", Kind: &runtimeKind,
				}},
			}},
			ListMeta: metav1.ListMeta{Continue: "more-inference-services"},
		}, nil
	})
	dependencies := fixedTreeDependencies()
	dependencies.limits.PageSize = 1
	dependencies.limits.MaxItems = 3
	dependencies.limits.MaxPages = 1
	var out bytes.Buffer

	errOut, err := executeTreeWithDependencies(
		t,
		factory.Static{OME: client, NS: "team-a"},
		dependencies,
		&out,
		"root", "--kind", "ClusterServingRuntime",
	)

	require.NoError(t, err)
	assert.Empty(t, errOut)
	assert.Contains(t, out.String(),
		"Collection: ClusterServingRuntime scope=Cluster status=Complete pages=1 items=1\n")
	assert.Contains(t, out.String(),
		"Collection: ServingRuntime scope=AllNamespaces status=Complete pages=1 items=0\n")
	assert.Contains(t, out.String(),
		"Collection: InferenceService scope=AllNamespaces status=Truncated pages=1 items=1\n")
	assert.Contains(t, out.String(), "Warning: PartialData\n")
	assert.Contains(t, out.String(), "Warning: Truncated\n")
	assert.NotContains(t, out.String(), "Warning: SourceUnavailable\n")
}

// TestTreeDrainsFinitePagesForEveryCollection catches command composition
// bypassing the bounded collectors or losing a Kubernetes continue token.
func TestTreeDrainsFinitePagesForEveryCollection(t *testing.T) {
	client := omefake.NewSimpleClientset()
	requests := map[string][]metav1.ListOptions{}
	client.PrependReactor("list", "*", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		resource := action.GetResource().Resource
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		requests[resource] = append(requests[resource], options)
		switch resource {
		case "clusterservingruntimes":
			if options.Continue == "" {
				return true, &omev1beta1.ClusterServingRuntimeList{
					Items:    []omev1beta1.ClusterServingRuntime{*clusterRuntimeWithExactName("root", "")},
					ListMeta: metav1.ListMeta{Continue: "cluster-next"},
				}, nil
			}
			assert.Equal(t, "cluster-next", options.Continue)
			return true, &omev1beta1.ClusterServingRuntimeList{
				Items: []omev1beta1.ClusterServingRuntime{*clusterRuntimeWithExactName("base", "root")},
			}, nil
		case "servingruntimes":
			if options.Continue == "" {
				return true, &omev1beta1.ServingRuntimeList{
					Items: []omev1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{
						Namespace: "team-a", Name: "local",
						Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "root"},
					}}},
					ListMeta: metav1.ListMeta{Continue: "runtime-next"},
				}, nil
			}
			assert.Equal(t, "runtime-next", options.Continue)
			return true, &omev1beta1.ServingRuntimeList{
				Items: []omev1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{
					Namespace: "team-a", Name: "leaf",
					Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "local"},
				}}},
			}, nil
		case "inferenceservices":
			kind := "ClusterServingRuntime"
			name := "root"
			if options.Continue != "" {
				assert.Equal(t, "service-next", options.Continue)
				kind = "ServingRuntime"
				name = "local"
			}
			service := omev1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "uses-" + name},
				Spec: omev1beta1.InferenceServiceSpec{Runtime: &omev1beta1.ServingRuntimeRef{
					Name: name, Kind: &kind,
				}},
			}
			list := &omev1beta1.InferenceServiceList{Items: []omev1beta1.InferenceService{service}}
			if options.Continue == "" {
				list.ListMeta.Continue = "service-next"
			}
			return true, list, nil
		default:
			t.Fatalf("unexpected resource %q", resource)
			return true, nil, nil
		}
	})
	dependencies := fixedTreeDependencies()
	dependencies.limits.PageSize = 1
	dependencies.limits.MaxItems = 10
	dependencies.limits.MaxPages = 3
	var out bytes.Buffer

	errOut, err := executeTreeWithDependencies(
		t,
		factory.Static{OME: client, NS: "team-a"},
		dependencies,
		&out,
		"root", "--kind", "ClusterServingRuntime",
	)

	require.NoError(t, err)
	assert.Empty(t, errOut)
	for _, resource := range []string{
		"clusterservingruntimes", "servingruntimes", "inferenceservices",
	} {
		require.Len(t, requests[resource], 2)
		assert.Equal(t, int64(1), requests[resource][0].Limit)
		assert.Equal(t, int64(1), requests[resource][1].Limit)
		assert.Empty(t, requests[resource][0].Continue)
		assert.NotEmpty(t, requests[resource][1].Continue)
	}
	for _, collection := range []string{
		"ClusterServingRuntime scope=Cluster",
		"ServingRuntime scope=AllNamespaces",
		"InferenceService scope=AllNamespaces",
	} {
		assert.Contains(t, out.String(), "Collection: "+collection+" status=Complete pages=2 items=2\n")
	}
	assert.NotContains(t, out.String(), "Warning:")
}

// TestTreeRejectsTruncatedSnapshotBeforeTargetLookup catches a bounded cutoff
// reaching graph projection before absence can be established.
func TestTreeRejectsTruncatedSnapshotBeforeTargetLookup(t *testing.T) {
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			ListMeta: metav1.ListMeta{Continue: "unobserved-cluster-runtimes"},
		}, nil
	})
	dependencies := fixedTreeDependencies()
	dependencies.limits.MaxPages = 1
	var out bytes.Buffer

	errOut, err := executeTreeWithDependencies(
		t,
		factory.Static{OME: client, NS: treeFixtureNamespace},
		dependencies,
		&out,
		"not-observed",
		"--kind", "ClusterServingRuntime",
	)

	require.Error(t, err)
	assert.NotErrorIs(t, err, runtimegraph.ErrTargetNotFound)
	assert.Contains(t, err.Error(), "ClusterServingRuntime collection is truncated")
	assert.Contains(t, err.Error(), "pages=1 items=0")
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
	assertNoInferenceServiceList(t, client.Actions())
}

// TestTreeDoesNotAutoDetectAcrossIncompleteKinds catches a partially
// observed list silently deciding a one-candidate name is collision-free.
func TestTreeDoesNotAutoDetectAcrossIncompleteKinds(t *testing.T) {
	client := omefake.NewSimpleClientset(clusterRuntimeWithExactName("root", ""))
	client.PrependReactor("list", "servingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "ome.io", Resource: "servingruntimes"},
			"", errors.New("namespaced discovery denied"),
		)
	})
	var out bytes.Buffer

	errOut, err := executeTree(
		t, factory.Static{OME: client, NS: "team-a"}, &out, "root",
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ServingRuntime collection is unavailable")
	assert.Contains(t, err.Error(), "restore list access or narrow the runtime scope")
	assert.NotContains(t, err.Error(), "namespaced discovery denied")
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
	assertNoInferenceServiceList(t, client.Actions())
}

// TestTreeRejectsTruncatedNamespacedRuntimeBeforeInheritanceFallback catches
// an unseen namespaced parent being shadowed by an observed same-name cluster
// runtime. The controller would select the unseen namespaced object first, so
// a bounded prefix cannot safely render the fallback edge.
func TestTreeRejectsTruncatedNamespacedRuntimeBeforeInheritanceFallback(t *testing.T) {
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items: []omev1beta1.ClusterServingRuntime{*clusterRuntimeWithExactName("base", "")},
		}, nil
	})
	client.PrependReactor("list", "servingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ServingRuntimeList{
			Items: []omev1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{
				Namespace: "team-a", Name: "leaf",
				Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "base"},
			}}},
			ListMeta: metav1.ListMeta{Continue: "unobserved-local-base"},
		}, nil
	})
	dependencies := fixedTreeDependencies()
	dependencies.limits.MaxPages = 1
	var out bytes.Buffer

	errOut, err := executeTreeWithDependencies(
		t, factory.Static{OME: client, NS: "team-a"}, dependencies,
		&out, "leaf", "--kind", "ServingRuntime",
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "runtime tree requires complete runtime evidence")
	assert.Contains(t, err.Error(), "ServingRuntime collection is truncated")
	assert.Contains(t, err.Error(), "pages=1 items=1")
	assert.Contains(t, err.Error(), "restore list access or narrow the runtime scope")
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
	assertNoInferenceServiceList(t, client.Actions())
}

// TestTreeRejectsTruncatedClusterRuntimeBeforeMissingParent catches an unseen
// cluster parent being reported as definitively missing from a bounded prefix.
func TestTreeRejectsTruncatedClusterRuntimeBeforeMissingParent(t *testing.T) {
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items: []omev1beta1.ClusterServingRuntime{
				*clusterRuntimeWithExactName("orphan", "unobserved-parent"),
			},
			ListMeta: metav1.ListMeta{Continue: "unobserved-cluster-parent"},
		}, nil
	})
	dependencies := fixedTreeDependencies()
	dependencies.limits.MaxPages = 1
	var out bytes.Buffer

	errOut, err := executeTreeWithDependencies(
		t, factory.Static{OME: client, NS: "team-a"}, dependencies,
		&out, "orphan", "--kind", "ClusterServingRuntime",
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "runtime tree requires complete runtime evidence")
	assert.Contains(t, err.Error(), "ClusterServingRuntime collection is truncated")
	assert.Contains(t, err.Error(), "pages=1 items=1")
	assert.NotContains(t, err.Error(), "ParentMissing")
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
	assertNoInferenceServiceList(t, client.Actions())
}

// TestTreeMachineOutputIsStable catches command composition leaking raw API
// objects, omitting required collection evidence, or producing unstable JSON
// and YAML around the versioned report contract.
func TestTreeMachineOutputIsStable(t *testing.T) {
	runtimeKind := "ClusterServingRuntime"
	objects := []k8sruntime.Object{
		&omev1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{
			Name: "root", Labels: map[string]string{"private": treeSecretCanary},
		}},
		&omev1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "team-a", Name: "predict", UID: types.UID("private-uid"),
				Annotations: map[string]string{"private": treeSecretCanary},
			},
			Spec: omev1beta1.InferenceServiceSpec{Runtime: &omev1beta1.ServingRuntimeRef{
				Name: "root", Kind: &runtimeKind,
			}},
		},
	}
	tests := []struct {
		format string
		want   string
	}{
		{format: "json", want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "RuntimeTreeReport",
  "metadata": {
    "name": "root"
  },
  "collectedAt": "2026-09-07T19:34:56Z",
  "sources": [],
  "content": {
    "target": {
      "kind": "ClusterServingRuntime",
      "name": "root"
    },
    "snapshot": {
      "completeness": "Complete",
      "collections": [
        {
          "kind": "ClusterServingRuntime",
          "scope": "Cluster",
          "status": "Complete",
          "observedPages": 1,
          "observedItems": 1
        },
        {
          "kind": "ServingRuntime",
          "scope": "AllNamespaces",
          "status": "Complete",
          "observedPages": 1,
          "observedItems": 0
        },
        {
          "kind": "InferenceService",
          "scope": "AllNamespaces",
          "status": "Complete",
          "observedPages": 1,
          "observedItems": 1
        }
      ]
    },
    "contexts": [
      {
        "context": {
          "mode": "Cluster"
        },
        "resolutionCompleteness": "Complete",
        "paths": [
          {
            "head": {
              "kind": "ClusterServingRuntime",
              "name": "root"
            },
            "runtimes": [
              {
                "identity": {
                  "kind": "ClusterServingRuntime",
                  "name": "root"
                }
              }
            ],
            "dependents": [
              {
                "kind": "InferenceService",
                "namespace": "team-a",
                "name": "predict"
              }
            ]
          }
        ]
      }
    ]
  },
  "warnings": []
}
`},
		{format: "yaml", want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-09-07T19:34:56Z"
content:
  contexts:
  - context:
      mode: Cluster
    paths:
    - dependents:
      - kind: InferenceService
        name: predict
        namespace: team-a
      head:
        kind: ClusterServingRuntime
        name: root
      runtimes:
      - identity:
          kind: ClusterServingRuntime
          name: root
    resolutionCompleteness: Complete
  snapshot:
    collections:
    - kind: ClusterServingRuntime
      observedItems: 1
      observedPages: 1
      scope: Cluster
      status: Complete
    - kind: ServingRuntime
      observedItems: 0
      observedPages: 1
      scope: AllNamespaces
      status: Complete
    - kind: InferenceService
      observedItems: 1
      observedPages: 1
      scope: AllNamespaces
      status: Complete
    completeness: Complete
  target:
    kind: ClusterServingRuntime
    name: root
kind: RuntimeTreeReport
metadata:
  name: root
sources: []
warnings: []
`},
	}

	for _, test := range tests {
		t.Run(test.format, func(t *testing.T) {
			render := func() string {
				var out bytes.Buffer
				errOut, err := executeTreeWithDependencies(
					t,
					factory.Static{OME: omefake.NewSimpleClientset(objects...), NS: "team-a"},
					fixedTreeDependencies(),
					&out,
					"root", "--kind", "ClusterServingRuntime", "--output", test.format,
				)
				require.NoError(t, err)
				assert.Empty(t, errOut)
				return out.String()
			}
			first := render()
			second := render()
			assert.Equal(t, test.want, first)
			assert.Equal(t, first, second)
			assert.NotContains(t, first, treeSecretCanary)
			assert.NotContains(t, first, "private-uid")
		})
	}
}

func TestTreeNamespacedMachineOutputReportsExactCollectionScopes(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var out bytes.Buffer
			errOut, err := executeTreeWithDependencies(
				t,
				factory.Static{OME: omefake.NewSimpleClientset(treeFixtureObjects()...), NS: treeFixtureNamespace},
				fixedTreeDependencies(),
				&out,
				treeFixturePrefix+"bridge", "--kind", "ServingRuntime", "--output", format,
			)
			require.NoError(t, err)
			assert.Empty(t, errOut)

			var got reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeTreeContent]
			if format == "json" {
				require.NoError(t, json.Unmarshal(out.Bytes(), &got))
			} else {
				require.NoError(t, yaml.Unmarshal(out.Bytes(), &got))
			}
			require.Len(t, got.Content.Snapshot.Collections, 3)
			assert.Equal(t, []reportv1alpha1.RuntimeTreeCollection{
				{
					Kind:          reportv1alpha1.RuntimeTreeCollectionClusterServingRuntime,
					Scope:         reportv1alpha1.RuntimeTreeCollectionScopeCluster,
					Status:        reportv1alpha1.RuntimeTreeCollectionStatusComplete,
					ObservedPages: 1, ObservedItems: 3,
				},
				{
					Kind:          reportv1alpha1.RuntimeTreeCollectionServingRuntime,
					Scope:         reportv1alpha1.RuntimeTreeCollectionScopeNamespace,
					Namespace:     treeFixtureNamespace,
					Status:        reportv1alpha1.RuntimeTreeCollectionStatusComplete,
					ObservedPages: 1, ObservedItems: 5,
				},
				{
					Kind:          reportv1alpha1.RuntimeTreeCollectionInferenceService,
					Scope:         reportv1alpha1.RuntimeTreeCollectionScopeNamespace,
					Namespace:     treeFixtureNamespace,
					Status:        reportv1alpha1.RuntimeTreeCollectionStatusComplete,
					ObservedPages: 1, ObservedItems: 5,
				},
			}, got.Content.Snapshot.Collections)
		})
	}
}

type treeTerminalBuffer struct {
	bytes.Buffer
	width int
}

func (w *treeTerminalBuffer) TerminalWidth() (int, bool) {
	return w.width, true
}

// TestTreeFitsConstrainedTerminals catches long fixture identities being
// delegated to terminal auto-wrap or leaving a detached ASCII branch marker.
func TestTreeFitsConstrainedTerminals(t *testing.T) {
	for _, width := range []int{80, 120} {
		t.Run(fmt.Sprintf("%d columns", width), func(t *testing.T) {
			client := omefake.NewSimpleClientset(treeFixtureObjects()...)
			out := &treeTerminalBuffer{width: width}

			errOut, err := executeTree(
				t,
				factory.Static{OME: client, NS: treeFixtureNamespace},
				out,
				treeFixturePrefix+"bridge", "--kind", "ServingRuntime",
			)

			require.NoError(t, err)
			assert.Empty(t, errOut)
			for number, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
				assert.LessOrEqualf(t, len([]rune(line)), width,
					"line %d exceeds terminal width: %q", number+1, line)
				assert.NotContainsf(t, []string{"`--", "|--"}, strings.TrimSpace(line),
					"line %d detached a tree branch marker: %q", number+1, line)
			}
		})
	}
}

// TestTreeKeepsBranchesAttachedForMaximumNamesAtMaximumDepth exercises the
// longest valid Kubernetes names at the controller's deepest supported
// inheritance path. A tree branch must share its physical line with the first
// runtime/dependent identity segment at both common constrained widths.
func TestTreeKeepsBranchesAttachedForMaximumNamesAtMaximumDepth(t *testing.T) {
	names := make([]string, constants.RuntimeInheritMaxDepth)
	for i := range names {
		names[i] = maximumDNSSubdomain(string(rune('a' + i)))
	}
	for _, name := range names {
		assert.Len(t, name, 253)
		assert.Empty(t, validation.IsDNS1123Subdomain(name))
	}
	objects := make([]k8sruntime.Object, 0, len(names)+1)
	for i, name := range names {
		parent := ""
		if i > 0 {
			parent = names[i-1]
		}
		objects = append(objects, clusterRuntimeWithExactName(name, parent))
	}
	kind := "ClusterServingRuntime"
	objects = append(objects, &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: maximumDNSSubdomain("z")},
		Spec: omev1beta1.InferenceServiceSpec{Runtime: &omev1beta1.ServingRuntimeRef{
			Name: names[len(names)-1], Kind: &kind,
		}},
	})

	for _, width := range []int{80, 120} {
		t.Run(fmt.Sprintf("%d columns", width), func(t *testing.T) {
			out := &treeTerminalBuffer{width: width}
			errOut, err := executeTree(
				t, factory.Static{OME: omefake.NewSimpleClientset(objects...), NS: "team-a"},
				out, names[0], "--kind", "ClusterServingRuntime",
			)

			require.NoError(t, err)
			assert.Empty(t, errOut)
			deepestIndent := strings.Repeat(" ", 4*(constants.RuntimeInheritMaxDepth-1))
			assert.Contains(t, out.String(), deepestIndent+"`-- InferenceService/")
			for number, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
				assert.LessOrEqualf(t, len([]rune(line)), width,
					"line %d exceeds terminal width: %q", number+1, line)
				trimmed := strings.TrimLeft(line, " ")
				if strings.HasPrefix(trimmed, "`-- ") || strings.HasPrefix(trimmed, "|-- ") {
					assert.Regexpf(t,
						"^(?:`--|\\|--) (?:ClusterServingRuntime|ServingRuntime|InferenceService)/",
						trimmed,
						"line %d detached a branch from its first identity segment: %q", number+1, line,
					)
				}
			}
		})
	}
}

func maximumDNSSubdomain(final string) string {
	return strings.Repeat("a", 63) + "." +
		strings.Repeat("a", 63) + "." +
		strings.Repeat("a", 63) + "." +
		strings.Repeat("a", 60) + final
}

// TestTreeMatchesControllerReferenceResolution catches command composition
// reinterpreting kind or APIGroup differently from the live controller.
func TestTreeMatchesControllerReferenceResolution(t *testing.T) {
	emptyKind := ""
	unknownKind := "OtherRuntime"
	clusterKind := "ClusterServingRuntime"
	namespacedKind := "ServingRuntime"
	nonstandardGroup := "runtime.example.test"
	objects := []k8sruntime.Object{
		clusterRuntimeWithExactName("shared", ""),
		&omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "shared"}},
		treeServiceWithReference("nil-kind", "shared", nil, nil),
		treeServiceWithReference("empty-kind", "shared", &emptyKind, nil),
		treeServiceWithReference("unknown-kind", "shared", &unknownKind, nil),
		treeServiceWithReference("nonstandard-group", "shared", &namespacedKind, &nonstandardGroup),
		treeServiceWithReference("cluster-kind", "shared", &clusterKind, &nonstandardGroup),
	}
	var out bytes.Buffer

	errOut, err := executeTree(
		t, factory.Static{OME: omefake.NewSimpleClientset(objects...), NS: "team-a"},
		&out, "shared", "--kind", "ServingRuntime",
	)

	require.NoError(t, err)
	assert.Empty(t, errOut)
	for _, name := range []string{"nil-kind", "empty-kind", "unknown-kind", "nonstandard-group"} {
		assert.Contains(t, out.String(), "InferenceService/"+name)
	}
	assert.NotContains(t, out.String(), "InferenceService/cluster-kind")
}

// TestTreeRejectsIncompleteClusterEvidenceBeforeDependencyResolution catches
// partial runtime evidence reaching InferenceService attribution.
func TestTreeRejectsIncompleteClusterEvidenceBeforeDependencyResolution(t *testing.T) {
	for _, test := range []struct {
		name            string
		configure       func(*omefake.Clientset)
		configureLimits func(*treeCommandDependencies)
		wantStatus      string
	}{
		{
			name: "forbidden",
			configure: func(client *omefake.Clientset) {
				client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
					return true, nil, apierrors.NewForbidden(
						schema.GroupResource{Group: "ome.io", Resource: "clusterservingruntimes"},
						"", errors.New("cluster runtime discovery denied"),
					)
				})
			},
			configureLimits: func(*treeCommandDependencies) {},
			wantStatus:      "unavailable",
		},
		{
			name: "truncated",
			configure: func(client *omefake.Clientset) {
				client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
					return true, &omev1beta1.ClusterServingRuntimeList{
						ListMeta: metav1.ListMeta{Continue: "unobserved-cluster-page"},
					}, nil
				})
			},
			configureLimits: func(dependencies *treeCommandDependencies) {
				dependencies.limits.MaxPages = 1
			},
			wantStatus: "truncated",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			clusterKind := "ClusterServingRuntime"
			client := omefake.NewSimpleClientset(
				&omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "shared"}},
				treeServiceWithReference("cluster-fallback", "shared", &clusterKind, nil),
				treeServiceWithReference("default-reference", "shared", nil, nil),
			)
			test.configure(client)
			dependencies := fixedTreeDependencies()
			test.configureLimits(&dependencies)
			var out bytes.Buffer

			errOut, err := executeTreeWithDependencies(
				t, factory.Static{OME: client, NS: "team-a"}, dependencies,
				&out, "shared", "--kind", "ServingRuntime",
			)

			require.Error(t, err)
			assert.Empty(t, errOut)
			assert.Contains(t, err.Error(),
				"ClusterServingRuntime collection is "+test.wantStatus)
			assert.Empty(t, out.String())
			assertNoInferenceServiceList(t, client.Actions())
		})
	}
}

func treeServiceWithReference(
	name string,
	runtimeName string,
	kind *string,
	apiGroup *string,
) *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name},
		Spec: omev1beta1.InferenceServiceSpec{Runtime: &omev1beta1.ServingRuntimeRef{
			Name: runtimeName, Kind: kind, APIGroup: apiGroup,
		}},
	}
}

// TestTreeKeepsCollidingRuntimeUsersOnTheirExactIdentity catches a
// namespaced runtime stealing a cluster runtime's users (or vice versa).
func TestTreeKeepsCollidingRuntimeUsersOnTheirExactIdentity(t *testing.T) {
	tests := []struct {
		kind       string
		wantTarget string
		wantUser   string
		otherUser  string
	}{
		{
			kind:       "ServingRuntime",
			wantTarget: "Target: ServingRuntime/ome-cli-tree-b4d9098/kome-tree-b4d9098-collision",
			wantUser:   "InferenceService/kome-tree-b4d9098-isvc-local-collision",
			otherUser:  "isvc-cluster-collision",
		},
		{
			kind:       "ClusterServingRuntime",
			wantTarget: "Target: ClusterServingRuntime/kome-tree-b4d9098-collision",
			wantUser:   "InferenceService/ome-cli-tree-b4d9098/kome-tree-b4d9098-isvc-cluster-collision",
			otherUser:  "isvc-local-collision",
		},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			client := omefake.NewSimpleClientset(treeFixtureObjects()...)
			var out bytes.Buffer

			errOut, err := executeTree(
				t,
				factory.Static{OME: client, NS: treeFixtureNamespace},
				&out,
				treeFixturePrefix+"collision", "--kind", test.kind,
			)

			require.NoError(t, err)
			assert.Empty(t, errOut)
			assert.Contains(t, out.String(), test.wantTarget)
			assert.Contains(t, out.String(), test.wantUser)
			assert.NotContains(t, out.String(), test.otherUser)
		})
	}
}

// TestTreePreservesInheritanceIssuePaths catches command composition
// flattening or dropping missing-parent, cycle, and maximum-depth evidence.
func TestTreePreservesInheritanceIssuePaths(t *testing.T) {
	tests := []struct {
		name    string
		objects []k8sruntime.Object
		target  string
		want    []string
	}{
		{
			name: "missing parent",
			objects: []k8sruntime.Object{
				clusterRuntimeWithExactName("orphan", "missing"),
			},
			target: "orphan",
			want: []string{
				"Issue: ParentMissing subject=ClusterServingRuntime/orphan parent=missing",
				"Issue path: ClusterServingRuntime/orphan",
			},
		},
		{
			name: "cycle",
			objects: []k8sruntime.Object{
				clusterRuntimeWithExactName("cycle-a", "cycle-b"),
				clusterRuntimeWithExactName("cycle-b", "cycle-a"),
			},
			target: "cycle-a",
			want: []string{
				"Issue: CycleDetected subject=ClusterServingRuntime/cycle-a parent=cycle-a",
				"Issue path: ClusterServingRuntime/cycle-a -> ClusterServingRuntime/cycle-b -> ClusterServingRuntime/cycle-a",
			},
		},
		{
			name: "maximum depth",
			objects: []k8sruntime.Object{
				clusterRuntimeWithExactName("depth-0", "depth-1"),
				clusterRuntimeWithExactName("depth-1", "depth-2"),
				clusterRuntimeWithExactName("depth-2", "depth-3"),
				clusterRuntimeWithExactName("depth-3", "depth-4"),
				clusterRuntimeWithExactName("depth-4", "depth-5"),
				clusterRuntimeWithExactName("depth-5", ""),
			},
			target: "depth-0",
			want: []string{
				"Issue: MaxDepthExceeded subject=ClusterServingRuntime/depth-0 parent=depth-5",
				"Issue path: ClusterServingRuntime/depth-0 -> ClusterServingRuntime/depth-1 -> ClusterServingRuntime/depth-2 -> ClusterServingRuntime/depth-3 -> ClusterServingRuntime/depth-4",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			errOut, err := executeTree(
				t,
				factory.Static{OME: omefake.NewSimpleClientset(test.objects...), NS: treeFixtureNamespace},
				&out,
				test.target, "--kind", "ClusterServingRuntime",
			)

			require.NoError(t, err)
			assert.Empty(t, errOut)
			for _, want := range test.want {
				assert.Contains(t, out.String(), want)
			}
		})
	}
}

func TestTreeValidationPrecedesClientAcquisition(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "zero arguments", wantError: "accepts 1 arg(s)"},
		{name: "two arguments", args: []string{"one", "two"}, wantError: "accepts 1 arg(s)"},
		{name: "invalid runtime name", args: []string{"Bad_Name"}, wantError: "runtime name"},
		{name: "invalid output", args: []string{"runtime", "--output", "xml"}, wantError: "unsupported output format"},
		{name: "invalid output first", args: []string{"Bad_Name", "--output", "xml"}, wantError: "unsupported output format"},
		{name: "invalid kind", args: []string{"runtime", "--kind", "Pod"}, wantError: "unsupported runtime kind"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &validationFactory{namespace: treeFixtureNamespace}
			var out bytes.Buffer

			errOut, err := executeTree(t, f, &out, test.args...)

			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantError)
			assert.Zero(t, f.namespaceCalls)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut)
		})
	}
}

// TestTreeHelpShowsCollisionSafeInvocations catches the discoverability path
// for operators who encounter same-name ServingRuntime/ClusterServingRuntime
// targets.
func TestTreeHelpShowsCollisionSafeInvocations(t *testing.T) {
	var out bytes.Buffer
	cmd := newTreeCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &out, ErrOut: &out,
	})
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	require.NoError(t, cmd.Execute())
	for _, want := range []string{
		"kubectl ome runtime tree vllm-runtime",
		"--kind ClusterServingRuntime",
		"--kind ServingRuntime -n team-a -o json",
		"explicitly reference each visible runtime head",
		"inheritance is rendered only from complete runtime collections",
		"InferenceService collection remains visible as partial dependency evidence",
	} {
		assert.Contains(t, out.String(), want)
	}
}

type treeNamespaceFactory struct {
	factory.Static
	namespaceCalls int
}

func (f *treeNamespaceFactory) Namespace() (string, bool, error) {
	f.namespaceCalls++
	return f.Static.Namespace()
}

// TestTreeUsesNamespaceOnlyForNamespacedDiscovery catches an explicit
// cluster-scoped target accidentally binding to a same-named namespaced
// runtime or failing because of an irrelevant current namespace.
func TestTreeUsesNamespaceOnlyForNamespacedDiscovery(t *testing.T) {
	clusterClient := omefake.NewSimpleClientset(clusterRuntimeWithExactName("root", ""))
	clusterFactory := &treeNamespaceFactory{Static: factory.Static{OME: clusterClient, NS: "Bad_NS"}}
	var clusterOut bytes.Buffer

	errOut, err := executeTree(
		t, clusterFactory, &clusterOut, "root", "--kind", "ClusterServingRuntime",
	)

	require.NoError(t, err)
	assert.Empty(t, errOut)
	assert.Zero(t, clusterFactory.namespaceCalls)
	assert.Contains(t, clusterOut.String(), "Target: ClusterServingRuntime/root")

	namespacedClient := omefake.NewSimpleClientset(treeServingRuntime("bridge", ""))
	namespacedFactory := &treeNamespaceFactory{Static: factory.Static{OME: namespacedClient, NS: "Bad_NS"}}
	var namespacedOut bytes.Buffer
	errOut, err = executeTree(
		t, namespacedFactory, &namespacedOut,
		treeFixturePrefix+"bridge", "--kind", "ServingRuntime",
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "runtime namespace")
	assert.Equal(t, 1, namespacedFactory.namespaceCalls)
	assert.Empty(t, namespacedClient.Actions())
	assert.Empty(t, namespacedOut.String())
	assert.Empty(t, errOut)
}

// TestTreeNamespacedTargetDoesNotWidenNamespacedReadsBeforeFailingClosed
// catches an unavailable cluster-runtime list triggering unnecessary
// all-namespace ServingRuntime or InferenceService reads.
func TestTreeNamespacedTargetDoesNotWidenNamespacedReadsBeforeFailingClosed(t *testing.T) {
	target := &omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Namespace: "team-a", Name: "local",
	}}
	serviceKind := "ServingRuntime"
	service := &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "predict"},
		Spec: omev1beta1.InferenceServiceSpec{Runtime: &omev1beta1.ServingRuntimeRef{
			Name: "local", Kind: &serviceKind,
		}},
	}
	client := omefake.NewSimpleClientset(target, service)
	client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "ome.io", Resource: "clusterservingruntimes"},
			"", errors.New("namespace role cannot list cluster runtimes"),
		)
	})
	client.PrependReactor("list", "servingruntimes", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if action.GetNamespace() != "team-a" {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: "ome.io", Resource: "servingruntimes"},
				"", errors.New("namespace role cannot list all runtimes"),
			)
		}
		return false, nil, nil
	})
	client.PrependReactor("list", "inferenceservices", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if action.GetNamespace() != "team-a" {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
				"", errors.New("namespace role cannot list all services"),
			)
		}
		return false, nil, nil
	})
	var out bytes.Buffer

	errOut, err := executeTree(
		t, factory.Static{OME: client, NS: "team-a"}, &out,
		"local", "--kind", "ServingRuntime",
	)

	require.Error(t, err)
	assert.Empty(t, errOut)
	assert.Contains(t, err.Error(), "ClusterServingRuntime collection is unavailable")
	assert.NotContains(t, err.Error(), "namespace role cannot list cluster runtimes")
	assert.Empty(t, out.String())
	require.Len(t, client.Actions(), 1)
	assert.Empty(t, client.Actions()[0].GetNamespace())
	assertNoInferenceServiceList(t, client.Actions())
}

// TestTreeImplicitClusterTargetExpandsOnlyAfterResolution catches implicit
// discovery either scanning every namespace before it knows the target kind,
// or omitting cross-namespace descendants after selecting a cluster runtime.
func TestTreeImplicitClusterTargetExpandsOnlyAfterResolution(t *testing.T) {
	client := omefake.NewSimpleClientset(
		clusterRuntimeWithExactName("root", ""),
		&omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{
			Namespace: "team-b", Name: "child",
			Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "root"},
		}},
	)
	var out bytes.Buffer

	errOut, err := executeTree(
		t, factory.Static{OME: client, NS: "team-a"}, &out, "root",
	)

	require.NoError(t, err)
	assert.Empty(t, errOut)
	assert.Contains(t, out.String(), "Context: Namespaced/team-b")
	assert.Contains(t, out.String(), "ServingRuntime/child")
	require.Len(t, client.Actions(), 4)
	assert.Equal(t, []string{
		"clusterservingruntimes/",
		"servingruntimes/team-a",
		"servingruntimes/",
		"inferenceservices/",
	}, []string{
		client.Actions()[0].GetResource().Resource + "/" + client.Actions()[0].GetNamespace(),
		client.Actions()[1].GetResource().Resource + "/" + client.Actions()[1].GetNamespace(),
		client.Actions()[2].GetResource().Resource + "/" + client.Actions()[2].GetNamespace(),
		client.Actions()[3].GetResource().Resource + "/" + client.Actions()[3].GetNamespace(),
	})
}

// TestTreeRejectsIncompleteImplicitClusterExpansionBeforeDependencies catches
// the namespace-scoped discovery pass being treated as a complete view of a
// cluster runtime's cross-namespace descendants.
func TestTreeRejectsIncompleteImplicitClusterExpansionBeforeDependencies(t *testing.T) {
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items: []omev1beta1.ClusterServingRuntime{*clusterRuntimeWithExactName("root", "")},
		}, nil
	})
	client.PrependReactor("list", "servingruntimes", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if action.GetNamespace() == "team-a" {
			return true, &omev1beta1.ServingRuntimeList{}, nil
		}
		assert.Empty(t, action.GetNamespace())
		return true, &omev1beta1.ServingRuntimeList{
			Items: []omev1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{
				Namespace: "team-b", Name: "child",
				Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "root"},
			}}},
			ListMeta: metav1.ListMeta{Continue: "more-cross-namespace-runtimes"},
		}, nil
	})
	dependencies := fixedTreeDependencies()
	dependencies.limits.MaxPages = 1
	var out bytes.Buffer

	errOut, err := executeTreeWithDependencies(
		t, factory.Static{OME: client, NS: "team-a"}, dependencies,
		&out, "root",
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "runtime tree requires complete runtime evidence")
	assert.Contains(t, err.Error(), "ServingRuntime collection is truncated")
	assert.Contains(t, err.Error(), "pages=1 items=1")
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
	assertNoInferenceServiceList(t, client.Actions())
	assert.Equal(t, []string{
		"clusterservingruntimes/",
		"servingruntimes/team-a",
		"servingruntimes/",
	}, []string{
		client.Actions()[0].GetResource().Resource + "/" + client.Actions()[0].GetNamespace(),
		client.Actions()[1].GetResource().Resource + "/" + client.Actions()[1].GetNamespace(),
		client.Actions()[2].GetResource().Resource + "/" + client.Actions()[2].GetNamespace(),
	})
}

// TestTreeRejectsUnknownTargetsBeforeDependencyCollection catches an invalid
// target paying for, or requiring RBAC to, an unrelated InferenceService scan.
func TestTreeRejectsUnknownTargetsBeforeDependencyCollection(t *testing.T) {
	client := omefake.NewSimpleClientset()
	var out bytes.Buffer

	errOut, err := executeTree(
		t, factory.Static{OME: client, NS: "team-a"}, &out,
		"missing", "--kind", "ServingRuntime",
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, runtimegraph.ErrTargetNotFound)
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
	for _, action := range client.Actions() {
		assert.NotEqual(t, "inferenceservices", action.GetResource().Resource,
			"unknown targets must fail before dependency collection")
	}
}

func TestTreePreservesAcquisitionAndSnapshotErrors(t *testing.T) {
	namespaceFailure := errors.New("namespace sentinel")
	clientFailure := errors.New("OME client sentinel")
	tests := []struct {
		name string
		f    factory.Factory
		args []string
		want error
	}{
		{
			name: "namespace",
			f:    &failingFactory{nsErr: namespaceFailure},
			args: []string{"runtime"},
			want: namespaceFailure,
		},
		{
			name: "client",
			f:    &failingFactory{omeErr: clientFailure},
			args: []string{"runtime", "--kind", "ClusterServingRuntime"},
			want: clientFailure,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			errOut, err := executeTree(t, test.f, &out, test.args...)
			require.Error(t, err)
			assert.ErrorIs(t, err, test.want)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut)
		})
	}

	duplicate := clusterRuntimeWithExactName("duplicate", "")
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items: []omev1beta1.ClusterServingRuntime{*duplicate, *duplicate.DeepCopy()},
		}, nil
	})
	var out bytes.Buffer
	errOut, err := executeTree(
		t,
		factory.Static{OME: client, NS: treeFixtureNamespace},
		&out,
		"duplicate", "--kind", "ClusterServingRuntime",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, runtimegraph.ErrDuplicateRuntime)
	assert.Empty(t, out.String())
	assert.Empty(t, errOut)
}

// TestTreeHonorsCancellationBeforeListing catches a canceled command issuing
// later API requests or downgrading cancellation to partial evidence.
func TestTreeHonorsCancellationBeforeListing(t *testing.T) {
	client := omefake.NewSimpleClientset(treeFixtureObjects()...)
	var out, errOut bytes.Buffer
	cmd := newTreeCmd(
		factory.Static{OME: client, NS: treeFixtureNamespace},
		genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut},
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{treeFixturePrefix + "root", "--kind", "ClusterServingRuntime"})

	err := cmd.Execute()

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, client.Actions())
	assert.Empty(t, out.String())
	assert.Empty(t, errOut.String())
}

func TestTreePreservesOutputWriterFailures(t *testing.T) {
	writerFailure := errors.New("tree writer sentinel")
	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			client := omefake.NewSimpleClientset(clusterRuntimeWithExactName("root", ""))
			_, err := executeTree(
				t,
				factory.Static{OME: client, NS: treeFixtureNamespace},
				failingWriter{err: writerFailure},
				"root", "--kind", "ClusterServingRuntime", "--output", format,
			)

			require.Error(t, err)
			assert.ErrorIs(t, err, writerFailure)
		})
	}
}

func clusterRuntimeWithExactName(name, parent string) *omev1beta1.ClusterServingRuntime {
	annotations := map[string]string{}
	if parent != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = parent
	}
	return &omev1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: name, Annotations: annotations,
	}}
}

func treeFixtureObjects() []k8sruntime.Object {
	return []k8sruntime.Object{
		treeClusterRuntime("root", ""),
		treeClusterRuntime("cluster-base", "root"),
		treeClusterRuntime("collision", "cluster-base"),
		treeServingRuntime("bridge", "cluster-base"),
		treeServingRuntime("branch-a", "bridge"),
		treeServingRuntime("collision", "bridge"),
		treeServingRuntime("leaf-a", "branch-a"),
		treeServingRuntime("leaf-b", "collision"),
		treeInferenceService("isvc-root", "root", "ClusterServingRuntime"),
		treeInferenceService("isvc-cluster-collision", "collision", "ClusterServingRuntime"),
		treeInferenceService("isvc-local-collision", "collision", "ServingRuntime"),
		treeInferenceService("isvc-leaf-a", "leaf-a", "ServingRuntime"),
		treeInferenceService("isvc-leaf-b", "leaf-b", "ServingRuntime"),
	}
}

func treeClusterRuntime(name, parent string) *omev1beta1.ClusterServingRuntime {
	return &omev1beta1.ClusterServingRuntime{
		ObjectMeta: treeObjectMeta("", name, parent),
		Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{
			Runner: &omev1beta1.RunnerSpec{},
		}},
	}
}

func treeServingRuntime(name, parent string) *omev1beta1.ServingRuntime {
	return &omev1beta1.ServingRuntime{
		ObjectMeta: treeObjectMeta(treeFixtureNamespace, name, parent),
		Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{
			Runner: &omev1beta1.RunnerSpec{},
		}},
	}
}

func treeObjectMeta(namespace, name, parent string) metav1.ObjectMeta {
	annotations := map[string]string{"private.example/token": treeSecretCanary}
	if parent != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = treeFixturePrefix + parent
	}
	return metav1.ObjectMeta{
		Namespace:   namespace,
		Name:        treeFixturePrefix + name,
		UID:         types.UID("uid-" + name),
		Labels:      map[string]string{"private.example/label": treeSecretCanary},
		Annotations: annotations,
	}
}

func treeInferenceService(name, runtimeName, runtimeKind string) *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: treeFixtureNamespace,
			Name:      treeFixturePrefix + name,
			UID:       types.UID("uid-" + name),
			Labels:    map[string]string{"private.example/label": treeSecretCanary},
		},
		Spec: omev1beta1.InferenceServiceSpec{
			Runtime: &omev1beta1.ServingRuntimeRef{
				Name: treeFixturePrefix + runtimeName, Kind: &runtimeKind,
			},
		},
	}
}

func assertNoInferenceServiceList(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		assert.NotEqual(t, "inferenceservices", action.GetResource().Resource,
			"incomplete runtime evidence must fail before dependency collection")
	}
}
