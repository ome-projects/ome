package runtimeusage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/runtimegraph"
)

func TestBuildSeparatesDefaultAndExplicitClusterKindsOnCollision(t *testing.T) {
	clusterKind := string(runtimegraph.KindClusterServingRuntime)
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "default-kind", runtimeReference("shared", nil, nil)),
		inferenceService("team-a", "explicit-cluster-kind", runtimeReference("shared", &clusterKind, nil)),
	}, runtimegraph.Snapshot{
		ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{{
			ObjectMeta: metav1.ObjectMeta{Name: "shared"},
		}},
		ServingRuntimes: []omev1beta1.ServingRuntime{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "shared"},
		}},
	})

	cluster, err := index.ForRuntime(runtimegraph.Identity{
		Kind: runtimegraph.KindClusterServingRuntime,
		Name: "shared",
	})
	require.NoError(t, err)
	local, err := index.ForRuntime(runtimegraph.Identity{
		Kind:      runtimegraph.KindServingRuntime,
		Namespace: "team-a",
		Name:      "shared",
	})
	require.NoError(t, err)

	assert.Equal(t, []InferenceServiceIdentity{
		{Namespace: "team-a", Name: "explicit-cluster-kind"},
	}, cluster.InferenceServices)
	assert.Equal(t, []InferenceServiceIdentity{
		{Namespace: "team-a", Name: "default-kind"},
	}, local.InferenceServices)
}

// TestBuildMatchesControllerReferenceResolution catches the offline usage
// index drifting from DefaultRuntimeFetcher.GetRuntime. A nonnil reference
// whose kind is empty or unrecognized resolves namespaced-first, while an
// explicit ClusterServingRuntime resolves cluster-first. APIGroup is not part
// of the controller's lookup decision.
func TestBuildMatchesControllerReferenceResolution(t *testing.T) {
	emptyKind := ""
	unknownKind := "OtherRuntime"
	clusterKind := string(runtimegraph.KindClusterServingRuntime)
	namespacedKind := string(runtimegraph.KindServingRuntime)
	nonstandardGroup := "runtime.example.test"
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "nil-kind", runtimeReference("shared", nil, nil)),
		inferenceService("team-a", "empty-kind", runtimeReference("shared", &emptyKind, nil)),
		inferenceService("team-a", "unknown-kind", runtimeReference("shared", &unknownKind, nil)),
		inferenceService("team-a", "nonstandard-group", runtimeReference("shared", &namespacedKind, &nonstandardGroup)),
		inferenceService("team-a", "cluster-kind", runtimeReference("shared", &clusterKind, &nonstandardGroup)),
	}, runtimeSnapshot(
		runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: "shared"},
		runtimegraph.Identity{Kind: runtimegraph.KindServingRuntime, Namespace: "team-a", Name: "shared"},
	))

	cluster, err := index.ForRuntime(runtimegraph.Identity{
		Kind: runtimegraph.KindClusterServingRuntime,
		Name: "shared",
	})
	require.NoError(t, err)
	local, err := index.ForRuntime(runtimegraph.Identity{
		Kind:      runtimegraph.KindServingRuntime,
		Namespace: "team-a",
		Name:      "shared",
	})
	require.NoError(t, err)

	assert.Equal(t, []InferenceServiceIdentity{{Namespace: "team-a", Name: "cluster-kind"}}, cluster.InferenceServices)
	assert.Equal(t, []InferenceServiceIdentity{
		{Namespace: "team-a", Name: "empty-kind"},
		{Namespace: "team-a", Name: "nil-kind"},
		{Namespace: "team-a", Name: "nonstandard-group"},
		{Namespace: "team-a", Name: "unknown-kind"},
	}, local.InferenceServices)
}

func TestBuildResolvesDefaultAndClusterKindsToNamespacedOnlyRuntime(t *testing.T) {
	clusterKind := string(runtimegraph.KindClusterServingRuntime)
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "default-kind", runtimeReference("local-only", nil, nil)),
		inferenceService("team-a", "explicit-cluster-kind", runtimeReference("local-only", &clusterKind, nil)),
	}, runtimegraph.Snapshot{
		ServingRuntimes: []omev1beta1.ServingRuntime{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "local-only"},
		}},
	})

	local, err := index.ForRuntime(runtimegraph.Identity{
		Kind:      runtimegraph.KindServingRuntime,
		Namespace: "team-a",
		Name:      "local-only",
	})
	require.NoError(t, err)

	assert.Equal(t, []InferenceServiceIdentity{
		{Namespace: "team-a", Name: "default-kind"},
		{Namespace: "team-a", Name: "explicit-cluster-kind"},
	}, local.InferenceServices)
}

func TestBuildDoesNotFallbackNamespacedKindToClusterRuntime(t *testing.T) {
	namespacedKind := string(runtimegraph.KindServingRuntime)
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "local-reference", runtimeReference("cluster-only", &namespacedKind, nil)),
	}, runtimegraph.Snapshot{
		ClusterServingRuntimes: []omev1beta1.ClusterServingRuntime{{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-only"},
		}},
	})

	cluster, err := index.ForRuntime(runtimegraph.Identity{
		Kind: runtimegraph.KindClusterServingRuntime,
		Name: "cluster-only",
	})
	require.NoError(t, err)
	assert.Empty(t, cluster.InferenceServices)
	assert.Equal(t, []ReferenceEvidence{{
		InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "local-reference"},
		State:            ReferenceUnresolved,
		RuntimeName:      "cluster-only",
		Reason:           ReasonRuntimeNotFound,
		Occurrences:      1,
	}}, index.References())
}

func TestBuildPreservesMissingRuntimeEvidence(t *testing.T) {
	clusterKind := string(runtimegraph.KindClusterServingRuntime)
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "default-kind", runtimeReference("missing", nil, nil)),
		inferenceService("team-a", "explicit-cluster-kind", runtimeReference("missing", &clusterKind, nil)),
	}, runtimegraph.Snapshot{})

	assert.Equal(t, []ReferenceEvidence{
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "default-kind"},
			State:            ReferenceUnresolved,
			RuntimeName:      "missing",
			Reason:           ReasonRuntimeNotFound,
			Occurrences:      1,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "explicit-cluster-kind"},
			State:            ReferenceUnresolved,
			RuntimeName:      "missing",
			Reason:           ReasonRuntimeNotFound,
			Occurrences:      1,
		},
	}, index.References())
}

func TestBuildIndexesClusterReferencesAcrossNamespaces(t *testing.T) {
	services := []omev1beta1.InferenceService{
		inferenceService("team-z", "z-service", runtimeReference("shared", nil, nil)),
		inferenceService("team-a", "a-service", runtimeReference(
			"shared",
			ptr.To(string(runtimegraph.KindClusterServingRuntime)),
			ptr.To(omev1beta1.SchemeGroupVersion.Group),
		)),
	}
	index := Build(services, runtimeSnapshot(
		runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: "shared"},
	))

	projection, err := index.ForRuntime(runtimegraph.Identity{
		Kind: runtimegraph.KindClusterServingRuntime,
		Name: "shared",
	})

	require.NoError(t, err)
	assert.Equal(t, Projection{
		Runtime: runtimegraph.Identity{
			Kind: runtimegraph.KindClusterServingRuntime,
			Name: "shared",
		},
		InferenceServices: []InferenceServiceIdentity{
			{Namespace: "team-a", Name: "a-service"},
			{Namespace: "team-z", Name: "z-service"},
		},
	}, projection)
	assert.Equal(t, []ReferenceEvidence{
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "a-service"},
			State:            ReferenceResolved,
			RuntimeName:      "shared",
			Runtime: identityPointer(runtimegraph.Identity{
				Kind: runtimegraph.KindClusterServingRuntime,
				Name: "shared",
			}),
			Occurrences: 1,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-z", Name: "z-service"},
			State:            ReferenceResolved,
			RuntimeName:      "shared",
			Runtime: identityPointer(runtimegraph.Identity{
				Kind: runtimegraph.KindClusterServingRuntime,
				Name: "shared",
			}),
			Occurrences: 1,
		},
	}, index.References())
}

func TestBuildUsesFullScopeForNamespacedReferencesAndNameCollisions(t *testing.T) {
	clusterKind := string(runtimegraph.KindClusterServingRuntime)
	namespacedKind := string(runtimegraph.KindServingRuntime)
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "cluster-user", runtimeReference("shared", &clusterKind, nil)),
		inferenceService("team-b", "local-user", runtimeReference("shared", &namespacedKind, nil)),
		inferenceService("team-a", "local-user", runtimeReference("shared", &namespacedKind, nil)),
	}, runtimeSnapshot(
		runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: "shared"},
		runtimegraph.Identity{Kind: runtimegraph.KindServingRuntime, Namespace: "team-a", Name: "shared"},
		runtimegraph.Identity{Kind: runtimegraph.KindServingRuntime, Namespace: "team-b", Name: "shared"},
	))

	cluster, err := index.ForRuntime(runtimegraph.Identity{
		Kind: runtimegraph.KindClusterServingRuntime,
		Name: "shared",
	})
	require.NoError(t, err)
	teamA, err := index.ForRuntime(runtimegraph.Identity{
		Kind:      runtimegraph.KindServingRuntime,
		Namespace: "team-a",
		Name:      "shared",
	})
	require.NoError(t, err)
	teamB, err := index.ForRuntime(runtimegraph.Identity{
		Kind:      runtimegraph.KindServingRuntime,
		Namespace: "team-b",
		Name:      "shared",
	})

	require.NoError(t, err)
	assert.Equal(t, []InferenceServiceIdentity{{Namespace: "team-a", Name: "cluster-user"}}, cluster.InferenceServices)
	assert.Equal(t, []InferenceServiceIdentity{{Namespace: "team-a", Name: "local-user"}}, teamA.InferenceServices)
	assert.Equal(t, []InferenceServiceIdentity{{Namespace: "team-b", Name: "local-user"}}, teamB.InferenceServices)
}

func TestBuildDistinguishesAutomaticSelectionFromEmptyRuntimeName(t *testing.T) {
	unsupportedKind := "OtherRuntime"
	unexpectedGroup := "other.example"
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "nil-reference", nil),
		inferenceService("team-a", "empty-reference", runtimeReference("", &unsupportedKind, &unexpectedGroup)),
	}, runtimegraph.Snapshot{})

	projection, err := index.ForRuntime(runtimegraph.Identity{
		Kind: runtimegraph.KindClusterServingRuntime,
		Name: "unused",
	})

	require.NoError(t, err)
	assert.Empty(t, projection.InferenceServices)
	assert.Equal(t, []ReferenceEvidence{
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "empty-reference"},
			State:            ReferenceInvalid,
			Reason:           ReasonInvalidRuntimeName,
			Occurrences:      1,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "nil-reference"},
			State:            ReferenceUnresolved,
			Reason:           ReasonAutomaticSelection,
			Occurrences:      1,
		},
	}, index.References())
}

func TestBuildIgnoresAPIGroupAndDefaultsOtherKindsForMissingRuntime(t *testing.T) {
	empty := ""
	unsupportedKind := "OtherRuntime"
	unexpectedGroup := "other.example"
	namespacedKind := string(runtimegraph.KindServingRuntime)
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "empty-kind", runtimeReference("runtime-a", &empty, nil)),
		inferenceService("team-a", "unsupported-kind", runtimeReference("runtime-b", &unsupportedKind, nil)),
		inferenceService("team-a", "empty-group", runtimeReference("runtime-c", &namespacedKind, &empty)),
		inferenceService("team-a", "unexpected-group", runtimeReference("runtime-d", &namespacedKind, &unexpectedGroup)),
	}, runtimegraph.Snapshot{})

	assert.Equal(t, []ReferenceEvidence{
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "empty-group"},
			State:            ReferenceUnresolved,
			RuntimeName:      "runtime-c",
			Reason:           ReasonRuntimeNotFound,
			Occurrences:      1,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "empty-kind"},
			State:            ReferenceUnresolved,
			RuntimeName:      "runtime-a",
			Reason:           ReasonRuntimeNotFound,
			Occurrences:      1,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "unexpected-group"},
			State:            ReferenceUnresolved,
			RuntimeName:      "runtime-d",
			Reason:           ReasonRuntimeNotFound,
			Occurrences:      1,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "unsupported-kind"},
			State:            ReferenceUnresolved,
			RuntimeName:      "runtime-b",
			Reason:           ReasonRuntimeNotFound,
			Occurrences:      1,
		},
	}, index.References())
}

func TestBuildPreservesDuplicateObjectsAsAmbiguousEvidence(t *testing.T) {
	clusterKind := string(runtimegraph.KindClusterServingRuntime)
	namespacedKind := string(runtimegraph.KindServingRuntime)
	index := Build([]omev1beta1.InferenceService{
		inferenceService("team-a", "duplicate", runtimeReference("cluster-runtime", &clusterKind, nil)),
		inferenceService("team-a", "healthy", runtimeReference("cluster-runtime", &clusterKind, nil)),
		inferenceService("team-a", "duplicate", runtimeReference("local-runtime", &namespacedKind, nil)),
	}, runtimeSnapshot(
		runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: "cluster-runtime"},
		runtimegraph.Identity{Kind: runtimegraph.KindServingRuntime, Namespace: "team-a", Name: "local-runtime"},
	))

	cluster, err := index.ForRuntime(runtimegraph.Identity{
		Kind: runtimegraph.KindClusterServingRuntime,
		Name: "cluster-runtime",
	})
	require.NoError(t, err)
	local, err := index.ForRuntime(runtimegraph.Identity{
		Kind:      runtimegraph.KindServingRuntime,
		Namespace: "team-a",
		Name:      "local-runtime",
	})

	require.NoError(t, err)
	assert.Equal(t, []InferenceServiceIdentity{{Namespace: "team-a", Name: "healthy"}}, cluster.InferenceServices)
	assert.Empty(t, local.InferenceServices)
	assert.Equal(t, []ReferenceEvidence{
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "duplicate"},
			State:            ReferenceAmbiguous,
			Reason:           ReasonDuplicateInferenceService,
			Occurrences:      2,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a", Name: "healthy"},
			State:            ReferenceResolved,
			RuntimeName:      "cluster-runtime",
			Runtime: identityPointer(runtimegraph.Identity{
				Kind: runtimegraph.KindClusterServingRuntime,
				Name: "cluster-runtime",
			}),
			Occurrences: 1,
		},
	}, index.References())
}

func TestBuildPreservesInvalidObjectIdentityEvidence(t *testing.T) {
	index := Build([]omev1beta1.InferenceService{
		inferenceService("", "missing-namespace", runtimeReference("runtime", nil, nil)),
		inferenceService("team-a", "", runtimeReference("runtime", nil, nil)),
	}, runtimegraph.Snapshot{})

	assert.Equal(t, []ReferenceEvidence{
		{
			InferenceService: InferenceServiceIdentity{Name: "missing-namespace"},
			State:            ReferenceInvalid,
			Reason:           ReasonInvalidInferenceService,
			Occurrences:      1,
		},
		{
			InferenceService: InferenceServiceIdentity{Namespace: "team-a"},
			State:            ReferenceInvalid,
			Reason:           ReasonInvalidInferenceService,
			Occurrences:      1,
		},
	}, index.References())
}

func TestIndexIsDeterministicAndImmutable(t *testing.T) {
	clusterKind := string(runtimegraph.KindClusterServingRuntime)
	group := omev1beta1.SchemeGroupVersion.Group
	services := []omev1beta1.InferenceService{
		inferenceService("team-z", "service-z", runtimeReference("runtime", &clusterKind, &group)),
		inferenceService("team-a", "service-a", runtimeReference("runtime", nil, nil)),
	}
	original := deepCopyInferenceServices(services)
	reversed := []omev1beta1.InferenceService{*services[1].DeepCopy(), *services[0].DeepCopy()}

	snapshot := runtimeSnapshot(
		runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: "runtime"},
	)
	index := Build(services, snapshot)
	reversedIndex := Build(reversed, snapshot)
	assert.Equal(t, original, services)
	services[0].Name = "changed"
	services[0].Spec.Runtime.Name = "changed"
	*services[0].Spec.Runtime.Kind = string(runtimegraph.KindServingRuntime)
	*services[0].Spec.Runtime.APIGroup = "changed.example"

	target := runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: "runtime"}
	first, err := index.ForRuntime(target)
	require.NoError(t, err)
	fromReversed, err := reversedIndex.ForRuntime(target)
	require.NoError(t, err)
	firstReferences := index.References()
	assert.Equal(t, first, fromReversed)
	assert.Equal(t, firstReferences, reversedIndex.References())

	first.InferenceServices[0].Name = "mutated"
	firstReferences[0].InferenceService.Name = "mutated"
	firstReferences[0].Runtime.Name = "mutated"
	again, err := index.ForRuntime(target)
	require.NoError(t, err)

	assert.Equal(t, []InferenceServiceIdentity{
		{Namespace: "team-a", Name: "service-a"},
		{Namespace: "team-z", Name: "service-z"},
	}, again.InferenceServices)
	assert.Equal(t, "runtime", index.References()[0].Runtime.Name)
}

func TestForRuntimeRejectsIncompleteOrUnsupportedIdentity(t *testing.T) {
	index := Build(nil, runtimegraph.Snapshot{})
	tests := []struct {
		name     string
		identity runtimegraph.Identity
	}{
		{name: "name required", identity: runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime}},
		{name: "kind required", identity: runtimegraph.Identity{Name: "runtime"}},
		{name: "kind supported", identity: runtimegraph.Identity{Kind: runtimegraph.Kind("Other"), Name: "runtime"}},
		{name: "namespaced namespace required", identity: runtimegraph.Identity{Kind: runtimegraph.KindServingRuntime, Name: "runtime"}},
		{name: "cluster namespace forbidden", identity: runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Namespace: "team-a", Name: "runtime"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := index.ForRuntime(test.identity)
			assert.ErrorIs(t, err, ErrInvalidRuntimeIdentity)
		})
	}
}

func inferenceService(namespace, name string, runtime *omev1beta1.ServingRuntimeRef) omev1beta1.InferenceService {
	return omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       omev1beta1.InferenceServiceSpec{Runtime: runtime},
	}
}

func runtimeReference(name string, kind, apiGroup *string) *omev1beta1.ServingRuntimeRef {
	return &omev1beta1.ServingRuntimeRef{Name: name, Kind: kind, APIGroup: apiGroup}
}

func runtimeSnapshot(identities ...runtimegraph.Identity) runtimegraph.Snapshot {
	var snapshot runtimegraph.Snapshot
	for _, identity := range identities {
		switch identity.Kind {
		case runtimegraph.KindClusterServingRuntime:
			snapshot.ClusterServingRuntimes = append(snapshot.ClusterServingRuntimes, omev1beta1.ClusterServingRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: identity.Name},
			})
		case runtimegraph.KindServingRuntime:
			snapshot.ServingRuntimes = append(snapshot.ServingRuntimes, omev1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{Namespace: identity.Namespace, Name: identity.Name},
			})
		}
	}
	return snapshot
}

func identityPointer(identity runtimegraph.Identity) *runtimegraph.Identity { return &identity }

func deepCopyInferenceServices(services []omev1beta1.InferenceService) []omev1beta1.InferenceService {
	result := make([]omev1beta1.InferenceService, len(services))
	for i := range services {
		result[i] = *services[i].DeepCopy()
	}
	return result
}
