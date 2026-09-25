package effective

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

var testPinLimits = paging.Limits{
	PageSize: 2, MaxItems: 10, MaxPages: 5, RequestTimeout: time.Second,
}

func runtimeRaw(raw string) runtime.RawExtension { return runtime.RawExtension{Raw: []byte(raw)} }

func runtimeSpecFixture(marker string) *v1beta1.ServingRuntimeSpec {
	return &v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{
			Container: corev1.Container{Name: "runner", Image: marker},
		}},
		ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
			NodeSelector: map[string]string{"version": marker},
		},
	}
}

func revisionFixture(t *testing.T, kind, sourceNamespace, runtimeName string, spec *v1beta1.ServingRuntimeSpec) *appsv1.ControllerRevision {
	t.Helper()
	full, short, err := runtimerevision.Hash(spec)
	require.NoError(t, err)
	assert.Len(t, full, 64)
	raw, err := json.Marshal(spec)
	require.NoError(t, err)
	name := runtimerevision.Name(runtimerevision.SourceKind(kind), sourceNamespace, runtimeName, short)
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ome",
			Labels: map[string]string{
				constants.RuntimeRevisionOfLabelKey:          runtimeName,
				constants.RuntimeRevisionOfKindLabelKey:      kind,
				constants.RuntimeRevisionOfNamespaceLabelKey: sourceNamespace,
				constants.RuntimeRevisionHashLabelKey:        short,
			},
			Annotations: map[string]string{
				constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue,
			},
		},
		Data: runtime.RawExtension{Raw: raw}, Revision: 1,
	}
}

type liveRuntimeResolverFunc func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error)

func (f liveRuntimeResolverFunc) ResolveLive(ctx context.Context, isvc *v1beta1.InferenceService) (*LiveConfiguration, error) {
	return f(ctx, isvc)
}

func TestResolveManagedPinUsesDeclaredClusterSourceDespiteNamespacedLiveFallback(t *testing.T) {
	autoSync := false
	spec := runtimeSpecFixture("managed")
	revision := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "same-name", spec)
	resolver, err := newRuntimePinResolver(
		func(namespace string) revisionNamespace {
			assert.Equal(t, "ome", namespace)
			return revisionNamespaceStub{get: func(_ context.Context, name string, _ metav1.GetOptions) (*appsv1.ControllerRevision, error) {
				assert.Equal(t, revision.Name, name)
				return revision.DeepCopy(), nil
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("same-name", runtimeselector.KindServingRuntime, "workloads", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("same-name", &autoSync, "")
	isvc.Status.PinnedRevisionName = revision.Name

	got, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, RuntimePinModeManagedPin, got.PinMode)
	assert.Equal(t, RuntimePinStateResolved, got.PinState)
	assert.Equal(t, runtimeselector.KindClusterServingRuntime, got.DeclaredSourceKind)
	assert.Empty(t, got.DeclaredSourceNamespace)
	active, err := got.RequireActive()
	require.NoError(t, err)
	assert.Equal(t, ConfigurationOriginControllerRevision, active.Origin)
	assert.Equal(t, revision.Name, got.ActiveRevisionName)
}

func TestResolveExplicitPinGetsRequestedThenReportedAndNeverFallsBack(t *testing.T) {
	autoSync := false
	liveSpec := runtimeSpecFixture("live")
	requestedSpec := runtimeSpecFixture("requested")
	reportedSpec := runtimeSpecFixture("reported")
	requested := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", requestedSpec)
	reported := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", reportedSpec)
	byName := map[string]*appsv1.ControllerRevision{requested.Name: requested, reported.Name: reported}
	var gets []string
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{get: func(_ context.Context, name string, _ metav1.GetOptions) (*appsv1.ControllerRevision, error) {
				gets = append(gets, name)
				return byName[name].DeepCopy(), nil
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			live := livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false)
			live.Runtime.spec = liveSpec
			return live, nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("runtime", &autoSync, requested.Name)
	isvc.Status.PinnedRevisionName = reported.Name

	got, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, []string{requested.Name, reported.Name}, gets)
	assert.Equal(t, RuntimePinModeExplicitPin, got.PinMode)
	assert.Equal(t, RuntimePinStateDesiredReportedMismatch, got.PinState)
	assert.Equal(t, requested.Name, got.ActiveRevisionName)
	active, err := got.RequireActive()
	require.NoError(t, err)
	assert.Equal(t, requested.Name, active.RevisionName)
	assert.Equal(t, RuntimeHashRelationDifferent, got.LiveToActive)
}

func TestResolveDeduplicatesEqualRequestedAndReportedRevision(t *testing.T) {
	autoSync := false
	revision := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("same"))
	var gets int
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
				gets++
				return revision.DeepCopy(), nil
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("runtime", &autoSync, revision.Name)
	isvc.Status.PinnedRevisionName = revision.Name

	got, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, 1, gets)
	observations := got.RevisionObservations()
	require.Len(t, observations, 1)
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleRequested, RuntimeRevisionRoleReported, RuntimeRevisionRoleActive}, observations[0].Roles())
}

func TestResolveDeduplicatesByExpectedNameWhenServerReturnsMismatchedIdentity(t *testing.T) {
	autoSync := false
	expectedName := "requested-name"
	returned := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("same"))
	returned.Name = "returned-other"
	var gets int
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
				gets++
				return returned.DeepCopy(), nil
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("runtime", &autoSync, expectedName)
	isvc.Status.PinnedRevisionName = expectedName

	state, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, 1, gets)
	assert.Equal(t, RuntimePinStateResolved, state.PinState)
	active, err := state.RequireActive()
	require.NoError(t, err)
	assert.Equal(t, expectedName, active.RevisionName)
	observations := state.RevisionObservations()
	require.Len(t, observations, 1)
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleRequested, RuntimeRevisionRoleReported, RuntimeRevisionRoleActive}, observations[0].Roles())
}

func TestResolveDoesNotDedupeReturnedNameAgainstDifferentExactRequestKey(t *testing.T) {
	autoSync := false
	requestedA := "requested-a"
	returnedB := revisionFixture(t, runtimeselector.KindClusterServingRuntime, "", "runtime", runtimeSpecFixture("b"))
	reportedB := returnedB.Name
	var gets []string
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{get: func(_ context.Context, name string, _ metav1.GetOptions) (*appsv1.ControllerRevision, error) {
				gets = append(gets, name)
				return returnedB.DeepCopy(), nil
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return livePinFixture("runtime", runtimeselector.KindClusterServingRuntime, "", false), nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("runtime", &autoSync, requestedA)
	isvc.Status.PinnedRevisionName = reportedB

	state, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, []string{requestedA, reportedB}, gets)
	assert.Equal(t, RuntimePinStateDesiredReportedMismatch, state.PinState)
	assert.Equal(t, requestedA, state.ActiveRevisionName)
	observations := state.RevisionObservations()
	require.Len(t, observations, 2)
	byExpected := make(map[string]RuntimeRevisionObservation, len(observations))
	for _, observation := range observations {
		byExpected[observation.ExpectedName()] = observation
	}
	assert.Equal(t, returnedB.Name, byExpected[requestedA].ReturnedName())
	assert.Equal(t, returnedB.Name, byExpected[reportedB].ReturnedName())
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleRequested, RuntimeRevisionRoleActive}, byExpected[requestedA].Roles())
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleReported}, byExpected[reportedB].Roles())
}

func TestResolveNilLiveSpecIsBoundedUnavailable(t *testing.T) {
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace { return revisionNamespaceStub{} },
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return &LiveConfiguration{Runtime: RuntimeResolution{Name: "runtime"}}, nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)

	state, err := resolver.Resolve(context.Background(), pinISVC("runtime", nil, ""), RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, RuntimePinStateUnavailable, state.PinState)
	_, err = state.RequireActive()
	assert.Error(t, err)
	issues := state.SourceIssues()
	require.Len(t, issues, 1)
	assert.Equal(t, RuntimeSourceIssueLiveUnavailable, issues[0].Code)
}

func TestResolveEmptyRuntimeNameDoesNotReadRetainedPinAgainstSelectedRuntime(t *testing.T) {
	var revisionCalls int
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{get: func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
				revisionCalls++
				return nil, errors.New("must not read retained pin")
			}}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			live := livePinFixture("selected-runtime", runtimeselector.KindClusterServingRuntime, "", false)
			live.Runtime.SelectionSource = RuntimeSelected
			return live, nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("", ptr.To(false), "")
	isvc.Status.PinnedRevisionName = "retained-old-pin"

	state, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, RuntimePinModeAutoSync, state.PinMode)
	assert.Equal(t, RuntimePinStateNotApplicable, state.PinState)
	assert.Equal(t, "retained-old-pin", state.ReportedRevisionName)
	assert.Zero(t, revisionCalls)
	assert.Empty(t, state.RevisionObservations())
}

type revisionNamespaceStub struct {
	get  func(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error)
	list func(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error)
}

func (s revisionNamespaceStub) Get(ctx context.Context, name string, opts metav1.GetOptions) (*appsv1.ControllerRevision, error) {
	if s.get == nil {
		return nil, errors.New("unexpected revision GET")
	}
	return s.get(ctx, name, opts)
}

func (s revisionNamespaceStub) List(ctx context.Context, opts metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
	if s.list == nil {
		return nil, errors.New("unexpected revision LIST")
	}
	return s.list(ctx, opts)
}

func livePinFixture(name, kind, namespace string, disabled bool) *LiveConfiguration {
	spec := &v1beta1.ServingRuntimeSpec{Disabled: ptr.To(disabled)}
	return &LiveConfiguration{Runtime: RuntimeResolution{
		Name: name, Kind: kind, Namespace: namespace, SelectionSource: RuntimeExplicit, spec: spec,
	}}
}

func pinISVC(name string, autoSync *bool, revision string) *v1beta1.InferenceService {
	ref := &v1beta1.ServingRuntimeRef{Name: name, AutoSync: autoSync}
	if revision != "" {
		ref.Revision = ptr.To(revision)
	}
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads", Generation: 3},
		Spec:       v1beta1.InferenceServiceSpec{Runtime: ref, Engine: &v1beta1.EngineSpec{}},
	}
}

func TestNewRuntimePinResolverRejectsInvalidDependencies(t *testing.T) {
	validLive := &RuntimeResolver{}
	validRevisions := k8sfake.NewSimpleClientset().AppsV1()

	_, err := NewRuntimePinResolver(nil, validLive, "ome", testPinLimits)
	assert.EqualError(t, err, "ControllerRevision client must not be nil")

	_, err = NewRuntimePinResolver(validRevisions, nil, "ome", testPinLimits)
	assert.EqualError(t, err, "live runtime resolver must not be nil")

	_, err = NewRuntimePinResolver(validRevisions, validLive, "", testPinLimits)
	assert.EqualError(t, err, "OME namespace must not be empty")

	invalidLimits := testPinLimits
	invalidLimits.RequestTimeout = 0
	_, err = NewRuntimePinResolver(validRevisions, validLive, "ome", invalidLimits)
	assert.EqualError(t, err, "revision paging limits are invalid")
}

func TestRuntimePinConstructorsRejectInvalidRecoveryOverridesBeforeClientUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*paging.Limits)
	}{
		{
			name: "request recovery limit",
			mutate: func(limits *paging.Limits) {
				limits.MaxRequests = limits.MaxPages*2 + 1
			},
		},
		{
			name: "item recovery limit",
			mutate: func(limits *paging.Limits) {
				limits.MaxConsumedItems = limits.MaxItems*2 + 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			invalid := testPinLimits
			test.mutate(&invalid)
			kube := k8sfake.NewSimpleClientset()

			resolver, err := NewRuntimePinResolver(
				kube.AppsV1(), &RuntimeResolver{}, "ome", invalid,
			)

			require.EqualError(t, err, "revision paging limits are invalid")
			assert.Nil(t, resolver)
			assert.Empty(t, kube.Actions())

			dependencyCalls := 0
			resolver, err = newRuntimePinResolver(
				func(string) revisionNamespace {
					dependencyCalls++
					return revisionNamespaceStub{}
				},
				liveRuntimeResolverFunc(func(
					context.Context, *v1beta1.InferenceService,
				) (*LiveConfiguration, error) {
					dependencyCalls++
					return nil, errors.New("unexpected live resolution")
				}),
				"ome", invalid,
			)

			require.EqualError(t, err, "revision paging limits are invalid")
			assert.Nil(t, resolver)
			assert.Zero(t, dependencyCalls)
		})
	}
}

func TestResolveAutoSyncUsesLiveAndRetainsStatusPinAsInactiveEvidence(t *testing.T) {
	var revisionCalls int
	live := livePinFixture("team-runtime", runtimeselector.KindClusterServingRuntime, "", false)
	resolver, err := newRuntimePinResolver(
		func(string) revisionNamespace {
			return revisionNamespaceStub{
				get: func(_ context.Context, name string, _ metav1.GetOptions) (*appsv1.ControllerRevision, error) {
					revisionCalls++
					return &appsv1.ControllerRevision{
						ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ome"},
						Data:       runtimeRaw(`{}`), Revision: 1,
					}, nil
				},
			}
		},
		liveRuntimeResolverFunc(func(context.Context, *v1beta1.InferenceService) (*LiveConfiguration, error) {
			return live, nil
		}),
		"ome", testPinLimits,
	)
	require.NoError(t, err)
	isvc := pinISVC("team-runtime", nil, "")
	isvc.Status.PinnedRevisionName = "stale-status-pin"

	got, err := resolver.Resolve(context.Background(), isvc, RuntimeResolveOptions{})

	require.NoError(t, err)
	assert.Equal(t, RuntimePinModeAutoSync, got.PinMode)
	assert.Equal(t, RuntimePinStateNotApplicable, got.PinState)
	assert.Equal(t, runtimeselector.KindClusterServingRuntime, got.DeclaredSourceKind)
	assert.Empty(t, got.DeclaredSourceNamespace)
	assert.Empty(t, got.RequestedRevisionName)
	assert.Equal(t, "stale-status-pin", got.ReportedRevisionName)
	assert.Empty(t, got.ActiveRevisionName)
	active, err := got.RequireActive()
	require.NoError(t, err)
	assert.Equal(t, ConfigurationOriginLiveRuntime, active.Origin)
	observations := got.RevisionObservations()
	require.Len(t, observations, 1)
	assert.Equal(t, []RuntimeRevisionRole{RuntimeRevisionRoleReported}, observations[0].Roles())
	assert.Equal(t, 1, revisionCalls)
	assert.False(t, got.HistoryRequested)
	assert.False(t, got.HistoryComplete)
}
