package effective

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	kfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/yaml"
)

func runtimeSyncSourceFixture(t *testing.T) (*v1beta1.InferenceService, *appsv1.ControllerRevision, *v1beta1.ClusterServingRuntime) {
	t.Helper()
	revision := revisionFixture(t, "ClusterServingRuntime", "", "runtime", runtimeSpecFixture("old"))
	revision.UID = "revision-uid"
	revision.ResourceVersion = "15"
	v := pinISVC("runtime", ptr.To(false), "")
	v.UID = "parent-uid"
	v.ResourceVersion = "16"
	v.Status.PinnedRevisionName = revision.Name
	v.Status.Conditions = []knapis.Condition{{Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: corev1.ConditionTrue, Reason: "RevisionMismatch"}}
	live := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", UID: "runtime-uid", ResourceVersion: "17", Generation: 2}, Spec: *runtimeSpecFixture("new")}
	return v, revision, live
}
func runtimeSyncSourceResolver(t *testing.T, kube *kfake.Clientset, objects ...ctrlclient.Object) *RuntimeSyncResolver {
	t.Helper()
	client := ctrlfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(objects...).Build()
	resolver, err := NewRuntimeSyncResolver(kube.AppsV1(), client, "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: time.Second})
	require.NoError(t, err)
	return resolver
}

func TestRuntimeSyncEvidenceKeepsGlobalGenerationAdvisoryAndPrivate(t *testing.T) {
	for _, observed := range []int64{0, 2, 3, 4} {
		t.Run(fmt.Sprint(observed), func(t *testing.T) {
			v, revision, live := runtimeSyncSourceFixture(t)
			v.Status.ObservedGeneration = observed
			v.Annotations = map[string]string{constants.RuntimeSyncAnnotationKey: "PRIVATE_OLD"}
			v.Status.LastRuntimeSyncToken = "PRIVATE_OLD"
			resolver := runtimeSyncSourceResolver(t, kfake.NewSimpleClientset(revision), live)
			e, err := resolver.Resolve(context.Background(), v)
			require.NoError(t, err)
			require.True(t, e.MatchesInferenceService(v))
			require.Empty(t, e.NativeComponents())
			require.False(t, e.TokenAvailable("PRIVATE_OLD"))
			again, err := resolver.Resolve(context.Background(), v)
			require.NoError(t, err)
			require.True(t, e.SameSnapshot(again))
			rows := e.PreviewRows()
			require.Contains(t, rows, [2]string{"Freshness", "Unverifiable (advisory global status)"})
			rows[0][1] = "attacker"
			require.NotEqual(t, rows, e.PreviewRows())
			require.NotContains(t, fmt.Sprintf("%v %+v %#v %q", e, e, e, e), "PRIVATE_OLD")
			_, err = json.Marshal(e)
			require.Error(t, err)
			_, err = yaml.Marshal(e)
			require.Error(t, err)
			modified := v.DeepCopy()
			modified.Status.LastRuntimeSyncToken = "changed"
			require.False(t, e.MatchesInferenceService(modified))
		})
	}
	require.False(t, (RuntimeSyncEvidence{}).SameSnapshot(RuntimeSyncEvidence{}))
}

func TestRuntimeSyncRejectsUnsafePinDriftAndSources(t *testing.T) {
	cases := []struct {
		name   string
		change func(*v1beta1.InferenceService, *appsv1.ControllerRevision, *v1beta1.ClusterServingRuntime)
	}{
		{"auto sync nil", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Spec.Runtime.AutoSync = nil
		}},
		{"auto sync true", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Spec.Runtime.AutoSync = ptr.To(true)
		}},
		{"automatic selection", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Spec.Runtime = nil
		}},
		{"explicit revision", func(v *v1beta1.InferenceService, r *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Spec.Runtime.Revision = ptr.To(r.Name)
		}},
		{"unsupported group", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Spec.Runtime.APIGroup = ptr.To("private")
		}},
		{"unsupported kind", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Spec.Runtime.Kind = ptr.To("private")
		}},
		{"pin missing", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Status.PinnedRevisionName = ""
		}},
		{"no drift", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Status.Conditions = nil
		}},
		{"duplicate drift", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Status.Conditions = append(v.Status.Conditions, v.Status.Conditions[0])
		}},
		{"wrong drift reason", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Status.Conditions[0].Reason = "PRIVATE_REASON"
		}},
		{"drift false", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Status.Conditions[0].Status = corev1.ConditionFalse
		}},
		{"pending token", func(v *v1beta1.InferenceService, _ *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			v.Annotations = map[string]string{constants.RuntimeSyncAnnotationKey: "PRIVATE_PENDING"}
		}},
		{"source gen absent", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.Generation = 0
		}},
		{"source UID absent", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.UID = ""
		}},
		{"source disabled", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.Spec.Disabled = ptr.To(true)
		}},
		{"source deleting", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.DeletionTimestamp = ptr.To(metav1.Now())
			l.Finalizers = []string{"test"}
		}},
		{"inherit missing", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: "missing"}
		}},
		{"inherit cycle", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: "runtime"}
		}},
		{"same content", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.Spec = *runtimeSpecFixture("old")
		}},
		{"pin writer invalid", func(_ *v1beta1.InferenceService, r *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			r.Revision = 2
		}},
		{"pin missing UID", func(_ *v1beta1.InferenceService, r *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			r.UID = ""
		}},
		{"pin wrong source", func(_ *v1beta1.InferenceService, r *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			r.Labels[constants.RuntimeRevisionOfKindLabelKey] = "ServingRuntime"
		}},
		{"pin bad data", func(_ *v1beta1.InferenceService, r *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			r.Data.Raw = []byte("PRIVATE_BAD_DATA")
		}},
		{"pin huge data", func(_ *v1beta1.InferenceService, r *appsv1.ControllerRevision, _ *v1beta1.ClusterServingRuntime) {
			r.Data.Raw = []byte(strings.Repeat("PRIVATE", 200000))
		}},
		{"source huge data", func(_ *v1beta1.InferenceService, _ *appsv1.ControllerRevision, l *v1beta1.ClusterServingRuntime) {
			l.Spec.EngineConfig.Runner.Args = []string{strings.Repeat("PRIVATE", 200000)}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v, r, l := runtimeSyncSourceFixture(t)
			c.change(v, r, l)
			resolver := runtimeSyncSourceResolver(t, kfake.NewSimpleClientset(r), l)
			_, err := resolver.Resolve(context.Background(), v)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}

func TestRuntimeSyncWriterTargetAndCompleteHistory(t *testing.T) {
	for _, scenario := range []string{"existing", "occupied predicted", "foreign same short", "duplicate list", "incomplete pages", "list private error", "missing active from list"} {
		t.Run(scenario, func(t *testing.T) {
			v, r, l := runtimeSyncSourceFixture(t)
			target := revisionFixture(t, "ClusterServingRuntime", "", "runtime", &l.Spec)
			target.UID = "target-uid"
			target.ResourceVersion = "22"
			objects := []kruntime.Object{r}
			if scenario == "existing" || scenario == "occupied predicted" {
				if scenario == "occupied predicted" {
					target.Labels[constants.RuntimeRevisionOfLabelKey] = "foreign"
				}
				objects = append(objects, target)
			}
			if scenario == "foreign same short" {
				foreign := revisionFixture(t, "ServingRuntime", "foreign", "runtime", &l.Spec)
				foreign.UID = "foreign-uid"
				foreign.ResourceVersion = "25"
				objects = append(objects, foreign)
			}
			kube := kfake.NewSimpleClientset(objects...)
			if scenario == "duplicate list" || scenario == "incomplete pages" || scenario == "list private error" || scenario == "missing active from list" {
				kube.PrependReactor("list", "controllerrevisions", func(ktesting.Action) (bool, kruntime.Object, error) {
					switch scenario {
					case "duplicate list":
						return true, &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{*r, *r}}, nil
					case "incomplete pages":
						return true, &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{*r}, ListMeta: metav1.ListMeta{Continue: "PRIVATE_CONTINUE"}}, nil
					case "missing active from list":
						return true, &appsv1.ControllerRevisionList{}, nil
					default:
						return true, nil, errors.New("PRIVATE_API_ERROR")
					}
				})
			}
			resolver := runtimeSyncSourceResolver(t, kube, l)
			e, err := resolver.Resolve(context.Background(), v)
			if scenario == "existing" {
				require.NoError(t, err)
				require.Contains(t, e.PreviewRows(), [2]string{"Target evidence", "Existing"})
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
			}
		})
	}
}

func TestRuntimeSyncDeclaredClusterPinNamespacedFallback(t *testing.T) {
	v, r, l := runtimeSyncSourceFixture(t)
	namespaced := &v1beta1.ServingRuntime{ObjectMeta: l.ObjectMeta, Spec: l.Spec}
	namespaced.Namespace = v.Namespace
	resolver := runtimeSyncSourceResolver(t, kfake.NewSimpleClientset(r), namespaced)
	e, err := resolver.Resolve(context.Background(), v)
	require.NoError(t, err)
	require.Contains(t, e.PreviewRows(), [2]string{"Pin source", "ClusterServingRuntime//runtime"})
	require.Contains(t, e.PreviewRows(), [2]string{"Source", "ServingRuntime/workloads/runtime"})
}

func TestRuntimeSyncReadCancellationAndChangedOrigin(t *testing.T) {
	v, r, l := runtimeSyncSourceFixture(t)
	kube := kfake.NewSimpleClientset(r)
	resolver := runtimeSyncSourceResolver(t, kube, l)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := resolver.Resolve(ctx, v)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, kube.Actions())
	first, err := resolver.Resolve(context.Background(), v)
	require.NoError(t, err)
	l.ResourceVersion = "changed"
	resolver = runtimeSyncSourceResolver(t, kube, l)
	second, err := resolver.Resolve(context.Background(), v)
	require.NoError(t, err)
	require.False(t, first.SameSnapshot(second))
	private := fmt.Errorf("PRIVATE_CREDENTIAL: %w", context.DeadlineExceeded)
	require.Equal(t, context.DeadlineExceeded, syncContextError(private))
	// Contradictory forged target hash relation is refused by full+short recomputation.
	_, short, err := runtimerevision.Hash(&l.Spec)
	require.NoError(t, err)
	require.Len(t, short, 8)
	require.False(t, (RuntimeSyncEvidence{}).MatchesInferenceService(v))
}

func TestRuntimeSyncPureEvidenceRefusesContradictoryBoundState(t *testing.T) {
	for _, scenario := range []string{"nil state", "wrong parent", "active origin", "active consistency", "requested pin", "incomplete history", "unavailable live", "identity unobserved", "zero chain", "wrong chain leaf", "invalid source gvk", "invalid source namespace", "invalid component", "invalid mode", "empty active components", "equal short"} {
		t.Run(scenario, func(t *testing.T) {
			v, revision, live := runtimeSyncSourceFixture(t)
			client := ctrlfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(live).Build()
			pin, err := NewRuntimePinResolver(kfake.NewSimpleClientset(revision).AppsV1(), NewRuntimeResolver(client), "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: time.Second})
			require.NoError(t, err)
			state, err := pin.Resolve(context.Background(), v, RuntimeResolveOptions{IncludeHistory: true})
			require.NoError(t, err)
			switch scenario {
			case "nil state":
				state = nil
			case "wrong parent":
				v.ResourceVersion = "changed"
			case "active origin":
				state.active.Origin = ConfigurationOriginLiveRuntime
			case "active consistency":
				state.active.Consistency = RevisionConsistencyInconsistent
			case "requested pin":
				state.RequestedRevisionName = revision.Name
			case "incomplete history":
				state.HistoryComplete = false
			case "unavailable live":
				state.liveAvailability = liveNotFound
			case "identity unobserved":
				state.live.Runtime.IdentityObserved = false
			case "zero chain":
				state.live.Runtime.DeclaredInheritance.chain = nil
			case "wrong chain leaf":
				state.live.Runtime.DeclaredInheritance.chain[0].ResourceVersion = "other"
			case "invalid source gvk":
				state.live.Runtime.DeclaredInheritance.chain[0].APIVersion = "private/v1"
			case "invalid source namespace":
				state.live.Runtime.DeclaredInheritance.chain[0].Namespace = "foreign"
			case "invalid component":
				state.active.components[0].Type = "private"
			case "invalid mode":
				state.live.Components[0].DeploymentMode = "private"
			case "empty active components":
				state.active.components = nil
			case "equal short":
				_, state.LiveShortHash, err = runtimerevision.Hash(state.active.spec)
				require.NoError(t, err)
			}
			_, err = prepareRuntimeSyncEvidence(v, state, RuntimeRevisionObservation{}, true, map[string]string{"fixture": "bound"})
			require.ErrorIs(t, err, ErrRuntimeSyncEvidence)
		})
	}
	_, err := NewRuntimeSyncResolver(nil, nil, "", paging.Limits{})
	require.ErrorIs(t, err, ErrRuntimeSyncEvidence)
	_, err = (*RuntimeSyncResolver)(nil).Resolve(context.Background(), nil)
	require.ErrorIs(t, err, ErrRuntimeSyncEvidence)
	require.Empty(t, (RuntimeSyncEvidence{}).TargetHash())
	_, err = (RuntimeSyncEvidence{}).MarshalYAML()
	require.Error(t, err)
	require.Empty(t, syncDigest(make(chan int)))
	require.ErrorIs(t, recordSyncRead(map[string]string{}, "key", make(chan int)), ErrRuntimeSyncEvidence)
	require.Equal(t, context.Canceled, syncContextError(fmt.Errorf("PRIVATE: %w", context.Canceled)))
	require.ErrorIs(t, (&syncReadClient{}).List(context.Background(), nil), ErrRuntimeSyncEvidence)
}

func TestRuntimeSyncInheritanceHashesMergedSpecAndPreservesModelFallback(t *testing.T) {
	v, revision, live := runtimeSyncSourceFixture(t)
	v.Spec.Model = &v1beta1.ModelRef{Name: "model"}
	model := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "model-uid", ResourceVersion: "model-rv", Generation: 2}}
	parent := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "parent", UID: "source-parent-uid", ResourceVersion: "source-parent-rv", Generation: 2}, Spec: *runtimeSpecFixture("inherited")}
	parent.Spec.EngineConfig.Runner.Env = []corev1.EnvVar{{Name: "EXAMPLE", Value: "PRIVATE_INHERITED"}}
	live.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: "parent"}
	resolver := runtimeSyncSourceResolver(t, kfake.NewSimpleClientset(revision), live, parent, model)
	e, err := resolver.Resolve(context.Background(), v)
	require.NoError(t, err)
	_, leafHash, err := runtimerevision.Hash(&live.Spec)
	require.NoError(t, err)
	require.NotEqual(t, leafHash, e.TargetHash(), "authoritative merged spec, not leaf data, defines target")
	require.Contains(t, e.PreviewRows(), [2]string{"Source", "ClusterServingRuntime//parent"})
	require.Contains(t, e.PreviewRows(), [2]string{"Source", "ClusterServingRuntime//runtime"})
	require.NotContains(t, fmt.Sprint(e.PreviewRows()), "PRIVATE")
	mode := constants.OMENative
	v.Spec.DeploymentMode = &mode
	e, err = resolver.Resolve(context.Background(), v)
	require.NoError(t, err)
	require.Equal(t, []string{"engine"}, e.NativeComponents())
}
