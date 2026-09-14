package status

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/observation"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func pod(name, ns, isvc, component string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: ns,
		Labels: map[string]string{
			constants.InferenceServiceLabel: isvc,
			constants.OMEComponentLabel:     component,
		},
	}}
}

func TestGatherGroupsPodsByComponent(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a"},
		}),
		Kube: kubefake.NewSimpleClientset(
			pod("llama-engine-1", "team-a", "llama", "engine"),
			pod("llama-decoder-1", "team-a", "llama", "decoder"),
			pod("other-engine-1", "team-a", "other", "engine"),
		),
		NS: "team-a",
	}
	r, err := gather(context.Background(), f, "team-a", "llama")
	require.NoError(t, err)
	require.Len(t, r.Pods[v1beta1.EngineComponent], 1)
	assert.Equal(t, "llama-engine-1", r.Pods[v1beta1.EngineComponent][0].Name)
	require.Len(t, r.Pods[v1beta1.DecoderComponent], 1)
	assert.Empty(t, r.Pods[v1beta1.RouterComponent])
}

func TestGatherKeepsOnlyWarningEventsForOurObjects(t *testing.T) {
	warn := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "e1", Namespace: "team-a"},
		Type:           corev1.EventTypeWarning,
		Reason:         "FailedScheduling",
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "llama-engine-1", Namespace: "team-a"},
	}
	other := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "e2", Namespace: "team-a"},
		Type:           corev1.EventTypeWarning,
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "unrelated", Namespace: "team-a"},
	}
	f := factory.Static{
		OME:  omefake.NewSimpleClientset(&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a"}}),
		Kube: kubefake.NewSimpleClientset(pod("llama-engine-1", "team-a", "llama", "engine"), warn, other),
		NS:   "team-a",
	}
	r, err := gather(context.Background(), f, "team-a", "llama")
	require.NoError(t, err)
	require.Len(t, r.Events, 1)
	assert.Equal(t, "FailedScheduling", r.Events[0].Reason)
}

// TestGatherIssuesFieldSelectorEventQueriesPerObject pins the kubectl-describe
// pattern: gather() must query events per involved object (the
// InferenceService, then each pod) with a field selector, rather than
// listing every Warning event in the namespace. The fake clientset ignores
// field selectors when serving the request (so this cannot be observed via
// gather()'s output), but it still parses and records the selector actually
// sent -- capture it with a reactor and check it real-selector-matches the
// right object and rejects a wrong one.
func TestGatherIssuesFieldSelectorEventQueriesPerObject(t *testing.T) {
	servicePod := pod("llama-engine-1", "team-a", "llama", "engine")
	servicePod.UID = "pod-uid"
	kube := kubefake.NewSimpleClientset(servicePod)
	var restrictionsMu sync.Mutex
	var restrictions []ktesting.ListRestrictions
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		la := action.(ktesting.ListActionImpl)
		restrictionsMu.Lock()
		restrictions = append(restrictions, la.GetListRestrictions())
		restrictionsMu.Unlock()
		return false, nil, nil // fall through to the default tracker-backed reactor
	})
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "llama", Namespace: "team-a", UID: "isvc-uid",
		}}),
		Kube: kube,
		NS:   "team-a",
	}

	_, err := gather(context.Background(), f, "team-a", "llama")

	require.NoError(t, err)
	require.Len(t, restrictions, 2, "one events query for the InferenceService, one for its pod")
	byKind := make(map[string]ktesting.ListRestrictions, len(restrictions))
	for _, restriction := range restrictions {
		kind, found := restriction.Fields.RequiresExactMatch("involvedObject.kind")
		require.True(t, found, "every event query must require involvedObject.kind")
		byKind[kind] = restriction
	}
	isvcRestriction, found := byKind["InferenceService"]
	require.True(t, found, "an InferenceService event query must be issued")
	podRestriction, found := byKind["Pod"]
	require.True(t, found, "a Pod event query must be issued")

	// Fields.Matches only checks that the selector's *required* terms hold;
	// it does not fail just because the candidate has extra keys. So
	// asserting Matches() on a full {name,kind,type} set alone would not
	// notice a selector that forgot to require "kind" at all. Nail that
	// down explicitly with RequiresExactMatch, then use Matches() with a
	// same-name-but-wrong-kind candidate (the real scenario this guards:
	// an InferenceService and a Pod that happen to share a name) as a
	// second, semantic check that kind is actually load-bearing.
	kind0, found0 := isvcRestriction.Fields.RequiresExactMatch("involvedObject.kind")
	require.True(t, found0, "InferenceService query must require involvedObject.kind")
	assert.Equal(t, "InferenceService", kind0)
	name0, found0n := isvcRestriction.Fields.RequiresExactMatch("involvedObject.name")
	require.True(t, found0n)
	assert.Equal(t, "llama", name0)
	uid0, found0u := isvcRestriction.Fields.RequiresExactMatch("involvedObject.uid")
	require.True(t, found0u)
	assert.Equal(t, "isvc-uid", uid0)
	assert.True(t, isvcRestriction.Fields.Matches(fields.Set{
		"involvedObject.name": "llama", "involvedObject.kind": "InferenceService",
		"involvedObject.uid": "isvc-uid", "type": "Warning",
	}), "InferenceService query scopes to the requested service")
	assert.False(t, isvcRestriction.Fields.Matches(fields.Set{
		"involvedObject.name": "llama", "involvedObject.kind": "Pod", "type": "Warning",
	}), "must not match a same-named Pod -- kind has to discriminate too")
	assert.False(t, isvcRestriction.Fields.Matches(fields.Set{
		"involvedObject.name": "someone-else", "involvedObject.kind": "InferenceService", "type": "Warning",
	}), "must not match some other object's events")

	kind1, found1 := podRestriction.Fields.RequiresExactMatch("involvedObject.kind")
	require.True(t, found1, "Pod query must require involvedObject.kind")
	assert.Equal(t, "Pod", kind1)
	uid1, found1u := podRestriction.Fields.RequiresExactMatch("involvedObject.uid")
	require.True(t, found1u)
	assert.Equal(t, "pod-uid", uid1)
	assert.True(t, podRestriction.Fields.Matches(fields.Set{
		"involvedObject.name": "llama-engine-1", "involvedObject.kind": "Pod",
		"involvedObject.uid": "pod-uid", "type": "Warning",
	}), "Pod query scopes to the service's pod")
	assert.False(t, podRestriction.Fields.Matches(fields.Set{
		"involvedObject.name": "llama-engine-1", "involvedObject.kind": "InferenceService", "type": "Warning",
	}), "must not match an InferenceService that happens to share the pod's name")
}

func TestGatherBoundsPodPagesAndPreservesRequestScope(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	var podRequests []ktesting.ListActionImpl
	kube.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		request := action.(ktesting.ListActionImpl)
		podRequests = append(podRequests, request)
		switch request.GetListOptions().Continue {
		case "":
			return true, &corev1.PodList{
				Items:    []corev1.Pod{*pod("llama-engine-z", "team-a", "llama", "engine")},
				ListMeta: metav1.ListMeta{Continue: "pods-next"},
			}, nil
		case "pods-next":
			return true, &corev1.PodList{
				Items:    []corev1.Pod{*pod("llama-engine-a", "team-a", "llama", "engine")},
				ListMeta: metav1.ListMeta{Continue: "pods-unobserved"},
			}, nil
		default:
			return true, nil, fmt.Errorf("unexpected pod continuation token %q", request.GetListOptions().Continue)
		}
	})
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{}, nil
	})
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a", UID: "isvc-uid"},
		}),
		Kube: kube,
		NS:   "team-a",
	}

	r, err := gather(context.Background(), f, "team-a", "llama")

	require.NoError(t, err)
	require.Len(t, podRequests, 2, "status must stop after its bounded pod-page budget")
	for _, request := range podRequests {
		assert.Equal(t, "team-a", request.GetNamespace())
		assert.Equal(t, int64(500), request.GetListOptions().Limit)
		value, found := request.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServiceLabel)
		require.True(t, found)
		assert.Equal(t, "llama", value)
	}
	assert.Empty(t, podRequests[0].GetListOptions().Continue)
	assert.Equal(t, "pods-next", podRequests[1].GetListOptions().Continue)
	require.Len(t, r.Pods[v1beta1.EngineComponent], 2)
	assert.Equal(t, []string{"llama-engine-a", "llama-engine-z"}, []string{
		r.Pods[v1beta1.EngineComponent][0].Name,
		r.Pods[v1beta1.EngineComponent][1].Name,
	})
}

func TestGatherPaginatesWarningEventsWithBoundedScopedRequests(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{}, nil
	})
	var eventRequests []ktesting.ListActionImpl
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		request := action.(ktesting.ListActionImpl)
		eventRequests = append(eventRequests, request)
		event := func(name string) corev1.Event {
			return corev1.Event{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID("uid-" + name)},
				Type:       corev1.EventTypeWarning,
				InvolvedObject: corev1.ObjectReference{
					Kind: "InferenceService", Name: "llama", Namespace: "team-a", UID: "isvc-uid",
				},
			}
		}
		switch request.GetListOptions().Continue {
		case "":
			return true, &corev1.EventList{
				Items:    []corev1.Event{event("warning-z")},
				ListMeta: metav1.ListMeta{Continue: "events-next"},
			}, nil
		case "events-next":
			return true, &corev1.EventList{
				Items:    []corev1.Event{event("warning-a")},
				ListMeta: metav1.ListMeta{Continue: "events-unobserved"},
			}, nil
		default:
			return true, nil, fmt.Errorf("unexpected event continuation token %q", request.GetListOptions().Continue)
		}
	})
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a", UID: "isvc-uid"},
		}),
		Kube: kube,
		NS:   "team-a",
	}

	limits := defaultGatherLimits()
	limits.events.Paging.MaxPages = 2
	r, err := gatherWithLimits(context.Background(), f, "team-a", "llama", limits)

	require.NoError(t, err)
	require.Len(t, eventRequests, 2, "status must follow continuation tokens within its event-page budget")
	for index, request := range eventRequests {
		assert.Equal(t, "team-a", request.GetNamespace())
		assert.Equal(t, []int64{25, 24}[index], request.GetListOptions().Limit)
		restrictions := request.GetListRestrictions().Fields
		for key, want := range map[string]string{
			"involvedObject.name": "llama",
			"involvedObject.kind": "InferenceService",
			"involvedObject.uid":  "isvc-uid",
			"type":                corev1.EventTypeWarning,
		} {
			got, found := restrictions.RequiresExactMatch(key)
			require.True(t, found, "request must retain %s", key)
			assert.Equal(t, want, got)
		}
	}
	assert.Empty(t, eventRequests[0].GetListOptions().Continue)
	assert.Equal(t, "events-next", eventRequests[1].GetListOptions().Continue)
	require.Len(t, r.Events, 2)
	assert.Equal(t, []string{"warning-a", "warning-z"}, []string{r.Events[0].Name, r.Events[1].Name})
}

func TestDefaultGatherLimitsKeepEventFanoutInteractive(t *testing.T) {
	limits := defaultGatherLimits()

	assert.Equal(t, 16, limits.events.MaxTargets)
	assert.Equal(t, 8, limits.events.MaxConcurrent)
	assert.Equal(t, int64(25), limits.events.Paging.PageSize)
	assert.Equal(t, 25, limits.events.Paging.MaxItems)
	assert.Equal(t, 1, limits.events.Paging.MaxPages)
	assert.Equal(t, 5*time.Second, limits.events.Paging.RequestTimeout)
	assert.Equal(t, 100, limits.maxEvents)
}

func TestGatherSurfacesBoundedObservationTruncation(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{
			Items:    []corev1.Pod{*pod("llama-engine-1", "team-a", "llama", "engine")},
			ListMeta: metav1.ListMeta{Continue: "more-pods"},
		}, nil
	})
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{
			Items: []corev1.Event{{
				ObjectMeta:     metav1.ObjectMeta{Name: "warning-1", Namespace: "team-a", UID: "warning-uid"},
				Type:           corev1.EventTypeWarning,
				InvolvedObject: corev1.ObjectReference{Kind: "InferenceService", Name: "llama", UID: "isvc-uid"},
			}},
			ListMeta: metav1.ListMeta{Continue: "more-events"},
		}, nil
	})
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a", UID: "isvc-uid"},
		}),
		Kube: kube,
		NS:   "team-a",
	}
	limits := gatherLimits{
		pods: paging.Limits{
			PageSize: 1, MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second,
		},
		events: observation.EventLimits{
			Paging: paging.Limits{
				PageSize: 1, MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second,
			},
			MaxTargets: 1, MaxConcurrent: 1,
		},
		maxEvents: 1,
	}

	r, err := gatherWithLimits(context.Background(), f, "team-a", "llama", limits)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, render(r, &out))

	assert.Contains(t, out.String(), "Observation Warnings:")
	assert.Contains(t, out.String(), "Pod observation truncated; collected pods: 1")
	assert.Contains(t, out.String(), "Warning Event observation truncated; object targets not queried: 1")
}

func TestGatherPrioritizesUnhealthyPodEventTargets(t *testing.T) {
	healthy := pod("llama-engine-healthy", "team-a", "llama", "engine")
	healthy.Status.Phase = corev1.PodRunning
	healthy.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pending := pod("llama-engine-pending", "team-a", "llama", "engine")
	pending.Status.Phase = corev1.PodPending
	failed := pod("llama-engine-failed", "team-a", "llama", "engine")
	failed.Status.Phase = corev1.PodFailed
	kube := kubefake.NewSimpleClientset(healthy, pending, failed)
	var mu sync.Mutex
	queried := map[string]bool{}
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		fields := action.(ktesting.ListActionImpl).GetListRestrictions().Fields
		kind, kindFound := fields.RequiresExactMatch("involvedObject.kind")
		name, nameFound := fields.RequiresExactMatch("involvedObject.name")
		require.True(t, kindFound)
		require.True(t, nameFound)
		mu.Lock()
		queried[kind+"/"+name] = true
		mu.Unlock()
		return true, &corev1.EventList{}, nil
	})
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a", UID: "isvc-uid"},
		}),
		Kube: kube,
		NS:   "team-a",
	}
	limits := gatherLimits{
		pods: paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		events: observation.EventLimits{
			Paging:     paging.Limits{PageSize: 5, MaxItems: 5, MaxPages: 1, RequestTimeout: time.Second},
			MaxTargets: 3, MaxConcurrent: 2,
		},
		maxEvents: 10,
	}

	r, err := gatherWithLimits(context.Background(), f, "team-a", "llama", limits)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{
		"InferenceService/llama":   true,
		"Pod/llama-engine-failed":  true,
		"Pod/llama-engine-pending": true,
	}, queried)
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "object targets not queried: 1")
}

func TestGatherCapsRenderedWarningEvents(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{}, nil
	})
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		items := make([]corev1.Event, 0, 3)
		for _, name := range []string{"warning-c", "warning-a", "warning-b"} {
			items = append(items, corev1.Event{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID(name)},
				Type:       corev1.EventTypeWarning,
				InvolvedObject: corev1.ObjectReference{
					Kind: "InferenceService", Name: "llama", Namespace: "team-a", UID: "isvc-uid",
				},
			})
		}
		return true, &corev1.EventList{Items: items}, nil
	})
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a", UID: "isvc-uid"},
		}),
		Kube: kube,
		NS:   "team-a",
	}
	limits := gatherLimits{
		pods: paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		events: observation.EventLimits{
			Paging:     paging.Limits{PageSize: 5, MaxItems: 5, MaxPages: 1, RequestTimeout: time.Second},
			MaxTargets: 1, MaxConcurrent: 1,
		},
		maxEvents: 2,
	}

	r, err := gatherWithLimits(context.Background(), f, "team-a", "llama", limits)
	require.NoError(t, err)
	require.Len(t, r.Events, 2)
	assert.Equal(t, []string{"warning-a", "warning-b"}, []string{r.Events[0].Name, r.Events[1].Name})
	assert.Contains(t, strings.Join(r.Warnings, "\n"), "events not shown: 1")
}

func TestGatherPreservesPrimaryReportWhenWarningEventsAreUnavailable(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{}, nil
	})
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("SECRET proxy response\x1b[31m")
	})
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a", UID: "isvc-uid"},
		}),
		Kube: kube,
		NS:   "team-a",
	}

	r, err := gather(context.Background(), f, "team-a", "llama")
	require.NoError(t, err, "an optional Event source must not discard the primary status report")
	var out bytes.Buffer
	require.NoError(t, render(r, &out))

	assert.Contains(t, out.String(), "Name:       llama")
	assert.Contains(t, out.String(), "Warning Events unavailable for InferenceService team-a/llama (Unreadable)")
	assert.NotContains(t, out.String(), "SECRET")
	assert.NotContains(t, out.String(), "\x1b")
}

func TestGatherPropagatesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a"},
		}),
		Kube: kubefake.NewSimpleClientset(),
		NS:   "team-a",
	}

	_, err := gather(ctx, f, "team-a", "llama")
	require.ErrorIs(t, err, context.Canceled)
}

func TestWarningEventFailureReasonIsBounded(t *testing.T) {
	resource := schema.GroupResource{Resource: "events"}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: "Timeout"},
		{name: "forbidden", err: kerrors.NewForbidden(resource, "SECRET", errors.New("SECRET")), want: "Forbidden"},
		{name: "unauthorized", err: kerrors.NewUnauthorized("SECRET"), want: "Unauthorized"},
		{name: "not found", err: kerrors.NewNotFound(resource, "SECRET"), want: "NotFound"},
		{name: "rate limited", err: kerrors.NewTooManyRequests("SECRET", 5), want: "Unavailable"},
		{name: "unknown", err: errors.New("SECRET"), want: "Unreadable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := warningEventFailureReason(tt.err)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "SECRET")
		})
	}
}
