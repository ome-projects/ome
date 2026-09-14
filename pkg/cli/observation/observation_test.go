package observation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/cli/paging"
)

func TestCollectPodsUsesOneBoundedSelectorAndSortsSnapshot(t *testing.T) {
	t.Parallel()

	const selector = "ome.io/inferenceservice=chat"
	kube := kubefake.NewSimpleClientset(
		pod("z", "chat"),
		pod("a", "chat"),
		pod("other", "other"),
	)
	var restrictions []ktesting.ListRestrictions
	kube.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		restrictions = append(restrictions, action.(ktesting.ListAction).GetListRestrictions())
		return false, nil, nil
	})

	got, err := CollectPods(context.Background(), kube.CoreV1(), "team-a", selector, paging.Limits{
		PageSize:       10,
		MaxItems:       10,
		MaxPages:       1,
		RequestTimeout: time.Second,
	})

	require.NoError(t, err)
	require.Len(t, got.Items, 2)
	assert.Equal(t, []string{"a", "z"}, []string{got.Items[0].Name, got.Items[1].Name})
	assert.Equal(t, 1, got.Requests)
	assert.False(t, got.Truncated)
	require.Len(t, restrictions, 1)
	value, found := restrictions[0].Labels.RequiresExactMatch("ome.io/inferenceservice")
	require.True(t, found)
	assert.Equal(t, "chat", value)
}

func TestCollectPodsDefendsAgainstSelectorBlindSource(t *testing.T) {
	t.Parallel()

	got, err := CollectPods(context.Background(), blindPodGetter{}, "team-a", "ome.io/inferenceservice=chat", paging.Limits{
		PageSize:       10,
		MaxItems:       10,
		MaxPages:       1,
		RequestTimeout: time.Second,
	})

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "wanted", got.Items[0].Name)
}

func TestCollectPodsRejectsWrongNamespaceFromSelectorBlindSource(t *testing.T) {
	t.Parallel()

	getter := staticPodGetter{list: &corev1.PodList{Items: []corev1.Pod{
		*pod("wanted", "chat"),
		{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "team-b", Labels: map[string]string{"ome.io/inferenceservice": "chat"}}},
	}}}
	got, err := CollectPods(context.Background(), getter, "team-a", "ome.io/inferenceservice=chat", paging.Limits{
		PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second,
	})
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "wanted", got.Items[0].Name)
}

func TestCollectPodsRejectsNilListResponse(t *testing.T) {
	t.Parallel()

	got, err := CollectPods(context.Background(), staticPodGetter{}, "team-a", "ome.io/inferenceservice=chat", paging.Limits{
		PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second,
	})

	require.ErrorContains(t, err, "Pod list returned nil")
	assert.Empty(t, got.Items)
}

func TestCollectPodsRejectsSuccessfulResponseAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	got, err := CollectPods(ctx, cancelingPodGetter{cancel: cancel}, "team-a", "ome.io/inferenceservice=chat", paging.Limits{
		PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second,
	})

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, got.Items)
}

func TestCollectedSnapshotsDoNotShareNestedSourceData(t *testing.T) {
	t.Parallel()

	sourcePod := pod("wanted", "chat")
	sourcePod.Labels["snapshot"] = "original"
	pods := staticPodGetter{list: &corev1.PodList{Items: []corev1.Pod{*sourcePod}}}
	podCollection, err := CollectPods(context.Background(), pods, "team-a", "ome.io/inferenceservice=chat", paging.Limits{
		PageSize:       10,
		MaxItems:       10,
		MaxPages:       1,
		RequestTimeout: time.Second,
	})
	require.NoError(t, err)
	require.Len(t, podCollection.Items, 1)

	sourceEvent := warningEvent("warning", "uid-warning", "pod-a")
	sourceEvent.Labels = map[string]string{"snapshot": "original"}
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{Items: []corev1.Event{sourceEvent}}, nil
	})
	eventCollection, err := CollectWarningEvents(context.Background(), kube.CoreV1(), []ObjectRef{{
		Namespace: "team-a", Kind: "Pod", Name: "pod-a",
	}}, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 1, MaxConcurrent: 1,
	})
	require.NoError(t, err)
	require.Len(t, eventCollection.Items, 1)

	sourcePod.Labels["snapshot"] = "mutated"
	sourceEvent.Labels["snapshot"] = "mutated"
	assert.Equal(t, "original", podCollection.Items[0].Labels["snapshot"])
	assert.Equal(t, "original", eventCollection.Items[0].Labels["snapshot"])
}

func TestCollectWarningEventsCapsTargetsAndConcurrencyDeterministically(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 3)
	release := make(chan struct{})
	getter := &blockingEventGetter{started: started, release: release}

	targets := []ObjectRef{
		{Namespace: "team-a", Kind: "Pod", Name: "z"},
		{Namespace: "team-a", Kind: "InferenceService", Name: "chat"},
		{Namespace: "team-a", Kind: "Pod", Name: "a"},
	}
	type outcome struct {
		result EventCollection
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := CollectWarningEvents(context.Background(), getter, targets, EventLimits{
			Paging: paging.Limits{
				PageSize:       10,
				MaxItems:       10,
				MaxPages:       1,
				RequestTimeout: time.Second,
			},
			MaxTargets:    2,
			MaxConcurrent: 2,
		})
		done <- outcome{result: result, err: err}
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two bounded workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("started more event queries than the target cap")
	default:
	}
	close(release)

	got := <-done
	require.NoError(t, got.err)
	assert.Equal(t, int32(2), getter.maximum.Load())
	assert.Equal(t, 2, got.result.Requests)
	assert.True(t, got.result.Truncated)
	assert.Equal(t, 1, got.result.SkippedTargets)
	require.Empty(t, got.result.Failures)
	require.Len(t, got.result.Items, 2)
	assert.Equal(t, []string{"event-chat", "event-a"}, []string{got.result.Items[0].Name, got.result.Items[1].Name})
}

func TestCollectWarningEventsPreservesSuccessfulSourcesWhenOneIsForbidden(t *testing.T) {
	t.Parallel()

	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		restrictions := action.(ktesting.ListAction).GetListRestrictions()
		name, _ := restrictions.Fields.RequiresExactMatch("involvedObject.name")
		if name == "b" {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, name, errors.New("denied"))
		}
		return true, &corev1.EventList{Items: []corev1.Event{{
			ObjectMeta:     metav1.ObjectMeta{Name: "event-a", Namespace: "team-a", UID: "event-a"},
			Type:           corev1.EventTypeWarning,
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "a"},
		}}}, nil
	})

	got, err := CollectWarningEvents(context.Background(), kube.CoreV1(), []ObjectRef{
		{Namespace: "team-a", Kind: "Pod", Name: "a"},
		{Namespace: "team-a", Kind: "Pod", Name: "b"},
	}, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 2, MaxConcurrent: 2,
	})

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "event-a", got.Items[0].Name)
	require.Len(t, got.Failures, 1)
	assert.Equal(t, "b", got.Failures[0].Target.Name)
	assert.True(t, apierrors.IsForbidden(got.Failures[0].Err))
}

func TestCollectWarningEventsRejectsIncompleteTargetWithoutRequest(t *testing.T) {
	t.Parallel()

	kube := kubefake.NewSimpleClientset()
	requested := false
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		requested = true
		return true, &corev1.EventList{}, nil
	})

	_, err := CollectWarningEvents(context.Background(), kube.CoreV1(), []ObjectRef{{
		Kind: "Pod",
		Name: "pod-a",
	}}, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 1, MaxConcurrent: 1,
	})

	require.ErrorContains(t, err, "namespace")
	assert.False(t, requested)
}

func TestCollectWarningEventsPreservesNilListResponseAsSourceFailure(t *testing.T) {
	t.Parallel()

	got, err := CollectWarningEvents(context.Background(), nilEventGetter{}, []ObjectRef{{
		Namespace: "team-a", Kind: "Pod", Name: "pod-a",
	}}, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 1, MaxConcurrent: 1,
	})

	require.NoError(t, err)
	require.Len(t, got.Failures, 1)
	assert.ErrorContains(t, got.Failures[0].Err, "Event list returned nil")
	assert.Empty(t, got.Items)
}

func TestCollectWarningEventsSortsAndDeduplicatesEachTarget(t *testing.T) {
	t.Parallel()

	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{Items: []corev1.Event{
			warningEvent("z", "uid-z", "pod-a"),
			warningEvent("a", "uid-a", "pod-a"),
			warningEvent("duplicate", "uid-a", "pod-a"),
		}}, nil
	})

	got, err := CollectWarningEvents(context.Background(), kube.CoreV1(), []ObjectRef{{
		Namespace: "team-a", Kind: "Pod", Name: "pod-a",
	}}, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 1, MaxConcurrent: 1,
	})

	require.NoError(t, err)
	require.Len(t, got.Items, 2)
	assert.Equal(t, []string{"a", "z"}, []string{got.Items[0].Name, got.Items[1].Name})
}

func TestCollectWarningEventsRejectsWrongNamespaceFromSelectorBlindSource(t *testing.T) {
	t.Parallel()

	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
		wrongNamespace := warningEvent("wrong-namespace", "uid-wrong", "pod-a")
		wrongNamespace.Namespace = "team-b"
		return true, &corev1.EventList{Items: []corev1.Event{
			wrongNamespace,
			warningEvent("wanted", "uid-wanted", "pod-a"),
		}}, nil
	})

	got, err := CollectWarningEvents(context.Background(), kube.CoreV1(), []ObjectRef{{
		Namespace: "team-a", Kind: "Pod", Name: "pod-a",
	}}, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 1, MaxConcurrent: 1,
	})

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "wanted", got.Items[0].Name)
}

func TestCollectWarningEventsDeduplicatesTargetsBeforeApplyingCap(t *testing.T) {
	t.Parallel()

	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		restrictions := action.(ktesting.ListAction).GetListRestrictions()
		name, _ := restrictions.Fields.RequiresExactMatch("involvedObject.name")
		return true, &corev1.EventList{Items: []corev1.Event{
			warningEvent("event-"+name, types.UID("uid-"+name), name),
		}}, nil
	})

	got, err := CollectWarningEvents(context.Background(), kube.CoreV1(), []ObjectRef{
		{Namespace: "team-a", Kind: "Pod", Name: "a"},
		{Namespace: "team-a", Kind: "Pod", Name: "a"},
		{Namespace: "team-a", Kind: "Pod", Name: "b"},
	}, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 2, MaxConcurrent: 2,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, got.Requests)
	assert.False(t, got.Truncated)
	assert.Zero(t, got.SkippedTargets)
	require.Len(t, got.Items, 2)
	assert.Equal(t, []string{"event-a", "event-b"}, []string{got.Items[0].Name, got.Items[1].Name})
}

func TestCollectWarningEventsAppliesPriorityBeforeTargetCap(t *testing.T) {
	t.Parallel()

	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		restrictions := action.(ktesting.ListAction).GetListRestrictions().Fields
		name, _ := restrictions.RequiresExactMatch("involvedObject.name")
		kind, _ := restrictions.RequiresExactMatch("involvedObject.kind")
		event := warningEvent("event-"+name, types.UID("uid-"+name), name)
		event.InvolvedObject.Kind = kind
		return true, &corev1.EventList{Items: []corev1.Event{event}}, nil
	})
	targets := []ObjectRef{
		{Namespace: "team-a", Kind: "Pod", Name: "healthy-a", Priority: 0},
		{Namespace: "team-a", Kind: "Pod", Name: "unhealthy-z", Priority: 10},
		{Namespace: "team-a", Kind: "InferenceReplica", Name: "ir", Priority: 20},
	}
	got, err := CollectWarningEvents(context.Background(), kube.CoreV1(), targets, EventLimits{
		Paging:     paging.Limits{PageSize: 10, MaxItems: 10, MaxPages: 1, RequestTimeout: time.Second},
		MaxTargets: 2, MaxConcurrent: 1,
	})
	require.NoError(t, err)
	require.Len(t, got.Items, 2)
	assert.Equal(t, []string{"event-ir", "event-unhealthy-z"}, []string{got.Items[0].Name, got.Items[1].Name})
	assert.True(t, got.Truncated)
}

func pod(name, service string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: "team-a",
		Labels:    map[string]string{"ome.io/inferenceservice": service},
	}}
}

func warningEvent(name string, uid types.UID, target string) corev1.Event {
	return corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: uid},
		Type:           corev1.EventTypeWarning,
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: target},
	}
}

type blockingEventGetter struct {
	started chan<- struct{}
	release <-chan struct{}
	active  atomic.Int32
	maximum atomic.Int32
}

type blindPodGetter struct{}

type cancelingPodGetter struct {
	cancel context.CancelFunc
}

func (getter cancelingPodGetter) Pods(string) coreclient.PodInterface {
	return cancelingPodInterface{PodInterface: nil, cancel: getter.cancel}
}

type cancelingPodInterface struct {
	coreclient.PodInterface
	cancel context.CancelFunc
}

func (pods cancelingPodInterface) List(context.Context, metav1.ListOptions) (*corev1.PodList, error) {
	pods.cancel()
	return &corev1.PodList{Items: []corev1.Pod{*pod("late", "chat")}}, nil
}

type nilEventGetter struct{}

func (nilEventGetter) Events(string) coreclient.EventInterface {
	return nilEventInterface{EventInterface: nil}
}

type nilEventInterface struct {
	coreclient.EventInterface
}

func (nilEventInterface) List(context.Context, metav1.ListOptions) (*corev1.EventList, error) {
	return nil, nil
}

type staticPodGetter struct {
	list *corev1.PodList
}

func (getter staticPodGetter) Pods(string) coreclient.PodInterface {
	return &staticPodInterface{PodInterface: nil, list: getter.list}
}

type staticPodInterface struct {
	coreclient.PodInterface
	list *corev1.PodList
}

func (pods *staticPodInterface) List(context.Context, metav1.ListOptions) (*corev1.PodList, error) {
	return pods.list, nil
}

func (blindPodGetter) Pods(namespace string) coreclient.PodInterface {
	return &blindPodInterface{PodInterface: nil, namespace: namespace}
}

type blindPodInterface struct {
	coreclient.PodInterface
	namespace string
}

func (pods *blindPodInterface) List(context.Context, metav1.ListOptions) (*corev1.PodList, error) {
	return &corev1.PodList{Items: []corev1.Pod{
		*pod("wanted", "chat"),
		*pod("unrelated", "other"),
	}}, nil
}

func (getter *blockingEventGetter) Events(namespace string) coreclient.EventInterface {
	return &blockingEventInterface{EventInterface: nil, getter: getter, namespace: namespace}
}

type blockingEventInterface struct {
	coreclient.EventInterface
	getter    *blockingEventGetter
	namespace string
}

func (events *blockingEventInterface) List(_ context.Context, opts metav1.ListOptions) (*corev1.EventList, error) {
	current := events.getter.active.Add(1)
	for {
		maximum := events.getter.maximum.Load()
		if current <= maximum || events.getter.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	events.getter.started <- struct{}{}
	<-events.getter.release
	events.getter.active.Add(-1)

	selector, err := fields.ParseSelector(opts.FieldSelector)
	if err != nil {
		return nil, err
	}
	name, _ := selector.RequiresExactMatch("involvedObject.name")
	kind, _ := selector.RequiresExactMatch("involvedObject.kind")
	return &corev1.EventList{Items: []corev1.Event{{
		ObjectMeta: metav1.ObjectMeta{Name: "event-" + name, Namespace: events.namespace, UID: types.UID("uid-" + name)},
		Type:       corev1.EventTypeWarning,
		InvolvedObject: corev1.ObjectReference{
			Kind: kind,
			Name: name,
		},
	}}}, nil
}
