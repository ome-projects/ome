package workloadcluster

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type metadataTransport struct {
	SelectivelyCachingClient
	list  func(context.Context, client.ObjectList, ...client.ListOption) error
	watch func(context.Context, client.ObjectList, ...client.ListOption) (watch.Interface, error)
}

func (c *metadataTransport) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.list(ctx, list, opts...)
}
func (c *metadataTransport) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	return c.watch(ctx, list, opts...)
}

func TestMetadataFunnelSnapshotAndEvents(t *testing.T) {
	for _, tt := range []struct {
		name    string
		event   watch.EventType
		rotate  bool
		wantErr bool
	}{
		{name: "admission update", event: watch.Modified},
		{name: "new component", event: watch.Added},
		{name: "deleted component", event: watch.Deleted},
		{name: "expired watch", event: watch.Error, wantErr: true},
		{name: "replaced connection", rotate: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stream := watch.NewRaceFreeFake()
			listed := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "snapshot", Labels: map[string]string{"origin": "source-a"}}}
			remote := &metadataTransport{}
			remote.list = func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("metadata list has no deadline")
				}
				lo := (&client.ListOptions{}).ApplyOptions(opts)
				if diff := cmp.Diff("origin=source-a", lo.LabelSelector.String()); diff != "" {
					t.Error(diff)
				}
				snapshot := list.(*metav1.PartialObjectMetadataList)
				snapshot.ResourceVersion = "41"
				snapshot.Items = []metav1.PartialObjectMetadata{*listed}
				return nil
			}
			remote.watch = func(_ context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
				if _, ok := list.(*metav1.PartialObjectMetadataList); !ok {
					t.Error("watch requests full resource payloads")
				}
				lo := (&client.ListOptions{}).ApplyOptions(opts)
				if diff := cmp.Diff("41", lo.Raw.ResourceVersion); diff != "" {
					t.Error(diff)
				}
				if diff := cmp.Diff("origin=source-a", lo.LabelSelector.String()); diff != "" {
					t.Error(diff)
				}
				return stream, nil
			}
			manager := NewManager(runtime.NewScheme())
			manager.newClient = func(context.Context, []byte, *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
				return remote, func() {}, nil
			}
			manager.SetReconnectBackoff(reconnectBackoff{establishInitial: time.Second, establishMax: time.Second, establishFactor: 2})
			if err := manager.Connect(t.Context(), "member-a", []byte("first")); err != nil {
				t.Fatal(err)
			}
			defer manager.Disconnect("member-a")
			funnel := NewStatusFunnel(manager, FunnelConfig{MetadataOnly: true, NewList: func() client.ObjectList { return &metav1.PartialObjectMetadataList{} }, NewObject: func() client.Object { return &metav1.PartialObjectMetadata{} }, Resolve: originResolver, WatchSelector: labels.SelectorFromSet(labels.Set{"origin": "source-a"}), BufferSize: 4, ResyncInterval: time.Millisecond})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				established, err := funnel.metadataRound(ctx, "member-a", 1)
				if !established {
					t.Error("watch was not established")
				}
				done <- err
			}()
			select {
			case got := <-funnel.Events():
				if diff := cmp.Diff("snapshot", got.Object.GetName()); diff != "" {
					t.Error(diff)
				}
			case <-time.After(time.Second):
				t.Fatal("snapshot did not wake source")
			}
			if tt.rotate {
				if err := manager.Connect(t.Context(), "member-a", []byte("second")); err != nil {
					t.Fatal(err)
				}
			} else if tt.event == watch.Error {
				stream.Error(&metav1.Status{Code: 410})
			} else {
				changed := listed.DeepCopy()
				changed.Name = "event"
				switch tt.event {
				case watch.Added:
					stream.Add(changed)
				case watch.Modified:
					stream.Modify(changed)
				case watch.Deleted:
					stream.Delete(changed)
				}
				select {
				case got := <-funnel.Events():
					if diff := cmp.Diff("event", got.Object.GetName()); diff != "" {
						t.Error(diff)
					}
				case <-time.After(time.Second):
					t.Fatal("metadata event did not wake source")
				}
				cancel()
			}
			select {
			case err := <-done:
				if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
					t.Errorf("round result: %s; error: %v", diff, err)
				}
			case <-time.After(time.Second):
				t.Fatal("metadata watch did not terminate")
			}
			if !stream.IsStopped() {
				t.Error("metadata watch leaked")
			}
		})
	}
}

func TestMetadataFunnelRejectsIncompleteSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name, version, errText string
		listErr                error
	}{
		{name: "missing snapshot version", errText: "no resource version"},
		{name: "failed list", listErr: errors.New("list unavailable"), errText: "list unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			remote := &metadataTransport{list: func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
				list.SetResourceVersion(tt.version)
				return tt.listErr
			}, watch: func(context.Context, client.ObjectList, ...client.ListOption) (watch.Interface, error) {
				t.Error("unverified snapshot started watch")
				return nil, errors.New("unexpected watch")
			}}
			manager := NewManager(runtime.NewScheme())
			manager.newClient = func(context.Context, []byte, *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
				return remote, func() {}, nil
			}
			if err := manager.Connect(t.Context(), "member-a", []byte("config")); err != nil {
				t.Fatal(err)
			}
			defer manager.Disconnect("member-a")
			funnel := NewStatusFunnel(manager, FunnelConfig{NewList: func() client.ObjectList { return &metav1.PartialObjectMetadataList{} }})
			established, err := funnel.metadataRound(t.Context(), "member-a", 1)
			if established {
				t.Error("incomplete snapshot established watch")
			}
			if err == nil || !strings.Contains(err.Error(), tt.errText) {
				t.Fatalf("error=%v, want %q", err, tt.errText)
			}
		})
	}
}

func TestMetadataFunnelRelistsAfterFailure(t *testing.T) {
	for _, tt := range []struct {
		name        string
		listFailure bool
		wantNames   []string
		wantWatches int
	}{
		{name: "expired watch", wantNames: []string{"snapshot-1", "snapshot-2"}, wantWatches: 2},
		{name: "initial list failure", listFailure: true, wantNames: []string{"snapshot-2"}, wantWatches: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			listCalls := 0
			streams := []*watch.RaceFreeFakeWatcher{}
			remote := &metadataTransport{}
			remote.list = func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
				listCalls++
				if tt.listFailure && listCalls == 1 {
					return errors.New("list unavailable")
				}
				snapshot := list.(*metav1.PartialObjectMetadataList)
				snapshot.ResourceVersion = strconv.Itoa(listCalls)
				snapshot.Items = []metav1.PartialObjectMetadata{{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "snapshot-" + snapshot.ResourceVersion, Labels: map[string]string{"origin": "source-a"}}}}
				return nil
			}
			remote.watch = func(context.Context, client.ObjectList, ...client.ListOption) (watch.Interface, error) {
				stream := watch.NewRaceFreeFake()
				streams = append(streams, stream)
				if !tt.listFailure && len(streams) == 1 {
					stream.Error(&metav1.Status{Code: 410})
				}
				return stream, nil
			}
			manager := NewManager(runtime.NewScheme())
			manager.newClient = func(context.Context, []byte, *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
				return remote, func() {}, nil
			}
			manager.SetReconnectBackoff(reconnectBackoff{establishInitial: time.Second, establishMax: time.Second, establishFactor: 2, retryInitial: time.Millisecond, retryMax: time.Millisecond})
			if err := manager.Connect(t.Context(), "member-a", []byte("config")); err != nil {
				t.Fatal(err)
			}
			defer manager.Disconnect("member-a")
			funnel := NewStatusFunnel(manager, FunnelConfig{NewList: func() client.ObjectList { return &metav1.PartialObjectMetadataList{} }, NewObject: func() client.Object { return &metav1.PartialObjectMetadata{} }, Resolve: originResolver, BufferSize: 4, ResyncInterval: time.Millisecond})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); funnel.watchMetadataCluster(ctx, "member-a") }()
			names := []string{}
			for range tt.wantNames {
				select {
				case ev := <-funnel.Events():
					names = append(names, ev.Object.GetName())
				case <-time.After(time.Second):
					t.Fatal("metadata snapshot was not recovered")
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("metadata watch did not stop")
			}
			if diff := cmp.Diff(tt.wantNames, names); diff != "" {
				t.Error(diff)
			}
			if diff := cmp.Diff(2, listCalls); diff != "" {
				t.Errorf("list calls: %s", diff)
			}
			if diff := cmp.Diff(tt.wantWatches, len(streams)); diff != "" {
				t.Errorf("watch calls: %s", diff)
			}
			for _, stream := range streams {
				if !stream.IsStopped() {
					t.Error("watch leaked across reconnect")
				}
			}
		})
	}
}
