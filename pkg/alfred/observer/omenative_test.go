package observer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

var capabilityReadNow = time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)

func TestOMENativeExecutorCapabilityBoundaryMatrix(t *testing.T) {
	tests := []struct {
		name          string
		readErr       error
		mutate        func(*coordinationv1.Lease)
		wantAvailable bool
		wantReason    string
		wantWire      string
		wantRenew     time.Time
	}{
		{
			name:       "not found",
			readErr:    apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "capability"),
			wantReason: "lease-absent",
		},
		{
			name:       "forbidden",
			readErr:    apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "capability", errors.New("secret-forbidden-payload")),
			wantReason: "lease-unreadable",
		},
		{name: "read error", readErr: errors.New("secret-reader-payload"), wantReason: "lease-unreadable"},
		{
			name: "deleting",
			mutate: func(lease *coordinationv1.Lease) {
				deleting := metav1.NewTime(capabilityReadNow)
				lease.DeletionTimestamp = &deleting
			},
			wantReason: "lease-deleting",
		},
		{
			name: "schema missing",
			mutate: func(lease *coordinationv1.Lease) {
				delete(lease.Annotations, "ome.io/migration-request-schema")
			},
			wantReason: "schema-missing",
		},
		{
			name: "schema whitespace",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Annotations["ome.io/migration-request-schema"] = " \t\n"
			},
			wantReason: "schema-missing",
		},
		{
			name: "schema incompatible",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Annotations["ome.io/migration-request-schema"] = "secret-hostile-schema-v999"
			},
			wantReason: "schema-incompatible",
		},
		{
			name: "holder nil",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Spec.HolderIdentity = nil
			},
			wantReason: "holder-missing",
		},
		{
			name: "holder whitespace",
			mutate: func(lease *coordinationv1.Lease) {
				holder := " \t\n"
				lease.Spec.HolderIdentity = &holder
			},
			wantReason: "holder-missing",
		},
		{
			name: "renew time missing",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Spec.RenewTime = nil
			},
			wantReason: "renew-time-missing",
		},
		{
			name: "renew time zero",
			mutate: func(lease *coordinationv1.Lease) {
				lease.Spec.RenewTime = &metav1.MicroTime{}
			},
			wantReason: "renew-time-missing",
		},
		{
			name: "exact stale boundary",
			mutate: func(lease *coordinationv1.Lease) {
				renewed := metav1.NewMicroTime(capabilityReadNow.Add(-30 * time.Second))
				lease.Spec.RenewTime = &renewed
			},
			wantAvailable: true,
			wantReason:    "ready",
			wantWire:      "v1",
			wantRenew:     capabilityReadNow.Add(-30 * time.Second),
		},
		{
			name: "one microsecond stale",
			mutate: func(lease *coordinationv1.Lease) {
				renewed := metav1.NewMicroTime(capabilityReadNow.Add(-30*time.Second - time.Microsecond))
				lease.Spec.RenewTime = &renewed
			},
			wantReason: "lease-stale",
		},
		{
			name: "exact future skew boundary",
			mutate: func(lease *coordinationv1.Lease) {
				renewed := metav1.NewMicroTime(capabilityReadNow.Add(5 * time.Second))
				lease.Spec.RenewTime = &renewed
			},
			wantAvailable: true,
			wantReason:    "ready",
			wantWire:      "v1",
			wantRenew:     capabilityReadNow.Add(5 * time.Second),
		},
		{
			name: "one microsecond beyond future skew",
			mutate: func(lease *coordinationv1.Lease) {
				renewed := metav1.NewMicroTime(capabilityReadNow.Add(5*time.Second + time.Microsecond))
				lease.Spec.RenewTime = &renewed
			},
			wantReason: "renew-time-future",
		},
		{
			name:          "fresh compatible",
			wantAvailable: true,
			wantReason:    "ready",
			wantWire:      "v1",
			wantRenew:     capabilityReadNow.Add(-10 * time.Second),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lease := validCapabilityLease("capability-system", "capability", capabilityReadNow.Add(-10*time.Second))
			if test.mutate != nil {
				test.mutate(lease)
			}
			reader := &recordingCapabilityReader{objects: map[client.ObjectKey]*coordinationv1.Lease{
				client.ObjectKeyFromObject(lease): lease,
			}, err: test.readErr}
			cfg := config.Default()
			cfg.OMENativeCapabilityLeaseNamespace = "capability-system"
			cfg.OMENativeCapabilityLeaseName = "capability"
			cfg.OMENativeCapabilityMaxStaleness = metav1.Duration{Duration: 30 * time.Second}
			capability := &OMENativeExecutorReader{Reader: reader, Now: func() time.Time { return capabilityReadNow }}

			got := capability.Read(context.Background(), cfg)
			if got.Available != test.wantAvailable || got.Reason != test.wantReason || got.WireVersion != test.wantWire ||
				!got.RenewTime.Equal(test.wantRenew) {
				t.Fatalf("Read() = %+v, want available=%t reason=%q wire=%q renew=%v",
					got, test.wantAvailable, test.wantReason, test.wantWire, test.wantRenew)
			}
			if len(reader.keys) != 1 || reader.keys[0] != (client.ObjectKey{Namespace: "capability-system", Name: "capability"}) {
				t.Fatalf("Get keys = %v, want one configured key", reader.keys)
			}
			if reader.listCalls != 0 {
				t.Fatalf("List calls = %d, want Get-only reader", reader.listCalls)
			}
			if len(got.Reason) > 64 {
				t.Fatalf("reason is unbounded: %q", got.Reason)
			}
			for _, payload := range []string{"secret-forbidden-payload", "secret-reader-payload", "secret-hostile-schema-v999"} {
				if strings.Contains(got.Reason, payload) {
					t.Fatalf("reason exposes hostile payload %q: %q", payload, got.Reason)
				}
			}
		})
	}
}

func TestOMENativeExecutorUsesRuntimeCapabilityNamespace(t *testing.T) {
	store, err := config.NewStoreForNamespace("ome-prod")
	if err != nil {
		t.Fatalf("NewStoreForNamespace: %v", err)
	}
	key := client.ObjectKey{
		Namespace: "ome-prod",
		Name:      "ome-inferencereplica-executor",
	}
	reader := &recordingCapabilityReader{objects: map[client.ObjectKey]*coordinationv1.Lease{
		key: validCapabilityLease(key.Namespace, key.Name, capabilityReadNow.Add(-10*time.Second)),
	}}

	got := (&OMENativeExecutorReader{
		Reader: reader,
		Now:    func() time.Time { return capabilityReadNow },
	}).Read(context.Background(), store.Get())

	if !got.Available || got.Reason != omenativeReasonReady {
		t.Fatalf("executor state = %+v, want ready", got)
	}
	if len(reader.keys) != 1 || reader.keys[0] != key {
		t.Fatalf("direct Get keys = %v, want only %v", reader.keys, key)
	}
}

func TestOMENativeExecutorUsesOneConfigGenerationPerObservationPass(t *testing.T) {
	active := config.NewStore()
	if _, err := active.Update([]byte(`
schemaVersion: 1
omenativeCapabilityLeaseNamespace: capability-a
omenativeCapabilityLeaseName: executor-a
omenativeCapabilityMaxStaleness: 30s
`)); err != nil {
		t.Fatal(err)
	}
	store := &countingConfigStore{Store: active}
	reader := &recordingCapabilityReader{objects: map[client.ObjectKey]*coordinationv1.Lease{
		{Namespace: "capability-a", Name: "executor-a"}: validCapabilityLease("capability-a", "executor-a", capabilityReadNow.Add(-20*time.Second)),
		{Namespace: "capability-b", Name: "executor-b"}: validCapabilityLease("capability-b", "executor-b", capabilityReadNow.Add(-31*time.Second)),
	}}
	reader.onGet = func(key client.ObjectKey) {
		if key.Namespace != "capability-a" {
			return
		}
		if _, err := active.Update([]byte(`
schemaVersion: 1
omenativeCapabilityLeaseNamespace: capability-b
omenativeCapabilityLeaseName: executor-b
omenativeCapabilityMaxStaleness: 30s
`)); err != nil {
			t.Fatalf("hot reload: %v", err)
		}
	}

	loop, _ := newTestLoop(t)
	loop.Store = store
	direct := &OMENativeExecutorReader{Reader: reader, Now: func() time.Time { return capabilityReadNow }}
	var executorConfigs, scorerConfigs []*config.Config
	loop.OMENativeExecutor = func(ctx context.Context, cfg *config.Config) snapshot.OMENativeExecutorState {
		executorConfigs = append(executorConfigs, cfg)
		return direct.Read(ctx, cfg)
	}
	loop.Scorer = func(_ *snapshot.ClusterSnapshot, cfg *config.Config, _ *metrics.Metrics) {
		scorerConfigs = append(scorerConfigs, cfg)
	}

	if err := loop.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := loop.Latest().OMENativeExecutor; !got.Available || got.Reason != "ready" {
		t.Fatalf("first pass executor = %+v, want config-a ready", got)
	}
	if store.gets != 1 {
		t.Fatalf("Store.Get calls after first pass = %d, want 1", store.gets)
	}
	if executorConfigs[0] != scorerConfigs[0] {
		t.Fatal("first pass mixed executor and scorer config generations")
	}

	if err := loop.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := loop.Latest().OMENativeExecutor; got.Available || got.Reason != "lease-stale" {
		t.Fatalf("second pass executor = %+v, want config-b stale", got)
	}
	if store.gets != 2 {
		t.Fatalf("Store.Get calls after two passes = %d, want one per pass", store.gets)
	}
	if executorConfigs[1] != scorerConfigs[1] || executorConfigs[0] == executorConfigs[1] {
		t.Fatal("hot reload did not advance atomically on the next pass")
	}
	wantKeys := []client.ObjectKey{
		{Namespace: "capability-a", Name: "executor-a"},
		{Namespace: "capability-b", Name: "executor-b"},
	}
	if len(reader.keys) != len(wantKeys) || reader.keys[0] != wantKeys[0] || reader.keys[1] != wantKeys[1] {
		t.Fatalf("direct Get keys = %v, want %v", reader.keys, wantKeys)
	}
	if reader.listCalls != 0 {
		t.Fatalf("List calls = %d, want 0", reader.listCalls)
	}
}

type recordingCapabilityReader struct {
	objects   map[client.ObjectKey]*coordinationv1.Lease
	err       error
	keys      []client.ObjectKey
	listCalls int
	onGet     func(client.ObjectKey)
}

func (r *recordingCapabilityReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	r.keys = append(r.keys, key)
	if r.onGet != nil {
		r.onGet(key)
	}
	if r.err != nil {
		return r.err
	}
	lease, ok := r.objects[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, key.Name)
	}
	lease.DeepCopyInto(obj.(*coordinationv1.Lease))
	return nil
}

func (r *recordingCapabilityReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	r.listCalls++
	return errors.New("List must not be used for executor capability")
}

type countingConfigStore struct {
	Store *config.Store
	gets  int
}

func (s *countingConfigStore) Get() *config.Config {
	s.gets++
	return s.Store.Get()
}

func validCapabilityLease(namespace, name string, renewed time.Time) *coordinationv1.Lease {
	holder := "ome-controller-manager-7"
	renewTime := metav1.NewMicroTime(renewed)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Name:        name,
			Annotations: map[string]string{"ome.io/migration-request-schema": "v1"},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, RenewTime: &renewTime},
	}
}
