package workloadcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestConnectionFollowsRegistryIdentity(t *testing.T) {
	ctx := context.Background()
	m, canceled := newTestManager()
	raw := []byte("credential")
	require.NoError(t, m.ConnectFor(ctx, "cluster-a", "old-uid", raw))
	old, oldGeneration, ok := m.clientForGeneration("cluster-a")
	require.True(t, ok)
	for _, tt := range []struct {
		name string
		uid  types.UID
	}{
		{name: "unknown UID"},
		{name: "replacement UID", uid: "new-uid"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cl, matches := m.ClientForUID("cluster-a", tt.uid)
			require.False(t, matches)
			require.Nil(t, cl)
		})
	}
	require.NoError(t, m.ConnectFor(ctx, "cluster-a", "old-uid", raw))
	require.Empty(t, *canceled, "equal identity and credential must preserve the cache")
	require.NoError(t, m.ConnectFor(ctx, "cluster-a", "new-uid", raw))
	current, currentGeneration, ok := m.clientForGeneration("cluster-a")
	require.True(t, ok)
	require.NotSame(t, old, current)
	require.Greater(t, currentGeneration, oldGeneration, "watch handlers must attach to a new connection")
	if diff := cmp.Diff([]string{"credential"}, *canceled); diff != "" {
		t.Fatalf("canceled connections (-want +got):\n%s", diff)
	}
	cl, ok := m.ClientForUID("cluster-a", "new-uid")
	require.True(t, ok)
	require.Same(t, current, cl)
	cl, ok = m.ClientForUID("cluster-a", "old-uid")
	require.False(t, ok)
	require.Nil(t, cl)
	require.ErrorContains(t, m.ConnectFor(ctx, "cluster-a", "", raw), "requires a registry UID")
	cl, ok = m.ClientForUID("cluster-a", "new-uid")
	require.True(t, ok)
	require.Same(t, current, cl, "an invalid caller cannot replace an identified connection")
	m.Disconnect("cluster-a")
	_, ok = m.ClientForUID("cluster-a", "new-uid")
	require.False(t, ok)
}

func TestUnidentifiedConnectionCannotProveRegistryIdentity(t *testing.T) {
	m, _ := newTestManager()
	require.NoError(t, m.Connect(context.Background(), "cluster-a", []byte("credential")))
	_, ok := m.ClientForUID("cluster-a", "uid")
	require.False(t, ok)
}

func TestIdentityRebuildFailureEvictsOldConnection(t *testing.T) {
	m, canceled := newTestManager()
	ctx := context.Background()
	require.NoError(t, m.ConnectFor(ctx, "cluster-a", "old-uid", []byte("credential")))
	m.newClient = func(context.Context, []byte, *runtime.Scheme) (SelectivelyCachingClient, context.CancelFunc, error) {
		return nil, nil, errors.New("cannot connect")
	}
	require.Error(t, m.ConnectFor(ctx, "cluster-a", "new-uid", []byte("credential")))
	_, ok := m.ClientFor("cluster-a")
	require.False(t, ok)
	if diff := cmp.Diff([]string{"credential"}, *canceled); diff != "" {
		t.Fatalf("canceled connections (-want +got):\n%s", diff)
	}
}

func TestRecreatedRegistrationCannotInheritConnectionGrace(t *testing.T) {
	ctx := context.Background()
	wc := wcWithSecret("cluster-a")
	wc.UID = "old-uid"
	probeFails := false
	r, cl := newReconciler(scheme(t), func(context.Context, []byte) error {
		if probeFails {
			return errors.New("probe unavailable")
		}
		return nil
	}, wc, kcSecret(validKubeconfig))
	m, canceled := newTestManager()
	r.Manager, r.ConnectionGracePeriod = m, time.Hour
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: wc.Name}}
	_, err := r.Reconcile(ctx, request)
	require.NoError(t, err)
	_, ok := m.ClientForUID(wc.Name, wc.UID)
	require.True(t, ok)
	require.NoError(t, cl.Delete(ctx, wc))
	wc.UID, wc.ResourceVersion = "new-uid", ""
	require.NoError(t, cl.Create(ctx, wc))
	probeFails = true
	_, err = r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.Len(t, *canceled, 1)
	_, ok = m.ClientFor(wc.Name)
	require.False(t, ok)
	if diff := cmp.Diff(metav1.ConditionFalse, readyCond(cl, wc.Name).Status); diff != "" {
		t.Fatalf("replacement readiness (-want +got):\n%s", diff)
	}
	probeFails = false
	_, err = r.Reconcile(ctx, request)
	require.NoError(t, err)
	_, ok = m.ClientForUID(wc.Name, "new-uid")
	require.True(t, ok)
	var current v1beta1.WorkloadCluster
	require.NoError(t, cl.Get(ctx, request.NamespacedName, &current))
	if diff := cmp.Diff(types.UID("new-uid"), current.UID); diff != "" {
		t.Fatalf("registry UID (-want +got):\n%s", diff)
	}
}
