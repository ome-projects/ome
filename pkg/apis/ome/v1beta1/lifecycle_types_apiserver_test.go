package v1beta1_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	ometesting "sigs.k8s.io/ome/pkg/utils/testing"
)

// TestLeaderGoneGracePeriod_OnARealAPIServer installs the InferenceReplica
// CRD this repository ships on a real API server and asks it which values of
// spec.lifecycle.leaderGoneGracePeriod it stores. No webhook runs here, so
// what refuses a value is the CRD's own rule.
//
// A replica with the field unset is stored. One with 30s is stored and reads
// back as 30s. One with 0s or -5s is refused on create, and the stored
// replica cannot be changed to 0s.
//
// It needs the control plane binaries KUBEBUILDER_ASSETS names and is
// skipped without them.
func TestLeaderGoneGracePeriod_OnARealAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: export KUBEBUILDER_ASSETS=\"$(bin/setup-envtest use <kubernetes-version> -p path)\"")
	}
	_, cfg := ometesting.StartOnBusyHost(t, func() *envtest.Environment {
		return &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "..", "config", "crd", "full", "ome.io_inferencereplicas.yaml")},
			ErrorIfCRDPathMissing: true,
		}
	})
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("register the API types: %v", err)
	}
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build the client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const refusal = "leaderGoneGracePeriod must be above 0s"
	cases := []struct {
		name    string
		grace   *metav1.Duration
		refused bool
	}{
		{name: "unset"},
		{name: "thirty-seconds", grace: &metav1.Duration{Duration: 30 * time.Second}},
		{name: "four-hundred-milliseconds", grace: &metav1.Duration{Duration: 400 * time.Millisecond}},
		{name: "zero", grace: &metav1.Duration{}, refused: true},
		{name: "negative", grace: &metav1.Duration{Duration: -5 * time.Second}, refused: true},
	}
	for _, tc := range cases {
		err := api.Create(ctx, gangReplica(tc.name, tc.grace))
		switch {
		case tc.refused && (err == nil || !strings.Contains(err.Error(), refusal)):
			t.Errorf("%s: want the create refused with %q, got %v", tc.name, refusal, err)
		case !tc.refused && err != nil:
			t.Errorf("%s: want the replica stored, got %v", tc.name, err)
		}
	}

	stored := &v1beta1.InferenceReplica{}
	if err := api.Get(ctx, client.ObjectKey{Namespace: "default", Name: "thirty-seconds"}, stored); err != nil {
		t.Fatalf("read the stored replica: %v", err)
	}
	if lc := stored.Spec.Lifecycle; lc == nil || lc.LeaderGoneGracePeriod == nil || lc.LeaderGoneGracePeriod.Duration != 30*time.Second {
		t.Fatalf("the stored replica reads back %+v, want a leaderGoneGracePeriod of 30s", lc)
	}
	stored.Spec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{}
	if err := api.Update(ctx, stored); err == nil || !strings.Contains(err.Error(), refusal) {
		t.Fatalf("want the change to 0s refused with %q, got %v", refusal, err)
	}
}

// gangReplica is a standalone replica of one Instance with a leader and
// three workers, each pod with a termination grace of 480 s. A grace that is
// not nil is set as its lifecycle.leaderGoneGracePeriod.
func gangReplica(name string, grace *metav1.Duration) *v1beta1.InferenceReplica {
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		TerminationGracePeriodSeconds: ptr.To(int64(480)),
		Containers:                    []corev1.Container{{Name: "ome-container", Image: "registry.example.com/runtime:v1"}},
	}}
	replica := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: v1beta1.EngineComponent,
			Replicas:  ptr.To(int32(1)),
			Runners: []v1beta1.Runner{
				{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template},
				{Name: v1beta1.RunnerNameWorker, Size: 3, Template: template},
			},
		},
	}
	if grace != nil {
		replica.Spec.Lifecycle = &v1beta1.LifecycleSpec{LeaderGoneGracePeriod: grace}
	}
	return replica
}
