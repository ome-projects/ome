package clusterstatus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	fake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

func fixture(name string) ome.WorkloadCluster {
	return ome.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 3}, Spec: ome.WorkloadClusterSpec{ClusterSource: ome.ClusterConnectionSource{ClusterProfileRef: &ome.ClusterProfileRef{Name: "profile"}}}, Status: ome.WorkloadClusterStatus{Conditions: []metav1.Condition{{Type: "Ready", Status: "True", ObservedGeneration: 3, Reason: "Connected", LastTransitionTime: metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))}}}}
}
func limits() paging.Limits {
	return paging.Limits{PageSize: 2, MaxItems: 3, MaxPages: 2, RequestTimeout: time.Second}
}
func clock() r.Clock {
	return r.ClockFunc(func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) })
}

type boundaryClient struct {
	omeclient.OmeV1beta1Interface
	workload omeclient.WorkloadClusterInterface
}

func (c boundaryClient) WorkloadClusters() omeclient.WorkloadClusterInterface { return c.workload }

type boundaryWorkload struct {
	omeclient.WorkloadClusterInterface
	wait bool
}

func (c boundaryWorkload) Get(ctx context.Context, _ string, _ metav1.GetOptions) (*ome.WorkloadCluster, error) {
	if c.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}
func (c boundaryWorkload) List(ctx context.Context, _ metav1.ListOptions) (*ome.WorkloadClusterList, error) {
	if c.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}

func TestCollectWirePaginationAndImmutableCopies(t *testing.T) {
	client := fake.NewSimpleClientset()
	first := &ome.WorkloadClusterList{ListMeta: metav1.ListMeta{Continue: "private-token"}, Items: []ome.WorkloadCluster{fixture("b"), fixture("a")}}
	second := &ome.WorkloadClusterList{ListMeta: metav1.ListMeta{Continue: "unfollowed-token"}, Items: []ome.WorkloadCluster{fixture("c"), fixture("d")}}
	var queries []metav1.ListOptions
	client.PrependReactor("list", "workloadclusters", func(action clienttesting.Action) (bool, runtime.Object, error) {
		queries = append(queries, action.(clienttesting.ListActionImpl).ListOptions)
		if len(queries) == 1 {
			return true, first, nil
		}
		return true, second, nil
	})
	s, err := Collect(context.Background(), client.OmeV1beta1(), "", limits())
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 || queries[0].Limit != 2 || queries[0].Continue != "" || queries[1].Limit != 1 || queries[1].Continue != "private-token" || queries[0].LabelSelector != "" || queries[1].FieldSelector != "" {
		t.Fatalf("bounded request wire: %+v", queries)
	}
	if s.Pages != 2 || s.Returned != 4 || len(s.Items) != 3 || !s.Truncated {
		t.Fatalf("bounded source counts: %+v", s)
	}
	first.Items[0].Spec.ClusterSource.ClusterProfileRef.Name = "changed"
	if s.Items[0].Spec.ClusterSource.ClusterProfileRef.Name != "profile" {
		t.Fatal("collection did not own source")
	}
	projected := Project(s, clock())
	if projected.Observation != "Partial" || projected.AdmittedSources != 3 || projected.ReturnedSources != 4 {
		t.Fatalf("partial source windows: %+v", projected)
	}
	data, _ := json.Marshal(projected)
	if bytes.Contains(data, []byte("token")) {
		t.Fatal("continue token leaked")
	}
}

func TestCollectClassifiedUnavailableAndPartial(t *testing.T) {
	gr := schema.GroupResource{Group: "ome.io", Resource: "workloadclusters"}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"missing", apierrors.NewNotFound(gr, "PRIVATE-NAME"), "NotFound"},
		{"forbidden", apierrors.NewForbidden(gr, "PRIVATE-NAME", errors.New("PRIVATE-ERROR")), "Forbidden"},
		{"unauthorized", apierrors.NewUnauthorized("PRIVATE-ERROR"), "Forbidden"},
		{"unreadable", errors.New("PRIVATE-ERROR"), "Unreadable"},
	}
	for _, tc := range cases {
		for _, named := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", tc.name, named), func(t *testing.T) {
				client := fake.NewSimpleClientset()
				verb := "list"
				name := ""
				if named {
					verb = "get"
					name = "gpu"
				}
				client.PrependReactor(verb, "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, tc.err })
				s, err := Collect(context.Background(), client.OmeV1beta1(), name, limits())
				if err != nil {
					t.Fatal(err)
				}
				value := Project(s, clock())
				if string(value.UnavailableReason) != tc.want || value.Observation != "Unavailable" {
					t.Fatalf("wrong availability: %+v", value)
				}
				data, _ := json.Marshal(value)
				if bytes.Contains(data, []byte("PRIVATE-")) {
					t.Fatalf("API error privacy: %s", data)
				}
			})
		}
	}
	client := fake.NewSimpleClientset()
	calls := 0
	client.PrependReactor("list", "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, &ome.WorkloadClusterList{ListMeta: metav1.ListMeta{Continue: "next"}, Items: []ome.WorkloadCluster{fixture("gpu")}}, nil
		}
		return true, nil, cases[1].err
	})
	s, err := Collect(context.Background(), client.OmeV1beta1(), "", limits())
	if err != nil {
		t.Fatal(err)
	}
	value := Project(s, clock())
	if value.Observation != "Partial" || value.UnavailableReason != "Forbidden" || value.ObservedPages != 2 || value.ReturnedSources != 1 || len(value.Clusters) != 1 || value.Clusters[0].ConnectionState != "ReportedReady" {
		t.Fatalf("partial discarded independent evidence: %+v", value)
	}
}

func TestCollectMalformedAndValidationNoCalls(t *testing.T) {
	for _, named := range []bool{false, true} {
		client := boundaryClient{workload: boundaryWorkload{}}
		name := ""
		if named {
			name = "gpu"
		}
		s, err := Collect(context.Background(), client, name, limits())
		if err != nil || s.Unavailable != "MalformedPayload" {
			t.Fatalf("nil response: %+v, %v", s, err)
		}
	}
	for _, w := range []ome.WorkloadCluster{fixture("wrong"), func() ome.WorkloadCluster { w := fixture("gpu"); w.Namespace = "private"; return w }()} {
		client := fake.NewSimpleClientset()
		client.PrependReactor("get", "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) { return true, &w, nil })
		s, err := Collect(context.Background(), client.OmeV1beta1(), "gpu", limits())
		if err != nil || s.Unavailable != "MalformedPayload" || s.Returned != 1 || len(s.Items) != 0 {
			t.Fatalf("wrong object admitted: %+v, %v", s, err)
		}
	}
	client := fake.NewSimpleClientset()
	bad := []paging.Limits{{PageSize: 0, MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second}, {PageSize: 1, MaxPages: 1, RequestTimeout: time.Second}, {PageSize: 1, MaxItems: 1, RequestTimeout: time.Second}, {PageSize: 1, MaxItems: 1, MaxPages: 1}}
	for _, l := range bad {
		if _, err := Collect(context.Background(), client.OmeV1beta1(), "", l); err == nil {
			t.Fatal("invalid bounds admitted")
		}
	}
	if _, err := Collect(context.Background(), client.OmeV1beta1(), "PRIVATE-NAME", limits()); err == nil {
		t.Fatal("invalid name admitted")
	}
	if _, err := Collect(context.Background(), nil, "", limits()); err == nil {
		t.Fatal("nil client admitted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Collect(ctx, client.OmeV1beta1(), "gpu", limits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("validation issued API requests: %v", client.Actions())
	}
}

func TestCollectRequestTimeoutIsBoundedUnavailable(t *testing.T) {
	for _, name := range []string{"", "gpu"} {
		l := limits()
		l.RequestTimeout = time.Millisecond
		s, err := Collect(context.Background(), boundaryClient{workload: boundaryWorkload{wait: true}}, name, l)
		if err != nil || s.Unavailable != "Unreadable" || s.Pages != 1 {
			t.Fatalf("bounded request timeout: %+v %v", s, err)
		}
	}
}

func TestCollectRepeatedTokenEmptyAndCancellation(t *testing.T) {
	client := fake.NewSimpleClientset()
	calls := 0
	client.PrependReactor("list", "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		return true, &ome.WorkloadClusterList{ListMeta: metav1.ListMeta{Continue: "same"}, Items: []ome.WorkloadCluster{fixture(fmt.Sprintf("gpu%d", calls))}}, nil
	})
	s, err := Collect(context.Background(), client.OmeV1beta1(), "", limits())
	if err != nil || s.Unavailable != "Unreadable" || len(s.Items) != 1 || s.Pages != 2 {
		t.Fatalf("repeated token bound: %+v, %v", s, err)
	}
	value := Project(Snapshot{Limits: limits()}, clock())
	if value.Observation != "Empty" || len(value.Clusters) != 0 {
		t.Fatalf("empty mislabeled: %+v", value)
	}
	client = fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	client.PrependReactor("list", "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		cancel()
		return true, &ome.WorkloadClusterList{}, nil
	})
	if _, err = Collect(ctx, client.OmeV1beta1(), "", limits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-list cancellation: %v", err)
	}
	client = fake.NewSimpleClientset()
	ctx, cancel = context.WithCancel(context.Background())
	client.PrependReactor("get", "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		cancel()
		w := fixture("gpu")
		return true, &w, nil
	})
	if _, err = Collect(ctx, client.OmeV1beta1(), "gpu", limits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-get cancellation: %v", err)
	}
}

func TestProjectGroupBoundariesClockAndOwnership(t *testing.T) {
	for _, count := range []int{31, 32, 33, 127, 128, 129} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			w := fixture("gpu")
			for len(w.Status.Conditions) < count {
				c := w.Status.Conditions[0]
				c.Type = fmt.Sprintf("Other%d", len(w.Status.Conditions))
				w.Status.Conditions = append(w.Status.Conditions, c)
			}
			before := w.DeepCopy()
			calls := 0
			value := Project(Snapshot{Items: []ome.WorkloadCluster{w}, Returned: 1, Limits: limits()}, r.ClockFunc(func() time.Time { calls++; return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }))
			if calls != 1 {
				t.Fatalf("clock sampled %d times", calls)
			}
			row := value.Clusters[0]
			wantKept := count
			if wantKept > 32 {
				wantKept = 32
			}
			wantScanned := count
			wantConnection := "ReportedReady"
			if count > 128 {
				wantKept = 0
				wantScanned = 0
				wantConnection = "Unknown"
			}
			if row.TotalConditions != count || row.ScannedConditions != wantScanned || row.RetainedConditions != wantKept || string(row.ConnectionState) != wantConnection || row.ConditionsTruncated != (count > 32) {
				t.Fatalf("condition bounds: %+v", row)
			}
			if count <= 128 && row.Conditions[0].Type != "Ready" {
				t.Fatal("Ready evidence dropped behind other conditions")
			}
			if !reflect.DeepEqual(before, &w) {
				t.Fatal("projection mutated API object")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ome.WorkloadCluster)
	}{
		{"invalid-other-status", func(w *ome.WorkloadCluster) {
			c := w.Status.Conditions[0]
			c.Type = "Other"
			c.Status = "PRIVATE-STATUS"
			w.Status.Conditions = append(w.Status.Conditions, c)
		}},
		{"invalid-other-type", func(w *ome.WorkloadCluster) {
			c := w.Status.Conditions[0]
			c.Type = "\nPRIVATE-TYPE"
			w.Status.Conditions = append(w.Status.Conditions, c)
		}},
		{"duplicate-other-type", func(w *ome.WorkloadCluster) {
			c := w.Status.Conditions[0]
			c.Type = "Other"
			w.Status.Conditions = append(w.Status.Conditions, c, c)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := fixture("gpu")
			tc.mutate(&w)
			row := Project(Snapshot{Items: []ome.WorkloadCluster{w}}, clock()).Clusters[0]
			if row.ConditionState != "Malformed" || row.ConnectionState != "Unknown" {
				t.Fatalf("complete admitted group was not validated: %+v", row)
			}
		})
	}
}

func TestProjectSourceAndObjectPrivacy(t *testing.T) {
	for _, name := range []string{"bad\nPRIVATE-NAME", "gpu"} {
		w := fixture(name)
		w.Spec.ClusterSource.ClusterProfileRef.Name = strings.Repeat("PRIVATE-", 100)
		row := Project(Snapshot{Items: []ome.WorkloadCluster{w}}, clock()).Clusters[0]
		if row.SourceState != "Truncated" || row.ConnectionState != "Unknown" || row.DeclaredProfile != "" {
			t.Fatalf("oversized source evidence: %+v", row)
		}
	}
	a := fixture("same")
	b := fixture("same")
	value := Project(Snapshot{Items: []ome.WorkloadCluster{a, b}}, clock())
	for _, row := range value.Clusters {
		if row.ConditionState != "Malformed" || row.ConnectionState != "Unknown" {
			t.Fatal("duplicate collection identity looked healthy")
		}
	}
	a = fixture("gpu")
	a.Namespace = "private"
	if row := Project(Snapshot{Items: []ome.WorkloadCluster{a}}, clock()).Clusters[0]; row.Name != "" || row.ConnectionState != "Unknown" {
		t.Fatal("namespaced cluster admitted")
	}
	_ = Project(Snapshot{}, nil)
}
