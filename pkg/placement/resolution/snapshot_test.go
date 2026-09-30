package resolution

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func snapshotObject() *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "input-a", Namespace: "team-a", UID: "input-uid", ResourceVersion: "1", Labels: map[string]string{"selected": "yes"}}, Data: map[string]string{"value": "original"}}
}

func newSnapshot(c client.Client, ctx context.Context) *snapshotClient {
	return &snapshotClient{Client: c, ctx: ctx, snapshots: map[string]snapshot{}}
}

func readSnapshot(c *snapshotClient, list bool) error {
	if list {
		return c.List(context.Background(), &corev1.ConfigMapList{}, client.InNamespace("team-a"), client.MatchingLabels{"selected": "yes"})
	}
	return c.Get(context.Background(), client.ObjectKeyFromObject(snapshotObject()), &corev1.ConfigMap{})
}

func TestSnapshotDependencyChanges(t *testing.T) {
	for _, tt := range []struct {
		name      string
		list      bool
		absent    bool
		reorder   bool
		mutation  string
		wantError bool
	}{
		{name: "unchanged object"},
		{name: "unchanged absence", absent: true},
		{name: "object updated", mutation: "update", wantError: true},
		{name: "object deleted", mutation: "delete", wantError: true},
		{name: "object recreated", mutation: "recreate", wantError: true},
		{name: "absent object appears", absent: true, mutation: "create", wantError: true},
		{name: "unchanged catalog", list: true},
		{name: "unchanged empty catalog", list: true, absent: true},
		{name: "catalog reordered", list: true, reorder: true},
		{name: "catalog object updated", list: true, mutation: "update", wantError: true},
		{name: "catalog object deleted", list: true, mutation: "delete", wantError: true},
		{name: "catalog object recreated", list: true, mutation: "recreate", wantError: true},
		{name: "catalog gains candidate", list: true, mutation: "create", wantError: true},
		{name: "empty catalog gains candidate", list: true, absent: true, mutation: "create", wantError: true},
		{name: "unmatched label stays outside catalog", list: true, mutation: "unmatched"},
		{name: "other namespace stays outside catalog", list: true, mutation: "other namespace"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			object := snapshotObject()
			var objects []client.Object
			if !tt.absent {
				objects = append(objects, object)
			}
			if tt.reorder {
				second := object.DeepCopy()
				second.Name, second.UID = "input-b", "second-uid"
				objects = append(objects, second)
			}
			base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(objects...).Build()
			calls := 0
			c := interceptor.NewClient(base, interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := cl.List(ctx, list, opts...); err != nil {
					return err
				}
				calls++
				if tt.reorder && calls%2 == 0 {
					slices.Reverse(list.(*corev1.ConfigMapList).Items)
				}
				return nil
			}})
			reads := newSnapshot(c, t.Context())
			if err := readSnapshot(reads, tt.list); err != nil && !(tt.absent && apierrors.IsNotFound(err)) {
				t.Fatal(err)
			}
			var err error
			switch tt.mutation {
			case "update":
				object.Data["value"] = "changed"
				err = base.Update(t.Context(), object)
			case "delete", "recreate":
				err = base.Delete(t.Context(), object)
				if err == nil && tt.mutation == "recreate" {
					object.UID, object.ResourceVersion = "replacement-uid", ""
					err = base.Create(t.Context(), object)
				}
			case "create", "unmatched", "other namespace":
				object.ResourceVersion = ""
				if !tt.absent {
					object.Name, object.UID = "added", "added-uid"
				}
				if tt.mutation == "unmatched" {
					object.Labels = nil
				}
				if tt.mutation == "other namespace" {
					object.Namespace = "team-b"
				}
				err = base.Create(t.Context(), object)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = reads.check(t.Context())
			if diff := cmp.Diff(tt.wantError, err != nil); diff != "" {
				t.Fatalf("dependency check (-want +got):\n%s\n%v", diff, err)
			}
		})
	}
}

func TestSnapshotReadFailures(t *testing.T) {
	for _, operation := range []string{"get", "list"} {
		for _, phase := range []string{"resolve", "check"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				want := errors.New("member API unavailable")
				fail := phase == "resolve"
				base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(snapshotObject()).Build()
				c := interceptor.NewClient(base, interceptor.Funcs{
					Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
						if fail {
							return want
						}
						return cl.Get(ctx, key, object, opts...)
					},
					List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if fail {
							return want
						}
						return cl.List(ctx, list, opts...)
					},
				})
				reads := newSnapshot(c, t.Context())
				err := readSnapshot(reads, operation == "list")
				if phase == "check" {
					if err != nil {
						t.Fatal(err)
					}
					fail = true
					err = reads.check(t.Context())
				}
				if diff := cmp.Diff(true, errors.Is(err, want)); diff != "" {
					t.Fatalf("API error lost:\n%s\n%v", diff, err)
				}
			})
		}
	}
}

// TestSnapshotRemembersReadFailure pins that a failed read poisons the
// observation even when the caller treats the lookup as optional and goes on.
func TestSnapshotRemembersReadFailure(t *testing.T) {
	want := errors.New("member API unavailable")
	base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(snapshotObject()).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
		return want
	}})
	reads := newSnapshot(c, t.Context())
	if err := readSnapshot(reads, false); !errors.Is(err, want) {
		t.Fatalf("read error = %v, want %v", err, want)
	}
	if err := reads.check(t.Context()); !errors.Is(err, want) {
		t.Fatalf("check after a dropped read error = %v, want %v", err, want)
	}
}

func TestSnapshotContexts(t *testing.T) {
	type contextKey struct{}
	for _, list := range []bool{false, true} {
		for _, phase := range []string{"none", "resolve", "check"} {
			t.Run(map[bool]string{false: "get", true: "list"}[list]+"/cancel-"+phase, func(t *testing.T) {
				resolveCtx, cancelResolve := context.WithCancel(context.WithValue(t.Context(), contextKey{}, "resolve"))
				defer cancelResolve()
				checkCtx, cancelCheck := context.WithCancel(context.WithValue(t.Context(), contextKey{}, "check"))
				defer cancelCheck()
				if phase == "resolve" {
					cancelResolve()
				}
				var got []string
				base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(snapshotObject()).Build()
				c := interceptor.NewClient(base, interceptor.Funcs{
					Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
						got = append(got, ctx.Value(contextKey{}).(string))
						return cl.Get(ctx, key, object, opts...)
					},
					List: func(ctx context.Context, cl client.WithWatch, objects client.ObjectList, opts ...client.ListOption) error {
						got = append(got, ctx.Value(contextKey{}).(string))
						return cl.List(ctx, objects, opts...)
					},
				})
				reads := newSnapshot(c, resolveCtx)
				err := readSnapshot(reads, list)
				want := []string{"resolve", "check"}
				if phase == "resolve" {
					want = nil
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if phase == "check" {
						cancelCheck()
						want = []string{"resolve"}
					}
					err = reads.check(checkCtx)
				}
				if diff := cmp.Diff(phase != "none", errors.Is(err, context.Canceled)); diff != "" {
					t.Fatalf("cancellation:\n%s\n%v", diff, err)
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("client contexts:\n%s", diff)
				}
			})
		}
	}
}

func TestSnapshotConcurrentChecks(t *testing.T) {
	for _, list := range []bool{false, true} {
		t.Run(map[bool]string{false: "get", true: "list"}[list], func(t *testing.T) {
			base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(snapshotObject()).Build()
			reads := newSnapshot(base, t.Context())
			if err := readSnapshot(reads, list); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					if err := reads.check(t.Context()); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
		})
	}
}

func TestSnapshotUnverifiedIdentity(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*corev1.ConfigMap)
	}{
		{name: "missing name", edit: func(cm *corev1.ConfigMap) { cm.Name = "" }},
		{name: "missing UID", edit: func(cm *corev1.ConfigMap) { cm.UID = "" }},
		{name: "missing resource version", edit: func(cm *corev1.ConfigMap) { cm.ResourceVersion = "" }},
		{name: "terminating", edit: func(cm *corev1.ConfigMap) { now := metav1.Now(); cm.DeletionTimestamp = &now }},
	} {
		for _, list := range []bool{false, true} {
			t.Run(tt.name+"/"+map[bool]string{false: "get", true: "list"}[list], func(t *testing.T) {
				base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(snapshotObject()).Build()
				c := interceptor.NewClient(base, interceptor.Funcs{
					Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
						if err := cl.Get(ctx, key, object, opts...); err != nil {
							return err
						}
						tt.edit(object.(*corev1.ConfigMap))
						return nil
					},
					List: func(ctx context.Context, cl client.WithWatch, objects client.ObjectList, opts ...client.ListOption) error {
						if err := cl.List(ctx, objects, opts...); err != nil {
							return err
						}
						tt.edit(&objects.(*corev1.ConfigMapList).Items[0])
						return nil
					},
				})
				if diff := cmp.Diff(true, readSnapshot(newSnapshot(c, t.Context()), list) != nil); diff != "" {
					t.Errorf("unverified identity accepted:\n%s", diff)
				}
			})
		}
	}
}

func TestSnapshotRepeatedReads(t *testing.T) {
	for _, list := range []bool{false, true} {
		t.Run(map[bool]string{false: "get", true: "list"}[list], func(t *testing.T) {
			object := snapshotObject()
			base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(object).Build()
			reads := newSnapshot(base, t.Context())
			if err := readSnapshot(reads, list); err != nil {
				t.Fatal(err)
			}
			object.Data["value"] = "changed"
			if err := base.Update(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			err := readSnapshot(reads, list)
			if diff := cmp.Diff(!list, err != nil); diff != "" {
				t.Errorf("repeated read:\n%s\n%v", diff, err)
			}
			if diff := cmp.Diff(true, reads.check(t.Context()) != nil); diff != "" {
				t.Errorf("inconsistent resolution accepted:\n%s", diff)
			}
		})
	}
}

func TestSnapshotIncompleteCatalog(t *testing.T) {
	for _, phase := range []string{"resolve", "check"} {
		t.Run(phase, func(t *testing.T) {
			incomplete := phase == "resolve"
			base := fake.NewClientBuilder().WithScheme(runtimeScheme(t)).WithObjects(snapshotObject()).Build()
			c := interceptor.NewClient(base, interceptor.Funcs{List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := cl.List(ctx, list, opts...); err != nil {
					return err
				}
				if incomplete {
					list.SetContinue("next-page")
				}
				return nil
			}})
			reads := newSnapshot(c, t.Context())
			err := readSnapshot(reads, true)
			if phase == "check" {
				if err != nil {
					t.Fatal(err)
				}
				incomplete = true
				err = reads.check(t.Context())
			}
			if diff := cmp.Diff(true, err != nil); diff != "" {
				t.Errorf("partial catalog accepted:\n%s", diff)
			}
		})
	}
}
