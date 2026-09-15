package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	clienttesting "k8s.io/client-go/testing"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	fake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/yaml"
)

func syntheticCluster() *ome.WorkloadCluster {
	return &ome.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-east", Generation: 3, Annotations: map[string]string{"private": "SECRET-METADATA"}},
		Spec:       ome.WorkloadClusterSpec{ClusterSource: ome.ClusterConnectionSource{KubeConfig: &ome.KubeConfigSource{SecretRef: corev1.SecretReference{Name: "secret-private", Namespace: "private"}, Key: "SECRET-KEY"}}},
		Status:     ome.WorkloadClusterStatus{Conditions: []metav1.Condition{{Type: "Ready", Status: "True", ObservedGeneration: 3, Reason: "Connected", Message: "SECRET-MESSAGE", LastTransitionTime: metav1.NewTime(time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))}}},
	}
}

func TestClusterStatusPartialFailureVisibleAllFormats(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			calls := 0
			client.PrependReactor("list", "workloadclusters", func(action clienttesting.Action) (bool, runtime.Object, error) {
				calls++
				query := action.(clienttesting.ListActionImpl).ListOptions
				if calls == 1 {
					if query.Limit != 32 || query.Continue != "" {
						t.Fatalf("first-page wire: %+v", query)
					}
					return true, &ome.WorkloadClusterList{ListMeta: metav1.ListMeta{Continue: "PRIVATE-TOKEN"}, Items: []ome.WorkloadCluster{*syntheticCluster()}}, nil
				}
				if query.Limit != 32 || query.Continue != "PRIVATE-TOKEN" {
					t.Fatalf("second-page wire: %+v", query)
				}
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "workloadclusters"}, "PRIVATE-NAME", errors.New("PRIVATE-ERROR"))
			})
			var out bytes.Buffer
			root := NewRootCmdWithFactory(factory.Static{OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
			root.SetArgs([]string{"cluster", "status", "-o", format})
			if err := root.Execute(); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"Partial", "Forbidden", "gpu-east", "True", "Current"} {
				if !strings.Contains(out.String(), want) {
					t.Fatalf("%s hides literal %q: %s", format, want, &out)
				}
			}
			if strings.Contains(out.String(), "PRIVATE-") || strings.Contains(out.String(), "SECRET-") {
				t.Fatalf("private evidence leaked: %s", &out)
			}
			if format == "table" {
				for _, want := range []string{"returned=1", "kept=1", "max=64", "read=2", "cut=true"} {
					if !strings.Contains(out.String(), want) {
						t.Fatalf("compact window missing %q: %s", want, &out)
					}
				}
			}
			if format == "json" || format == "yaml" {
				data := out.Bytes()
				if format == "yaml" {
					var err error
					data, err = yaml.YAMLToJSON(data)
					if err != nil {
						t.Fatal(err)
					}
				}
				var got struct {
					Observation, UnavailableReason                  string
					ObservedPages, ReturnedSources, AdmittedSources int
					SourcesTruncated                                bool
					Clusters                                        []struct{ ConnectionState string }
				}
				if err := json.Unmarshal(data, &got); err != nil {
					t.Fatal(err)
				}
				if got.Observation != "Partial" || got.UnavailableReason != "Forbidden" || got.ObservedPages != 2 || got.ReturnedSources != 1 || got.AdmittedSources != 1 || !got.SourcesTruncated || len(got.Clusters) != 1 || got.Clusters[0].ConnectionState != "ReportedReady" {
					t.Fatalf("partial machine semantics: %+v", got)
				}
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "list" || action.GetResource().Resource != "workloadclusters" || action.GetNamespace() != "" {
					t.Fatalf("out-of-scope read: %v", action)
				}
			}
			if len(client.Actions()) != 2 {
				t.Fatal("pagination action count")
			}
		})
	}
}

func TestClusterStatusUnavailableCauseVisibleAllFormats(t *testing.T) {
	gr := schema.GroupResource{Group: "ome.io", Resource: "workloadclusters"}
	for _, tc := range []struct {
		reason string
		err    error
	}{{"NotFound", apierrors.NewNotFound(gr, "PRIVATE-NAME")}, {"Forbidden", apierrors.NewForbidden(gr, "PRIVATE-NAME", errors.New("PRIVATE-ERROR"))}} {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			t.Run(tc.reason+"/"+format, func(t *testing.T) {
				client := fake.NewSimpleClientset()
				client.PrependReactor("get", "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, tc.err })
				var out bytes.Buffer
				root := NewRootCmdWithFactory(factory.Static{OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
				root.SetArgs([]string{"cluster", "status", "gpu-east", "-o", format})
				if err := root.Execute(); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(out.String(), "Unavailable") || !strings.Contains(out.String(), tc.reason) || strings.Contains(out.String(), "PRIVATE-") {
					t.Fatalf("%s unavailable semantics: %s", format, &out)
				}
				if len(client.Actions()) != 1 || client.Actions()[0].GetVerb() != "get" || client.Actions()[0].GetResource().Resource != "workloadclusters" || client.Actions()[0].GetNamespace() != "" {
					t.Fatalf("named unavailable wire: %v", client.Actions())
				}
			})
		}
	}
}

// Literal outcomes catch incorrectly trusting a stale/invalid Ready condition
// or silently using a prefix of a contradictory or oversized condition group.
func TestClusterStatusLiteralStatesAllFormats(t *testing.T) {
	tests := []struct {
		name                                            string
		mutate                                          func(*ome.WorkloadCluster)
		ready, freshness, condition, source, connection string
	}{
		{"healthy", func(*ome.WorkloadCluster) {}, "True", "Current", "Reported", "KubeConfigSecret", "ReportedReady"},
		{"disconnected", func(w *ome.WorkloadCluster) {
			w.Status.Conditions[0].Status = "False"
			w.Status.Conditions[0].Reason = "Disconnected"
		}, "False", "Current", "Reported", "KubeConfigSecret", "ReportedNotReady"},
		{"unknown", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].Status = "Unknown" }, "Unknown", "Current", "Reported", "KubeConfigSecret", "Unknown"},
		{"stale", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].ObservedGeneration = 2 }, "True", "Stale", "Reported", "KubeConfigSecret", "Unknown"},
		{"future", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].ObservedGeneration = 4 }, "True", "Invalid", "Reported", "KubeConfigSecret", "Unknown"},
		{"unobserved", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].ObservedGeneration = 0 }, "True", "Unobserved", "Reported", "KubeConfigSecret", "Unknown"},
		{"zero-object", func(w *ome.WorkloadCluster) { w.Generation = 0; w.Status.Conditions[0].ObservedGeneration = 0 }, "True", "Invalid", "Reported", "KubeConfigSecret", "Unknown"},
		{"negative", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].ObservedGeneration = -1 }, "True", "Invalid", "Reported", "KubeConfigSecret", "Unknown"},
		{"missing", func(w *ome.WorkloadCluster) { w.Status.Conditions = nil }, "Unknown", "Unobserved", "Missing", "KubeConfigSecret", "Unknown"},
		{"other-only", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].Type = "HostileCondition" }, "Unknown", "Unobserved", "Missing", "KubeConfigSecret", "Unknown"},
		{"profile-declared", func(w *ome.WorkloadCluster) {
			w.Spec.ClusterSource = ome.ClusterConnectionSource{ClusterProfileRef: &ome.ClusterProfileRef{Name: "east-profile"}}
			w.Status.Conditions = nil
		}, "Unknown", "Unobserved", "Missing", "ClusterProfile", "Unknown"},
		{"no-source", func(w *ome.WorkloadCluster) { w.Spec.ClusterSource = ome.ClusterConnectionSource{} }, "True", "Current", "Reported", "Invalid", "Unknown"},
		{"two-sources", func(w *ome.WorkloadCluster) {
			w.Spec.ClusterSource.ClusterProfileRef = &ome.ClusterProfileRef{Name: "east"}
		}, "True", "Current", "Reported", "Invalid", "Unknown"},
		{"invalid-profile", func(w *ome.WorkloadCluster) {
			w.Spec.ClusterSource = ome.ClusterConnectionSource{ClusterProfileRef: &ome.ClusterProfileRef{Name: "SECRET-PROFILE\n"}}
		}, "True", "Current", "Reported", "Invalid", "Unknown"},
		{"contradiction", func(w *ome.WorkloadCluster) {
			c := w.Status.Conditions[0]
			c.Status = "False"
			w.Status.Conditions = append(w.Status.Conditions, c)
		}, "Unknown", "Invalid", "Malformed", "KubeConfigSecret", "Unknown"},
		{"duplicate", func(w *ome.WorkloadCluster) {
			w.Status.Conditions = append(w.Status.Conditions, w.Status.Conditions[0])
		}, "Unknown", "Invalid", "Malformed", "KubeConfigSecret", "Unknown"},
		{"invalid-status", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].Status = "SECRET-STATUS" }, "Unknown", "Invalid", "Malformed", "KubeConfigSecret", "Unknown"},
		{"invalid-transition", func(w *ome.WorkloadCluster) {
			w.Status.Conditions[0].LastTransitionTime = metav1.NewTime(time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		}, "True", "Current", "Reported", "KubeConfigSecret", "Unknown"},
		{"missing-transition", func(w *ome.WorkloadCluster) { w.Status.Conditions[0].LastTransitionTime = metav1.Time{} }, "True", "Current", "Reported", "KubeConfigSecret", "Unknown"},
		{"late-contradiction", func(w *ome.WorkloadCluster) {
			for i := 0; i < 32; i++ {
				c := w.Status.Conditions[0]
				c.Type = fmt.Sprintf("Other%d", i)
				w.Status.Conditions = append(w.Status.Conditions, c)
			}
			c := w.Status.Conditions[0]
			c.Status = "False"
			w.Status.Conditions = append(w.Status.Conditions, c)
		}, "Unknown", "Invalid", "Malformed", "KubeConfigSecret", "Unknown"},
		{"oversized", func(w *ome.WorkloadCluster) {
			for len(w.Status.Conditions) <= 128 {
				w.Status.Conditions = append(w.Status.Conditions, w.Status.Conditions[0])
			}
		}, "Unknown", "Unobserved", "Truncated", "KubeConfigSecret", "Unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, format := range []string{"table", "wide", "json", "yaml"} {
				t.Run(format, func(t *testing.T) {
					w := syntheticCluster()
					tc.mutate(w)
					client := fake.NewSimpleClientset(w)
					var out bytes.Buffer
					root := NewRootCmdWithFactory(factory.Static{OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
					root.SetArgs([]string{"cluster", "status", "gpu-east", "-o", format})
					if err := root.Execute(); err != nil {
						t.Fatal(err)
					}
					if strings.Contains(out.String(), "SECRET-") || strings.Contains(out.String(), "HostileCondition") {
						t.Fatalf("private data leaked: %s", &out)
					}
					if format == "table" {
						for _, want := range []string{tc.ready, tc.freshness, tc.condition, tc.source} {
							if !strings.Contains(out.String(), want) {
								t.Fatalf("missing %s: %s", want, &out)
							}
						}
						return
					}
					if format == "wide" {
						if !strings.Contains(out.String(), tc.connection) {
							t.Fatalf("missing connection: %s", &out)
						}
						return
					}
					data := out.Bytes()
					if format == "yaml" {
						var err error
						data, err = yaml.YAMLToJSON(data)
						if err != nil {
							t.Fatal(err)
						}
					}
					var got struct {
						Clusters []struct{ ReportedReady, Freshness, ConditionState, DeclaredConnectionKind, ConnectionState, ProfileResolution string }
					}
					if err := json.Unmarshal(data, &got); err != nil {
						t.Fatal(err)
					}
					if len(got.Clusters) != 1 {
						t.Fatalf("row count: %s", data)
					}
					r := got.Clusters[0]
					if r.ReportedReady != tc.ready || r.Freshness != tc.freshness || r.ConditionState != tc.condition || r.DeclaredConnectionKind != tc.source || r.ConnectionState != tc.connection || r.ProfileResolution != "NotAttempted" {
						t.Fatalf("literal outcome mismatch: %+v", r)
					}
				})
			}
		})
	}
}
