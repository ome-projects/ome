package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	clienttesting "k8s.io/client-go/testing"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	fake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

type guardedFactory struct {
	factory.Static
	calls int
	err   error
}

func (f *guardedFactory) OMEClient() (versioned.Interface, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.Static.OMEClient()
}
func (f *guardedFactory) Namespace() (string, bool, error) {
	panic("cluster-scoped diagnostics must not resolve namespace")
}
func fixedClock() r.Clock {
	return r.ClockFunc(func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) })
}

func TestValidationHelpAndCancellationBeforeFactory(t *testing.T) {
	for _, args := range [][]string{{"status", "Bad\nPRIVATE"}, {"status", "gpu", "-o", "PRIVATE"}, {"status", "a", "b"}, {"status", "--help"}} {
		f := &guardedFactory{}
		var out bytes.Buffer
		cmd := newCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &out}, fixedClock())
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		cmd.SetArgs(args)
		err := cmd.Execute()
		if args[len(args)-1] == "--help" {
			if err != nil {
				t.Fatal(err)
			}
			for _, literal := range []string{"observational alpha", "No Secret", "not resolved", "current context", "wide"} {
				if !strings.Contains(out.String(), literal) {
					t.Fatalf("help missing %q: %s", literal, &out)
				}
			}
		} else if err == nil {
			t.Fatalf("invalid input admitted: %v", args)
		}
		if f.calls != 0 {
			t.Fatalf("factory called before validation/help: %v", args)
		}
		if err != nil && strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("private argument echoed: %v", err)
		}
	}
	f := &guardedFactory{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := newCmd(f, genericiooptions.IOStreams{Out: io.Discard, ErrOut: io.Discard}, fixedClock())
	cmd.SetArgs([]string{"status"})
	if err := cmd.ExecuteContext(ctx); !errors.Is(err, context.Canceled) || f.calls != 0 {
		t.Fatalf("cancel before factory: %v, %d", err, f.calls)
	}
	f.err = errors.New("PRIVATE-FACTORY-TEXT")
	cmd = newCmd(f, genericiooptions.IOStreams{Out: io.Discard, ErrOut: io.Discard}, fixedClock())
	cmd.SetArgs([]string{"status"})
	if err := cmd.Execute(); err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("factory error privacy: %v", err)
	}
}

func TestLiteralSyntheticNamedAndEmptyListReports(t *testing.T) {
	fixture, err := os.ReadFile("testdata/synthetic_workloadcluster.json")
	if err != nil {
		t.Fatal(err)
	}
	w := &ome.WorkloadCluster{}
	if err := json.Unmarshal(fixture, w); err != nil {
		t.Fatal(err)
	}
	const want = `{"apiVersion":"cli.ome.io/v1alpha1","kind":"ClusterStatusReport","collectedAt":"2026-01-01T00:00:00Z","observation":"Complete","observedPages":1,"returnedSources":1,"admittedSources":1,"sourceLimit":64,"pageLimit":2,"pageSize":32,"conditionScanLimit":128,"conditionOutputLimit":32,"sourcesTruncated":false,"clusters":[{"name":"gpu","generation":2,"declaredConnectionKind":"ClusterProfile","sourceState":"Reported","declaredProfile":"profile","profileResolution":"NotAttempted","evidence":"Reported","reportedReady":"False","conditionState":"Reported","freshness":"Stale","connectionState":"Unknown","totalConditions":1,"scannedConditions":1,"retainedConditions":1,"conditionsTruncated":false,"conditions":[{"type":"Ready","status":"False","observedGeneration":1,"freshness":"Stale","reason":"Other","transitionTime":"2020-01-02T03:04:05Z","transitionState":"Reported"}]}]}`
	client := fake.NewSimpleClientset(w)
	var out bytes.Buffer
	cmd := newCmd(&guardedFactory{Static: factory.Static{OME: client}}, genericiooptions.IOStreams{Out: &out, ErrOut: &out}, fixedClock())
	cmd.SetArgs([]string{"status", "gpu", "-o", "json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	var gotValue, wantValue any
	if err := json.Unmarshal(out.Bytes(), &gotValue); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatal(err)
	}
	gotBytes, _ := json.Marshal(gotValue)
	wantBytes, _ := json.Marshal(wantValue)
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("independent literal report differs:\ngot %s\nwant %s", gotBytes, wantBytes)
	}
	a := client.Actions()
	if len(a) != 1 || a[0].GetVerb() != "get" || a[0].(clienttesting.GetAction).GetName() != "gpu" || a[0].GetResource().Resource != "workloadclusters" || a[0].GetNamespace() != "" {
		t.Fatalf("named GET wire: %v", a)
	}
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		client = fake.NewSimpleClientset()
		out.Reset()
		cmd = newCmd(&guardedFactory{Static: factory.Static{OME: client}}, genericiooptions.IOStreams{Out: &out, ErrOut: &out}, fixedClock())
		cmd.SetArgs([]string{"status", "-o", format})
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "Empty") {
			t.Fatalf("empty output mislabeled: %s", &out)
		}
		a = client.Actions()
		if len(a) != 1 || a[0].GetVerb() != "list" || a[0].GetResource().Resource != "workloadclusters" || a[0].GetNamespace() != "" || a[0].(clienttesting.ListActionImpl).ListOptions.Limit != 32 {
			t.Fatalf("list-only wire: %v", a)
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func TestWriteErrorsAndReadCancellation(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		cmd := newCmd(factory.Static{OME: fake.NewSimpleClientset()}, genericiooptions.IOStreams{Out: failingWriter{}, ErrOut: io.Discard}, fixedClock())
		cmd.SetArgs([]string{"status", "-o", format})
		if err := cmd.Execute(); err == nil {
			t.Fatalf("%s swallowed writer error", format)
		}
	}
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	client.PrependReactor("list", "workloadclusters", func(clienttesting.Action) (bool, runtime.Object, error) {
		cancel()
		return true, &ome.WorkloadClusterList{}, nil
	})
	cmd := newCmd(factory.Static{OME: client}, genericiooptions.IOStreams{Out: io.Discard, ErrOut: io.Discard}, fixedClock())
	cmd.SetArgs([]string{"status"})
	if err := cmd.ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation swallowed: %v", err)
	}
	_ = NewCmd(factory.Static{}, genericiooptions.IOStreams{})
}
