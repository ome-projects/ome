package capacity

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// rootReads serves the fleet capacity root from memory and counts how often
// it is read.
type rootReads struct {
	root *v1beta1.AcceleratorQuota
	err  error
	gets int
}

func (m *rootReads) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	m.gets++
	if m.err != nil {
		return m.err
	}
	if m.root == nil || key.Name != m.root.Name {
		return apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "acceleratorquotas"}, key.Name)
	}
	m.root.DeepCopyInto(obj.(*v1beta1.AcceleratorQuota))
	return nil
}

func (m *rootReads) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("the capacity reader lists nothing")
}

// withMember adds a matched registration that reads clean on its own: the
// cluster, its report row and the same demand as the first member.
func withMember(f *fixture, name string) {
	f.clusters = append(f.clusters, v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("registration-" + name)}})
	report := row(f).DeepCopy()
	report.Cluster, report.ClusterUID = name, types.UID("registration-"+name)
	f.root.Status.Capacity[0].PerCluster = append(f.root.Status.Capacity[0].PerCluster, *report)
	f.demands[name] = f.demands["member-a"]
}

func readCounted(f fixture, reads *rootReads) (map[string]Sample, error) {
	return (Reader{Client: reads, RootName: "root", MaxAge: time.Minute}).Read(context.Background(), f.clusters, f.demands, sampleTime.Add(time.Second))
}

func TestReadReadsTheRootOnceForEveryRegistration(t *testing.T) {
	f := inputs()
	withMember(&f, "member-b")
	withMember(&f, "member-c")
	reads := &rootReads{root: f.root}
	got, err := readCounted(f, reads)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || reads.gets != 1 {
		t.Fatalf("Read() = %d samples after %d root reads, want 3 samples from one read", len(got), reads.gets)
	}
}

func TestReadRefusesBeforeItReadsTheRoot(t *testing.T) {
	f := inputs()
	reads := &rootReads{root: f.root}
	got, err := (Reader{Client: reads, RootName: "root"}).Read(context.Background(), f.clusters, f.demands, sampleTime)
	if err == nil || !strings.Contains(err.Error(), "positive maximum age") || got != nil {
		t.Fatalf("Read() without a maximum age = %v, %v; want the configuration refusal and no samples", got, err)
	}
	if reads.gets != 0 {
		t.Fatalf("a misconfigured reader read the root %d times, want none", reads.gets)
	}
}

func TestReadNamesTheRootReadFailure(t *testing.T) {
	f := inputs()
	reads := &rootReads{err: errors.New("the server did not answer")}
	got, err := readCounted(f, reads)
	if err == nil || err.Error() != "read fleet capacity: the server did not answer" || got != nil {
		t.Fatalf("Read() = %v, %v; want the root read failure wrapped and no samples", got, err)
	}
}

func TestReadRefusesTheRootBeforeAnyRegistration(t *testing.T) {
	f := inputs()
	f.root.UID = ""
	f.clusters[0].UID = ""
	got, err := readCounted(f, &rootReads{root: f.root})
	if err == nil || err.Error() != "fleet capacity root is not an identified live root" || got != nil {
		t.Fatalf("Read() = %v, %v; want the root refusal before the registration's", got, err)
	}
}

func TestReadRefusalsNameTheRegistrationTheyStopAt(t *testing.T) {
	for _, tt := range []struct {
		name       string
		mutate     func(*fixture)
		wantExact  string
		wantPrefix string
		wantInner  string
	}{
		{name: "a second registration without a UID", mutate: func(f *fixture) {
			withMember(f, "member-b")
			f.clusters[1].UID = ""
		}, wantExact: `capacity requires an identified live registration for cluster "member-b"`},
		{name: "a second registration without a report", mutate: func(f *fixture) {
			withMember(f, "member-b")
			f.root.Status.Capacity[0].PerCluster = f.root.Status.Capacity[0].PerCluster[:1]
		}, wantPrefix: `cluster "member-b" capacity unknown: `, wantInner: "identified available report is missing"},
		{name: "a repeated registration", mutate: func(f *fixture) {
			f.clusters = append(f.clusters, f.clusters[0])
		}, wantExact: `duplicate matched registration "member-a"`},
		{name: "a repeated second registration names itself", mutate: func(f *fixture) {
			withMember(f, "member-b")
			f.clusters = append(f.clusters, f.clusters[1])
		}, wantExact: `duplicate matched registration "member-b"`},
		{name: "a repeated registration without a UID is unidentified first", mutate: func(f *fixture) {
			f.clusters = append(f.clusters, f.clusters[0])
			f.clusters[1].UID = ""
		}, wantExact: `capacity requires an identified live registration for cluster "member-a"`},
		{name: "the first registration that fails names the refusal", mutate: func(f *fixture) {
			withMember(f, "member-b")
			withMember(f, "member-c")
			f.root.Status.Capacity[0].PerCluster = f.root.Status.Capacity[0].PerCluster[:1]
			f.clusters[2].UID = ""
		}, wantPrefix: `cluster "member-b" capacity unknown: `, wantInner: "identified available report is missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := inputs()
			tt.mutate(&f)
			got, err := readCounted(f, &rootReads{root: f.root})
			if err == nil || got != nil {
				t.Fatalf("Read() = %v, %v; want a refusal and no samples", got, err)
			}
			text := err.Error()
			if tt.wantExact != "" && text != tt.wantExact {
				t.Fatalf("Read() error = %q, want %q", text, tt.wantExact)
			}
			if tt.wantPrefix != "" && (!strings.HasPrefix(text, tt.wantPrefix) || !strings.Contains(text, tt.wantInner)) {
				t.Fatalf("Read() error = %q, want it to open with %q and name %q", text, tt.wantPrefix, tt.wantInner)
			}
		})
	}
}
