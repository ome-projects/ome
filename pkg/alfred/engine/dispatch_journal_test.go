package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var dispatchTestTime = time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)

func validDispatchEntry() dispatchEntry {
	return dispatchEntry{
		UUID:              "822f19d4-0c7e-4c0b-93b8-b10f628d645f",
		Workload:          types.NamespacedName{Namespace: "team-a", Name: "text-generation"},
		WorkloadUID:       "workload-uid",
		IRName:            "text-generation-engine",
		IRUID:             "ir-uid",
		Component:         v1beta1.EngineComponent,
		Instance:          0,
		FromNode:          "worker-a",
		Targets:           []string{"worker-b", "worker-c"},
		Payload:           `{"id":"822f19d4-0c7e-4c0b-93b8-b10f628d645f"}`,
		SourceFingerprint: strings.Repeat("a", 64),
		CreatedAt:         dispatchTestTime,
		Phase:             dispatchPrepared,
	}
}

func validDispatchJournal() *dispatchJournal {
	return &dispatchJournal{Version: "v1", Entries: []dispatchEntry{validDispatchEntry()}}
}

func dispatchTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func dispatchStateConfigMap(t *testing.T, namespace string, journal *dispatchJournal) *corev1.ConfigMap {
	t.Helper()
	raw, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: dispatchStateName},
		Data:       map[string]string{dispatchStateKey: string(raw)},
	}
}

func mutateDispatchDocument(t *testing.T, mutate func(map[string]any)) string {
	t.Helper()
	raw, err := json.Marshal(validDispatchJournal())
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	mutate(document)
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func loadDispatchDocument(t *testing.T, raw string) error {
	t.Helper()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "alfred", Name: dispatchStateName},
		Data:       map[string]string{dispatchStateKey: raw},
	}
	reader := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).WithObjects(cm).Build()
	_, _, err := loadDispatchJournal(context.Background(), reader, "alfred")
	return err
}

func TestLoadDispatchJournalAcceptsValidAndEmptyStates(t *testing.T) {
	tests := []struct {
		name    string
		journal *dispatchJournal
	}{
		{name: "one prepared intent", journal: validDispatchJournal()},
		{name: "empty manifest", journal: &dispatchJournal{Version: "v1", Entries: []dispatchEntry{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := dispatchStateConfigMap(t, "alfred", tt.journal)
			cm.ResourceVersion = "17"
			cm.Data["unrelated"] = "preserved"
			reader := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).WithObjects(cm).Build()

			gotCM, gotJournal, err := loadDispatchJournal(context.Background(), reader, "alfred")
			if err != nil {
				t.Fatalf("load valid journal: %v", err)
			}
			if gotCM.Namespace != "alfred" || gotCM.Name != dispatchStateName || gotCM.ResourceVersion != "17" {
				t.Fatalf("loaded ConfigMap identity = %s/%s rv=%q", gotCM.Namespace, gotCM.Name, gotCM.ResourceVersion)
			}
			if gotCM.Data["unrelated"] != "preserved" {
				t.Fatal("load discarded an unrelated ConfigMap key")
			}
			if !reflect.DeepEqual(gotJournal, tt.journal) {
				t.Fatalf("journal mismatch\ngot:  %#v\nwant: %#v", gotJournal, tt.journal)
			}
		})
	}
}

func TestLoadDispatchJournalRequiresPrecreatedNamedKey(t *testing.T) {
	t.Run("missing ConfigMap", func(t *testing.T) {
		reader := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).Build()
		if _, _, err := loadDispatchJournal(context.Background(), reader, "alfred"); !apierrors.IsNotFound(err) {
			t.Fatalf("missing ConfigMap error = %v, want NotFound", err)
		}
	})

	t.Run("missing state key", func(t *testing.T) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "alfred", Name: dispatchStateName}}
		reader := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).WithObjects(cm).Build()
		if _, _, err := loadDispatchJournal(context.Background(), reader, "alfred"); err == nil {
			t.Fatal("load accepted a ConfigMap without the state key")
		}
	})
}

func TestLoadDispatchJournalRejectsNonStrictDocuments(t *testing.T) {
	validRaw, err := json.Marshal(validDispatchJournal())
	if err != nil {
		t.Fatal(err)
	}
	duplicateTopLevelEntries := strings.TrimSuffix(string(validRaw), "}") + `,"entries":[]}`
	caseAliasTopLevelEntries := strings.TrimSuffix(string(validRaw), "}") + `,"Entries":[]}`
	unicodeAliasTopLevelEntries := strings.TrimSuffix(string(validRaw), "}") + `,"\u0065\u006e\u0074\u0072\u0069\u0065\u017f":[]}`
	duplicateEntryIdentity := strings.Replace(
		string(validRaw),
		`"workloadUID":"workload-uid"`,
		`"workloadUID":"workload-uid","workloadUID":"replacement-uid"`,
		1,
	)
	caseAliasEntryUUID := strings.Replace(
		string(validRaw),
		`"uuid":"822f19d4-0c7e-4c0b-93b8-b10f628d645f"`,
		`"uuid":"822f19d4-0c7e-4c0b-93b8-b10f628d645f","UUID":"22ad74d0-899d-45e3-9015-53351fa8edc0"`,
		1,
	)
	duplicateEntryPhase := strings.Replace(
		string(validRaw),
		`"phase":"prepared"`,
		`"phase":"prepared","phase":"stalled"`,
		1,
	)
	caseAliasEntryPhase := strings.Replace(
		string(validRaw),
		`"phase":"prepared"`,
		`"phase":"prepared","Phase":"stalled"`,
		1,
	)
	duplicate := mutateDispatchDocument(t, func(document map[string]any) {
		entries := document["entries"].([]any)
		document["entries"] = append(entries, entries[0])
	})
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "malformed", raw: `{"version":`},
		{name: "unknown top-level field", raw: mutateDispatchDocument(t, func(document map[string]any) { document["future"] = true })},
		{name: "unknown entry field", raw: mutateDispatchDocument(t, func(document map[string]any) {
			document["entries"].([]any)[0].(map[string]any)["future"] = true
		})},
		{name: "trailing document", raw: string(validRaw) + `{}`},
		{name: "null document", raw: "null"},
		{name: "missing version", raw: mutateDispatchDocument(t, func(document map[string]any) { delete(document, "version") })},
		{name: "wrong version", raw: mutateDispatchDocument(t, func(document map[string]any) { document["version"] = "v2" })},
		{name: "missing entries", raw: mutateDispatchDocument(t, func(document map[string]any) { delete(document, "entries") })},
		{name: "null entries", raw: mutateDispatchDocument(t, func(document map[string]any) { document["entries"] = nil })},
		{name: "missing instance", raw: mutateDispatchDocument(t, func(document map[string]any) {
			delete(document["entries"].([]any)[0].(map[string]any), "instance")
		})},
		{name: "null instance", raw: mutateDispatchDocument(t, func(document map[string]any) {
			document["entries"].([]any)[0].(map[string]any)["instance"] = nil
		})},
		{name: "duplicate top-level entries member", raw: duplicateTopLevelEntries},
		{name: "case alias top-level entries member", raw: caseAliasTopLevelEntries},
		{name: "Unicode case alias cannot erase unresolved entries", raw: unicodeAliasTopLevelEntries},
		{name: "duplicate entry identity member", raw: duplicateEntryIdentity},
		{name: "case alias entry UUID member", raw: caseAliasEntryUUID},
		{name: "duplicate entry phase member", raw: duplicateEntryPhase},
		{name: "case alias entry phase member", raw: caseAliasEntryPhase},
		{name: "duplicate UUID", raw: duplicate},
		{name: "over state size quota", raw: string(validRaw) + strings.Repeat(" ", 512*1024-len(validRaw)+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := loadDispatchDocument(t, tt.raw); err == nil {
				t.Fatal("load accepted invalid dispatch state")
			}
		})
	}

	t.Run("over entry quota", func(t *testing.T) {
		journal := &dispatchJournal{Version: "v1", Entries: make([]dispatchEntry, 257)}
		for i := range journal.Entries {
			journal.Entries[i] = validDispatchEntry()
			journal.Entries[i].UUID = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		}
		raw, err := json.Marshal(journal)
		if err != nil {
			t.Fatal(err)
		}
		if err := loadDispatchDocument(t, string(raw)); err == nil {
			t.Fatal("load accepted more than 256 entries")
		}
	})
}

func TestLoadDispatchJournalRejectsInvalidEntryFields(t *testing.T) {
	before := dispatchTestTime.Add(-time.Nanosecond)
	zero := time.Time{}
	completed := dispatchTestTime.Add(time.Minute)
	tests := []struct {
		name   string
		mutate func(*dispatchJournal)
	}{
		{name: "non-RFC4122 UUID", mutate: func(j *dispatchJournal) { j.Entries[0].UUID = "request-1" }},
		{name: "missing workload namespace", mutate: func(j *dispatchJournal) { j.Entries[0].Workload.Namespace = "" }},
		{name: "invalid workload namespace", mutate: func(j *dispatchJournal) { j.Entries[0].Workload.Namespace = "Team_A" }},
		{name: "missing workload name", mutate: func(j *dispatchJournal) { j.Entries[0].Workload.Name = "" }},
		{name: "invalid workload name", mutate: func(j *dispatchJournal) { j.Entries[0].Workload.Name = "UPPER" }},
		{name: "missing workload UID", mutate: func(j *dispatchJournal) { j.Entries[0].WorkloadUID = "" }},
		{name: "missing IR name", mutate: func(j *dispatchJournal) { j.Entries[0].IRName = "" }},
		{name: "invalid IR name", mutate: func(j *dispatchJournal) { j.Entries[0].IRName = "bad/name" }},
		{name: "missing IR UID", mutate: func(j *dispatchJournal) { j.Entries[0].IRUID = "" }},
		{name: "invalid component", mutate: func(j *dispatchJournal) { j.Entries[0].Component = "sidecar" }},
		{name: "negative instance", mutate: func(j *dispatchJournal) { j.Entries[0].Instance = -1 }},
		{name: "missing source node", mutate: func(j *dispatchJournal) { j.Entries[0].FromNode = "" }},
		{name: "missing source fingerprint", mutate: func(j *dispatchJournal) { j.Entries[0].SourceFingerprint = "" }},
		{name: "empty payload", mutate: func(j *dispatchJournal) { j.Entries[0].Payload = "" }},
		{name: "malformed payload", mutate: func(j *dispatchJournal) { j.Entries[0].Payload = `{"id":` }},
		{name: "payload over 4096 bytes", mutate: func(j *dispatchJournal) { j.Entries[0].Payload = `"` + strings.Repeat("x", 4095) + `"` }},
		{name: "zero creation time", mutate: func(j *dispatchJournal) { j.Entries[0].CreatedAt = zero }},
		{name: "last attempt predates creation", mutate: func(j *dispatchJournal) { j.Entries[0].LastAttempt = &before }},
		{name: "acknowledgement predates creation", mutate: func(j *dispatchJournal) { j.Entries[0].AcknowledgedAt = &before }},
		{name: "unknown phase", mutate: func(j *dispatchJournal) { j.Entries[0].Phase = "cancelled" }},
		{name: "completed missing completion time", mutate: func(j *dispatchJournal) { j.Entries[0].Phase = dispatchCompleted }},
		{name: "failed missing completion time", mutate: func(j *dispatchJournal) { j.Entries[0].Phase = dispatchFailed }},
		{name: "completion predates creation", mutate: func(j *dispatchJournal) {
			j.Entries[0].Phase = dispatchCompleted
			j.Entries[0].CompletedAt = &before
		}},
		{name: "unresolved carries completion time", mutate: func(j *dispatchJournal) { j.Entries[0].CompletedAt = &completed }},
		{name: "zero backoff time", mutate: func(j *dispatchJournal) { j.BackoffUntil = &zero }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			journal := validDispatchJournal()
			tt.mutate(journal)
			raw, err := json.Marshal(journal)
			if err != nil {
				t.Fatal(err)
			}
			if err := loadDispatchDocument(t, string(raw)); err == nil {
				t.Fatal("load accepted an invalid entry")
			}
		})
	}
}

func TestLoadDispatchJournalErrorsDoNotExposePayload(t *testing.T) {
	journal := validDispatchJournal()
	journal.Entries[0].Component = "invalid"
	journal.Entries[0].Payload = `{"credential":"TOP-SECRET-CONTENT"}`
	raw, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	err = loadDispatchDocument(t, string(raw))
	if err == nil {
		t.Fatal("load accepted invalid component")
	}
	if strings.Contains(err.Error(), "TOP-SECRET-CONTENT") {
		t.Fatalf("validation error exposed payload: %v", err)
	}
}

func TestDispatchEntryTerminalRequiresResolvedPhaseAndValidCompletion(t *testing.T) {
	created := dispatchTestTime
	equal := created
	after := created.Add(time.Minute)
	before := created.Add(-time.Nanosecond)
	tests := []struct {
		name      string
		phase     string
		completed *time.Time
		want      bool
	}{
		{name: "completed", phase: dispatchCompleted, completed: &equal, want: true},
		{name: "failed", phase: dispatchFailed, completed: &after, want: true},
		{name: "completed without timestamp", phase: dispatchCompleted},
		{name: "failed before creation", phase: dispatchFailed, completed: &before},
		{name: "stalled remains unresolved", phase: dispatchStalled, completed: &after},
		{name: "acknowledged remains unresolved", phase: dispatchAcknowledged},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := validDispatchEntry()
			entry.CreatedAt = created
			entry.Phase = tt.phase
			entry.CompletedAt = tt.completed
			if got := entry.terminal(); got != tt.want {
				t.Fatalf("terminal() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestDispatchJournalPruneRetainsUnresolvedAndOneHourHistory(t *testing.T) {
	now := dispatchTestTime.Add(4 * time.Hour)
	atBoundary := now.Add(-time.Hour)
	tooOld := atBoundary.Add(-time.Nanosecond)
	recent := now.Add(-time.Minute)
	backoff := now.Add(5 * time.Minute)
	entries := []dispatchEntry{
		{UUID: "prepared", Phase: dispatchPrepared},
		{UUID: "submitted", Phase: dispatchSubmitted},
		{UUID: "acknowledged", Phase: dispatchAcknowledged},
		{UUID: "stalled", Phase: dispatchStalled},
		{UUID: "completed-boundary", Phase: dispatchCompleted, CreatedAt: dispatchTestTime, CompletedAt: &atBoundary},
		{UUID: "failed-recent", Phase: dispatchFailed, CreatedAt: dispatchTestTime, CompletedAt: &recent},
		{UUID: "completed-old", Phase: dispatchCompleted, CreatedAt: dispatchTestTime, CompletedAt: &tooOld},
	}
	journal := &dispatchJournal{Version: "v1", Entries: entries, BackoffUntil: &backoff}

	journal.prune(now)

	wantUUIDs := []string{"prepared", "submitted", "acknowledged", "stalled", "completed-boundary", "failed-recent"}
	gotUUIDs := make([]string, 0, len(journal.Entries))
	for _, entry := range journal.Entries {
		gotUUIDs = append(gotUUIDs, entry.UUID)
	}
	if !reflect.DeepEqual(gotUUIDs, wantUUIDs) {
		t.Fatalf("retained UUIDs = %v, want %v", gotUUIDs, wantUUIDs)
	}
	if journal.BackoffUntil == nil || !journal.BackoffUntil.Equal(backoff) {
		t.Fatalf("prune changed failure backoff: %v", journal.BackoffUntil)
	}
}

func TestSaveDispatchJournalUpdatesOnlyStateOnDeepCopy(t *testing.T) {
	initial := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "alfred",
			Name:            dispatchStateName,
			UID:             "state-uid",
			ResourceVersion: "7",
			Labels:          map[string]string{"app": "alfred"},
			Annotations:     map[string]string{"owner": "chart"},
		},
		Data:       map[string]string{dispatchStateKey: `{"version":"v1","entries":[]}`, "unrelated": "keep"},
		BinaryData: map[string][]byte{"opaque": {1, 2, 3}},
	}
	cl := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).WithObjects(initial.DeepCopy()).Build()
	callerCM := initial.DeepCopy()
	callerBefore := callerCM.DeepCopy()
	journal := validDispatchJournal()
	journalBefore, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}

	if err := saveDispatchJournal(context.Background(), cl, callerCM, journal); err != nil {
		t.Fatalf("save valid journal: %v", err)
	}
	if !reflect.DeepEqual(callerCM, callerBefore) {
		t.Fatalf("save mutated caller ConfigMap\ngot:  %#v\nwant: %#v", callerCM, callerBefore)
	}
	journalAfter, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if string(journalAfter) != string(journalBefore) {
		t.Fatalf("save mutated caller journal\ngot:  %s\nwant: %s", journalAfter, journalBefore)
	}

	var live corev1.ConfigMap
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(initial), &live); err != nil {
		t.Fatal(err)
	}
	if live.Data["unrelated"] != "keep" || !reflect.DeepEqual(live.BinaryData["opaque"], []byte{1, 2, 3}) {
		t.Fatalf("save discarded unrelated ConfigMap data: data=%v binary=%v", live.Data, live.BinaryData)
	}
	if !reflect.DeepEqual(live.Labels, initial.Labels) || !reflect.DeepEqual(live.Annotations, initial.Annotations) || live.UID != initial.UID {
		t.Fatalf("save changed unrelated metadata: %#v", live.ObjectMeta)
	}
	var stored dispatchJournal
	if err := json.Unmarshal([]byte(live.Data[dispatchStateKey]), &stored); err != nil {
		t.Fatalf("stored state is not JSON: %v", err)
	}
	if !reflect.DeepEqual(&stored, journal) {
		t.Fatalf("stored journal mismatch\ngot:  %#v\nwant: %#v", &stored, journal)
	}
}

func TestSaveDispatchJournalRequiresObservedResourceVersion(t *testing.T) {
	updates := 0
	cl := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				return c.Update(ctx, obj, opts...)
			},
		}).Build()
	cm := dispatchStateConfigMap(t, "alfred", validDispatchJournal())
	if err := saveDispatchJournal(context.Background(), cl, cm, validDispatchJournal()); err == nil {
		t.Fatal("save accepted a ConfigMap without resourceVersion")
	}
	if updates != 0 {
		t.Fatalf("save attempted %d updates without an observed resourceVersion", updates)
	}
}

func TestSaveDispatchJournalConflictDoesNotRetryOrOverwrite(t *testing.T) {
	initial := dispatchStateConfigMap(t, "alfred", &dispatchJournal{Version: "v1", Entries: []dispatchEntry{}})
	initial.ResourceVersion = "7"
	updates := 0
	cl := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).WithObjects(initial.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				return c.Update(ctx, obj, opts...)
			},
		}).Build()
	stale := initial.DeepCopy()
	stale.ResourceVersion = "6"
	staleBefore := stale.DeepCopy()

	err := saveDispatchJournal(context.Background(), cl, stale, validDispatchJournal())
	if !apierrors.IsConflict(err) {
		t.Fatalf("save stale ConfigMap error = %v, want Conflict", err)
	}
	if updates != 1 {
		t.Fatalf("conflicting save attempted %d updates, want exactly one", updates)
	}
	if !reflect.DeepEqual(stale, staleBefore) {
		t.Fatal("conflicting save mutated the caller ConfigMap")
	}
	var live corev1.ConfigMap
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(initial), &live); err != nil {
		t.Fatal(err)
	}
	if live.Data[dispatchStateKey] != initial.Data[dispatchStateKey] || live.ResourceVersion != "7" {
		t.Fatalf("conflicting save changed live state: data=%q rv=%q", live.Data[dispatchStateKey], live.ResourceVersion)
	}
}

func TestSaveDispatchJournalPreservesUnknownUpdateErrors(t *testing.T) {
	storageErr := errors.New("storage transport stopped")
	initial := dispatchStateConfigMap(t, "alfred", &dispatchJournal{Version: "v1", Entries: []dispatchEntry{}})
	initial.ResourceVersion = "7"
	cl := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).WithObjects(initial.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return storageErr
			},
		}).Build()
	caller := initial.DeepCopy()
	callerBefore := caller.DeepCopy()

	err := saveDispatchJournal(context.Background(), cl, caller, validDispatchJournal())
	if !errors.Is(err, storageErr) {
		t.Fatalf("save error = %v, want wrapped storage error", err)
	}
	if !reflect.DeepEqual(caller, callerBefore) {
		t.Fatal("failed save mutated the caller ConfigMap")
	}
}

func TestSaveDispatchJournalNeverCreatesMissingConfigMap(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).Build()
	cm := dispatchStateConfigMap(t, "alfred", validDispatchJournal())
	cm.ResourceVersion = "1"
	err := saveDispatchJournal(context.Background(), cl, cm, validDispatchJournal())
	if !apierrors.IsNotFound(err) {
		t.Fatalf("save missing ConfigMap error = %v, want NotFound", err)
	}
	var live corev1.ConfigMap
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(cm), &live); !apierrors.IsNotFound(err) {
		t.Fatalf("save created missing ConfigMap: %v", err)
	}
}

func TestSaveDispatchJournalRejectsQuotasBeforeUpdate(t *testing.T) {
	initial := dispatchStateConfigMap(t, "alfred", &dispatchJournal{Version: "v1", Entries: []dispatchEntry{}})
	initial.ResourceVersion = "7"
	updates := 0
	cl := fake.NewClientBuilder().WithScheme(dispatchTestScheme(t)).WithObjects(initial.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				return c.Update(ctx, obj, opts...)
			},
		}).Build()

	t.Run("entry count", func(t *testing.T) {
		journal := &dispatchJournal{Version: "v1", Entries: make([]dispatchEntry, 257)}
		if err := saveDispatchJournal(context.Background(), cl, initial, journal); err == nil {
			t.Fatal("save accepted more than 256 entries")
		}
	})

	t.Run("serialized size", func(t *testing.T) {
		journal := &dispatchJournal{Version: "v1", Entries: make([]dispatchEntry, 130)}
		for i := range journal.Entries {
			journal.Entries[i] = validDispatchEntry()
			journal.Entries[i].UUID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("entry-%d", i))).String()
			journal.Entries[i].Payload = `"` + strings.Repeat("x", 4094) + `"`
		}
		if err := saveDispatchJournal(context.Background(), cl, initial, journal); err == nil {
			t.Fatal("save accepted a state document over 512 KiB")
		}
	})

	if updates != 0 {
		t.Fatalf("invalid journals reached storage with %d update calls", updates)
	}
}
