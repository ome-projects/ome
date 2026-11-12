package inferenceservice

import (
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/crdskew"
)

func drainRecordedEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// 1 service on a cluster whose InferenceService CRD lacks a status field of
// this release: the service's condition and an event name the CRD and the
// field; the same picture read again writes nothing more; once the CRD is
// current the condition turns True with one event.
func TestSetCRDSchemaConditionNamesTheLaggingCRDOnTheService(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"}}
	recorder := record.NewFakeRecorder(8)
	report := &crdskew.Report{}
	report.Set(crdskew.Finding{CRD: "inferenceservices.example", MissingFields: []string{"status.newField"}})

	setCRDSchemaCondition(isvc, report, recorder)
	cond := isvc.Status.GetCondition(v1beta1.CRDSchemaCurrent)
	if cond == nil || cond.Status != v1.ConditionFalse || cond.Reason != crdskew.ReasonLagging {
		t.Fatalf("condition = %+v, want False %s", cond, crdskew.ReasonLagging)
	}
	for _, part := range []string{"inferenceservices.example", "status.newField"} {
		if !strings.Contains(cond.Message, part) {
			t.Fatalf("condition message %q does not name %q", cond.Message, part)
		}
	}
	events := drainRecordedEvents(recorder)
	if len(events) != 1 || !strings.Contains(events[0], crdskew.ReasonLagging) || !strings.Contains(events[0], "status.newField") {
		t.Fatalf("events = %q, want one %s naming the field", events, crdskew.ReasonLagging)
	}

	setCRDSchemaCondition(isvc, report, recorder)
	if events := drainRecordedEvents(recorder); len(events) != 0 {
		t.Fatalf("the same picture written again sent %q", events)
	}

	report.Set(crdskew.Finding{CRD: "inferenceservices.example"})
	setCRDSchemaCondition(isvc, report, recorder)
	cond = isvc.Status.GetCondition(v1beta1.CRDSchemaCurrent)
	if cond == nil || cond.Status != v1.ConditionTrue || cond.Reason != crdskew.ReasonCurrent {
		t.Fatalf("condition = %+v, want True %s", cond, crdskew.ReasonCurrent)
	}
	events = drainRecordedEvents(recorder)
	if len(events) != 1 || !strings.Contains(events[0], crdskew.ReasonCurrent) {
		t.Fatalf("events = %q, want one %s", events, crdskew.ReasonCurrent)
	}
	setCRDSchemaCondition(isvc, report, recorder)
	if events := drainRecordedEvents(recorder); len(events) != 0 {
		t.Fatalf("a current picture written again sent %q", events)
	}
}

// The case that must hold: a service on a cluster whose CRDs match
// the manager's release gets no condition and no event, whether nothing
// compared the CRDs or the comparison found them current.
func TestSetCRDSchemaConditionWritesNothingWhenTheCRDsAreCurrent(t *testing.T) {
	recorder := record.NewFakeRecorder(8)
	current := &crdskew.Report{}
	current.Set(crdskew.Finding{CRD: "inferenceservices.example"})
	for name, report := range map[string]*crdskew.Report{"nothing compared": nil, "current": current} {
		isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "team-a"}}
		setCRDSchemaCondition(isvc, report, recorder)
		if cond := isvc.Status.GetCondition(v1beta1.CRDSchemaCurrent); cond != nil {
			t.Fatalf("%s: condition = %+v, want none", name, cond)
		}
		if events := drainRecordedEvents(recorder); len(events) != 0 {
			t.Fatalf("%s: events = %q, want none", name, events)
		}
	}
}
