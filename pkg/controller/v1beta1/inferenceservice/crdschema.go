package inferenceservice

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	knapis "knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/crdskew"
)

// reportCRDSchema writes the manager's picture of the installed CRDs on the
// service, so an operator who looks at the service learns of a CRD that
// drops this release's fields.
func (p *servicePass) reportCRDSchema(context.Context) (bool, error) {
	setCRDSchemaCondition(p.isvc, p.reconciler.CRDSchema, p.reconciler.Recorder)
	return true, nil
}

// setCRDSchemaCondition sets CRDSchemaCurrent False, naming the CRDs and
// the fields, while any CRD lags this release, and True once none does. A
// service on a cluster whose CRDs never lagged gets no condition. An event
// goes with every change of the condition's text.
func setCRDSchemaCondition(isvc *v1beta1.InferenceService, report *crdskew.Report, recorder record.EventRecorder) {
	held := isvc.Status.GetCondition(v1beta1.CRDSchemaCurrent)
	message := report.Message()
	if message == "" {
		if held == nil || held.Status == v1.ConditionTrue {
			return
		}
		isvc.Status.SetCondition(v1beta1.CRDSchemaCurrent, &knapis.Condition{
			Type:   v1beta1.CRDSchemaCurrent,
			Status: v1.ConditionTrue,
			Reason: crdskew.ReasonCurrent,
		})
		if recorder != nil {
			recorder.Event(isvc, v1.EventTypeNormal, crdskew.ReasonCurrent, "Every CRD of the manager's API group carries the fields this release of the manager knows.")
		}
		return
	}
	if held != nil && held.Status == v1.ConditionFalse && held.Message == message {
		return
	}
	isvc.Status.SetCondition(v1beta1.CRDSchemaCurrent, &knapis.Condition{
		Type:    v1beta1.CRDSchemaCurrent,
		Status:  v1.ConditionFalse,
		Reason:  crdskew.ReasonLagging,
		Message: message,
	})
	if recorder != nil {
		recorder.Event(isvc, v1.EventTypeWarning, crdskew.ReasonLagging, message)
	}
}
