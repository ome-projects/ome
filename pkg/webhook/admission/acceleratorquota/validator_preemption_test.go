package acceleratorquota

import (
	"context"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPreemptionAdmission(t *testing.T) {
	cases := []struct {
		name    string
		policy  *v1beta1.AcceleratorQuotaPreemption
		allowed bool
		message string
	}{
		{name: "omitted", allowed: true},
		{name: "empty", policy: &v1beta1.AcceleratorQuotaPreemption{}, allowed: true},
		{name: "reclaim", policy: &v1beta1.AcceleratorQuotaPreemption{ReclaimWithinCohort: "Any"}, allowed: true},
		{name: "invalid policy", policy: &v1beta1.AcceleratorQuotaPreemption{WithinClusterQueue: "Any"}, message: "preemption.withinClusterQueue"},
		{name: "borrowing without reclamation", policy: &v1beta1.AcceleratorQuotaPreemption{
			BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{Policy: "LowerPriority"},
		}, message: "requires reclaimWithinCohort Any or LowerPriority"},
	}
	for _, tc := range cases {
		for _, op := range []admissionv1.Operation{admissionv1.Create, admissionv1.Update} {
			t.Run(tc.name+"/"+string(op), func(t *testing.T) {
				queue := leaf("team-a", rootName, budget("64"))
				v := newValidator(t, cohort(rootName, "", budget("128")), queue)
				incoming := queue.DeepCopy()
				incoming.Spec.Preemption = tc.policy
				resp := v.Handle(context.Background(), request(t, op, incoming))
				if resp.Allowed != tc.allowed || !strings.Contains(responseMessage(resp), tc.message) {
					t.Fatalf("Allowed=%v, message=%q; want Allowed=%v containing %q", resp.Allowed, responseMessage(resp), tc.allowed, tc.message)
				}
			})
		}
	}
}

func TestPreemptionAdmissionAllowsRepair(t *testing.T) {
	queue := leaf("team-a", rootName, budget("64"))
	queue.Spec.Preemption = &v1beta1.AcceleratorQuotaPreemption{WithinClusterQueue: "Any"}
	v := newValidator(t, cohort(rootName, "", budget("128")), queue)
	incoming := queue.DeepCopy()
	incoming.Spec.Preemption = nil
	resp := v.Handle(context.Background(), request(t, admissionv1.Update, incoming))
	if !resp.Allowed {
		t.Fatalf("removing invalid preemption was denied: %s", responseMessage(resp))
	}
}

func TestPreemptionAdmissionRejectsCohort(t *testing.T) {
	for _, name := range []string{rootName, "org"} {
		t.Run(name, func(t *testing.T) {
			root := cohort(rootName, "", budget("128"))
			org := cohort("org", rootName, budget("64"))
			v := newValidator(t, root, org)
			incoming := org.DeepCopy()
			if name == rootName {
				incoming = root.DeepCopy()
			}
			incoming.Spec.Preemption = &v1beta1.AcceleratorQuotaPreemption{}
			resp := v.Handle(context.Background(), request(t, admissionv1.Update, incoming))
			if resp.Allowed || !strings.Contains(responseMessage(resp), "only a non-root ClusterQueue node may set preemption") {
				t.Fatalf("Allowed=%v, message=%q", resp.Allowed, responseMessage(resp))
			}
		})
	}
}
