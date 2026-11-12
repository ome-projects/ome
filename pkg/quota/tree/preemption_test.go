package tree

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPreemptionPolicyValues(t *testing.T) {
	fields := []struct {
		name    string
		allowed string
		set     func(*v1beta1.AcceleratorQuotaPreemption, v1beta1.AcceleratorQuotaPreemptionPolicy)
	}{
		{"reclaimWithinCohort", "Never, LowerPriority, Any", func(p *v1beta1.AcceleratorQuotaPreemption, value v1beta1.AcceleratorQuotaPreemptionPolicy) {
			p.ReclaimWithinCohort = value
		}},
		{"withinClusterQueue", "Never, LowerPriority, LowerOrNewerEqualPriority", func(p *v1beta1.AcceleratorQuotaPreemption, value v1beta1.AcceleratorQuotaPreemptionPolicy) {
			p.WithinClusterQueue = value
		}},
		{"borrowWithinCohort.policy", "Never, LowerPriority", func(p *v1beta1.AcceleratorQuotaPreemption, value v1beta1.AcceleratorQuotaPreemptionPolicy) {
			p.ReclaimWithinCohort = v1beta1.AcceleratorQuotaPreemptionAny
			p.BorrowWithinCohort = &v1beta1.AcceleratorQuotaBorrowWithinCohort{Policy: value}
		}},
	}
	values := []string{"", "Never", "LowerPriority", "Any", "LowerOrNewerEqualPriority", "unknown"}
	for _, field := range fields {
		for _, value := range values {
			t.Run(field.name+"/"+value, func(t *testing.T) {
				policy := &v1beta1.AcceleratorQuotaPreemption{}
				field.set(policy, v1beta1.AcceleratorQuotaPreemptionPolicy(value))
				var want Violations
				if value != "" && !containsPolicy(field.allowed, value) {
					want = Violations{{Node: "team-a", Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
						Subject: "preemption." + field.name,
						Message: fmt.Sprintf("preemption.%s is %q; use %s or leave it unset", field.name, value, field.allowed)}}
				}
				assertPreemptionViolations(t, policy, want)
			})
		}
	}
}

func containsPolicy(allowed, policy string) bool {
	for _, choice := range strings.Split(allowed, ", ") {
		if choice == policy {
			return true
		}
	}
	return false
}

func TestBorrowPreemptionRequiresReclamation(t *testing.T) {
	for _, reclaim := range []v1beta1.AcceleratorQuotaPreemptionPolicy{"", "Never", "LowerPriority", "Any"} {
		for _, borrow := range []v1beta1.AcceleratorQuotaPreemptionPolicy{"", "Never", "LowerPriority"} {
			t.Run(string(reclaim)+"/"+string(borrow), func(t *testing.T) {
				policy := &v1beta1.AcceleratorQuotaPreemption{ReclaimWithinCohort: reclaim,
					BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{Policy: borrow}}
				var want Violations
				if borrow == "LowerPriority" && (reclaim == "" || reclaim == "Never") {
					want = Violations{{Node: "team-a", Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
						Subject: "preemption.borrowWithinCohort",
						Message: "preemption.borrowWithinCohort.policy LowerPriority requires reclaimWithinCohort Any or LowerPriority"}}
				}
				assertPreemptionViolations(t, policy, want)
			})
		}
	}
}

func TestPreemptionOmissionAndThreshold(t *testing.T) {
	policies := []*v1beta1.AcceleratorQuotaPreemption{nil, {}, {BorrowWithinCohort: &v1beta1.AcceleratorQuotaBorrowWithinCohort{}}}
	for i, policy := range policies {
		t.Run(fmt.Sprintf("omission-%d", i), func(t *testing.T) { assertPreemptionViolations(t, policy, nil) })
	}
	for _, borrow := range []v1beta1.AcceleratorQuotaPreemptionPolicy{"", "Never", "LowerPriority"} {
		for _, threshold := range []int32{-2147483648, 0, 2147483647} {
			t.Run(fmt.Sprintf("%s/%d", borrow, threshold), func(t *testing.T) {
				assertPreemptionViolations(t, &v1beta1.AcceleratorQuotaPreemption{
					ReclaimWithinCohort: v1beta1.AcceleratorQuotaPreemptionAny,
					BorrowWithinCohort:  &v1beta1.AcceleratorQuotaBorrowWithinCohort{Policy: borrow, MaxPriorityThreshold: ptr.To(threshold)},
				}, nil)
			})
		}
	}
}

func TestPreemptionOnlyOnLeaves(t *testing.T) {
	for _, name := range []string{rootName, "org"} {
		for _, policy := range []*v1beta1.AcceleratorQuotaPreemption{{}, {ReclaimWithinCohort: "Any"}} {
			t.Run(name+"/"+string(policy.ReclaimWithinCohort), func(t *testing.T) {
				quotas := []v1beta1.AcceleratorQuota{cohort(rootName, "", budget("128")), cohort("org", rootName, budget("64"))}
				for i := range quotas {
					if quotas[i].Name == name {
						quotas[i].Spec.Preemption = policy
					}
				}
				_, got := mustBuild(t, quotas, opts())
				want := Violations{{Node: name, Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
					Subject: "preemption", Message: "only a non-root ClusterQueue node may set preemption"}}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("violations (-want +got):\n%s", diff)
				}
			})
		}
	}
}

func TestPreemptionBlocksOnlyInvalidQueue(t *testing.T) {
	bad := leaf("team-a", rootName, budget("64"))
	bad.Spec.Preemption = &v1beta1.AcceleratorQuotaPreemption{WithinClusterQueue: "Any"}
	quotas := []v1beta1.AcceleratorQuota{cohort(rootName, "", budget("128")), bad, leaf("team-b", rootName, budget("64"))}
	tr, vs := mustBuild(t, quotas, opts())
	blocked := tr.Frozen(vs)
	if len(blocked) != 1 || blocked["team-a"].Subject != "preemption.withinClusterQueue" {
		t.Fatalf("blocked = %+v, want only team-a for its invalid preemption policy", blocked)
	}
}

func TestPreemptionOnRootWithLeafRoleIsRejected(t *testing.T) {
	root := leaf(rootName, "", budget("128"))
	root.Spec.ParentRef = nil
	root.Spec.Preemption = &v1beta1.AcceleratorQuotaPreemption{}
	_, got := mustBuild(t, []v1beta1.AcceleratorQuota{root}, opts())
	want := Violations{{Node: rootName, Reason: v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
		Subject: "preemption", Message: "only a non-root ClusterQueue node may set preemption"}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("violations (-want +got):\n%s", diff)
	}
}

func assertPreemptionViolations(t *testing.T, policy *v1beta1.AcceleratorQuotaPreemption, want Violations) {
	t.Helper()
	queue := leaf("team-a", rootName, budget("64"))
	queue.Spec.Preemption = policy
	_, got := mustBuild(t, []v1beta1.AcceleratorQuota{cohort(rootName, "", budget("128")), queue}, opts())
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("violations (-want +got):\n%s", diff)
	}
}
