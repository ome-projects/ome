package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusAutoscaleKeepsPolicyIssuesAsKnownEvidence(t *testing.T) {
	for _, code := range []AutoscaleIssueCode{
		AutoscaleIssuePolicyEvidenceInvalid,
		AutoscaleIssuePolicyConditionConflict,
	} {
		t.Run(string(code), func(t *testing.T) {
			got, issues := statusAutoscaleCanonical(StatusAutoscale{
				Summary:  AutoscaleSummary{State: AutoscaleStateInvalid},
				Evidence: EvidenceReported,
				Components: []StatusAutoscaleComponent{{
					Type: RuntimeComponentEngine, State: AutoscaleComponentInvalid,
					Class: AutoscaleClassHPA, ManagedBy: AutoscaleManagedByOME,
					TargetEvidence: AutoscaleTargetReported, ReplicaEvidence: AutoscaleReplicasReported,
					ConditionEvidence: AutoscaleConditionsReported,
					CurrentReplicas:   ptrInt32(1), DesiredReplicas: ptrInt32(1),
				}},
				Issues: []AutoscaleIssue{{Code: code, Component: RuntimeComponentEngine}},
			})

			require.Empty(t, issues)
			assert.Equal(t, []AutoscaleIssue{{Code: code, Component: RuntimeComponentEngine}}, got.Issues)
		})
	}
}

func ptrInt32(value int32) *int32 { return &value }
