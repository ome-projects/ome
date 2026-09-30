package types

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WaitingReasonCapacityProvisioning is the InstanceOperation.Waiting
// token recorded while a pod the operation needs is withheld because the
// capacity Deps.Provisioner confines it to is not ready yet.
//
// ENVIRONMENT-CAUSED: a provisioner outside the workload readies the
// capacity, so the wait is externally held — it parks the
// InstanceReadyTimeout clock and never charges a retry ladder.
const WaitingReasonCapacityProvisioning = "CapacityProvisioning"

// Provisioner confines an Instance's pods to capacity a provisioner
// outside the workload readies for them. The create site asks Place
// before creating each pod, and the hold pass asks Pending about each pod
// the Instance still lacks, so a withheld create parks the operation's
// deadline instead of spending it.
type Provisioner interface {
	// Place reports whether the pod may be created now and the node
	// selector that confines it. ready=false withholds the create; Place
	// may start provisioning the capacity. Every member of a multi-pod
	// Instance that Place confines is confined to the same capacity, so a
	// confined gang shares one topology domain.
	Place(ctx context.Context, input ReconcileInput, plan ComponentPlan, inst InstancePlan, runner RunnerPlan, ordinal int32) (nodeSelector map[string]string, ready bool, err error)

	// Pending reports whether Place would withhold the pod, with the
	// provisioner's explanation. It never changes the cluster. The
	// explanation changes only when the state of the capacity does: the
	// hold records each new explanation as a new fact.
	Pending(ctx context.Context, input ReconcileInput, plan ComponentPlan, inst InstancePlan, runner RunnerPlan, ordinal int32) (message string, pending bool, err error)
}

// OperationCapacityProvisioning reports whether an operation is parked on
// the provisioning hold's waiting token.
func OperationCapacityProvisioning(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonCapacityProvisioning
}

// CapacityProvisioningTermination is the evidence record for the hold:
// the pod withheld, the provisioner's explanation, and when that
// explanation was first seen.
func CapacityProvisioningTermination(podName, message string, at metav1.Time) *InstanceTermination {
	return &InstanceTermination{
		PodName: podName,
		Reason:  WaitingReasonCapacityProvisioning,
		Message: fmt.Sprintf("%s: pod %s is withheld until its capacity is ready: %s",
			WaitingReasonCapacityProvisioning, podName, message),
		Time: at,
	}
}
