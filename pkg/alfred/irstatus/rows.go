// Package irstatus provides Alfred's bounded, read-only view of the
// InferenceReplica instance-status representation union.
package irstatus

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	codec "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// MaxRows is Alfred's existing per-replica observation limit.
const MaxRows = 10_000

// Rows returns independent logical instance statuses without changing the
// captured representation. Both DenseV1 and ColumnarV2 are bounded to MaxRows.
// Invalid representation unions and malformed columns return no rows.
func Rows(status *v1beta1.InferenceReplicaStatus) ([]v1beta1.OMENativeInstanceStatus, error) {
	rows, _, err := codec.DecodeStatus(status, MaxRows)
	if err != nil {
		return nil, err
	}
	if len(rows) > MaxRows {
		return nil, fmt.Errorf("inference replica status has %d rows, exceeds Alfred limit %d", len(rows), MaxRows)
	}
	copied := make([]v1beta1.OMENativeInstanceStatus, len(rows))
	for i := range rows {
		rows[i].DeepCopyInto(&copied[i])
	}
	return copied, nil
}
