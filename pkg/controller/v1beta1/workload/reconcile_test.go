package workload

import (
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestScaleDownPollResult(t *testing.T) {
	t.Run("due policy deadline requeues immediately", func(t *testing.T) {
		result := scaleDownPollResult(types.ReconcileInput{ScaleDownRequeueInterval: time.Hour}, 30*time.Minute, true)
		if !result.Requeue || result.RequeueAfter != 0 {
			t.Fatalf("result = %+v, want immediate requeue", result)
		}
	})

	t.Run("earliest configured or policy timer wins", func(t *testing.T) {
		result := scaleDownPollResult(types.ReconcileInput{ScaleDownRequeueInterval: time.Minute}, 30*time.Second, false)
		if result.Requeue || result.RequeueAfter != 30*time.Second {
			t.Fatalf("result = %+v, want 30s policy timer", result)
		}
		result = scaleDownPollResult(types.ReconcileInput{ScaleDownRequeueInterval: 10 * time.Second}, 30*time.Second, false)
		if result.Requeue || result.RequeueAfter != 10*time.Second {
			t.Fatalf("result = %+v, want 10s configured timer", result)
		}
	})

	t.Run("absent timers remain watch driven", func(t *testing.T) {
		if result := scaleDownPollResult(types.ReconcileInput{}, 0, false); !result.IsZero() {
			t.Fatalf("result = %+v, want zero", result)
		}
	})
}
