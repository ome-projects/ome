package coordination

import (
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var (
	keepLiveCopyAt = metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	keepLiveLiveAt = metav1.NewTime(time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC))
)

// keepLiveStatus is a status with one group entry and the readiness
// condition it sets, written at the given instant.
func keepLiveStatus(phase v1beta1.CoordinationPhase, ready corev1.ConditionStatus, at metav1.Time) *v1beta1.InferenceServiceStatus {
	status := &v1beta1.InferenceServiceStatus{RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
		Groups: []v1beta1.RolloutCoordinationGroupStatus{{Name: "0", Phase: phase, CompositePhase: string(phase), LastTransitionTime: &at}},
	}}
	status.SetConditions(apis.Conditions{{
		Type: apis.ConditionType(v1beta1.RolloutCoordinationReady), Status: ready, LastTransitionTime: apis.VolatileTime{Inner: at},
	}})
	return status
}

// A write made before the coordination step carries the entries of the
// pass's copy; the live object's entries and readiness condition replace
// them, time included, an entry or a condition the live object lacks is
// dropped, and the conditions stay sorted by type as the condition manager
// keeps them.
func TestKeepLiveEntries(t *testing.T) {
	ready := apis.ConditionType(v1beta1.RolloutCoordinationReady)

	t.Run("the live entry and its condition replace the copy's", func(t *testing.T) {
		g := gomega.NewGomegaWithT(t)
		dst := keepLiveStatus(v1beta1.CoordinationPhaseSurging, corev1.ConditionFalse, keepLiveCopyAt)
		live := keepLiveStatus(v1beta1.CoordinationPhaseIdle, corev1.ConditionTrue, keepLiveLiveAt)
		KeepLiveEntries(dst, live)
		g.Expect(dst.RolloutCoordination.Groups).To(gomega.HaveLen(1))
		g.Expect(dst.RolloutCoordination.Groups[0].Phase).To(gomega.Equal(v1beta1.CoordinationPhaseIdle))
		g.Expect(dst.RolloutCoordination.Groups[0].LastTransitionTime.Time).To(gomega.BeTemporally("==", keepLiveLiveAt.Time))
		g.Expect(dst.GetCondition(ready).Status).To(gomega.Equal(corev1.ConditionTrue))
		g.Expect(dst.GetCondition(ready).LastTransitionTime.Inner.Time).To(gomega.BeTemporally("==", keepLiveLiveAt.Time))
		live.RolloutCoordination.Groups[0].Phase = v1beta1.CoordinationPhasePaused
		g.Expect(dst.RolloutCoordination.Groups[0].Phase).To(gomega.Equal(v1beta1.CoordinationPhaseIdle), "dst holds its own copy")
	})

	t.Run("an entry and a condition the live object lacks are dropped and the other conditions stay", func(t *testing.T) {
		g := gomega.NewGomegaWithT(t)
		dst := keepLiveStatus(v1beta1.CoordinationPhaseIdle, corev1.ConditionTrue, keepLiveCopyAt)
		dst.Conditions = append(dst.Conditions, apis.Condition{Type: apis.ConditionReady, Status: corev1.ConditionTrue})
		KeepLiveEntries(dst, &v1beta1.InferenceServiceStatus{})
		g.Expect(dst.RolloutCoordination).To(gomega.BeNil())
		g.Expect(dst.GetCondition(ready)).To(gomega.BeNil(), "the copy's condition goes with its entry")
		g.Expect(dst.GetCondition(apis.ConditionReady)).NotTo(gomega.BeNil())
	})

	t.Run("a condition the copy lacks is added in type order", func(t *testing.T) {
		g := gomega.NewGomegaWithT(t)
		dst := &v1beta1.InferenceServiceStatus{}
		dst.SetConditions(apis.Conditions{{Type: apis.ConditionReady, Status: corev1.ConditionTrue}, {Type: "RoutesReady", Status: corev1.ConditionTrue}})
		live := keepLiveStatus(v1beta1.CoordinationPhasePaused, corev1.ConditionFalse, keepLiveLiveAt)
		KeepLiveEntries(dst, live)
		g.Expect(dst.RolloutCoordination.Groups[0].Phase).To(gomega.Equal(v1beta1.CoordinationPhasePaused))
		g.Expect(dst.GetCondition(ready)).NotTo(gomega.BeNil())
		g.Expect(dst.GetCondition(ready).Status).To(gomega.Equal(corev1.ConditionFalse))
		types := make([]apis.ConditionType, 0, len(dst.Conditions))
		for _, cond := range dst.Conditions {
			types = append(types, cond.Type)
		}
		g.Expect(types).To(gomega.Equal([]apis.ConditionType{apis.ConditionReady, ready, "RoutesReady"}), "sorted by type, as the condition manager keeps the list")
	})

	t.Run("a missing side changes nothing", func(t *testing.T) {
		g := gomega.NewGomegaWithT(t)
		dst := keepLiveStatus(v1beta1.CoordinationPhaseIdle, corev1.ConditionTrue, keepLiveCopyAt)
		KeepLiveEntries(dst, nil)
		KeepLiveEntries(nil, dst)
		g.Expect(dst.RolloutCoordination.Groups[0].Phase).To(gomega.Equal(v1beta1.CoordinationPhaseIdle))
	})
}
