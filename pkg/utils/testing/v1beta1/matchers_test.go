package v1beta1testing_test

import (
	"errors"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	v1beta1testing "sigs.k8s.io/ome/pkg/utils/testing/v1beta1"
)

var _ = ginkgo.Describe("Condition matchers", func() {
	conditions := []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllGood"},
		{Type: "SourceReachable", Status: metav1.ConditionFalse, Reason: "Unreachable"},
	}

	ginkgo.It("HaveConditionStatusTrue matches a True condition", func() {
		Expect(conditions).To(v1beta1testing.HaveConditionStatusTrue("Ready"))
	})

	ginkgo.It("HaveConditionStatusTrue does not match a False condition", func() {
		Expect(conditions).NotTo(v1beta1testing.HaveConditionStatusTrue("SourceReachable"))
	})

	ginkgo.It("HaveConditionStatusTrue does not match an absent condition", func() {
		Expect(conditions).NotTo(v1beta1testing.HaveConditionStatusTrue("Nonexistent"))
	})

	ginkgo.It("HaveConditionStatusTrueAndReason matches type+status+reason", func() {
		Expect(conditions).To(v1beta1testing.HaveConditionStatusTrueAndReason("Ready", "AllGood"))
	})

	ginkgo.It("HaveConditionStatusTrueAndReason fails on a reason mismatch", func() {
		Expect(conditions).NotTo(v1beta1testing.HaveConditionStatusTrueAndReason("Ready", "WrongReason"))
	})

	ginkgo.It("HaveConditionStatusFalse matches a False condition", func() {
		Expect(conditions).To(v1beta1testing.HaveConditionStatusFalse("SourceReachable"))
	})

	ginkgo.It("HaveCondition matches presence regardless of status", func() {
		Expect(conditions).To(v1beta1testing.HaveCondition("SourceReachable"))
	})

	ginkgo.It("errors (not just fails) when given a non-condition-slice", func() {
		ok, err := v1beta1testing.HaveConditionStatusTrue("Ready").Match("not a slice")
		Expect(ok).To(BeFalse())
		Expect(err).To(HaveOccurred())
	})
})

var _ = ginkgo.Describe("API error matchers", func() {
	gr := schema.GroupResource{Group: "ome.io", Resource: "basemodels"}

	ginkgo.It("BeNotFoundError matches IsNotFound", func() {
		Expect(apierrors.NewNotFound(gr, "x")).To(v1beta1testing.BeNotFoundError())
	})

	ginkgo.It("BeNotFoundError does not match a different error", func() {
		Expect(apierrors.NewConflict(gr, "x", errors.New("boom"))).NotTo(v1beta1testing.BeNotFoundError())
	})

	ginkgo.It("BeConflictError matches IsConflict", func() {
		Expect(apierrors.NewConflict(gr, "x", errors.New("boom"))).To(v1beta1testing.BeConflictError())
	})

	ginkgo.It("BeInvalidError matches IsInvalid", func() {
		Expect(apierrors.NewInvalid(schema.GroupKind{Group: "ome.io", Kind: "BaseModel"}, "x", nil)).
			To(v1beta1testing.BeInvalidError())
	})

	ginkgo.It("a nil error matches nothing", func() {
		ok, err := v1beta1testing.BeNotFoundError().Match(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
	})
})
