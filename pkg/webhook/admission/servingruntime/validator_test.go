package servingruntime

import (
	"testing"

	"github.com/onsi/gomega"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestAreModelSizeRangesEqual(t *testing.T) {
	testcases := []struct {
		name     string
		range1   *v1beta1.ModelSizeRangeSpec
		range2   *v1beta1.ModelSizeRangeSpec
		expected bool
	}{
		{name: "Both nil", expected: true},
		{name: "First nil, second not nil", range2: &v1beta1.ModelSizeRangeSpec{}},
		{name: "First not nil, second nil", range1: &v1beta1.ModelSizeRangeSpec{}},
		{name: "Both empty", range1: &v1beta1.ModelSizeRangeSpec{}, range2: &v1beta1.ModelSizeRangeSpec{}, expected: true},
		{
			name:   "Different Min values",
			range1: &v1beta1.ModelSizeRangeSpec{Min: ptr.To("10")},
			range2: &v1beta1.ModelSizeRangeSpec{Min: ptr.To("20")},
		},
		{
			name:   "Different Max values",
			range1: &v1beta1.ModelSizeRangeSpec{Max: ptr.To("100")},
			range2: &v1beta1.ModelSizeRangeSpec{Max: ptr.To("200")},
		},
		{
			name:   "First Min nil, second Min not nil",
			range1: &v1beta1.ModelSizeRangeSpec{},
			range2: &v1beta1.ModelSizeRangeSpec{Min: ptr.To("10")},
		},
		{
			name:   "First Max nil, second Max not nil",
			range1: &v1beta1.ModelSizeRangeSpec{},
			range2: &v1beta1.ModelSizeRangeSpec{Max: ptr.To("100")},
		},
		{
			name:     "Equal Min and Max values",
			range1:   &v1beta1.ModelSizeRangeSpec{Min: ptr.To("10"), Max: ptr.To("100")},
			range2:   &v1beta1.ModelSizeRangeSpec{Min: ptr.To("10"), Max: ptr.To("100")},
			expected: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := areModelSizeRangesEqual(tc.range1, tc.range2)
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}

func TestContains(t *testing.T) {
	testcases := []struct {
		name     string
		slice    []string
		element  string
		expected bool
	}{
		{name: "Empty slice", slice: []string{}, element: "test"},
		{name: "Element not in slice", slice: []string{"a", "b", "c"}, element: "d"},
		{name: "Element in slice", slice: []string{"a", "b", "c"}, element: "b", expected: true},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := contains(tc.slice, tc.element)
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}

func TestContainsComplete(t *testing.T) {
	testcases := []struct {
		name     string
		slice    []string
		element  string
		expected bool
	}{
		{name: "Empty slice", slice: []string{}, element: "test"},
		{name: "Element not in slice", slice: []string{"a", "b", "c"}, element: "d"},
		{name: "Element in slice", slice: []string{"a", "b", "c"}, element: "b", expected: true},
		{name: "Edge case - nil slice", slice: nil, element: "test"},
		{name: "Edge case - empty string", slice: []string{"a", "", "c"}, element: "", expected: true},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewGomegaWithT(t)
			result := contains(tc.slice, tc.element)
			g.Expect(result).To(gomega.Equal(tc.expected))
		})
	}
}
