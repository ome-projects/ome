package types

import (
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

func TestScaleDownPodFootprint(t *testing.T) {
	now := metav1.Now()
	statuses := []InstanceStatus{{Index: 0}, {Index: 1}, {Index: 2}}
	pods := map[int32][]*corev1.Pod{
		0: {{}, {ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}}},
		1: nil,
		9: {{}},
	}
	if got := ScaleDownPodFootprint(statuses, pods); got != 5 {
		t.Fatalf("footprint = %d, want 5 including Terminating, Podless and statusless work", got)
	}
	if got := ScaleDownPodFootprint(nil, nil); got != 0 {
		t.Fatalf("empty footprint = %d, want 0", got)
	}
}

func TestResolveScaleDownPodBatchSize(t *testing.T) {
	tests := []struct {
		name       string
		configured *intstr.IntOrString
		footprint  int32
		want       int32
		wantError  bool
	}{
		{name: "unconfigured", footprint: 2000},
		{name: "absolute", configured: ptr.To(intstr.FromInt32(100)), footprint: 2000, want: 100},
		{name: "large percentage", configured: ptr.To(intstr.FromString("10%")), footprint: 2000, want: 200},
		{name: "small percentage rounds up", configured: ptr.To(intstr.FromString("10%")), footprint: 8, want: 1},
		{name: "fraction rounds up", configured: ptr.To(intstr.FromString("10%")), footprint: 101, want: 11},
		{name: "whole footprint", configured: ptr.To(intstr.FromString("100%")), footprint: 2000, want: 2000},
		{name: "empty snapshot cleanup", configured: ptr.To(intstr.FromString("10%")), want: 1},
		{name: "maximum integer", configured: ptr.To(intstr.FromInt32(math.MaxInt32)), footprint: 1, want: math.MaxInt32},
		{name: "maximum footprint", configured: ptr.To(intstr.FromString("100%")), footprint: math.MaxInt32, want: math.MaxInt32},
		{name: "maximum footprint fraction", configured: ptr.To(intstr.FromString("99%")), footprint: math.MaxInt32, want: 2126008811},
		{name: "zero integer", configured: ptr.To(intstr.FromInt32(0)), wantError: true},
		{name: "negative integer", configured: ptr.To(intstr.FromInt32(-1)), wantError: true},
		{name: "zero percentage", configured: ptr.To(intstr.FromString("0%")), wantError: true},
		{name: "over percentage", configured: ptr.To(intstr.FromString("101%")), wantError: true},
		{name: "negative percentage", configured: ptr.To(intstr.FromString("-1%")), wantError: true},
		{name: "fractional percentage", configured: ptr.To(intstr.FromString("0.5%")), wantError: true},
		{name: "quoted integer", configured: ptr.To(intstr.FromString("100")), wantError: true},
		{name: "signed percentage", configured: ptr.To(intstr.FromString("+10%")), wantError: true},
		{name: "whitespace", configured: ptr.To(intstr.FromString(" 10%")), wantError: true},
		{name: "empty percentage", configured: ptr.To(intstr.FromString("%")), wantError: true},
		{name: "overflow percentage", configured: ptr.To(intstr.FromString("9999999999999999999999%")), wantError: true},
		{name: "unknown type", configured: &intstr.IntOrString{Type: 2}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ResolveScaleDownPodBatchSize(test.configured, test.footprint)
			if test.wantError {
				if err == nil || got != nil {
					t.Fatalf("got %v, %v; want error with no budget", got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.configured == nil {
				if got != nil {
					t.Fatalf("unconfigured budget = %d, want nil", *got)
				}
				return
			}
			if got == nil || *got != test.want {
				t.Fatalf("budget = %v, want %d", got, test.want)
			}
		})
	}
}
