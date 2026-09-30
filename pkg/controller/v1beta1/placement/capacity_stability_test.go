package placement

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/capacity"
)

func TestCapacityStability(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.InferenceService, *capacity.Sample)
		wait bool
	}{
		{name: "accepted hardware"},
		{name: "first allocation", edit: func(s *v1beta1.InferenceService, _ *capacity.Sample) { s.Status.Placement = nil }},
		{name: "heartbeat", edit: func(_ *v1beta1.InferenceService, c *capacity.Sample) {
			c.Pools[0].ReportResourceVersion = "2"
			c.Pools[0].ObservedAt.Time = c.Pools[0].ObservedAt.Add(time.Minute)
		}},
		{name: "demand change with same hardware", edit: func(_ *v1beta1.InferenceService, c *capacity.Sample) {
			c.Pools[0].Demand *= 2
			c.Weight /= 2
			c.DemandFingerprint = strings.Repeat("c", 64)
			c.DemandContract.Fingerprint = c.DemandFingerprint
		}},
		{name: "hardware change", edit: func(_ *v1beta1.InferenceService, c *capacity.Sample) { c.Pools[0].Allocatable *= 2; c.Weight *= 2 }, wait: true},
		{name: "registration change", edit: func(_ *v1beta1.InferenceService, c *capacity.Sample) { c.ClusterUID = "replacement" }, wait: true},
		{name: "root replacement", edit: func(_ *v1beta1.InferenceService, c *capacity.Sample) { c.Pools[0].ReportUID = "replacement" }, wait: true},
		{name: "mapping change", edit: func(_ *v1beta1.InferenceService, c *capacity.Sample) {
			c.Pools[0].Attribution.FlavorSetHash = "replacement"
		}, wait: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedTestSource()
			attachCapacityContract(t, source)
			sample := plannedCapacitySample(*plannedTestRegistration(), 4)
			if tt.edit != nil {
				tt.edit(source, &sample)
			}
			samples := map[string]capacity.Sample{"member-a": sample}
			r := &Reconciler{Capacity: providerConfig()}
			now := time.Unix(1000, 0)
			for _, step := range []struct {
				elapsed time.Duration
				wait    bool
			}{{0, tt.wait}, {providerConfig().StabilityWindow - time.Nanosecond, tt.wait}, {providerConfig().StabilityWindow, false}} {
				err := r.stableCapacity(source, samples, now.Add(step.elapsed))
				if diff := cmp.Diff(step.wait, err != nil); diff != "" {
					t.Fatalf("at %v: %s: %v", step.elapsed, diff, err)
				}
			}
		})
	}
}

func TestCapacityStabilityRestartsObservation(t *testing.T) {
	for _, reason := range []string{"unknown report", "restart", "hardware recovery", "source replacement", "clock reversal"} {
		t.Run(reason, func(t *testing.T) {
			source := plannedTestSource()
			attachCapacityContract(t, source)
			baseline := plannedCapacitySample(*plannedTestRegistration(), 4)
			changed := plannedCapacitySample(*plannedTestRegistration(), 8)
			samples := map[string]capacity.Sample{"member-a": changed}
			r := &Reconciler{Capacity: providerConfig()}
			now := time.Unix(1000, 0)
			if err := r.stableCapacity(source, samples, now); err == nil {
				t.Fatal("hardware change bypassed window")
			}
			now = now.Add(providerConfig().StabilityWindow / 2)
			switch reason {
			case "unknown report":
				r.forgetCapacity(client.ObjectKeyFromObject(source))
			case "restart":
				r = &Reconciler{Capacity: providerConfig()}
			case "hardware recovery":
				if err := r.stableCapacity(source, map[string]capacity.Sample{"member-a": baseline}, now); err != nil {
					t.Fatal(err)
				}
			case "source replacement":
				source.UID = "replacement"
			case "clock reversal":
				now = time.Unix(900, 0)
			}
			if err := r.stableCapacity(source, samples, now); err == nil {
				t.Fatal("observation inherited old window")
			}
			if err := r.stableCapacity(source, samples, now.Add(providerConfig().StabilityWindow-time.Nanosecond)); err == nil {
				t.Fatal("incomplete observation window accepted")
			}
			if err := r.stableCapacity(source, samples, now.Add(providerConfig().StabilityWindow)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
