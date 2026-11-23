package main

import (
	"fmt"
	"reflect"
	"testing"
)

// pod returns the eight hosts of one TPU pod: two 2x2x4 partitions (q0 on
// hosts 0-3, q1 on 4-7), four 2x2x2 and eight 2x2x1 partitions.
func pod(busy ...int) []host {
	taken := map[int]bool{}
	for _, b := range busy {
		taken[b] = true
	}
	hosts := make([]host, 8)
	for i := range hosts {
		hosts[i] = host{
			name: fmt.Sprintf("tpu-%d", i),
			ids: map[string]string{
				"2x2x1": fmt.Sprintf("p%d", i),
				"2x2x2": fmt.Sprintf("d%d", i/2),
				"2x2x4": fmt.Sprintf("q%d", i/4),
			},
			healthy: map[string]bool{"2x2x1": true, "2x2x2": true, "2x2x4": true},
			free:    !taken[i],
		}
	}
	return hosts
}

func TestChooseBestFit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hosts   []host
		shape   string
		want    string
		members []string
	}{
		{"empty pod takes the lowest id", pod(), "2x2x1", "p0", []string{"tpu-0"}},
		{"fills the hole next to a busy host", pod(0), "2x2x1", "p1", []string{"tpu-1"}},
		{"prefers a broken 2x2x2 over a whole one", pod(0, 4), "2x2x1", "p1", []string{"tpu-1"}},
		{"keeps a whole 2x2x4 free", pod(0, 1, 4), "2x2x1", "p5", []string{"tpu-5"}},
		{"places a whole 2x2x4", pod(0), "2x2x4", "q1", []string{"tpu-4", "tpu-5", "tpu-6", "tpu-7"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, members, ok := choose(tc.hosts, tc.shape)
			if !ok || id != tc.want || !reflect.DeepEqual(members, tc.members) {
				t.Fatalf("choose() = %q %v %t, want %q %v", id, members, ok, tc.want, tc.members)
			}
		})
	}
}

// TestChooseSkipsCordonedHosts checks a cordoned host is neither a candidate
// nor part of a wholly free ancestor: with hosts 1-3 cordoned, a slice next
// to busy host 0 must go to the other partition.
func TestChooseSkipsCordonedHosts(t *testing.T) {
	hosts := pod(0)
	for i := 1; i <= 3; i++ {
		hosts[i].free = false
	}
	if id, _, ok := choose(hosts, "2x2x1"); !ok || id != "p4" {
		t.Fatalf("choose() = %q %t, want p4", id, ok)
	}
	if _, _, ok := choose(hosts, "2x2x4"); !ok {
		t.Fatal("the other 2x2x4 is wholly free")
	}
	hosts[5].free = false
	if id, _, ok := choose(hosts, "2x2x4"); ok {
		t.Fatalf("choose() = %q, want no wholly free 2x2x4", id)
	}
}

func TestChooseSkipsUnhealthyAndIncompletePartitions(t *testing.T) {
	hosts := pod()
	hosts[0].healthy["2x2x1"] = false
	if id, _, _ := choose(hosts, "2x2x1"); id != "p1" {
		t.Fatalf("choose() = %q, want p1 past the unhealthy p0", id)
	}
	hosts = pod()[:7]
	if id, _, _ := choose(hosts, "2x2x4"); id != "q0" {
		t.Fatalf("choose() = %q, want q0 since q1 lost a host", id)
	}
	// An unhealthy 2x2x2 does not hide the whole free 2x2x4 around it.
	hosts = pod(0, 1)
	hosts[4].healthy["2x2x2"], hosts[5].healthy["2x2x2"] = false, false
	if id, _, _ := choose(hosts, "2x2x1"); id != "p2" {
		t.Fatalf("choose() = %q, want p2 since p4 lies in the whole free q1", id)
	}
}

// TestReservedResumesAnInterruptedPlacement: a placement that labeled host 4
// for slice s1 and then failed resumes on host 4's partition.
func TestReservedResumesAnInterruptedPlacement(t *testing.T) {
	hosts := pod()
	hosts[4].slice, hosts[4].free = "s1", false
	id, members, ok := reserved(hosts, "s1", "2x2x4")
	if !ok || id != "q1" || !reflect.DeepEqual(members, []string{"tpu-4", "tpu-5", "tpu-6", "tpu-7"}) {
		t.Fatalf("reserved() = %q %v %t, want q1 on hosts 4-7", id, members, ok)
	}
	if id, _, ok := reserved(hosts, "s2", "2x2x4"); ok {
		t.Fatalf("reserved() = %q for a slice no host carries", id)
	}
}
