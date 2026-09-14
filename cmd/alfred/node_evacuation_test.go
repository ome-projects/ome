package main

import (
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

func TestConfiguredPoliciesReportHealthyMaintenanceNode(t *testing.T) {
	now := time.Now()
	snap := &snapshot.ClusterSnapshot{Timestamp: now, Nodes: map[string]*snapshot.Node{
		"patching": {Name: "patching", UID: "node-uid", Health: snapshot.NodeHealthObservation{State: snapshot.NodeHealthClear},
			Maintenance: snapshot.NodeMaintenanceObservation{Requested: true, Triggers: []string{"patching"}}},
	}}
	for _, p := range decisionPolicies() {
		for _, candidate := range p.Evaluate(snap, config.NewStore().Get()) {
			if marker := candidate.Remediation; marker != nil && marker.Node == "patching" && marker.Maintenance.Requested && marker.ObservedAt.Equal(now) {
				return
			}
		}
	}
	t.Fatal("production policy registration did not report the healthy maintenance node")
}
