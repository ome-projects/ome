package placement

import (
	"encoding/json"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestCheckNewSurge(t *testing.T) {
	for _, tc := range []struct {
		name    string
		edit    func(*v1beta1.InferenceService, *v1beta1.InferenceReplica, *v1beta1.PlacementExecutionPolicy)
		wantErr bool
	}{
		{name: "valid released policy"},
		{name: "local nil policy", edit: func(s *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			s.Labels = nil
			ir.Spec.PlacementExecution = nil
		}},
		{name: "local annotation is not authority", edit: func(s *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			s.Labels = nil
			ir.Spec.PlacementExecution = nil
			p.PauseSurge = true
		}},
		{name: "origin without envelope is ordinary", edit: func(s *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			s.Annotations[constants.PlacementExecution] = ""
			ir.Spec.PlacementExecution = nil
		}},
		{name: "legacy pause", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			ir.Spec.Paused = true
		}, wantErr: true},
		{name: "projected pause", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			p.PauseSurge = true
			ir.Spec.PlacementExecution.PauseSurge = true
		}, wantErr: true},
		{name: "pause before projection", edit: func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			p.Revision++
			p.PauseSurge = true
		}, wantErr: true},
		{name: "release before projection", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			p.Revision++
			ir.Spec.PlacementExecution.PauseSurge = true
		}, wantErr: true},
		{name: "release after projection", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			p.Revision++
			ir.Spec.PlacementExecution = p.DeepCopy()
		}},
		{name: "missing plan", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			ir.Spec.PlacementExecution.PlanID = ""
		}, wantErr: true},
		{name: "missing source", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			ir.Spec.PlacementExecution.SourceUID = ""
		}, wantErr: true},
		{name: "missing cluster", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			ir.Spec.PlacementExecution.ClusterUID = ""
		}, wantErr: true},
		{name: "zero revision", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			ir.Spec.PlacementExecution.Revision = 0
		}, wantErr: true},
		{name: "negative revision", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			ir.Spec.PlacementExecution.Revision = -1
		}, wantErr: true},
		{name: "malformed owner", edit: func(s *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			s.Annotations[constants.PlacementExecution] = "{"
		}, wantErr: true},
		{name: "incomplete owner", edit: func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			p.PlanID = ""
		}, wantErr: true},
		{name: "mismatched origin", edit: func(s *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			s.Labels[constants.PlacementOrigin] = "other"
		}, wantErr: true},
		{name: "annotation origin precedes label", edit: func(s *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			s.Annotations[constants.PlacementOriginUID] = "other"
		}, wantErr: true},
		{name: "removed owner policy", edit: func(s *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			s.Annotations[constants.PlacementExecution] = ""
		}, wantErr: true},
		{name: "stale owner", edit: func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *v1beta1.PlacementExecutionPolicy) {
			ir.Spec.PlacementExecution.Revision++
		}, wantErr: true},
		{name: "conflicting owner", edit: func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			p.PlanID = "other"
		}, wantErr: true},
		{name: "changed identity", edit: func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *v1beta1.PlacementExecutionPolicy) {
			p.Revision++
			p.ClusterUID = "other"
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &v1beta1.PlacementExecutionPolicy{PlanID: "plan", Revision: 1, SourceUID: "source", ClusterUID: "cluster"}
			s := &v1beta1.InferenceService{}
			s.Labels = map[string]string{constants.PlacementOrigin: "source"}
			s.Annotations = map[string]string{}
			ir := &v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{PlacementExecution: p.DeepCopy()}}
			if tc.edit != nil {
				tc.edit(s, ir, p)
			}
			if raw, exists := s.Annotations[constants.PlacementExecution]; !exists {
				data, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				s.Annotations[constants.PlacementExecution] = string(data)
			} else if raw == "" {
				delete(s.Annotations, constants.PlacementExecution)
			}
			beforeService, beforeIR := s.DeepCopy(), ir.DeepCopy()
			if err := CheckNewSurge(s, ir); (err != nil) != tc.wantErr {
				t.Fatalf("CheckNewSurge error = %v, want error %t", err, tc.wantErr)
			}
			before, _ := json.Marshal([]any{beforeService, beforeIR})
			after, _ := json.Marshal([]any{s, ir})
			if string(before) != string(after) {
				t.Fatal("read-only gate mutated observed objects")
			}
		})
	}
	if err := CheckNewSurge(nil, nil); err == nil {
		t.Fatal("missing IR admitted")
	}
}
