package protocol

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func executionPolicy() *v1beta1.PlacementExecutionPolicy {
	return &v1beta1.PlacementExecutionPolicy{
		PlanID: "plan-a", Revision: 2, SourceUID: "source-a", ClusterUID: "cluster-a", PauseSurge: true,
	}
}

func TestFromDerived(t *testing.T) {
	policy := executionPolicy()
	raw, err := Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name        string
		annotations map[string]string
		labels      map[string]string
		want        *v1beta1.PlacementExecutionPolicy
		wantErr     bool
	}{
		{name: "ordinary local service"},
		{name: "local ignores execution annotation", annotations: map[string]string{constants.PlacementExecution: raw}},
		{name: "local ignores malformed annotation", annotations: map[string]string{constants.PlacementExecution: "{"}},
		{name: "derived without policy", labels: map[string]string{constants.PlacementOrigin: "source-a"}},
		{name: "source label", labels: map[string]string{constants.PlacementOrigin: "source-a"}, annotations: map[string]string{constants.PlacementExecution: raw}, want: policy},
		{name: "source annotation", annotations: map[string]string{constants.PlacementOriginUID: "source-a", constants.PlacementExecution: raw}, want: policy},
		{name: "malformed", labels: map[string]string{constants.PlacementOrigin: "source-a"}, annotations: map[string]string{constants.PlacementExecution: "{"}, wantErr: true},
		{name: "null", labels: map[string]string{constants.PlacementOrigin: "source-a"}, annotations: map[string]string{constants.PlacementExecution: "null"}, wantErr: true},
		{name: "wrong source", labels: map[string]string{constants.PlacementOrigin: "source-b"}, annotations: map[string]string{constants.PlacementExecution: raw}, wantErr: true},
		{name: "annotation identity takes precedence", labels: map[string]string{constants.PlacementOrigin: "source-a"}, annotations: map[string]string{constants.PlacementOriginUID: "source-b", constants.PlacementExecution: raw}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations, Labels: tt.labels}}
			before := service.DeepCopy()
			got, err := FromDerived(service)
			if (err != nil) != tt.wantErr {
				t.Fatalf("FromDerived error = %v, want error %t", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("policy (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, service); diff != "" {
				t.Errorf("service mutated (-want +got):\n%s", diff)
			}
		})
	}
	if got, err := FromDerived(nil); got != nil || err != nil {
		t.Fatalf("nil service = %v, %v", got, err)
	}
}

func TestPolicyValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.PlacementExecutionPolicy)
	}{
		{"missing plan", func(p *v1beta1.PlacementExecutionPolicy) { p.PlanID = "" }},
		{"missing revision", func(p *v1beta1.PlacementExecutionPolicy) { p.Revision = 0 }},
		{"negative revision", func(p *v1beta1.PlacementExecutionPolicy) { p.Revision = -1 }},
		{"missing source", func(p *v1beta1.PlacementExecutionPolicy) { p.SourceUID = "" }},
		{"missing cluster", func(p *v1beta1.PlacementExecutionPolicy) { p.ClusterUID = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := executionPolicy()
			tt.edit(policy)
			if _, err := Encode(policy); err == nil {
				t.Fatal("expected invalid policy rejection")
			}
		})
	}
	if _, err := Encode(nil); err == nil {
		t.Fatal("expected nil policy rejection")
	}
}

func TestAuthorize(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.PlacementExecutionPolicy)
		wantErr bool
	}{
		{name: "same authority"},
		{name: "new revision", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++ }},
		{name: "explicit release", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++; p.PlanID = "plan-b"; p.PauseSurge = false }},
		{name: "old revision", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision-- }, wantErr: true},
		{name: "conflicting plan", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PlanID = "plan-b" }, wantErr: true},
		{name: "conflicting pause", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PauseSurge = false }, wantErr: true},
		{name: "different source", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++; p.SourceUID = "source-b" }, wantErr: true},
		{name: "different cluster", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision++; p.ClusterUID = "cluster-b" }, wantErr: true},
		{name: "invalid next", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PlanID = "" }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current := executionPolicy()
			next := current.DeepCopy()
			if tt.edit != nil {
				tt.edit(next)
			}
			if err := Authorize(current, next); (err != nil) != tt.wantErr {
				t.Fatalf("Authorize error = %v, want error %t", err, tt.wantErr)
			}
		})
	}
	for _, tt := range []struct {
		name          string
		current, next *v1beta1.PlacementExecutionPolicy
		wantErr       bool
	}{
		{name: "local"},
		{name: "first policy", next: executionPolicy()},
		{name: "cannot clear authority", current: executionPolicy(), wantErr: true},
		{name: "corrupt authority", current: &v1beta1.PlacementExecutionPolicy{}, next: executionPolicy(), wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := Authorize(tt.current, tt.next); (err != nil) != tt.wantErr {
				t.Fatalf("Authorize error = %v, want error %t", err, tt.wantErr)
			}
		})
	}
}
