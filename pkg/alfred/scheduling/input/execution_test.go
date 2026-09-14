package input

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func TestExecutionModelsSingleExcludedNodeForCompleteGang(t *testing.T) {
	objects, source := validGangSourceObjects()
	for _, object := range objects {
		if node, ok := object.(*corev1.Node); ok {
			node.Labels[corev1.LabelHostname] = node.Name
		}
	}
	snap := captureSourceFixture(t, objects)
	before, _ := json.Marshal(snap)
	r, err := BuildExecutionRequest(snap, source, testProfiles(true), "execution", []string{"target-a"}, captureTime, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.ExcludedNodes, []string{"source-a"}) || len(r.ReplacementPods) != 2 || len(r.SourcePods) != 2 || !r.RequireGang {
		t.Fatalf("execution must model one v1 from_node for the whole gang: %+v", r)
	}
	for _, pod := range r.ReplacementPods {
		na := pod.Spec.Affinity.NodeAffinity
		for _, term := range na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			last := term.MatchExpressions[len(term.MatchExpressions)-1]
			if last.Key != corev1.LabelHostname || last.Operator != corev1.NodeSelectorOpNotIn || !reflect.DeepEqual(last.Values, []string{"source-a"}) {
				t.Fatalf("missing public migration exclusion: %+v", term)
			}
		}
		pref := na.PreferredDuringSchedulingIgnoredDuringExecution
		if len(pref) != 1 || pref[0].Weight != 50 || pref[0].Preference.MatchExpressions[0].Key != corev1.LabelHostname ||
			pref[0].Preference.MatchExpressions[0].Operator != corev1.NodeSelectorOpIn || !reflect.DeepEqual(pref[0].Preference.MatchExpressions[0].Values, []string{"target-a"}) {
			t.Fatalf("hints must be soft runtime-equivalent preference: %+v", pref)
		}
	}
	after, _ := json.Marshal(snap)
	if string(before) != string(after) {
		t.Fatal("execution input mutated capture")
	}
}

func TestExecutionRejectsHostnameMismatchAndInvalidHints(t *testing.T) {
	objects, source := validSingleSourceObjects()
	snap := captureSourceFixture(t, objects)
	if _, err := BuildExecutionRequest(snap, source, testProfiles(false), "execution", nil, captureTime, time.Minute); err == nil {
		t.Fatal("hostname/name mismatch must not silently strengthen the simulated exclusion")
	}
	for _, object := range objects {
		if node, ok := object.(*corev1.Node); ok {
			node.Labels[corev1.LabelHostname] = node.Name
		}
	}
	snap = captureSourceFixture(t, objects)
	for _, hints := range [][]string{{"BAD NAME"}, {"source-a"}, {"target-a", "target-a"}} {
		if _, err := BuildExecutionRequest(snap, source, testProfiles(false), "execution", hints, captureTime, time.Minute); err == nil {
			t.Fatalf("unsafe hints accepted: %v", hints)
		}
	}
}

func TestExecutionOverlayPreservesRequiredORTerms(t *testing.T) {
	pod := corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}},
			{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}}},
		}},
	}}}}
	applyExecutionOverlay(&pod, "source", []string{"target"})
	applyExecutionOverlay(&pod, "source", []string{"target"})
	na := pod.Spec.Affinity.NodeAffinity
	if len(na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) != 2 || len(na.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatal("overlay duplicated constraints or changed OR shape")
	}
	for _, term := range na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if len(term.MatchExpressions) != 2 || term.MatchExpressions[0].Key != "zone" || term.MatchExpressions[1].Values[0] != "source" {
			t.Fatalf("OR branch can bypass exclusion or lost original selector: %+v", term)
		}
	}
}
