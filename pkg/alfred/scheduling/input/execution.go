package input

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

// BuildExecutionRequest models the public migration-v1 placement contract,
// not an instruction to bind at the returned placements. The consumer excludes
// only from_node and adds weight-50 hostname preferences. Source Pods remain
// occupied until their complete replacement instance becomes ready.
func BuildExecutionRequest(s *Snapshot, source Source, profiles scheduling.Config, requestID string, hints []string, now time.Time, maxAge time.Duration) (scheduling.Request, error) {
	if len(hints) > 8 {
		return scheduling.Request{}, fmt.Errorf("too many migration hints")
	}
	seen := map[string]bool{}
	for _, hint := range hints {
		if len(validation.IsDNS1123Subdomain(hint)) != 0 || hint == source.FromNode || seen[hint] {
			return scheduling.Request{}, fmt.Errorf("invalid migration hints")
		}
		seen[hint] = true
	}
	r, err := BuildRequest(s, source, profiles, requestID, now, maxAge)
	if err != nil {
		return scheduling.Request{}, err
	}
	state, err := resolveSource(s, source)
	if err != nil {
		return scheduling.Request{}, err
	}
	// A live pod may retain migration-only affinity from an earlier surge.
	// The consumer renders the stable revision, not that live PodSpec. Only
	// accept node affinity equal to the current, observed runner baseline;
	// do not guess which constraints came from a previous migration.
	for _, member := range state.members {
		matchedRunner := false
		for _, runner := range state.ir.Spec.Runners {
			if string(runner.Name) != member.pod.Labels[labelRunner] {
				continue
			}
			matchedRunner = true
			if !reflect.DeepEqual(executionNodeAffinity(member.pod.Spec), executionNodeAffinity(runner.Template.Spec)) {
				return scheduling.Request{}, fmt.Errorf("source node affinity differs from stable runner template")
			}
		}
		if !matchedRunner {
			return scheduling.Request{}, fmt.Errorf("source runner template is missing")
		}
	}
	matched := false
	for _, object := range s.Objects {
		var header metav1.TypeMeta
		if err := json.Unmarshal(object.Raw, &header); err != nil {
			return scheduling.Request{}, fmt.Errorf("invalid snapshot object")
		}
		if header.APIVersion != "v1" || header.Kind != "Node" {
			continue
		}
		var node corev1.Node
		if err := json.Unmarshal(object.Raw, &node); err != nil {
			return scheduling.Request{}, fmt.Errorf("invalid snapshot node")
		}
		if node.Name == source.FromNode {
			matched = node.Labels[corev1.LabelHostname] == source.FromNode
		}
	}
	if !matched {
		return scheduling.Request{}, fmt.Errorf("source hostname does not match migration exclusion")
	}
	r.ExcludedNodes = []string{source.FromNode}
	r.MigrationFromNode = source.FromNode
	for i := range r.ReplacementPods {
		applyExecutionOverlay(&r.ReplacementPods[i], source.FromNode, hints)
	}
	return r, nil
}

func executionNodeAffinity(spec corev1.PodSpec) *corev1.NodeAffinity {
	if spec.Affinity == nil {
		return nil
	}
	return spec.Affinity.NodeAffinity
}

// The public API's hostname overlay is reproduced here at the wire boundary;
// Alfred does not import the owning controller's renderer or execute it.
func applyExecutionOverlay(pod *corev1.Pod, from string, hints []string) {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity
	exclude := corev1.NodeSelectorRequirement{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpNotIn, Values: []string{from}}
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{exclude}}}}
	} else {
		for i := range na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			term := &na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
			found := false
			for _, expression := range term.MatchExpressions {
				if reflect.DeepEqual(expression, exclude) {
					found = true
				}
			}
			if !found {
				term.MatchExpressions = append(term.MatchExpressions, exclude)
			}
		}
	}
	if len(hints) == 0 {
		return
	}
	preference := corev1.PreferredSchedulingTerm{Weight: 50, Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: append([]string(nil), hints...)}}}}
	for _, existing := range na.PreferredDuringSchedulingIgnoredDuringExecution {
		if reflect.DeepEqual(existing, preference) {
			return
		}
	}
	na.PreferredDuringSchedulingIgnoredDuringExecution = append(na.PreferredDuringSchedulingIgnoredDuringExecution, preference)
}
