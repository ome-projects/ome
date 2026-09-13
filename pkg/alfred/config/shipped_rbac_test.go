package config

import (
	"os"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// rbacRule is the slice of a PolicyRule the parity check compares.
type rbacRule struct {
	APIGroups []string `json:"apiGroups"`
	Resources []string `json:"resources"`
	Verbs     []string `json:"verbs"`
}

// grants flattens the named kind's rules into group/resource:verb keys.
// Lines carrying Helm template directives are dropped first: in these
// manifests they hold only labels, namespaces, the enabled guard and
// resourceNames — never a group, resource or verb — so what remains is plain
// YAML with every grant intact.
func grants(t *testing.T, path, kind string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, "{{") {
			lines = append(lines, line)
		}
	}
	out := map[string]bool{}
	found := false
	for _, part := range strings.Split(strings.Join(lines, "\n"), "\n---") {
		var obj struct {
			Kind  string     `json:"kind"`
			Rules []rbacRule `json:"rules"`
		}
		if err := yaml.Unmarshal([]byte(part), &obj); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if obj.Kind != kind {
			continue
		}
		found = true
		for _, r := range obj.Rules {
			for _, group := range r.APIGroups {
				for _, resource := range r.Resources {
					for _, verb := range r.Verbs {
						out[group+"/"+resource+":"+verb] = true
					}
				}
			}
		}
	}
	if !found {
		t.Fatalf("%s: no %s document", path, kind)
	}
	return out
}

// TestChartRBACMatchesKustomize keeps the two install paths granting the same
// permissions. The chart and config/alfred are maintained separately, so a
// change that needs a new read can update one and not the other — and the
// binary then fails RBAC only when installed the other way.
func TestChartRBACMatchesKustomize(t *testing.T) {
	const chart = "../../../charts/ome-alfred/templates/rbac.yaml"
	for kind, kustomize := range map[string]string{
		"ClusterRole": "../../../config/alfred/clusterrole.yaml",
		"Role":        "../../../config/alfred/role.yaml",
	} {
		t.Run(kind, func(t *testing.T) {
			want := grants(t, kustomize, kind)
			got := grants(t, chart, kind)
			var missing, extra []string
			for key := range want {
				if !got[key] {
					missing = append(missing, key)
				}
			}
			for key := range got {
				if !want[key] {
					extra = append(extra, key)
				}
			}
			sort.Strings(missing)
			sort.Strings(extra)
			if len(missing) > 0 || len(extra) > 0 {
				t.Fatalf("chart %s drifted from %s\n  missing from chart: %v\n  only in chart:      %v",
					kind, kustomize, missing, extra)
			}
		})
	}
}
