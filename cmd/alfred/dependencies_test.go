package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestAlfredDoesNotDependOnControllers checks the resolved dependency graph,
// including test fixtures, so a transitive import cannot bypass the boundary.
func TestAlfredDoesNotDependOnControllers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-mod=readonly", "-deps", "-test", ".", "../../pkg/alfred/...")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("resolve Alfred dependency graph: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "sigs.k8s.io/ome/pkg/controller/") {
			t.Errorf("Alfred must consume public APIs, not controller implementation: %s", dependency)
		}
	}
}
