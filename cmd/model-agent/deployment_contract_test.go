package main

import "testing"

// Validate the actual command, rather than a second FlagSet constructed by a test.
// These flags are passed by the GPU/CPU deployment contract.
func TestDeploymentFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"model-verification-concurrency",
		"num-download-worker",
		"num-high-priority-worker",
		"same-path-wait-timeout",
	} {
		t.Run(name, func(t *testing.T) {
			if rootCmd.PersistentFlags().Lookup(name) == nil {
				t.Fatalf("deployment flag --%s is not registered by the pinned agent", name)
			}
		})
	}
}
