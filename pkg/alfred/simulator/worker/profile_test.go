package worker

import (
	"context"
	"runtime/debug"
	"strings"
	"testing"

	"sigs.k8s.io/ome/alfred-simulator/protocol"
)

func TestCompiledSchedulerVersionFence(t *testing.T) {
	for _, version := range []string{"v1.35.3", "v1.36.0", ""} {
		info := &debug.BuildInfo{Deps: []*debug.Module{{Path: "k8s.io/kubernetes", Version: version}}}
		if err := validateSchedulerBuild(info); err == nil {
			t.Fatalf("compiled scheduler %q incorrectly accepted", version)
		}
	}
	info := &debug.BuildInfo{Deps: []*debug.Module{{Path: "k8s.io/kubernetes", Version: "v0.0.0", Replace: &debug.Module{Path: "k8s.io/kubernetes", Version: "v1.35.4"}}}}
	if err := validateSchedulerBuild(info); err != nil {
		t.Fatal(err)
	}
}

func TestProfileDefaultsAndIdentity(t *testing.T) {
	implicit := testProfile(t, defaultConfig)
	explicit := testProfile(t, defaultConfig+"parallelism: 16\npodInitialBackoffSeconds: 1\npodMaxBackoffSeconds: 10\n")
	if implicit.Identity != explicit.Identity {
		t.Fatal("equivalent upstream defaults produced different identities")
	}
	changed := testProfile(t, defaultConfig+"parallelism: 4\n")
	if implicit.Identity.ConfigurationID == changed.Identity.ConfigurationID {
		t.Fatal("configuration change did not fence identity")
	}
	if implicit.GangScheduling {
		t.Fatal("default profile falsely claims gang support")
	}
	if !testProfile(t, omeConfig).GangScheduling {
		t.Fatal("complete OME profile is not marked gang-capable")
	}
}

func TestProfileRejectsUnsupportedConfiguration(t *testing.T) {
	for name, config := range map[string]string{
		"unknown field":            defaultConfig + "unknown: true\n",
		"client credentials":       defaultConfig + "clientConnection:\n  kubeconfig: /production/config\n",
		"extender":                 defaultConfig + "extenders:\n- urlPrefix: https://production.invalid\n",
		"unknown plugin":           strings.Replace(omeConfig, "OMEGangPack", "UnknownPlugin", -1),
		"partial gang":             strings.Replace(omeConfig, "multiPoint:", "filter:", 1),
		"unknown OME argument":     omeConfig + "      invented: true\n",
		"missing OME config":       strings.Split(omeConfig, "  pluginConfig:")[0],
		"invalid OME topology key": strings.Replace(omeConfig, "topology.example/domain", "invalid key", 1),
		"overflowing OME timeout":  strings.Replace(omeConfig, "podGroupSyncTimeoutSeconds: 1", "podGroupSyncTimeoutSeconds: 9223372036854775807", 1),
		"multiple profiles":        defaultConfig + "- schedulerName: second\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadProfile("test", []byte(config)); err == nil {
				t.Fatal("unsupported configuration accepted")
			}
		})
	}
}

func TestProfileMismatchAndCancellation(t *testing.T) {
	p := testProfile(t, defaultConfig)
	r := testRequest(t, p)
	r.Profile.ConfigurationID = "claimed"
	if got, err := p.Evaluate(context.Background(), r); err == nil || got.Decision == protocol.DecisionFeasible {
		t.Fatal("claimed identity accepted")
	}
	r = testRequest(t, p)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := p.Evaluate(ctx, r)
	if err != nil || got.Decision != protocol.DecisionUnsupported || len(got.Placements) != 0 {
		t.Fatalf("cancellation was not Unsupported: %+v %v", got, err)
	}
	p.Identity.ConfigurationID = "mutated"
	r.Profile = p.Identity
	if _, err := p.Evaluate(context.Background(), r); err == nil {
		t.Fatal("mutated public profile identity accepted")
	}
}
