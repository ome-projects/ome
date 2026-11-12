package replay

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

// runtimeWalk is a service whose engine renders the runtime it names and
// whose decoder names an image of its own.
const runtimeWalk = `scenario: runtime
cells: ["run/example-cell"]
initial:
  service:
    runtime: {image: registry.example.com/runtime:v1}
    components:
      engine: {replicas: 2}
      decoder: {replicas: 2, image: registry.example.com/decoder:v1}
    groups:
      - components: [engine, decoder]
        rollingUpdate: {}
  config: {requeue: 10s}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - replica.targetBumped[runtime]: {image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - ctrl.resync
`

// recordedWalk is the same service rendering a recorded revision of its
// runtime.
const recordedWalk = `scenario: recorded
cells: ["run/example-cell"]
initial:
  service:
    runtime: {image: registry.example.com/runtime:v1, autoSync: false}
    components:
      engine: {replicas: 2}
    groups:
      - components: [engine]
        blueGreen: {}
  config: {requeue: 10s}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - runtime.drifted: {image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - runtime.synced[annotation]
  - tick: 4
    events:
      - ctrl.resync
`

func runText(t *testing.T, text string) string {
	t.Helper()
	s, err := Parse([]byte(text), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	trace, err := Run(context.Background(), s, Options{})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	return trace
}

func wantLines(t *testing.T, trace string, lines ...string) {
	t.Helper()
	for _, want := range lines {
		if !strings.Contains(trace, want) {
			t.Errorf("trace lacks %q\n%s", want, trace)
		}
	}
}

func wantNoLines(t *testing.T, trace string, lines ...string) {
	t.Helper()
	for _, unwanted := range lines {
		if strings.Contains(trace, unwanted) {
			t.Errorf("trace carries %q\n%s", unwanted, trace)
		}
	}
}

// An edit of the runtime a service follows moves the template of every
// Component that renders it and nothing of the service: the generation
// stays, the projection carries the new template in the pass of the edit,
// the replica records it on the next publication and the run opens on the
// pass after. A Component with its own image is not touched.
func TestRuntimeEditMovesTheTemplateAndNotTheServiceGeneration(t *testing.T) {
	trace := runText(t, runtimeWalk)
	wantLines(t, trace,
		"tick=2 apply replica.targetBumped[runtime] image=registry.example.com/runtime:v2 pieces=all templates=[engine] generation=1",
		"tick=2 pass cache=live generation=1",
		"tick=2 ir patch name=svc-a-engine stage=project image=registry.example.com/runtime:v1→registry.example.com/runtime:v2 generation=1→2",
		"tick=2 run outcome=none",
		"tick=3 publish component=engine current=svc-a-engine-f332566b update=svc-a-engine-46e64ada observedGeneration=2",
		"tick=3 pass cache=live generation=1",
		"tick=3 run outcome=opened",
	)
	wantNoLines(t, trace,
		"tick=2 publish component=engine",
		"ir patch name=svc-a-decoder",
		"parentGen=1→2",
	)
}

// An edit of one piece of the runtime reaches the Component that renders
// that piece; a piece no Component renders moves no template at all.
func TestRuntimeEditOnOnePieceMovesOneTemplate(t *testing.T) {
	both := strings.Replace(runtimeWalk, "decoder: {replicas: 2, image: registry.example.com/decoder:v1}", "decoder: {replicas: 2}", 1)
	engineOnly := strings.Replace(both, "replica.targetBumped[runtime]: {image: registry.example.com/runtime:v2}",
		"replica.targetBumped[runtime]: {component: engine, image: registry.example.com/runtime:v2}", 1)
	trace := runText(t, engineOnly)
	wantLines(t, trace,
		"tick=2 apply replica.targetBumped[runtime] image=registry.example.com/runtime:v2 pieces=engine templates=[engine] generation=1",
		"tick=2 ir patch name=svc-a-engine stage=project image=registry.example.com/runtime:v1→registry.example.com/runtime:v2",
	)
	wantNoLines(t, trace, "ir patch name=svc-a-decoder")

	ownImage := strings.Replace(runtimeWalk, "replica.targetBumped[runtime]: {image: registry.example.com/runtime:v2}",
		"replica.targetBumped[runtime]: {component: decoder, image: registry.example.com/runtime:v2}", 1)
	trace = runText(t, ownImage)
	wantLines(t, trace, "tick=2 apply replica.targetBumped[runtime] image=registry.example.com/runtime:v2 pieces=decoder templates=[] generation=1")
	wantNoLines(t, trace, "ir patch", "tick=3 run outcome=opened")
}

// While the live runtime differs from the recorded revision a service
// renders, no pass runs; a new sync value advances the recorded revision
// and the pass runs again with the live template, the generation unmoved.
func TestRuntimeDriftRunsNoPassUntilTheOperatorSyncs(t *testing.T) {
	trace := runText(t, recordedWalk)
	wantLines(t, trace,
		"tick=2 apply runtime.drifted image=registry.example.com/runtime:v2 pieces=all recorded=cr-runtime-a-",
		"tick=2 pass skipped reason=RuntimeDrifted recorded=cr-runtime-a-",
		"tick=3 apply runtime.synced[annotation] ome.io/runtime-sync=1 recorded=cr-runtime-a-",
		"tick=3 pass cache=live generation=1",
		"tick=3 ir patch name=svc-a-engine stage=project image=registry.example.com/runtime:v1→registry.example.com/runtime:v2",
		"tick=4 run outcome=opened",
	)
	wantNoLines(t, trace, "tick=2 pass cache", "tick=2 pass result")
	if strings.Count(trace, "pass skipped") != 1 {
		t.Errorf("one skipped pass expected\n%s", trace)
	}
}

// Setting spec.runtime.autoSync true is a write of the service: the
// generation moves and the projection carries the live template with the
// new parent generation.
func TestRuntimeDriftRollsWhenAutoSyncTurnsOn(t *testing.T) {
	trace := runText(t, strings.Replace(recordedWalk, "runtime.synced[annotation]", "runtime.synced[autoSync]", 1))
	wantLines(t, trace,
		"tick=3 apply runtime.synced[autoSync] spec.runtime.autoSync=true generation=2 templates=[engine]",
		"tick=3 pass cache=live generation=2",
		"tick=3 ir patch name=svc-a-engine stage=project image=registry.example.com/runtime:v1→registry.example.com/runtime:v2 parentGen=1→2",
		"tick=4 run outcome=opened",
	)
}

// A refused edit leaves the runtime, the templates and the pass alone.
func TestRefusedRuntimeEditChangesNothing(t *testing.T) {
	refused := strings.Replace(runtimeWalk, "replica.targetBumped[runtime]: {image: registry.example.com/runtime:v2}",
		"runtime.editRefused: {image: registry.example.com/runtime:v2, reason: UnknownAcceleratorClass}", 1)
	trace := runText(t, refused)
	wantLines(t, trace,
		"tick=2 apply runtime.editRefused image=registry.example.com/runtime:v2 pieces=all reason=UnknownAcceleratorClass live=",
		"tick=2 pass cache=live generation=1",
		"tick=3 run outcome=none",
	)
	wantNoLines(t, trace, "ir patch", "run outcome=opened")
}

// The runtime events refuse what they cannot mean, and the parser refuses
// a Component with nothing to render.
func TestRuntimeEventsRefuseWhatTheyCannotMean(t *testing.T) {
	edit := "replica.targetBumped[runtime]: {image: registry.example.com/runtime:v2}"
	cases := map[string]struct {
		text string
		want string
	}{
		"no runtime declared": {
			text: strings.Replace(strings.Replace(runtimeWalk, "    runtime: {image: registry.example.com/runtime:v1}\n", "", 1),
				"engine: {replicas: 2}", "engine: {replicas: 2, image: registry.example.com/engine:v1}", 1),
			want: "names no runtime",
		},
		"a live edit under a recorded revision": {
			text: strings.Replace(runtimeWalk, "runtime: {image: registry.example.com/runtime:v1}", "runtime: {image: registry.example.com/runtime:v1, autoSync: false}", 1),
			want: "renders a recorded revision",
		},
		"a drift on a service that follows the live runtime": {
			text: strings.Replace(runtimeWalk, edit, "runtime.drifted: {image: registry.example.com/runtime:v2}", 1),
			want: "follows the live runtime",
		},
		"a sync with no drift": {
			text: strings.Replace(recordedWalk, "runtime.drifted: {image: registry.example.com/runtime:v2}", "ctrl.resync", 1),
			want: "nothing to sync",
		},
		"an edit that changes no template": {
			text: strings.Replace(runtimeWalk, "runtime:v2}", "runtime:v1}", 1),
			want: "already renders",
		},
		"a refusal with no reason": {
			text: strings.Replace(runtimeWalk, edit, "runtime.editRefused: {image: registry.example.com/runtime:v2}", 1),
			want: "needs the reason",
		},
		"a protocol on a runtime edit": {
			text: strings.Replace(runtimeWalk, edit, "replica.targetBumped[runtime]: {image: registry.example.com/runtime:v2, protocol: nixl}", 1),
			want: "spec.rollout.pairingProtocol",
		},
		"an unknown sync variant": {
			text: strings.Replace(recordedWalk, "runtime.synced[annotation]", "runtime.synced[sideways]", 1),
			want: "annotation or autoSync",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := Parse([]byte(tc.text), Vocabulary{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Run(context.Background(), s, Options{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want one naming %q", err, tc.want)
			}
		})
	}
	parse := map[string]struct {
		text string
		want string
	}{
		"a component with nothing to render": {
			text: strings.Replace(runtimeWalk, "    runtime: {image: registry.example.com/runtime:v1}\n", "", 1),
			want: "names no runtime to render",
		},
		"a runtime with no image": {
			text: strings.Replace(runtimeWalk, "runtime: {image: registry.example.com/runtime:v1}", "runtime: {name: runtime-b}", 1),
			want: "declares no image",
		},
	}
	for name, tc := range parse {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(tc.text), Vocabulary{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parse error = %v, want one naming %q", err, tc.want)
			}
		})
	}
}

// pushUnderDrift is a service rendering a recorded revision of its runtime
// whose own image is pushed while the live runtime has drifted, then synced.
const pushUnderDrift = `scenario: push-under-drift
cells: ["run/example-cell"]
initial:
  service:
    runtime: {image: registry.example.com/runtime:v1, autoSync: false}
    components:
      engine: {replicas: 2, image: registry.example.com/engine:v1}
    groups:
      - components: [engine]
        blueGreen: {}
  config: {requeue: 10s}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - runtime.drifted: {image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/engine:v2}
  - tick: 4
    events:
      - ctrl.resync
  - tick: 5
    events:
      - runtime.synced[annotation]
  - tick: 6
    events:
      - ctrl.resync
`

// A push of the service's own image under a drifted recorded runtime moves
// the generation and nothing else: no pass runs, so the projection does not
// carry the push and the replica keeps publishing version 1 as its target.
// The pass after the sync projects the push, the replica records it on the
// publication after that, and the run opens.
func TestAPushUnderADriftIsRecordedAfterTheSync(t *testing.T) {
	trace := runText(t, pushUnderDrift)
	wantLines(t, trace,
		"tick=3 apply spec.revision[new] component=engine image=registry.example.com/engine:v2 projection=after-sync generation=2",
		"tick=3 pass skipped reason=RuntimeDrifted",
		"tick=4 pass skipped reason=RuntimeDrifted",
		"tick=5 ir patch name=svc-a-engine stage=project image=registry.example.com/engine:v1→registry.example.com/engine:v2",
		"tick=6 run outcome=opened",
	)
	// A member publishes a line only when its status moves, so the first
	// publish after the seed is the one that records the push. It belongs
	// on tick 6, the publication after the pass that projected the push,
	// not on tick 3, the publication after the push itself.
	tick, current, update := firstPublishAfterTheSeed(t, trace, "engine")
	if tick != 6 || current == update {
		t.Errorf("the push was recorded on tick %d (current=%s update=%s), want tick 6 with a new update revision\n%s", tick, current, update, trace)
	}
}

// firstPublishAfterTheSeed is the tick, current revision and update revision
// of component's first publish line after tick 0; zero and empty when the
// trace carries none.
func firstPublishAfterTheSeed(t *testing.T, trace, component string) (tick int, current, update string) {
	t.Helper()
	publishLine := " publish component=" + component + " "
	for _, line := range strings.Split(trace, "\n") {
		if strings.HasPrefix(line, "tick=0 ") || !strings.Contains(line, publishLine) {
			continue
		}
		fields := strings.Fields(line)
		n, err := strconv.Atoi(strings.TrimPrefix(fields[0], "tick="))
		if err != nil {
			t.Fatalf("publish line with no tick: %q", line)
		}
		for _, field := range fields[1:] {
			current = firstValue(current, field, "current=")
			update = firstValue(update, field, "update=")
		}
		return n, current, update
	}
	return 0, "", ""
}

// firstValue keeps have unless it is empty and field carries the key.
func firstValue(have, field, key string) string {
	if have != "" || !strings.HasPrefix(field, key) {
		return have
	}
	return strings.TrimPrefix(field, key)
}
