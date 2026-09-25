package replay

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// ClockAdvance is the one event id a scenario may use that the behavior
// tables do not declare. It moves the injected clock and touches nothing
// else, so it belongs to the harness rather than to the machine's event
// vocabulary.
const ClockAdvance = "clock.advance"

// DefaultRunner is the Runner name of a single-pod Instance, and the
// value a pod reference omitting the field resolves to.
const DefaultRunner = "default"

// RunnerLeader and RunnerWorker are the Runner names of a multi-pod
// Instance. The plan splits an Instance into one leader and the worker
// count the scenario asks for.
const (
	RunnerLeader = "leader"
	RunnerWorker = "worker"
)

// Vocabulary is the behavior table a scenario is checked against. The
// caller builds it from the machine definition so a scenario cannot name
// an event, variant, arrow or cell the table does not declare. A zero
// Vocabulary disables the corresponding check, which is what a caller
// driving the engine outside the table wants.
type Vocabulary struct {
	// Events maps an event id to its declared variants. An id present
	// with no variants accepts only the unqualified form.
	Events map[string][]string
	// Arrows is the set of transition ids.
	Arrows map[string]struct{}
	// Cells is the set of canonical grid cell ids.
	Cells map[string]struct{}
}

// Scenario is one replayable timeline: the table coordinates it claims to
// drive, the state the engine starts from, and the ticks it is driven
// through.
type Scenario struct {
	// Scenario names the run. By convention it is the id of the arrow the
	// run is written for; a name that starts with "T-" is checked against
	// the table's transitions.
	Scenario string `json:"scenario"`
	// Arrows lists every transition the run drives, including the one the
	// scenario is named for. A run that drives no transition — a hold
	// parking a deadline, an ignored event — cites cells instead.
	Arrows []string `json:"arrows,omitempty"`
	// Cells lists the canonical grid cell ids the run drives.
	Cells []string `json:"cells,omitempty"`
	// Intentional records why a golden change is expected, for the review
	// rule that an unexplained golden diff is a defect.
	Intentional string  `json:"intentional,omitempty"`
	Initial     Initial `json:"initial"`
	Timeline    []Tick  `json:"timeline"`
}

// Initial is the state the first tick observes.
type Initial struct {
	Spec   SpecState   `json:"spec"`
	Config ConfigState `json:"config,omitempty"`
	Rows   []RowSpec   `json:"rows,omitempty"`
	Pods   []PodSpec   `json:"pods,omitempty"`
	// RetryBlocks seeds the owner's per-revision retry authority.
	RetryBlocks []RetryBlockSpec `json:"retryBlocks,omitempty"`
}

// SpecState is the part of the owner spec a scenario edits. It is the
// driver's mutable copy: every tick rebuilds the plan and the target
// revision from it, so a spec event is a field write and nothing more.
type SpecState struct {
	Replicas        int32  `json:"replicas"`
	Strategy        string `json:"strategy,omitempty"`
	RestartPolicy   string `json:"restartPolicy,omitempty"`
	MinReadySeconds int32  `json:"minReadySeconds,omitempty"`
	// Image is the single container image of the rendered pod template.
	// A spec.revision event rewrites it, which mints a new revision the
	// way an ordinary template edit does.
	Image string `json:"image,omitempty"`
	// ContainerName names the single container of the rendered pod
	// template, overriding the fixture default. The restart trigger reads
	// restart evidence off the runner container alone, and identifies it
	// by the name production renders it under, so a scenario driving a
	// container restart has to render that name.
	ContainerName string `json:"containerName,omitempty"`
	// Env is the environment the rendered container carries. It is part
	// of the pod template and therefore of the revision, and unlike the
	// image it is not an in-place-eligible diff — which is what makes an
	// InPlaceOnly rejection reachable.
	Env map[string]string `json:"env,omitempty"`
	// MaxUnavailable and MaxSurge are the rolling-update budgets, as an
	// int-or-percent string ("1", "25%"). Empty leaves the engine default.
	MaxUnavailable string `json:"maxUnavailable,omitempty"`
	MaxSurge       string `json:"maxSurge,omitempty"`
	// InstanceReadyTimeout bounds a newly created Instance becoming Ready
	// and is what an operation deadline is derived from.
	InstanceReadyTimeout string `json:"instanceReadyTimeout,omitempty"`
	MarkNotReady         *bool  `json:"markNotReady,omitempty"`
	Paused               bool   `json:"paused,omitempty"`
	PauseFreeze          bool   `json:"pauseFreeze,omitempty"`
	// Workers is the per-Instance worker-pod count. A positive value
	// makes the Component multi-pod — one "leader" Runner and a "worker"
	// Runner of this size — which is the shape a gang is scheduled in.
	// A spec.gangWidth event rewrites it.
	Workers int32 `json:"workers,omitempty"`
	// TopologyKey is the node-label key a gang's workers are co-located
	// on. A multi-pod Component without one renders workers with no
	// co-location affinity, which the engine warns about once per
	// process — a one-shot a replay cannot reproduce, so a gang scenario
	// declares the key.
	TopologyKey string `json:"topologyKey,omitempty"`
	// SchedulerName is the scheduler the rendered pods ask for. A gang
	// announced to a Component whose pods name no gang-aware scheduler
	// draws a Warning the engine emits once per process, which a replay
	// cannot reproduce twice.
	SchedulerName string `json:"schedulerName,omitempty"`
	// GangScheduling advertises the scheduler-plugins PodGroup CRD as
	// installed, which is what makes a multi-pod Instance's PodGroup be
	// announced before its first member.
	GangScheduling bool `json:"gangScheduling,omitempty"`
	// MigrationMode is lifecycle.migrationPolicy.mode. Empty leaves the
	// engine default.
	MigrationMode string `json:"migrationMode,omitempty"`
}

// ConfigState carries the operator configuration the engine reads through
// ReconcileInput. Every field is explicit: the engine's unconfigured
// behavior differs from its configured behavior, and a scenario has to be
// able to pin either.
type ConfigState struct {
	RequeueOperation         string       `json:"requeueOperation,omitempty"`
	RequeueGate              string       `json:"requeueGate,omitempty"`
	ScaleDownRequeueInterval string       `json:"scaleDownRequeueInterval,omitempty"`
	StuckPodGrace            string       `json:"stuckPodGrace,omitempty"`
	UnschedulableGrace       string       `json:"unschedulableGrace,omitempty"`
	UpdateRetry              *RetrySpec   `json:"updateRetry,omitempty"`
	ForceDelete              *ForceDelete `json:"forceDelete,omitempty"`
	// MigrationAudit bounds migration admission. Unset leaves the
	// engine unconfigured, which holds every request rather than
	// admitting it unbounded.
	MigrationAudit *MigrationAudit `json:"migrationAudit,omitempty"`
	// AutoMigrateBudget is lifecycle.autoMigrate.maxAttempts, the
	// per-Instance relocation budget the disposition spends. Unset
	// leaves the engine unconfigured, which never records a directive.
	AutoMigrateBudget int32 `json:"autoMigrateBudget,omitempty"`
}

// MigrationAudit is the migration admission budget.
type MigrationAudit struct {
	MaxInFlight  int32  `json:"maxInFlight"`
	MaxPerWindow int32  `json:"maxPerWindow"`
	Window       string `json:"window"`
}

// RetrySpec is the same-target update retry budget.
type RetrySpec struct {
	MaxAttempts  int32   `json:"maxAttempts"`
	InitialDelay string  `json:"initialDelay"`
	MaxDelay     string  `json:"maxDelay"`
	Multiplier   float64 `json:"multiplier"`
}

// ForceDelete is the stuck-Terminating force-delete policy.
type ForceDelete struct {
	OverdueSlack             string `json:"overdueSlack"`
	NodeUnreachableThreshold string `json:"nodeUnreachableThreshold"`
}

// RowSpec seeds one persisted InstanceStatus. A seeded row carries no
// in-flight operation: a scenario reaches an operation by driving the
// engine into it, so the row it acts on is one the engine itself wrote
// and cannot contradict an invariant the engine maintains.
type RowSpec struct {
	Index       int32  `json:"index"`
	Incarnation int64  `json:"incarnation,omitempty"`
	Phase       string `json:"phase,omitempty"`
	// RunningRevision names the revision the row's pods run, as the
	// symbolic "current" (what the initial spec renders) or "previous"
	// (what PreviousImage renders).
	RunningRevision string `json:"runningRevision,omitempty"`
	ActiveOrdinal   int32  `json:"activeOrdinal,omitempty"`
	ReadySince      string `json:"readySince,omitempty"`
}

// PodSpec seeds one pod the engine observes at the first tick.
type PodSpec struct {
	Index       int32  `json:"index"`
	Runner      string `json:"runner,omitempty"`
	Ordinal     int32  `json:"ordinal,omitempty"`
	Incarnation int64  `json:"incarnation,omitempty"`
	// PreviousImage renders the pod from an older template, so it carries
	// a revision hash the current spec no longer targets. Empty renders
	// the pod on the revision the initial spec mints.
	PreviousImage   string `json:"previousImage,omitempty"`
	Node            string `json:"node,omitempty"`
	Phase           string `json:"phase,omitempty"`
	ContainersReady bool   `json:"containersReady,omitempty"`
	Ready           bool   `json:"ready,omitempty"`
	Serving         bool   `json:"serving,omitempty"`
	// Routed puts the pod in the headless Service's EndpointSlice as a
	// ready endpoint.
	Routed bool `json:"routed,omitempty"`
	// ReadyAt offsets the Ready condition's transition time from the
	// scenario start, so a minReadySeconds window can be seeded open.
	ReadyAt string `json:"readyAt,omitempty"`
}

// RetryBlockSpec seeds one per-revision retry block.
type RetryBlockSpec struct {
	// Revision names the blocked revision, as the symbolic "current".
	Revision        string `json:"revision"`
	State           string `json:"state"`
	AttemptsStarted int32  `json:"attemptsStarted,omitempty"`
	NextRetryAt     string `json:"nextRetryAt,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

// Tick is one reconcile: the events applied first, then exactly one call
// into workload.Reconcile.
type Tick struct {
	Tick   int             `json:"tick"`
	Events []TimelineEvent `json:"events,omitempty"`
	// Note is carried into the trace so a golden reads as a walk through
	// the table rather than a list of writes.
	Note string `json:"note,omitempty"`
}

// TimelineEvent is one observation handed to the engine before a tick's
// reconcile. ID and Variant are the table's own event coordinates.
type TimelineEvent struct {
	ID      string
	Variant string
	Args    EventArgs
}

// EventArgs is the union of every event's parameters. Each event declares
// the subset it reads in eventArgKeys, and the parser rejects an event
// carrying anything outside that subset.
type EventArgs struct {
	Pod       *PodRef `json:"pod,omitempty"`
	Duration  string  `json:"duration,omitempty"`
	Image     string  `json:"image,omitempty"`
	To        *int32  `json:"to,omitempty"`
	Count     int     `json:"count,omitempty"`
	Container string  `json:"container,omitempty"`
	Message   string  `json:"message,omitempty"`
	// Env rewrites the rendered container's environment. It is the
	// template edit a spec.revision makes when the edit is not an image
	// rewrite.
	Env map[string]string `json:"env,omitempty"`
	// Verb and Resource scope an injected apiserver rejection.
	Verb     string `json:"verb,omitempty"`
	Resource string `json:"resource,omitempty"`
	// Slack pushes the clock past a deadline rather than exactly onto it.
	// Required by the timer events: how far past a deadline an
	// observation lands is a property of the scenario, not a default the
	// harness is entitled to invent.
	Slack string `json:"slack,omitempty"`
	// Strategy is the new lifecycle.updateStrategy of a spec.strategy edit.
	Strategy string `json:"strategy,omitempty"`
	// Instance names the Instance index a migration request is made
	// against.
	Instance *int32 `json:"instance,omitempty"`
	// FromNode is the node a migration request moves its source off.
	FromNode string `json:"fromNode,omitempty"`
	// UUID identifies a migration request. Empty numbers the request in
	// the order the scenario makes them.
	UUID string `json:"uuid,omitempty"`
	// Reason is the requester-supplied reason on a migration request.
	Reason string `json:"reason,omitempty"`
}

// PodRef addresses one pod by the coordinates its stable name is built
// from.
type PodRef struct {
	Index   int32  `json:"index"`
	Runner  string `json:"runner,omitempty"`
	Ordinal int32  `json:"ordinal,omitempty"`
}

// RunnerName resolves the Runner, defaulting to the single-pod Runner.
func (p PodRef) RunnerName() string {
	if p.Runner == "" {
		return DefaultRunner
	}
	return p.Runner
}

// UnmarshalJSON accepts the one-key mapping form the timeline uses:
// `{pod.deleted[source]: {pod: {index: 0}}}`, or a bare duration scalar
// for the events that take one.
func (e *TimelineEvent) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("event: expected a one-key mapping: %w", err)
	}
	if len(raw) != 1 {
		return fmt.Errorf("event: expected exactly one key, got %d", len(raw))
	}
	var key string
	var body json.RawMessage
	for k, v := range raw {
		key, body = k, v
	}
	id, variant, err := splitEventKey(key)
	if err != nil {
		return err
	}
	e.ID, e.Variant = id, variant
	if len(body) == 0 || string(body) == "null" {
		return nil
	}
	// A scalar body is the event's duration, which is how clock.advance
	// and the timer events read in a timeline.
	var scalar string
	if err := json.Unmarshal(body, &scalar); err == nil {
		e.Args.Duration = scalar
		return nil
	}
	if err := json.Unmarshal(body, &e.Args); err != nil {
		return fmt.Errorf("event %s: %w", key, err)
	}
	// The union is decoded strictly: an argument the event has no use for
	// is a scenario that does not do what it reads as, so it fails to load
	// rather than being silently dropped.
	var seen map[string]json.RawMessage
	if err := json.Unmarshal(body, &seen); err != nil {
		return fmt.Errorf("event %s: %w", key, err)
	}
	allowed := eventArgKeys[id]
	for name := range seen {
		if !contains(allowed, name) {
			return fmt.Errorf("event %s: argument %q is not one of %s", key, name, strings.Join(allowed, ", "))
		}
	}
	return nil
}

// eventArgKeys is the argument each event reads. An event absent from the
// map takes no arguments.
var eventArgKeys = map[string][]string{
	ClockAdvance:              {"duration"},
	"pod.scheduled":           {"pod", "message"},
	"pod.containersReady":     {"pod"},
	"pod.ready":               {"pod"},
	"pod.servingOn":           {"pod"},
	"pod.servingOff":          {"pod"},
	"pod.terminating":         {"pod"},
	"pod.stuckTerminating":    {"pod"},
	"pod.deleted":             {"pod"},
	"pod.phaseTerminal":       {"pod"},
	"pod.waiting":             {"pod", "container", "message"},
	"pod.unschedulable":       {"pod"},
	"pod.containerRestart":    {"pod", "container", "message"},
	"endpoint.rotationIn":     {"pod"},
	"endpoint.rotationOut":    {"pod"},
	"api.quotaDenied":         {"verb", "resource", "count"},
	"api.invalid":             {"verb", "resource", "count"},
	"spec.revision":           {"image", "env"},
	"spec.strategy":           {"strategy"},
	"spec.replicasUp":         {"to"},
	"spec.replicasDown":       {"to"},
	"spec.gangWidth":          {"to"},
	"spec.rollbackTarget":     {"image"},
	"spec.migrateRequest":     {"instance", "fromNode", "uuid", "reason"},
	"gang.podGroup":           {"instance"},
	"timer.operationDeadline": {"slack"},
	"timer.stuckPodGrace":     {"slack"},
	"timer.retryAt":           {"slack"},
	"timer.migrationDeadline": {"slack"},
	"timer.forceDelete":       {"slack"},
}

// Key renders the event the way a timeline and a trace both spell it.
func (e TimelineEvent) Key() string {
	if e.Variant == "" {
		return e.ID
	}
	return e.ID + "[" + e.Variant + "]"
}

func splitEventKey(key string) (id, variant string, err error) {
	open := strings.IndexByte(key, '[')
	if open < 0 {
		return key, "", nil
	}
	if !strings.HasSuffix(key, "]") {
		return "", "", fmt.Errorf("event %q: unterminated variant", key)
	}
	return key[:open], key[open+1 : len(key)-1], nil
}

// Parse decodes one scenario and checks it against the table. Unknown
// YAML fields are rejected so a mistyped key fails loudly rather than
// silently changing nothing.
func Parse(data []byte, vocab Vocabulary) (*Scenario, error) {
	s := &Scenario{}
	if err := yaml.UnmarshalStrict(data, s); err != nil {
		return nil, fmt.Errorf("replay: parse scenario: %w", err)
	}
	if err := s.validate(vocab); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Scenario) validate(vocab Vocabulary) error {
	if s.Scenario == "" {
		return fmt.Errorf("replay: scenario has no name")
	}
	if len(s.Arrows) == 0 && len(s.Cells) == 0 {
		return fmt.Errorf("replay: %s: cites neither an arrow nor a cell", s.Scenario)
	}
	if len(s.Timeline) == 0 {
		return fmt.Errorf("replay: %s: empty timeline", s.Scenario)
	}
	if vocab.Arrows != nil {
		for _, arrow := range s.Arrows {
			if _, ok := vocab.Arrows[arrow]; !ok {
				return fmt.Errorf("replay: %s: arrow %q is not in the table", s.Scenario, arrow)
			}
		}
		if strings.HasPrefix(s.Scenario, "T-") {
			if _, ok := vocab.Arrows[s.Scenario]; !ok {
				return fmt.Errorf("replay: %s: scenario name reads as an arrow but is not in the table", s.Scenario)
			}
		}
	}
	if vocab.Cells != nil {
		for _, cell := range s.Cells {
			if _, ok := vocab.Cells[cell]; !ok {
				return fmt.Errorf("replay: %s: cell %q is not in the grid", s.Scenario, cell)
			}
		}
	}
	for i, tick := range s.Timeline {
		if tick.Tick != i+1 {
			return fmt.Errorf("replay: %s: timeline entry %d declares tick %d; ticks are consecutive from 1", s.Scenario, i+1, tick.Tick)
		}
		for _, ev := range tick.Events {
			if err := validateEvent(s.Scenario, tick.Tick, ev, vocab); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateEvent(scenario string, tick int, ev TimelineEvent, vocab Vocabulary) error {
	if ev.ID == ClockAdvance {
		if ev.Variant != "" {
			return fmt.Errorf("replay: %s tick %d: %s takes no variant", scenario, tick, ClockAdvance)
		}
		if ev.Args.Duration == "" {
			return fmt.Errorf("replay: %s tick %d: %s needs a duration", scenario, tick, ClockAdvance)
		}
		return nil
	}
	if vocab.Events != nil {
		variants, declared := vocab.Events[ev.ID]
		if !declared {
			return fmt.Errorf("replay: %s tick %d: event %q is not declared in the table", scenario, tick, ev.ID)
		}
		if ev.Variant != "" && !contains(variants, ev.Variant) {
			return fmt.Errorf("replay: %s tick %d: event %s has no variant %q (declared: %s)",
				scenario, tick, ev.ID, ev.Variant, strings.Join(variants, ", "))
		}
	}
	if _, known := eventAppliers[ev.ID]; !known {
		return fmt.Errorf("replay: %s tick %d: event %s is declared but the driver does not apply it", scenario, tick, ev.ID)
	}
	return nil
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// ParseDuration reads a scenario duration field. An empty string is the
// zero duration, which every caller treats as "unset".
func ParseDuration(field, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("replay: %s: %w", field, err)
	}
	return d, nil
}

// SortedEventIDs returns the event ids the driver can apply, for the
// error message a scenario author reads when one is missing.
func SortedEventIDs() []string {
	out := make([]string, 0, len(eventAppliers)+1)
	out = append(out, ClockAdvance)
	for id := range eventAppliers {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
