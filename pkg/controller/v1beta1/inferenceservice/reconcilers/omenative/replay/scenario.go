package replay

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ClockAdvance is the one event id a scenario may use that no machine
// declares in its vocabulary. It moves the injected clock and touches nothing
// else, so it belongs to the harness rather than to a machine's vocabulary.
const ClockAdvance = "clock.advance"

// Vocabulary is the set of event ids, variants and cell ids a scenario may
// cite: the union of what the run, coordination, canary and revision
// machines declare. A zero Vocabulary disables the check.
type Vocabulary struct {
	// Events maps an event id to its declared variants, unioned over the
	// machines that declare the id.
	Events map[string][]string
	// Cells is the set of machine-prefixed cell ids.
	Cells map[string]struct{}
}

// Scenario is one replayable timeline: the cells it claims to drive, the
// state the first pass observes, and the ticks it is driven through.
type Scenario struct {
	// Scenario names the run; the file name is the scenario name.
	Scenario string `json:"scenario"`
	// Cells lists the machine-prefixed cell ids the run drives.
	Cells []string `json:"cells"`
	// Intentional records why a golden change is expected.
	Intentional string  `json:"intentional,omitempty"`
	Initial     Initial `json:"initial"`
	Timeline    []Tick  `json:"timeline"`
}

// Initial is the state the first pass observes.
type Initial struct {
	Service ServiceState `json:"service"`
	// Policies are the RolloutPolicy objects in the service's namespace.
	Policies []PolicySpec `json:"policies,omitempty"`
	Config   ConfigState  `json:"config,omitempty"`
	// Replicas is the status each Component's InferenceReplica publishes
	// before the first pass, keyed by Component. A Component the service
	// declares and the map omits publishes one Ready Instance per replica
	// on the Component's image.
	Replicas map[string]ReplicaState `json:"replicas,omitempty"`
}

// ServiceState is the part of the InferenceService spec a scenario edits:
// the driver's mutable copy, re-projected on every pass.
type ServiceState struct {
	// Components declares the engine, decoder and router the service runs.
	Components map[string]ComponentState `json:"components"`
	// Groups is spec.rollout.groups, in the API's own spelling.
	Groups []v1beta1.RolloutGroup `json:"groups,omitempty"`
	// PairingProtocol is spec.rollout.pairingProtocol.
	PairingProtocol *string `json:"pairingProtocol,omitempty"`
	// Annotations are the service's operator annotations at the start.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ComponentState is one Component's declaration.
type ComponentState struct {
	// Replicas is the Component's minReplicas, the steady Instance count.
	Replicas int32 `json:"replicas"`
	// Image is the single container image the Component's template renders;
	// a revision is named by the image whose template mints it.
	Image string `json:"image"`
	// Partition is the user's lifecycle.updateStrategy.rollingUpdate.partition.
	Partition *int32 `json:"partition,omitempty"`
}

// PolicySpec is one RolloutPolicy object.
type PolicySpec struct {
	Name string                    `json:"name"`
	Spec v1beta1.RolloutPolicySpec `json:"spec"`
}

// ConfigState is the operator configuration the pass reads. Every field is
// explicit: an unconfigured cadence asks for the rate-limited requeue, an
// unconfigured timeout never escalates.
type ConfigState struct {
	// Requeue and ParkedRequeue are rollout.canaryRequeue and
	// rollout.canaryParkedRequeue.
	Requeue       string `json:"requeue,omitempty"`
	ParkedRequeue string `json:"parkedRequeue,omitempty"`
	// DefaultReadyTimeout is rollout.defaultReadyTimeout.
	DefaultReadyTimeout string `json:"defaultReadyTimeout,omitempty"`
	// RolloutPolicyEnabled is the policy feature gate: references resolve
	// only when it is on.
	RolloutPolicyEnabled bool `json:"rolloutPolicyEnabled,omitempty"`
	// BoundProviders are the metric provider names bound on the cluster.
	BoundProviders []string `json:"boundProviders,omitempty"`
	// TrafficWeightDeadbandPercent is coordination.trafficWeightDeadbandPercent.
	TrafficWeightDeadbandPercent int32 `json:"trafficWeightDeadbandPercent,omitempty"`
	// RatioTolerancePercent is coordination.defaultRatioTolerancePercent.
	RatioTolerancePercent *int32 `json:"ratioTolerancePercent,omitempty"`
}

// ReplicaState is what one Component's InferenceReplica publishes before
// the first pass. A revision names the image whose template minted it;
// "current" is the Component's image.
type ReplicaState struct {
	// Current and Target are the replica's current and update revisions.
	// Empty Target reads as Current; empty Current reads as the image.
	Current string `json:"current,omitempty"`
	Target  string `json:"target,omitempty"`
	// Rows are the dense per-Instance rows; absent rows are one Ready row
	// per replica on Current.
	Rows []RowSpec `json:"rows,omitempty"`
	// Pods are the member's pods; absent pods are one Ready pod per row on
	// the row's running revision.
	Pods []PodSpec `json:"pods,omitempty"`
	// Stale publishes an ObservedGeneration behind the replica's generation.
	Stale bool `json:"stale,omitempty"`
}

// RowSpec seeds one per-Instance row.
type RowSpec struct {
	Index           int32  `json:"index"`
	Phase           string `json:"phase,omitempty"`
	RunningRevision string `json:"runningRevision,omitempty"`
	TargetRevision  string `json:"targetRevision,omitempty"`
	// ReadySince is an offset from the scenario start.
	ReadySince string `json:"readySince,omitempty"`
}

// PodSpec seeds one pod of a member.
type PodSpec struct {
	Index   int32 `json:"index"`
	Ordinal int32 `json:"ordinal,omitempty"`
	// Revision is the image whose template the pod runs; empty is the row's
	// running revision.
	Revision string `json:"revision,omitempty"`
	Ready    bool   `json:"ready,omitempty"`
	// Serving is the controller-managed serving gate; a Ready pod serves
	// unless the scenario says otherwise.
	Serving *bool `json:"serving,omitempty"`
}

// Tick is one pass: the events applied first, then exactly one pass of the
// InferenceService controller's OMENative order.
type Tick struct {
	Tick   int             `json:"tick"`
	Events []TimelineEvent `json:"events,omitempty"`
	// Note is carried into the trace as a claim about the tick.
	Note string `json:"note,omitempty"`
}

// TimelineEvent is one observation staged before a tick's pass. ID and
// Variant are a machine's own event coordinates.
type TimelineEvent struct {
	ID      string
	Variant string
	Args    EventArgs
}

// EventArgs is the union of every event's parameters. Each event declares
// the subset it reads in eventArgKeys; the parser rejects anything else.
type EventArgs struct {
	// Component names the member an event is about.
	Component string `json:"component,omitempty"`
	// Image names a revision by the image whose template mints it.
	Image string `json:"image,omitempty"`
	// Index names an Instance.
	Index *int32 `json:"index,omitempty"`
	// To is the new count of a replica or partition edit.
	To *int32 `json:"to,omitempty"`
	// Count is how many writes a refusal answers.
	Count int `json:"count,omitempty"`
	// Write is the ordinal, within the pass, of the status write a
	// conflict answers.
	Write int `json:"write,omitempty"`
	// Value is an annotation value.
	Value string `json:"value,omitempty"`
	// Duration is the clock move of clock.advance.
	Duration string `json:"duration,omitempty"`
	// Slack pushes the clock past a deadline rather than exactly onto it.
	Slack string `json:"slack,omitempty"`
	// Name names a RolloutPolicy.
	Name string `json:"name,omitempty"`
	// Steps is the traffic ladder of a policy or inline canary body edit.
	Steps []int32 `json:"steps,omitempty"`
	// Groups replaces spec.rollout.groups on a plan edit.
	Groups []v1beta1.RolloutGroup `json:"groups,omitempty"`
	// Trailing is how many Instances still trail the target.
	Trailing int32 `json:"trailing,omitempty"`
	// Verb and Resource scope an armed apiserver refusal.
	Verb     string `json:"verb,omitempty"`
	Resource string `json:"resource,omitempty"`
}

// UnmarshalJSON accepts the one-key mapping form the timeline uses, a
// bare event key for an event that takes no arguments, or a bare duration
// scalar for the events that take one.
func (e *TimelineEvent) UnmarshalJSON(data []byte) error {
	var bare string
	if err := json.Unmarshal(data, &bare); err == nil {
		id, variant, err := splitEventKey(bare)
		if err != nil {
			return err
		}
		e.ID, e.Variant = id, variant
		return nil
	}
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
	var scalar string
	if err := json.Unmarshal(body, &scalar); err == nil {
		e.Args.Duration = scalar
		return nil
	}
	if err := json.Unmarshal(body, &e.Args); err != nil {
		return fmt.Errorf("event %s: %w", key, err)
	}
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

// Parse decodes one scenario and checks it against the event vocabulary. Unknown
// YAML fields are rejected so a mistyped key fails loudly.
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
	if len(s.Cells) == 0 {
		return fmt.Errorf("replay: %s: cites no cell", s.Scenario)
	}
	if len(s.Timeline) == 0 {
		return fmt.Errorf("replay: %s: empty timeline", s.Scenario)
	}
	if len(s.Initial.Service.Components) == 0 {
		return fmt.Errorf("replay: %s: the service declares no Component", s.Scenario)
	}
	for name := range s.Initial.Service.Components {
		if !knownComponent(name) {
			return fmt.Errorf("replay: %s: component %q is not engine, decoder or router", s.Scenario, name)
		}
	}
	for name := range s.Initial.Replicas {
		if _, ok := s.Initial.Service.Components[name]; !ok {
			return fmt.Errorf("replay: %s: replicas.%s names a Component the service does not declare", s.Scenario, name)
		}
	}
	if vocab.Cells != nil {
		for _, cell := range s.Cells {
			if _, ok := vocab.Cells[cell]; !ok {
				return fmt.Errorf("replay: %s: cell %q is not in the vocabulary", s.Scenario, cell)
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

func knownComponent(name string) bool {
	switch v1beta1.ComponentType(name) {
	case v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent:
		return true
	}
	return false
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
			return fmt.Errorf("replay: %s tick %d: event %q is not declared by any machine", scenario, tick, ev.ID)
		}
		if ev.Variant != "" && !contains(variants, ev.Variant) {
			return fmt.Errorf("replay: %s tick %d: event %s has no variant %q (declared: %s)",
				scenario, tick, ev.ID, ev.Variant, strings.Join(variants, ", "))
		}
	}
	if reason, produced := producedEvents[ev.ID]; produced {
		return fmt.Errorf("replay: %s tick %d: event %s is not staged by a scenario: %s", scenario, tick, ev.ID, reason)
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
// zero duration, which every caller treats as unset.
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

// SortedEventIDs returns the event ids the driver can stage.
func SortedEventIDs() []string {
	out := make([]string, 0, len(eventAppliers)+1)
	out = append(out, ClockAdvance)
	for id := range eventAppliers {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
