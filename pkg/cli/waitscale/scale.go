// Package waitscale evaluates an exact InferenceReplica spec/status snapshot.
// A match is current desired-and-logical Instance count, not readiness,
// durable parent intent, or attribution to a preceding scale action.
package waitscale

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

const (
	ReasonMatched     waitengine.Reason = "ReplicaScaleMatched"
	ReasonNotMatched  waitengine.Reason = "ReplicaScaleNotMatched"
	ReasonNotRecorded waitengine.Reason = "ReplicaScaleNotRecorded"
	ReasonInvalid     waitengine.Reason = "InvalidReplicaScaleEvidence"
	ReasonUnsupported waitengine.Reason = "UnsupportedReplicaScaleTarget"
	// Match the CLI instance codec's absolute expansion ceiling. A 4,000-row
	// compacted status must remain usable for scale diagnostics.
	maxStatusRows = 20000
)

// Target optionally binds the wait to the exact IR name and UID returned by a
// scale action. With neither, the current parent scaleTargetRef selects it.
type Target struct {
	Component ome.ComponentType
	Replicas  int32
	IRName    string
	IRUID     types.UID
}

// Evidence contains only a named parent and, when selected, a named IR read.
// No namespace list or sibling inference is needed.
type Evidence struct {
	Parent  *ome.InferenceService
	Replica *ome.InferenceReplica
}

var errRawEvidence = errors.New("raw scale wait evidence is not reportable")

func (Evidence) MarshalJSON() ([]byte, error) { return nil, errRawEvidence }
func (Evidence) MarshalYAML() (any, error)    { return nil, errRawEvidence }
func (Evidence) String() string               { return "<waitscale.Evidence redacted>" }
func (Evidence) GoString() string             { return "<waitscale.Evidence redacted>" }

// Observation is a safe, typed summary. CurrentReplicas counts logical
// Instances in any phase; ReadyReplicas is diagnostic, not the predicate.
type Observation struct {
	Component       ome.ComponentType `json:"component"`
	Requested       int32             `json:"requested"`
	SpecReplicas    *int32            `json:"specReplicas,omitempty"`
	CurrentReplicas *int32            `json:"currentReplicas,omitempty"`
	ReadyReplicas   *int32            `json:"readyReplicas,omitempty"`
	Encoding        string            `json:"encoding,omitempty"`
	Validity        string            `json:"validity"`
	Freshness       string            `json:"freshness"`
}

// Evaluator pins the first selected IR UID. A replacement cannot inherit an
// earlier observation even if the count happens to match.
type Evaluator struct {
	target Target
	name   string
	uid    types.UID
}

func NewEvaluator(target Target) *Evaluator { return &Evaluator{target: target} }

func (e *Evaluator) Evaluate(v Evidence) (waitengine.Decision, Observation) {
	o := Observation{Validity: "Unavailable", Freshness: "Unavailable"}
	if e == nil {
		return waitengine.Decision{Reason: ReasonInvalid}, o
	}
	o.Component, o.Requested = e.target.Component, e.target.Replicas
	invalid := func() (waitengine.Decision, Observation) {
		o.Validity = "Invalid"
		return waitengine.Decision{Reason: ReasonInvalid}, o
	}
	if !validTarget(e.target) {
		return invalid()
	}
	p := v.Parent
	if p == nil || !validIdentity(p.Name, p.Namespace, string(p.UID), p.ResourceVersion) || p.Generation <= 0 ||
		p.Kind != "" && p.Kind != "InferenceService" || p.APIVersion != "" && p.APIVersion != "ome.io/v1beta1" {
		return invalid()
	}
	if p.DeletionTimestamp != nil {
		return waitengine.Decision{Reason: ReasonNotRecorded}, o
	}
	status, found := p.Status.Components[e.target.Component]
	ref := status.ScaleTargetRef
	if found && ref != nil && (ref.APIVersion != "ome.io/v1beta1" || ref.Kind != "InferenceReplica") {
		return waitengine.Decision{Reason: ReasonUnsupported}, o
	}
	if e.target.IRName == "" && (!found || ref == nil) {
		return waitengine.Decision{Reason: ReasonNotRecorded}, o
	}
	selected := e.target.IRName
	if selected == "" {
		selected = ref.Name
	}
	if len(utilvalidation.IsDNS1123Subdomain(selected)) != 0 || ref != nil && ref.Name != selected {
		return invalid()
	}
	ir := v.Replica
	if ir == nil {
		return waitengine.Decision{Reason: ReasonNotRecorded}, o
	}
	if ir.Name != selected || ir.Namespace != p.Namespace || !safeIdentity(string(ir.UID)) || !safeIdentity(ir.ResourceVersion) || ir.Generation <= 0 ||
		ir.Kind != "" && ir.Kind != "InferenceReplica" || ir.APIVersion != "" && ir.APIVersion != "ome.io/v1beta1" ||
		ir.Spec.ParentRef.Name != p.Name || ir.Spec.Component != e.target.Component || ir.DeletionTimestamp != nil ||
		(e.target.IRUID != "" && ir.UID != e.target.IRUID) ||
		(e.name != "" && (e.name != ir.Name || e.uid != ir.UID)) ||
		ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] != strconv.FormatInt(p.Generation, 10) ||
		!ownedBy(ir, p) {
		return invalid()
	}
	e.name, e.uid = ir.Name, ir.UID
	if ir.Spec.Replicas == nil || *ir.Spec.Replicas < 0 || ir.Status.Replicas < 0 || ir.Status.ReadyReplicas < 0 || ir.Status.ReadyReplicas > ir.Status.Replicas ||
		ir.Status.ServingReplicas < 0 || ir.Status.ServingReplicas > ir.Status.Replicas ||
		ir.Status.AvailableReplicas < 0 || ir.Status.AvailableReplicas > ir.Status.Replicas ||
		ir.Status.UpdatedReplicas < 0 || ir.Status.UpdatedReplicas > ir.Status.Replicas ||
		ir.Status.UpdatedReadyReplicas < 0 || ir.Status.UpdatedReadyReplicas > ir.Status.UpdatedReplicas ||
		ir.Status.UpdatedReadyReplicas > ir.Status.ReadyReplicas {
		return invalid()
	}
	if ir.Status.ObservedGeneration <= 0 || ir.Status.ObservedGeneration != ir.Generation {
		o.Freshness = "Stale"
		return waitengine.Decision{Reason: ReasonNotRecorded}, o
	}
	rows, encoding, err := irstatus.DecodeStatus(&ir.Status, maxStatusRows)
	if err != nil || len(rows) > maxStatusRows || int32(len(rows)) != ir.Status.Replicas || !validRows(rows) {
		return invalid()
	}
	spec, current, ready := *ir.Spec.Replicas, ir.Status.Replicas, ir.Status.ReadyReplicas
	o.SpecReplicas, o.CurrentReplicas, o.ReadyReplicas = &spec, &current, &ready
	o.Encoding, o.Validity, o.Freshness = string(encoding), "Valid", "Current"
	if spec == e.target.Replicas && current == e.target.Replicas {
		return waitengine.Decision{Matched: true, Reason: ReasonMatched}, o
	}
	return waitengine.Decision{Reason: ReasonNotMatched}, o
}

func validTarget(t Target) bool {
	if t.Replicas < 1 || (t.Component != ome.EngineComponent && t.Component != ome.DecoderComponent && t.Component != ome.RouterComponent) ||
		(t.IRName == "") != (t.IRUID == "") {
		return false
	}
	return t.IRName == "" || len(utilvalidation.IsDNS1123Subdomain(t.IRName)) == 0 && safeIdentity(string(t.IRUID))
}

func validIdentity(name, namespace, uid, rv string) bool {
	return len(utilvalidation.IsDNS1123Subdomain(name)) == 0 && len(utilvalidation.IsDNS1123Label(namespace)) == 0 && safeIdentity(uid) && safeIdentity(rv)
}

func safeIdentity(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsFunc(value, unicode.IsControl)
}

func ownedBy(ir *ome.InferenceReplica, p *ome.InferenceService) bool {
	if len(ir.OwnerReferences) == 0 || len(ir.OwnerReferences) > 16 {
		return false
	}
	controllers := 0
	for _, ref := range ir.OwnerReferences {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if ref.APIVersion != "ome.io/v1beta1" || ref.Kind != "InferenceService" || ref.Name != p.Name || ref.UID != p.UID {
			return false
		}
		controllers++
	}
	return controllers == 1
}

func validRows(rows []ome.OMENativeInstanceStatus) bool {
	seen := make(map[int32]bool, len(rows))
	for _, row := range rows {
		if row.Index < 0 || seen[row.Index] {
			return false
		}
		switch row.Phase {
		case ome.OMENativeInstancePending, ome.OMENativeInstanceCreating, ome.OMENativeInstanceReady,
			ome.OMENativeInstanceUpdating, ome.OMENativeInstanceRestarting, ome.OMENativeInstanceMigrating,
			ome.OMENativeInstanceFailed, ome.OMENativeInstanceDeleting:
		default:
			return false
		}
		seen[row.Index] = true
	}
	return true
}
