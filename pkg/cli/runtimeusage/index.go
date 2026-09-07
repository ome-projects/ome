// Package runtimeusage indexes explicit runtime references from an
// already-collected InferenceService snapshot. It performs no cluster reads
// and keeps references that cannot be attributed as bounded evidence.
package runtimeusage

import (
	"errors"
	"fmt"
	"sort"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/runtimegraph"
)

// InferenceServiceIdentity uniquely identifies an InferenceService in a
// cluster snapshot.
type InferenceServiceIdentity struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// ReferenceState classifies whether one InferenceService can be attributed to
// exactly one explicitly referenced runtime.
type ReferenceState string

const (
	// ReferenceResolved means the declared reference has one complete runtime
	// identity.
	ReferenceResolved ReferenceState = "Resolved"
	// ReferenceUnresolved means the service cannot be attributed to one runtime,
	// either because it requests automatic selection or because its declared
	// runtime was authoritatively absent from the supplied snapshot.
	ReferenceUnresolved ReferenceState = "Unresolved"
	// ReferenceInvalid means the object or declared reference is malformed.
	ReferenceInvalid ReferenceState = "Invalid"
	// ReferenceAmbiguous means a snapshot repeats an InferenceService identity,
	// so none of the competing observations can be treated as current.
	ReferenceAmbiguous ReferenceState = "Ambiguous"
)

// ReferenceReason is a bounded explanation for a non-resolved reference.
type ReferenceReason string

const (
	ReasonAutomaticSelection ReferenceReason = "AutomaticSelection"
	ReasonInvalidRuntimeName ReferenceReason = "InvalidRuntimeName"
	// ReasonInvalidKind is retained for source compatibility; controller-accurate
	// indexing treats unrecognized kinds as namespaced-first.
	ReasonInvalidKind ReferenceReason = "InvalidKind"
	// ReasonInvalidAPIGroup is retained for source compatibility; the controller
	// does not consult APIGroup during runtime lookup.
	ReasonInvalidAPIGroup           ReferenceReason = "InvalidAPIGroup"
	ReasonInvalidInferenceService   ReferenceReason = "InvalidInferenceService"
	ReasonDuplicateInferenceService ReferenceReason = "DuplicateInferenceService"
	ReasonRuntimeNotFound           ReferenceReason = "RuntimeNotFound"
)

// ReferenceEvidence is a safe, immutable-by-value observation of one service
// reference. Runtime is present only when State is Resolved. Occurrences is
// greater than one only for an ambiguous duplicate identity.
type ReferenceEvidence struct {
	InferenceService InferenceServiceIdentity `json:"inferenceService"`
	State            ReferenceState           `json:"state"`
	RuntimeName      string                   `json:"runtimeName,omitempty"`
	Runtime          *runtimegraph.Identity   `json:"runtime,omitempty"`
	Reason           ReferenceReason          `json:"reason,omitempty"`
	Occurrences      int                      `json:"occurrences"`
}

// Projection contains the explicit InferenceService users of one runtime.
type Projection struct {
	Runtime           runtimegraph.Identity      `json:"runtime"`
	InferenceServices []InferenceServiceIdentity `json:"inferenceServices"`
}

// ErrInvalidRuntimeIdentity indicates that a query is not one complete
// ServingRuntime or ClusterServingRuntime identity.
var ErrInvalidRuntimeIdentity = errors.New("runtime identity is invalid")

// Index is an immutable index over an InferenceService snapshot.
type Index struct {
	users      map[runtimegraph.Identity][]InferenceServiceIdentity
	references []ReferenceEvidence
}

// Build indexes explicit references against an already-collected complete
// runtime snapshot without mutating or retaining either input. Duplicate and
// malformed services remain visible through References but are never silently
// attributed to a runtime. ServingRuntime kinds resolve namespaced-only;
// ClusterServingRuntime kinds resolve cluster-first; and empty or unrecognized
// kinds resolve namespaced-first. APIGroup is ignored, matching the controller
// lookup.
func Build(services []omev1beta1.InferenceService, snapshot runtimegraph.Snapshot) *Index {
	runtimes := runtimeIdentitySet(snapshot)
	counts := make(map[InferenceServiceIdentity]int, len(services))
	for i := range services {
		identity := serviceIdentity(&services[i])
		if validServiceIdentity(identity) {
			counts[identity]++
		}
	}

	index := &Index{
		users:      make(map[runtimegraph.Identity][]InferenceServiceIdentity),
		references: make([]ReferenceEvidence, 0, len(services)),
	}
	ambiguous := make(map[InferenceServiceIdentity]struct{})
	for i := range services {
		service := &services[i]
		identity := serviceIdentity(service)
		if !validServiceIdentity(identity) {
			index.references = append(index.references, ReferenceEvidence{
				InferenceService: identity,
				State:            ReferenceInvalid,
				Reason:           ReasonInvalidInferenceService,
				Occurrences:      1,
			})
			continue
		}
		if counts[identity] > 1 {
			if _, emitted := ambiguous[identity]; emitted {
				continue
			}
			ambiguous[identity] = struct{}{}
			index.references = append(index.references, ReferenceEvidence{
				InferenceService: identity,
				State:            ReferenceAmbiguous,
				Reason:           ReasonDuplicateInferenceService,
				Occurrences:      counts[identity],
			})
			continue
		}

		evidence := classifyReference(identity, service.Spec.Runtime, runtimes)
		index.references = append(index.references, evidence)
		if evidence.State == ReferenceResolved {
			index.users[*evidence.Runtime] = append(index.users[*evidence.Runtime], identity)
		}
	}

	for runtime := range index.users {
		sort.Slice(index.users[runtime], func(i, j int) bool {
			return serviceIdentityLess(index.users[runtime][i], index.users[runtime][j])
		})
	}
	sort.Slice(index.references, func(i, j int) bool {
		return serviceIdentityLess(
			index.references[i].InferenceService,
			index.references[j].InferenceService,
		)
	})
	return index
}

// ForRuntime returns a defensive copy of every service that explicitly
// references runtime. A valid runtime with no users returns an empty slice.
func (i *Index) ForRuntime(runtime runtimegraph.Identity) (Projection, error) {
	if err := validateRuntimeIdentity(runtime); err != nil {
		return Projection{}, err
	}
	services := make([]InferenceServiceIdentity, len(i.users[runtime]))
	copy(services, i.users[runtime])
	return Projection{Runtime: runtime, InferenceServices: services}, nil
}

// References returns a defensive copy of every normalized reference
// observation, including unresolved, invalid, and ambiguous evidence.
func (i *Index) References() []ReferenceEvidence {
	result := make([]ReferenceEvidence, len(i.references))
	for index, evidence := range i.references {
		result[index] = evidence
		if evidence.Runtime != nil {
			runtime := *evidence.Runtime
			result[index].Runtime = &runtime
		}
	}
	return result
}

func classifyReference(
	identity InferenceServiceIdentity,
	reference *omev1beta1.ServingRuntimeRef,
	runtimes map[runtimegraph.Identity]struct{},
) ReferenceEvidence {
	evidence := ReferenceEvidence{InferenceService: identity, Occurrences: 1}
	if reference == nil {
		evidence.State = ReferenceUnresolved
		evidence.Reason = ReasonAutomaticSelection
		return evidence
	}
	if reference.Name == "" {
		evidence.State = ReferenceInvalid
		evidence.Reason = ReasonInvalidRuntimeName
		return evidence
	}
	evidence.RuntimeName = reference.Name

	cluster := runtimegraph.Identity{Kind: runtimegraph.KindClusterServingRuntime, Name: reference.Name}
	local := runtimegraph.Identity{
		Kind: runtimegraph.KindServingRuntime, Namespace: identity.Namespace, Name: reference.Name,
	}
	candidates := []runtimegraph.Identity{local, cluster}
	if reference.Kind != nil {
		switch runtimegraph.Kind(*reference.Kind) {
		case runtimegraph.KindServingRuntime:
			candidates = []runtimegraph.Identity{local}
		case runtimegraph.KindClusterServingRuntime:
			candidates = []runtimegraph.Identity{cluster, local}
		}
	}
	for _, candidate := range candidates {
		if _, exists := runtimes[candidate]; exists {
			runtime := candidate
			evidence.State = ReferenceResolved
			evidence.Runtime = &runtime
			return evidence
		}
	}
	evidence.State = ReferenceUnresolved
	evidence.Reason = ReasonRuntimeNotFound
	return evidence
}

func runtimeIdentitySet(snapshot runtimegraph.Snapshot) map[runtimegraph.Identity]struct{} {
	runtimes := make(map[runtimegraph.Identity]struct{},
		len(snapshot.ClusterServingRuntimes)+len(snapshot.ServingRuntimes))
	for i := range snapshot.ClusterServingRuntimes {
		runtimes[runtimegraph.Identity{
			Kind: runtimegraph.KindClusterServingRuntime,
			Name: snapshot.ClusterServingRuntimes[i].Name,
		}] = struct{}{}
	}
	for i := range snapshot.ServingRuntimes {
		runtime := &snapshot.ServingRuntimes[i]
		runtimes[runtimegraph.Identity{
			Kind: runtimegraph.KindServingRuntime, Namespace: runtime.Namespace, Name: runtime.Name,
		}] = struct{}{}
	}
	return runtimes
}

func serviceIdentity(service *omev1beta1.InferenceService) InferenceServiceIdentity {
	return InferenceServiceIdentity{Namespace: service.Namespace, Name: service.Name}
}

func validServiceIdentity(identity InferenceServiceIdentity) bool {
	return identity.Namespace != "" && identity.Name != ""
}

func validateRuntimeIdentity(identity runtimegraph.Identity) error {
	if identity.Name == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidRuntimeIdentity)
	}
	switch identity.Kind {
	case runtimegraph.KindServingRuntime:
		if identity.Namespace == "" {
			return fmt.Errorf("%w: namespace is required for ServingRuntime", ErrInvalidRuntimeIdentity)
		}
	case runtimegraph.KindClusterServingRuntime:
		if identity.Namespace != "" {
			return fmt.Errorf("%w: namespace is forbidden for ClusterServingRuntime", ErrInvalidRuntimeIdentity)
		}
	default:
		return fmt.Errorf("%w: unsupported kind %q", ErrInvalidRuntimeIdentity, identity.Kind)
	}
	return nil
}

func serviceIdentityLess(left, right InferenceServiceIdentity) bool {
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	return left.Name < right.Name
}
