package mutate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	validation "k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
	"sigs.k8s.io/ome/pkg/trafficdrain"
)

const (
	trafficDrainMaxAnnotationBytes = 32 * 1024
	trafficDrainMaxOverrides       = 64
	trafficDrainMaxReasonBytes     = 256
)

var (
	ErrTrafficDrainAnnotation = errors.New("traffic action refused: existing traffic-drain annotation is malformed or unsafe")
	ErrTrafficDrainBounds     = errors.New("traffic action refused: traffic-drain state exceeds safety bounds")
	ErrTrafficDrainExists     = errors.New("traffic drain refused: override ID already exists")
	ErrTrafficDrainAbsent     = errors.New("traffic undrain refused: override ID does not exist")
	ErrTrafficDrainIneligible = errors.New("traffic action refused: target is not eligible for cross-cluster traffic routing")
	ErrTrafficDrainPlan       = errors.New("traffic action plan is private")
)

// TrafficDrainRequest is one exact mutation of the durable traffic-drain map.
type TrafficDrainRequest struct {
	Action  string
	ID      string
	Cluster string
	Reason  string
}

// TrafficDrainPlan is an immutable, UID/resourceVersion-guarded annotation
// patch. It never carries arbitrary server payloads.
type TrafficDrainPlan struct {
	patch    []byte
	target   reportv1alpha1.ActionTarget
	request  TrafficDrainRequest
	affected trafficdrain.Override
	previous int
	result   int
}

func (TrafficDrainPlan) MarshalJSON() ([]byte, error)          { return nil, ErrTrafficDrainPlan }
func (TrafficDrainPlan) MarshalYAML() (any, error)             { return nil, ErrTrafficDrainPlan }
func (TrafficDrainPlan) String() string                        { return "<mutate.TrafficDrainPlan redacted>" }
func (TrafficDrainPlan) GoString() string                      { return "<mutate.TrafficDrainPlan redacted>" }
func (p TrafficDrainPlan) Patch() []byte                       { return append([]byte(nil), p.patch...) }
func (p TrafficDrainPlan) Target() reportv1alpha1.ActionTarget { return p.target }
func (p TrafficDrainPlan) Request() TrafficDrainRequest        { return p.request }
func (p TrafficDrainPlan) Details() reportv1alpha1.TrafficActionDetails {
	return reportv1alpha1.TrafficActionDetails{
		OverrideID: p.affected.ID, Cluster: p.affected.Cluster,
		OverridesBefore: p.previous, OverridesAfter: p.result,
	}
}

// ValidateTrafficDrainRequest applies the traffic-drain parser's non-empty,
// trimmed text contract plus the Kubernetes identity shapes used by the
// referenced WorkloadCluster and by operator-maintained override IDs.
func ValidateTrafficDrainRequest(request TrafficDrainRequest) error {
	if request.Action != "drain" && request.Action != "undrain" {
		return errors.New("traffic action must be drain or undrain")
	}
	if len(validation.IsDNS1123Label(request.ID)) != 0 || !SafeScalar(request.ID) {
		return errors.New("traffic override ID must be a DNS-1123 label")
	}
	if request.Action == "undrain" {
		if request.Cluster != "" || request.Reason != "" {
			return errors.New("traffic undrain accepts only an override ID")
		}
		return nil
	}
	if len(validation.IsDNS1123Subdomain(request.Cluster)) != 0 || !SafeScalar(request.Cluster) {
		return errors.New("traffic drain cluster must be a DNS-1123 subdomain")
	}
	if !safeTrafficDrainReason(request.Reason) {
		return errors.New("traffic drain reason must be bounded, trimmed, printable, and non-secret")
	}
	return nil
}

// PrepareTrafficDrain parses and validates the complete existing annotation
// before changing one ID. Unknown, duplicate, oversized, or unsafe existing
// values fail closed instead of being normalized away.
func PrepareTrafficDrain(service *omev1beta1.InferenceService, request TrafficDrainRequest) (TrafficDrainPlan, error) {
	if err := ValidateTrafficDrainRequest(request); err != nil {
		return TrafficDrainPlan{}, err
	}
	if err := validateTrafficDrainTarget(service); err != nil {
		return TrafficDrainPlan{}, err
	}
	raw, present := service.Annotations[constants.TrafficDrainAnnotation]
	if present && len(raw) > trafficDrainMaxAnnotationBytes {
		return TrafficDrainPlan{}, ErrTrafficDrainBounds
	}
	overrides, err := trafficdrain.FromAnnotations(service.Annotations)
	if err != nil {
		return TrafficDrainPlan{}, ErrTrafficDrainAnnotation
	}
	if len(overrides) > trafficDrainMaxOverrides {
		return TrafficDrainPlan{}, ErrTrafficDrainBounds
	}
	for _, existing := range overrides {
		if err := validateExistingTrafficDrain(existing); err != nil {
			return TrafficDrainPlan{}, ErrTrafficDrainAnnotation
		}
	}

	index := -1
	for i := range overrides {
		if overrides[i].ID == request.ID {
			index = i
			break
		}
	}
	previous := len(overrides)
	affected := trafficdrain.Override{ID: request.ID, Cluster: request.Cluster, Reason: request.Reason}
	switch request.Action {
	case "drain":
		if index >= 0 {
			return TrafficDrainPlan{}, ErrTrafficDrainExists
		}
		overrides = append(overrides, trafficdrain.Override{ID: request.ID, Cluster: request.Cluster, Reason: request.Reason})
	case "undrain":
		if index < 0 {
			return TrafficDrainPlan{}, ErrTrafficDrainAbsent
		}
		affected = overrides[index]
		overrides = append(overrides[:index:index], overrides[index+1:]...)
	}
	if len(overrides) > trafficDrainMaxOverrides {
		return TrafficDrainPlan{}, ErrTrafficDrainBounds
	}
	sort.Slice(overrides, func(i, j int) bool { return overrides[i].ID < overrides[j].ID })

	patch := []patchOperation{
		{Op: "test", Path: "/metadata/uid", Value: string(service.UID)},
		{Op: "test", Path: "/metadata/resourceVersion", Value: service.ResourceVersion},
	}
	annotationPath := "/metadata/annotations/" + escapeJSONPointer(constants.TrafficDrainAnnotation)
	if len(overrides) == 0 {
		patch = append(patch, patchOperation{Op: "remove", Path: annotationPath})
	} else {
		encoded, encodeErr := encodeTrafficDrainOverrides(overrides)
		if encodeErr != nil {
			return TrafficDrainPlan{}, ErrTrafficDrainAnnotation
		}
		if len(encoded) > trafficDrainMaxAnnotationBytes {
			return TrafficDrainPlan{}, ErrTrafficDrainBounds
		}
		if !present && len(service.Annotations) >= 256 {
			return TrafficDrainPlan{}, ErrTrafficDrainBounds
		}
		projectedMetadataBytes := trafficDrainMetadataBytes(service)
		if present {
			projectedMetadataBytes -= len(constants.TrafficDrainAnnotation) + len(raw)
		}
		projectedMetadataBytes += len(constants.TrafficDrainAnnotation) + len(encoded)
		if projectedMetadataBytes > 65536 {
			return TrafficDrainPlan{}, ErrTrafficDrainBounds
		}
		if service.Annotations == nil {
			patch = append(patch, patchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]string{}})
		}
		op := "add"
		if present {
			op = "replace"
		}
		patch = append(patch, patchOperation{Op: op, Path: annotationPath, Value: string(encoded)})
	}
	encodedPatch, err := json.Marshal(patch)
	if err != nil {
		return TrafficDrainPlan{}, errors.New("encode guarded traffic patch")
	}
	return TrafficDrainPlan{
		patch: encodedPatch,
		target: reportv1alpha1.ActionTarget{
			Kind: "InferenceService", Namespace: service.Namespace, Name: service.Name,
			UID: string(service.UID), ResourceVersion: service.ResourceVersion,
		},
		request: request, affected: affected, previous: previous, result: len(overrides),
	}, nil
}

func validateExistingTrafficDrain(value trafficdrain.Override) error {
	return ValidateTrafficDrainRequest(TrafficDrainRequest{
		Action: "drain", ID: value.ID, Cluster: value.Cluster, Reason: value.Reason,
	})
}

func safeTrafficDrainReason(value string) bool {
	if value == "" || len(value) > trafficDrainMaxReasonBytes || !utf8.ValidString(value) || value != strings.TrimSpace(value) || credentialPattern.MatchString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return false
		}
	}
	return true
}

func validateTrafficDrainTarget(service *omev1beta1.InferenceService) error {
	if service == nil || !boundedPrivatePayload(service) {
		return ErrTrafficDrainBounds
	}
	if len(service.Annotations) > 256 || len(service.Labels) > 256 || len(service.Finalizers) > 64 ||
		len(service.Status.Conditions) > 64 || len(service.Status.Components) > 3 {
		return ErrTrafficDrainBounds
	}
	for _, key := range []string{constants.PlacementOrigin, constants.PlacementControlPlane} {
		if _, exists := service.Labels[key]; exists {
			return ErrPlacement
		}
	}
	if _, exists := service.Annotations[constants.PlacementOriginUID]; exists {
		return ErrPlacement
	}
	// TrafficMap routing and placement share this exported eligibility predicate.
	// Reusing it here prevents accepting a durable drain on an ordinary service
	// that the routing controller will deliberately ignore.
	if !placementcontroller.IsPlacementEligible(service) {
		return ErrTrafficDrainIneligible
	}
	if trafficDrainMetadataBytes(service) > 65536 {
		return ErrTrafficDrainBounds
	}

	// Generic guarded actions reject placement sources because their lifecycle
	// writes belong on a workload object. A traffic drain is the inverse: its
	// annotation is control-plane-only. Validate the same identity and bounds on
	// a copy with only those source-only markers removed from the generic gate.
	copy := service.DeepCopy()
	copy.Spec.Placement = nil
	copy.Status.Placement = nil
	delete(copy.Annotations, placementcontroller.AcceleratorRequirementsAnnotation)
	delete(copy.Annotations, placementcontroller.ClusterSelectorAnnotation)
	filtered := copy.Finalizers[:0]
	for _, finalizer := range copy.Finalizers {
		if finalizer != placementcontroller.PlacementFinalizer {
			filtered = append(filtered, finalizer)
		}
	}
	copy.Finalizers = filtered
	return ValidateTarget(copy)
}

func trafficDrainMetadataBytes(service *omev1beta1.InferenceService) int {
	metadataBytes := 0
	for key, value := range service.Annotations {
		metadataBytes += len(key) + len(value)
	}
	for key, value := range service.Labels {
		metadataBytes += len(key) + len(value)
	}
	for _, value := range service.Finalizers {
		metadataBytes += len(value)
	}
	return metadataBytes
}

type trafficDrainWireValue struct {
	Cluster string `json:"cluster"`
	Reason  string `json:"reason"`
}

func encodeTrafficDrainOverrides(overrides []trafficdrain.Override) ([]byte, error) {
	values := make(map[string]trafficDrainWireValue, len(overrides))
	for _, value := range overrides {
		if _, duplicate := values[value.ID]; duplicate {
			return nil, ErrTrafficDrainAnnotation
		}
		values[value.ID] = trafficDrainWireValue{Cluster: value.Cluster, Reason: value.Reason}
	}
	return json.Marshal(values)
}

func escapeJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

// WritePreview writes only bounded, reviewed logical values to stderr.
func (p TrafficDrainPlan) WritePreview(out io.Writer, contextName string, mode reportv1alpha1.DryRunMode) error {
	if len(p.patch) == 0 || !SafeScalar(contextName) || p.target.UID == "" || ValidateTrafficDrainRequest(p.request) != nil {
		return ErrUnsafeValue
	}
	rows := [][]string{}
	add := func(field, value string) {
		if value == "" {
			value = "<absent>"
		}
		remaining := value
		for remaining != "" {
			// Preserve exact values across rows. The table sanitizes each chunk
			// only while rendering; patch and preview state remain byte-exact.
			chunk := displayPrefixForTrafficDrain(remaining, 60)
			rows = append(rows, []string{field, chunk})
			remaining = strings.TrimPrefix(remaining, chunk)
			field = "(continued)"
		}
	}
	for _, row := range [][2]string{
		{"Action", "traffic " + p.request.Action},
		{"Context", contextName},
		{"Workload NS", p.target.Namespace},
		{"Target", p.target.Kind + "/" + p.target.Name},
		{"UID", p.target.UID},
		{"ResourceVersion", p.target.ResourceVersion},
		{"Dry-run", string(mode)},
		{"Override ID", p.affected.ID},
		{"Cluster", p.affected.Cluster},
		{"Reason", strconv.Quote(p.affected.Reason)},
		{"Override count", fmt.Sprintf("%d -> %d", p.previous, p.result)},
		{"Annotation", constants.TrafficDrainAnnotation},
		{"Follow-up", "kubectl ome traffic status " + p.target.Name + " -n " + p.target.Namespace + " --context=" + contextName},
	} {
		add(row[0], row[1])
	}
	if _, err := fmt.Fprintln(out, "ALPHA guarded traffic action (not TrafficMap convergence)"); err != nil {
		return errors.New("write traffic action preview failed")
	}
	if err := (report.Table{Headers: []string{"FIELD", "VALUE"}, Rows: rows}).Write(out); err != nil {
		return errors.New("write traffic action preview failed")
	}
	for _, warning := range []string{
		"API acceptance is not TrafficMap convergence or data-plane realization.",
		"The exact InferenceService UID/resourceVersion is tested atomically.",
		"Other annotations and traffic-drain override IDs are preserved.",
		"Run the previewed traffic status command to observe controller evidence.",
	} {
		for _, line := range wrapTrafficDrainLine(warning, 80) {
			if _, err := fmt.Fprintln(out, line); err != nil {
				return errors.New("write traffic action preview failed")
			}
		}
	}
	return nil
}

func displayPrefixForTrafficDrain(value string, width int) string {
	result := ""
	for _, r := range value {
		candidate := result + string(r)
		if result != "" && printers.CellDisplayWidth(candidate) > width {
			break
		}
		result = candidate
	}
	return result
}

func wrapTrafficDrainLine(value string, width int) []string {
	words := strings.Fields(value)
	lines := []string{}
	line := ""
	for _, word := range words {
		candidate := word
		if line != "" {
			candidate = line + " " + word
		}
		if line != "" && printers.CellDisplayWidth(candidate) > width {
			lines = append(lines, line)
			line = word
			continue
		}
		line = candidate
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}
