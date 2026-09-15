package mutate

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	validation "k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

// MigrationOptions holds only explicitly reviewed logical request values.
type MigrationOptions struct {
	Component   v1beta1.ComponentType
	Instance    int32
	FromNode    string
	HintNodes   []string
	Reason      string
	RequestedBy string
	RequestID   string
}

type migrationRequest struct {
	SchemaVersion string   `json:"schemaVersion"`
	Component     string   `json:"component"`
	Instance      int32    `json:"instance"`
	FromNode      string   `json:"from_node"`
	HintNodes     []string `json:"hint_target_nodes,omitempty"`
	Reason        string   `json:"reason,omitempty"`
	RequestedAt   string   `json:"requested_at,omitempty"`
	RequestedBy   string   `json:"requested_by,omitempty"`
}

// MigrationPlan owns the exact payload; private operational input cannot be
// serialized as a CLI report. Patch returns independent bytes.
type MigrationPlan struct {
	patch    []byte
	target   reportv1alpha1.ActionTarget
	request  migrationRequest
	id       string
	existing bool
	evidence MigrationEvidence
}

func (p MigrationPlan) Patch() []byte     { return append([]byte{}, p.patch...) }
func (p MigrationPlan) RequestID() string { return p.id }
func (p MigrationPlan) Existing() bool    { return p.existing }

func (MigrationPlan) MarshalJSON() ([]byte, error) {
	return nil, errors.New("private migration plan cannot be serialized")
}
func (MigrationPlan) String() string   { return "<mutate.MigrationPlan redacted>" }
func (MigrationPlan) GoString() string { return "<mutate.MigrationPlan redacted>" }

func migrationConflict() error {
	return &exitcode.PreconditionError{Err: errors.New("migration precondition conflicts or is stale; inspect migration status and retry explicitly")}
}

// ParseMigrationIndex refuses lossy normalization of an instance identity.
func ParseMigrationIndex(raw string) (int32, error) {
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value < 0 || strconv.FormatInt(value, 10) != raw {
		return 0, errors.New("instance must be a canonical integer from 0 through 2147483647")
	}
	return int32(value), nil
}

func validMigrationUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value && parsed.Variant() == uuid.RFC4122 && parsed.Version() >= 1 && parsed.Version() <= 8
}

func validMigrationNode(value string) bool {
	return value != "" && SafeScalar(value) && len(validation.IsDNS1123Subdomain(value)) == 0
}

func safeMigrationText(value string) bool {
	if len(value) > 256 || !utf8.ValidString(value) || credentialPattern.MatchString(value) {
		return false
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"bearer ", "password=", "token=", "://"} {
		if strings.Contains(lower, marker) {
			return false
		}
	}
	for _, r := range value {
		if !unicode.IsPrint(r) || unicode.Is(unicode.Cf, r) {
			return false
		}
	}
	return value != "AutoRecover" && value != "ForceDelete"
}

// ValidateMigrationOptions is acquisition-free and never echoes raw inputs.
func ValidateMigrationOptions(o MigrationOptions) error {
	if o.Component != v1beta1.EngineComponent && o.Component != v1beta1.DecoderComponent && o.Component != v1beta1.RouterComponent || o.Instance < 0 || o.FromNode != "" && !validMigrationNode(o.FromNode) || len(o.HintNodes) > 8 || !safeMigrationText(o.Reason) || o.RequestedBy != "" && (len(o.RequestedBy) > 128 || !SafeScalar(o.RequestedBy)) || o.RequestID != "" && !validMigrationUUID(o.RequestID) {
		return errors.New("invalid migration request values; use --help")
	}
	seen := map[string]bool{}
	for _, node := range o.HintNodes {
		if !validMigrationNode(node) || seen[node] || node == o.FromNode {
			return errors.New("invalid migration node hints")
		}
		seen[node] = true
	}
	return nil
}

// strictJSON rejects duplicate keys at every depth before decoding; bounds are
// applied to raw input by its caller, not claimed as an HTTP body limit.
func strictJSON(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	nodes := 0
	var visit func(int) bool
	visit = func(depth int) bool {
		nodes++
		if depth > 32 || nodes > 32768 {
			return false
		}
		t, err := d.Token()
		if err != nil {
			return false
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					k, e := d.Token()
					s, ok := k.(string)
					if e != nil || !ok || seen[s] {
						return false
					}
					seen[s] = true
					if !visit(depth + 1) {
						return false
					}
				}
			case '[':
				for d.More() {
					if !visit(depth + 1) {
						return false
					}
				}
			default:
				return false
			}
			end, e := d.Token()
			return e == nil && (delim == '{' && end == json.Delim('}') || delim == '[' && end == json.Delim(']'))
		}
		return true
	}
	if !visit(0) {
		return false
	}
	_, err := d.Token()
	return errors.Is(err, io.EOF)
}

func parseMigrationRequest(raw string) (migrationRequest, error) {
	var request migrationRequest
	if len(raw) > 8192 || !strictJSON([]byte(raw)) {
		return request, errors.New("invalid retained migration payload")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &fields) != nil {
		return request, errors.New("invalid retained migration payload")
	}
	for key, value := range fields {
		// Go struct decoding folds field names; retained request authority
		// requires these exact wire tags, not case aliases of any field.
		switch key {
		case "schemaVersion", "component", "instance", "from_node", "hint_target_nodes", "reason", "requested_at", "requested_by":
		default:
			return request, errors.New("invalid retained migration field")
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return request, errors.New("invalid retained migration field type")
		}
	}
	for _, key := range []string{"schemaVersion", "component", "instance", "from_node"} {
		if len(fields[key]) == 0 || bytes.Equal(fields[key], []byte("null")) {
			return request, errors.New("incomplete retained migration payload")
		}
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&request) != nil || request.SchemaVersion != "v1" || !validMigrationNode(request.FromNode) {
		return request, errors.New("invalid retained migration payload")
	}
	if request.RequestedAt != "" {
		if _, err := time.Parse(time.RFC3339, request.RequestedAt); err != nil {
			return request, errors.New("invalid retained migration timestamp")
		}
	}
	err := ValidateMigrationOptions(MigrationOptions{Component: v1beta1.ComponentType(request.Component), Instance: request.Instance, FromNode: request.FromNode, HintNodes: request.HintNodes, Reason: request.Reason, RequestedBy: request.RequestedBy})
	return request, err
}

func PrepareMigration(parent *v1beta1.InferenceService, state *effective.RuntimeState, evidence MigrationEvidence, o MigrationOptions, newUUID func() (string, error), clock reportv1alpha1.Clock) (MigrationPlan, error) {
	if err := ValidateTarget(parent); err != nil {
		return MigrationPlan{}, err
	}
	if err := ValidateMigrationOptions(o); err != nil {
		return MigrationPlan{}, err
	}
	native, err := RequireNativeRuntime(parent, state)
	if err != nil {
		return MigrationPlan{}, err
	}
	if !slices.Contains(native, string(o.Component)) || o.Component == v1beta1.DecoderComponent && parent.Spec.Decoder == nil || o.Component == v1beta1.RouterComponent && parent.Spec.Router == nil {
		return MigrationPlan{}, ErrRuntime
	}
	if !evidence.complete || evidence.uid != string(parent.UID) || evidence.rv != parent.ResourceVersion || evidence.options != migrationOptionKey(o) {
		return MigrationPlan{}, migrationConflict()
	}
	p := MigrationPlan{target: reportv1alpha1.ActionTarget{Kind: "InferenceService", Namespace: parent.Namespace, Name: parent.Name, UID: string(parent.UID), ResourceVersion: parent.ResourceVersion}, evidence: evidence}
	if o.RequestID != "" {
		r, ok := evidence.pending[o.RequestID]
		if !ok {
			return MigrationPlan{}, migrationConflict()
		}
		from := o.FromNode
		if from == "" {
			from = r.FromNode
		}
		if r.Component != string(o.Component) || r.Instance != o.Instance || r.FromNode != from || !slices.Equal(r.HintNodes, o.HintNodes) || r.Reason != o.Reason || r.RequestedBy != o.RequestedBy || evidence.conflicting[o.RequestID] {
			return MigrationPlan{}, migrationConflict()
		}
		p.request = r
		p.id = o.RequestID
		p.existing = true
		return p, nil
	}
	if evidence.source == nil || evidence.fromNode == "" {
		return MigrationPlan{}, migrationConflict()
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	if newUUID == nil {
		newUUID = func() (string, error) { v, e := uuid.NewRandom(); return v.String(), e }
	}
	p.id, err = newUUID()
	if err != nil || !validMigrationUUID(p.id) {
		return MigrationPlan{}, errors.New("could not generate migration request UUID")
	}
	parsed, _ := uuid.Parse(p.id)
	if parsed.Version() != 4 || evidence.known[p.id] {
		return MigrationPlan{}, migrationConflict()
	}
	p.request = migrationRequest{SchemaVersion: "v1", Component: string(o.Component), Instance: o.Instance, FromNode: evidence.fromNode, HintNodes: append([]string(nil), o.HintNodes...), Reason: o.Reason, RequestedAt: clock.Now().UTC().Format(time.RFC3339), RequestedBy: o.RequestedBy}
	for _, node := range o.HintNodes {
		if node == evidence.fromNode {
			return MigrationPlan{}, migrationConflict()
		}
	}
	raw, err := json.Marshal(p.request)
	if err != nil {
		return MigrationPlan{}, errors.New("encode migration request failed")
	}
	patch := []patchOperation{{Op: "test", Path: "/metadata/uid", Value: string(parent.UID)}, {Op: "test", Path: "/metadata/resourceVersion", Value: parent.ResourceVersion}}
	if parent.Annotations == nil {
		patch = append(patch, patchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]string{}})
	}
	key := strings.ReplaceAll(strings.ReplaceAll(constants.MigrationRequestAnnotationPrefix+p.id, "~", "~0"), "/", "~1")
	patch = append(patch, patchOperation{Op: "add", Path: "/metadata/annotations/" + key, Value: string(raw)})
	p.patch, err = json.Marshal(patch)
	if err != nil {
		return MigrationPlan{}, errors.New("encode guarded migration patch failed")
	}
	return p, nil
}

func migrationOptionKey(o MigrationOptions) string { raw, _ := json.Marshal(o); return string(raw) }
