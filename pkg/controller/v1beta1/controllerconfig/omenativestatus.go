package controllerconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// OMENativeStatusConfigName is the inferenceservice-config ConfigMap key
// holding the InferenceReplica per-Instance status representation block.
const OMENativeStatusConfigName = "omenativeStatus"

const (
	omenativeStatusEncodingField = "instanceStatusEncoding"
	omenativeStatusBoundField    = "maxDecodedInstances"
)

// +kubebuilder:object:generate=false
// OMENativeStatusConfig is the operator-selected InferenceReplica status
// representation policy. The block is required and read once at manager
// startup; the binary carries no default for either value, so a missing or
// malformed block prevents the manager from starting.
type OMENativeStatusConfig struct {
	// InstanceStatusEncoding is the representation the status writer targets:
	// DenseV1 or ColumnarV2.
	InstanceStatusEncoding irstatus.Encoding

	// MaxDecodedInstances bounds how many logical rows a stored ColumnarV2
	// payload may expand to. Required under ColumnarV2; optional under
	// DenseV1, where nil means no ColumnarV2 payload can be decoded.
	MaxDecodedInstances *uint64
}

// DecodeBound is the ColumnarV2 row bound to decode with; zero when none is
// configured, which the codec treats as "never expand columns".
func (c *OMENativeStatusConfig) DecodeBound() uint64 {
	if c == nil || c.MaxDecodedInstances == nil {
		return 0
	}
	return *c.MaxDecodedInstances
}

// NewOMENativeStatusConfig loads and validates the block once at startup.
func NewOMENativeStatusConfig(clientset kubernetes.Interface) (*OMENativeStatusConfig, error) {
	return loadBlock(clientset, ParseOMENativeStatusConfig)
}

// ParseOMENativeStatusConfig decodes the block strictly: it must be a single
// JSON object with no unknown, duplicate, or trailing content, and every
// present value must be well typed and in range. Operator tooling that reads
// a cluster's ConfigMap applies the same rules the manager applies at startup.
func ParseOMENativeStatusConfig(configMap *v1.ConfigMap) (*OMENativeStatusConfig, error) {
	raw, ok := configMap.Data[OMENativeStatusConfigName]
	if !ok {
		return nil, fmt.Errorf("inferenceservice-config is missing the required %q block; set %s to %s or %s",
			OMENativeStatusConfigName, omenativeStatusEncodingField, irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2)
	}
	cfg, err := decodeOMENativeStatusConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid %s config: %w", OMENativeStatusConfigName, err)
	}
	return cfg, nil
}

func decodeOMENativeStatusConfig(raw string) (*OMENativeStatusConfig, error) {
	fields, err := readOMENativeStatusFields(json.NewDecoder(strings.NewReader(raw)))
	if err != nil {
		return nil, err
	}
	return fields.config()
}

// omenativeStatusFields is the block as written, each field present or
// absent, before the values are checked against each other.
type omenativeStatusFields struct {
	encoding *string
	bound    *int64
}

// readOMENativeStatusFields reads one JSON object holding the block's fields
// and nothing else: no unknown or repeated field, nothing after the closing
// brace.
func readOMENativeStatusFields(decoder *json.Decoder) (omenativeStatusFields, error) {
	var fields omenativeStatusFields
	if err := expectDelim(decoder, '{'); err != nil {
		return fields, err
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		key, err := readFieldName(decoder, seen)
		if err != nil {
			return fields, err
		}
		if err := fields.read(decoder, key); err != nil {
			return fields, err
		}
	}
	if err := expectDelim(decoder, '}'); err != nil {
		return fields, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fields, errors.New("trailing data after the JSON object")
	}
	return fields, nil
}

// readFieldName reads the next field name of the object and refuses one
// read before.
func readFieldName(decoder *json.Decoder, seen map[string]struct{}) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("malformed JSON object: %w", err)
	}
	key, ok := token.(string)
	if !ok {
		return "", fmt.Errorf("malformed JSON object: expected a field name, got %v", token)
	}
	if _, dup := seen[key]; dup {
		return "", fmt.Errorf("duplicate field %q", key)
	}
	seen[key] = struct{}{}
	return key, nil
}

// read decodes the value of one field into its place; an unknown field is
// refused.
func (f *omenativeStatusFields) read(decoder *json.Decoder, key string) error {
	switch key {
	case omenativeStatusEncodingField:
		var value string
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("%s must be a string: %w", key, err)
		}
		f.encoding = &value
	case omenativeStatusBoundField:
		var value int64
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("%s must be a positive integer: %w", key, err)
		}
		f.bound = &value
	default:
		return fmt.Errorf("unknown field %q", key)
	}
	return nil
}

// config checks the fields against each other: the encoding is required and
// must be one the status writer targets, a bound present must be positive,
// and ColumnarV2 requires the bound.
func (f omenativeStatusFields) config() (*OMENativeStatusConfig, error) {
	if f.encoding == nil {
		return nil, fmt.Errorf("%s is required (%s or %s)", omenativeStatusEncodingField, irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2)
	}
	target := irstatus.Encoding(*f.encoding)
	if target != irstatus.EncodingDenseV1 && target != irstatus.EncodingColumnarV2 {
		return nil, fmt.Errorf("%s %q is not supported; use %s or %s", omenativeStatusEncodingField, *f.encoding, irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2)
	}
	cfg := &OMENativeStatusConfig{InstanceStatusEncoding: target}
	if f.bound != nil {
		if *f.bound <= 0 {
			return nil, fmt.Errorf("%s must be a positive integer, got %d", omenativeStatusBoundField, *f.bound)
		}
		value := uint64(*f.bound)
		cfg.MaxDecodedInstances = &value
	}
	if target == irstatus.EncodingColumnarV2 && cfg.MaxDecodedInstances == nil {
		return nil, fmt.Errorf("%s is required when %s is %s", omenativeStatusBoundField, omenativeStatusEncodingField, irstatus.EncodingColumnarV2)
	}
	return cfg, nil
}

func expectDelim(decoder *json.Decoder, want json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			if want == '{' {
				return errors.New("expected a JSON object")
			}
			return errors.New("malformed JSON object: unexpected end of input")
		}
		return fmt.Errorf("malformed JSON object: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != want {
		return fmt.Errorf("expected a JSON object, got %v", token)
	}
	return nil
}
