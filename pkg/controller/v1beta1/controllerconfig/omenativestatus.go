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
	configMap, err := getInferenceServiceConfigMap(clientset)
	if err != nil {
		return nil, err
	}
	return ParseOMENativeStatusConfig(configMap)
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
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := expectDelim(decoder, '{'); err != nil {
		return nil, err
	}
	var (
		encoding *string
		bound    *int64
	)
	seen := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("malformed JSON object: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("malformed JSON object: expected a field name, got %v", token)
		}
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("duplicate field %q", key)
		}
		seen[key] = struct{}{}
		switch key {
		case omenativeStatusEncodingField:
			var value string
			if err := decoder.Decode(&value); err != nil {
				return nil, fmt.Errorf("%s must be a string: %w", key, err)
			}
			encoding = &value
		case omenativeStatusBoundField:
			var value int64
			if err := decoder.Decode(&value); err != nil {
				return nil, fmt.Errorf("%s must be a positive integer: %w", key, err)
			}
			bound = &value
		default:
			return nil, fmt.Errorf("unknown field %q", key)
		}
	}
	if err := expectDelim(decoder, '}'); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after the JSON object")
	}

	if encoding == nil {
		return nil, fmt.Errorf("%s is required (%s or %s)", omenativeStatusEncodingField, irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2)
	}
	target := irstatus.Encoding(*encoding)
	switch target {
	case irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2:
	default:
		return nil, fmt.Errorf("%s %q is not supported; use %s or %s", omenativeStatusEncodingField, *encoding, irstatus.EncodingDenseV1, irstatus.EncodingColumnarV2)
	}
	cfg := &OMENativeStatusConfig{InstanceStatusEncoding: target}
	if bound != nil {
		if *bound <= 0 {
			return nil, fmt.Errorf("%s must be a positive integer, got %d", omenativeStatusBoundField, *bound)
		}
		value := uint64(*bound)
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
