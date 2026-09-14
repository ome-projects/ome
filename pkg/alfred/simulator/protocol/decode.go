package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Decode reads exactly one bounded Request and rejects unknown JSON fields.
func Decode(reader io.Reader, maxBytes int64) (Request, error) {
	if reader == nil {
		return Request{}, fmt.Errorf("request reader must not be nil")
	}
	if maxBytes <= 0 {
		return Request{}, fmt.Errorf("maximum request size must be positive")
	}

	payload, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return Request{}, fmt.Errorf("read simulation request: %w", err)
	}
	if int64(len(payload)) > maxBytes {
		return Request{}, fmt.Errorf("simulation request exceeds %d byte limit", maxBytes)
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return Request{}, fmt.Errorf("simulation request must be one JSON object")
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request Request
	if err := decoder.Decode(&request); err != nil {
		return Request{}, fmt.Errorf("decode simulation request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Request{}, fmt.Errorf("decode simulation request: trailing JSON value")
		}
		return Request{}, fmt.Errorf("decode simulation request: trailing data: %w", err)
	}
	return request, nil
}
