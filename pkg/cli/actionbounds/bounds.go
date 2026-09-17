// Package actionbounds caps retained typed action evidence after API decoding.
package actionbounds

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"time"
)

// PrivatePayload caps the complete typed object before resolver or
// projector defensive copies. Inspection retains no strings and cannot emit
// user content. Time is fixed-size evidence; its process-local Location graph
// is not API payload. Both node and byte budgets bound nested pod/status data.
func PrivatePayload(input any) bool {
	bytes, nodes := 0, 0
	var visit func(reflect.Value, int) bool
	visit = func(value reflect.Value, depth int) bool {
		nodes++
		bytes += 8
		if depth > 32 || nodes > 32768 || bytes > 1024*1024 {
			return false
		}
		if !value.IsValid() {
			return true
		}
		if value.Type() == reflect.TypeFor[time.Time]() {
			return true
		}
		switch value.Kind() {
		case reflect.String:
			bytes += value.Len()
			return bytes <= 1024*1024
		case reflect.Pointer, reflect.Interface:
			return value.IsNil() || visit(value.Elem(), depth+1)
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if !visit(value.Field(i), depth+1) {
					return false
				}
			}
		case reflect.Map:
			if value.Len() > 32768 {
				return false
			}
			iter := value.MapRange()
			for iter.Next() {
				if !visit(iter.Key(), depth+1) || !visit(iter.Value(), depth+1) {
					return false
				}
			}
		case reflect.Slice, reflect.Array:
			if value.Type().Elem().Kind() == reflect.Uint8 {
				bytes += value.Len()
				return bytes <= 1024*1024
			}
			if value.Len() > 32768 {
				return false
			}
			for i := 0; i < value.Len(); i++ {
				if !visit(value.Index(i), depth+1) {
					return false
				}
			}
		}
		return true
	}
	return visit(reflect.ValueOf(input), 0)
}

// JSONPayload bounds raw revision JSON before decoding a ServingRuntimeSpec.
// It does not change typed evidence policy or claim a network response bound.
func JSONPayload(raw []byte) bool {
	if len(raw) == 0 || len(raw) > 1048576 {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	depth, nodes := 0, 0
	complete := false
	for {
		token, err := d.Token()
		if err == io.EOF {
			return complete && depth == 0
		}
		if err != nil || complete {
			return false
		}
		nodes++
		if nodes > 32768 {
			return false
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
			if depth < 0 || depth > 32 {
				return false
			}
		}
		complete = depth == 0
	}
}
