package instancestatus

import "errors"

// ErrorReason is a stable, payload-free classification for normalization
// failures. Callers can turn it into unavailable evidence without parsing an
// error string or exposing status contents.
type ErrorReason string

const (
	ErrorReasonUnknownEncoding ErrorReason = "unknown_encoding"
	ErrorReasonRepresentation  ErrorReason = "invalid_representation"
	ErrorReasonIndexSyntax     ErrorReason = "invalid_index_syntax"
	ErrorReasonIndexCanonical  ErrorReason = "noncanonical_index_set"
	ErrorReasonCardinality     ErrorReason = "cardinality_limit"
	ErrorReasonCoverage        ErrorReason = "invalid_coverage"
	ErrorReasonRowOrder        ErrorReason = "invalid_row_order"
	ErrorReasonValueDomain     ErrorReason = "invalid_value"
)

// DecodeError deliberately carries only a bounded reason. Source payloads can
// include revision names and operation data that must not leak through an
// error assembled for logs or machine-readable evidence.
type DecodeError struct {
	reason ErrorReason
}

func (e *DecodeError) Error() string {
	if e == nil {
		return "instance status normalization failed"
	}
	switch e.reason {
	case ErrorReasonUnknownEncoding:
		return "instance status encoding is unknown"
	case ErrorReasonRepresentation:
		return "instance status representation is invalid"
	case ErrorReasonIndexSyntax:
		return "instance index set syntax is invalid"
	case ErrorReasonIndexCanonical:
		return "instance index set is not canonical"
	case ErrorReasonCardinality:
		return "instance status exceeds the normalization limit"
	case ErrorReasonCoverage:
		return "instance status column coverage is invalid"
	case ErrorReasonRowOrder:
		return "instance status row order is invalid"
	case ErrorReasonValueDomain:
		return "instance status value is outside its supported domain"
	default:
		return "instance status normalization failed"
	}
}

// ErrorReasonOf extracts a normalization error reason through wrapping.
func ErrorReasonOf(err error) (ErrorReason, bool) {
	var decodeErr *DecodeError
	if !errors.As(err, &decodeErr) || decodeErr == nil {
		return "", false
	}
	switch decodeErr.reason {
	case ErrorReasonUnknownEncoding,
		ErrorReasonRepresentation,
		ErrorReasonIndexSyntax,
		ErrorReasonIndexCanonical,
		ErrorReasonCardinality,
		ErrorReasonCoverage,
		ErrorReasonRowOrder,
		ErrorReasonValueDomain:
		return decodeErr.reason, true
	default:
		return "", false
	}
}

func newDecodeError(reason ErrorReason) error {
	return &DecodeError{reason: reason}
}
