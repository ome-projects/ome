package irstatus

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Decoder decodes fetched InferenceReplica objects into the dense logical
// shape under the configured ColumnarV2 row bound. The zero value carries no
// bound: it decodes DenseV1 unchanged and fails closed on every ColumnarV2
// payload, because expanding columns without a configured limit is never
// allowed.
type Decoder struct {
	maxInstances uint64
}

// NewDecoder returns a Decoder that expands at most maxDecodedInstances
// logical rows from a ColumnarV2 payload. Zero means no bound is configured.
func NewDecoder(maxDecodedInstances uint64) Decoder {
	return Decoder{maxInstances: maxDecodedInstances}
}

// MaxDecodedInstances is the configured ColumnarV2 row bound; zero when none
// is configured.
func (d Decoder) MaxDecodedInstances() uint64 {
	return d.maxInstances
}

// Decode rewrites the per-Instance representation of a fetched object into
// the dense logical shape and reports the encoding it was stored in. A
// DenseV1 object is returned unchanged. A ColumnarV2 payload is validated and
// expanded under the bound, the rows replace instanceStatuses, and the marker
// and columns are cleared so the object carries exactly one representation.
// On error the object is not modified and must not drive any effect.
func (d Decoder) Decode(ir *v1beta1.InferenceReplica) (Encoding, error) {
	if ir == nil {
		return EncodingDenseV1, nil
	}
	rows, encoding, err := DecodeStatus(&ir.Status, d.maxInstances)
	if err != nil {
		return "", err
	}
	if encoding == EncodingColumnarV2 {
		ir.Status.InstanceStatuses = rows
		ir.Status.InstanceStatusEncoding = nil
		ir.Status.InstanceStatusColumns = nil
	}
	return encoding, nil
}

// Reader is a client.Reader that carries the Decoder for the InferenceReplica
// objects fetched through it. Its own Get and List leave the stored
// representation untouched; GetDecoded is the accessor that decodes.
type Reader struct {
	client.Reader
	decoder Decoder
}

// NewReader pairs reads with decoder. A nil reads stays nil so callers that
// treat a missing reader as "no observation" keep that behavior.
func NewReader(reads client.Reader, decoder Decoder) client.Reader {
	if reads == nil {
		return nil
	}
	return Reader{Reader: reads, decoder: decoder}
}

// Decoder returns the Decoder this reader carries.
func (r Reader) Decoder() Decoder {
	return r.decoder
}

// DecoderOf returns the Decoder carried by reads. Any other reader carries
// the zero Decoder, which has no bound and therefore fails closed on
// ColumnarV2.
func DecoderOf(reads client.Reader) Decoder {
	if r, ok := reads.(Reader); ok {
		return r.decoder
	}
	return Decoder{}
}

// GetDecoded is the decoded-accessor boundary for a fetched InferenceReplica:
// it reads key into ir through reads and decodes the stored per-Instance
// representation with the reader's Decoder. The returned encoding is the one
// the rows were read from; ir is left in the dense logical shape. A fetch
// error is returned before any decode and keeps its API error identity.
func GetDecoded(ctx context.Context, reads client.Reader, key client.ObjectKey, ir *v1beta1.InferenceReplica) (Encoding, error) {
	if err := reads.Get(ctx, key, ir); err != nil {
		return "", err
	}
	return DecoderOf(reads).Decode(ir)
}
