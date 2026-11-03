// Package digest holds the content hash that the policy digests, the rollout
// run identities, the chart generator's schema partial names and the runtime
// revision names share.
package digest

import (
	"crypto/sha256"
	"encoding/hex"
)

// Hex returns the SHA-256 of data as 64 lowercase hex characters.
func Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Short returns the first n hex characters of the SHA-256 of data, so n is at
// most 64. Each caller fixes n once: a stored digest or a partial name stays
// comparable only while its length stays the same.
func Short(data []byte, n int) string {
	return Hex(data)[:n]
}
