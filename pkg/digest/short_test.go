package digest

import "testing"

// TestShort checks the hash for known inputs and lengths. The 12-character rows
// are the production length: a stored RolloutPolicy or AutoscalerPolicy
// digest, a rollout run identity and a chart schema partial name are all
// prefixes of that length and must keep their bytes.
func TestShort(t *testing.T) {
	cases := []struct {
		name  string
		input string
		n     int
		want  string
	}{
		{name: "empty, 12", input: "", n: 12, want: "e3b0c44298fc"},
		{name: "one byte, 12", input: "a", n: 12, want: "ca978112ca1b"},
		{name: "word, 12", input: "hello", n: 12, want: "2cf24dba5fb0"},
		{name: "canonical json, 12", input: `{"a":1}`, n: 12, want: "015abd7f5cc5"},
		{name: "joined digests, 12", input: "rp1:abc|rp1:def", n: 12, want: "0b92c1d2749c"},
		{name: "empty, 0", input: "", n: 0, want: ""},
		{name: "empty, 8", input: "", n: 8, want: "e3b0c442"},
		{name: "word, 8", input: "hello", n: 8, want: "2cf24dba"},
		{name: "empty, 64", input: "", n: 64, want: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{name: "word, 64", input: "hello", n: 64, want: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		{name: "canonical json, 64", input: `{"a":1}`, n: 64, want: "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Short([]byte(tc.input), tc.n); got != tc.want {
				t.Fatalf("Short(%q, %d) = %q, want %q", tc.input, tc.n, got, tc.want)
			}
		})
	}
}

// TestShortLengthsNest checks that every short hash is a prefix of the full
// hex of the same data, so a caller that lengthens its digest can still
// compare old values by prefix.
func TestShortLengthsNest(t *testing.T) {
	data := []byte(`{"canary":{"steps":[{"weight":10}]}}`)
	full := Hex(data)
	if len(full) != 64 {
		t.Fatalf("Hex(data) has %d characters, want 64", len(full))
	}
	for _, n := range []int{0, 1, 8, 12, 32, 64} {
		if got := Short(data, n); got != full[:n] {
			t.Fatalf("Short(data, %d) = %q, want prefix %q of the full hash", n, got, full[:n])
		}
	}
}
