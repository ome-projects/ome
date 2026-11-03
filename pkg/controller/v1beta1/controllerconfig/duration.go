package controllerconfig

import (
	"encoding/json"
	"fmt"
	"time"
)

// durationValue is a duration the operator writes as text in the ConfigMap
// ("30s", "5m"). Decoding the block parses the text once; the text is kept so
// a Validate method can quote it.
type durationValue struct {
	text       string        // the key as written; empty when the operator left it out
	parsed     time.Duration // what the text parsed to; zero when it did not parse
	wellFormed bool          // whether the text is a duration at all
}

// durationText reads a duration from its text, as decoding the block does.
func durationText(text string) durationValue {
	parsed, err := time.ParseDuration(text)
	return durationValue{text: text, parsed: parsed, wellFormed: err == nil}
}

// Duration is the configured value, or zero when the key is absent, does not
// parse or is not positive. Every consumer treats zero as "not configured" and
// applies its own fallback; where a wrong value must stop the manager instead,
// a Validate method refuses the text at start-up.
func (d durationValue) Duration() time.Duration {
	if d.wellFormed && d.parsed > 0 {
		return d.parsed
	}
	return 0
}

// written reports whether the operator wrote the key, whatever its text.
func (d durationValue) written() bool {
	return d.text != ""
}

// validatePositive refuses text that does not parse or is not positive. An
// absent key passes: the consumer applies its own fallback.
func (d durationValue) validatePositive(key string) error {
	switch {
	case !d.written():
		return nil
	case !d.wellFormed:
		return fmt.Errorf("%s: %q is not a duration (use forms like \"30s\", \"5m\")", key, d.text)
	case d.parsed <= 0:
		return fmt.Errorf("%s: %q must be positive", key, d.text)
	}
	return nil
}

// UnmarshalJSON reads the text form. A JSON null leaves the key absent, as it
// does for a string.
func (d *durationValue) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	*d = durationText(text)
	return nil
}

// MarshalJSON writes the text as written.
func (d durationValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.text)
}
