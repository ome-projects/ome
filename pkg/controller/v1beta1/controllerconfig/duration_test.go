package controllerconfig

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurationValue_Duration_ZeroUnlessWellFormedAndPositive(t *testing.T) {
	for text, want := range map[string]time.Duration{
		"":      0,
		"30s":   30 * time.Second,
		"1h30m": 90 * time.Minute,
		"0s":    0,
		"-1s":   0,
		"30":    0,
		"1 m":   0,
	} {
		assert.Equal(t, want, durationText(text).Duration(), "text %q", text)
	}
}

func TestDurationValue_ValidatePositive_AbsentPassesMalformedAndNonPositiveNameTheKey(t *testing.T) {
	require.NoError(t, durationText("").validatePositive("placement.gcInterval"))
	require.NoError(t, durationText("5m").validatePositive("placement.gcInterval"))
	assert.EqualError(t, durationText("30").validatePositive("placement.gcInterval"),
		`placement.gcInterval: "30" is not a duration (use forms like "30s", "5m")`)
	assert.EqualError(t, durationText("0s").validatePositive("placement.gcInterval"),
		`placement.gcInterval: "0s" must be positive`)
	assert.EqualError(t, durationText("-1s").validatePositive("placement.gcInterval"),
		`placement.gcInterval: "-1s" must be positive`)
}

func TestDurationValue_JSON_TextOnTheWireNullAbsentNumberRefused(t *testing.T) {
	type block struct {
		Wait durationValue `json:"wait,omitempty"`
	}
	var written block
	require.NoError(t, json.Unmarshal([]byte(`{"wait":"30s"}`), &written))
	assert.Equal(t, 30*time.Second, written.Wait.Duration())
	assert.True(t, written.Wait.written())

	var malformed block
	require.NoError(t, json.Unmarshal([]byte(`{"wait":"30"}`), &malformed), "loading stays forgiving")
	assert.Equal(t, time.Duration(0), malformed.Wait.Duration())
	assert.True(t, malformed.Wait.written())

	for _, text := range []string{`{}`, `{"wait":null}`, `{"wait":""}`} {
		var absent block
		require.NoError(t, json.Unmarshal([]byte(text), &absent))
		assert.Equal(t, durationValue{}, absent.Wait, text)
	}

	var number block
	err := json.Unmarshal([]byte(`{"wait":30}`), &number)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot unmarshal number into Go struct field block.wait of type string")

	out, err := json.Marshal(written)
	require.NoError(t, err)
	assert.JSONEq(t, `{"wait":"30s"}`, string(out))
}
