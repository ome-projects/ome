package trafficdrain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestParse(t *testing.T) {
	t.Run("valid overrides are sorted by ID", func(t *testing.T) {
		got, err := Parse(`{
			"drain-z":{"reason":"second hold","cluster":"cluster-b"},
			"drain-a":{"cluster":"cluster-a","reason":"first hold"}
		}`)
		require.NoError(t, err)
		assert.Equal(t, []Override{
			{ID: "drain-a", Cluster: "cluster-a", Reason: "first hold"},
			{ID: "drain-z", Cluster: "cluster-b", Reason: "second hold"},
		}, got)
	})

	t.Run("empty object means no overrides", func(t *testing.T) {
		got, err := Parse(`{}`)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty", raw: ``, want: "decode top-level object"},
		{name: "array", raw: `[]`, want: "top-level value must be a JSON object"},
		{name: "null", raw: `null`, want: "top-level value must be a JSON object"},
		{name: "duplicate ID", raw: `{"drain":{"cluster":"a","reason":"one"},"drain":{"cluster":"b","reason":"two"}}`, want: `duplicate override ID "drain"`},
		{name: "empty ID", raw: `{"":{"cluster":"a","reason":"one"}}`, want: "override ID must be non-empty"},
		{name: "whitespace ID", raw: `{" drain":{"cluster":"a","reason":"one"}}`, want: "override ID must not have leading or trailing whitespace"},
		{name: "non-object override", raw: `{"drain":"cluster-a"}`, want: `override "drain" must be a JSON object`},
		{name: "missing cluster", raw: `{"drain":{"reason":"one"}}`, want: `override "drain" cluster must be non-empty`},
		{name: "missing reason", raw: `{"drain":{"cluster":"a"}}`, want: `override "drain" reason must be non-empty`},
		{name: "empty reason", raw: `{"drain":{"cluster":"a","reason":" "}}`, want: `override "drain" reason must be non-empty`},
		{name: "whitespace cluster", raw: `{"drain":{"cluster":" a","reason":"one"}}`, want: `override "drain" cluster must not have leading or trailing whitespace`},
		{name: "unknown field", raw: `{"drain":{"cluster":"a","reason":"one","weight":"0"}}`, want: `override "drain" has unknown field "weight"`},
		{name: "duplicate field", raw: `{"drain":{"cluster":"a","cluster":"b","reason":"one"}}`, want: `override "drain" has duplicate field "cluster"`},
		{name: "non-string field", raw: `{"drain":{"cluster":1,"reason":"one"}}`, want: `override "drain" field "cluster" must be a string`},
		{name: "trailing data", raw: `{} {}`, want: "unexpected trailing JSON token"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.raw)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestFromAnnotations(t *testing.T) {
	got, err := FromAnnotations(nil)
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = FromAnnotations(map[string]string{
		constants.TrafficDrainAnnotation: `{"drain":{"cluster":"a","reason":"mitigation"}}`,
	})
	require.NoError(t, err)
	assert.Equal(t, []Override{{ID: "drain", Cluster: "a", Reason: "mitigation"}}, got)
}
