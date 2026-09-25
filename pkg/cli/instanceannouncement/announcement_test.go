package instanceannouncement_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/ome/pkg/cli/instanceannouncement"
)

func TestParseAcceptsBoundedCanonicalOperationOrIncarnationEpisodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		marker string
		want   instanceannouncement.Value
		ok     bool
	}{
		{marker: "RepairHeld@update-2-9", want: instanceannouncement.Value{Reason: "RepairHeld", Episode: "update-2-9"}, ok: true},
		{marker: "RepairHeld@old-operation", want: instanceannouncement.Value{Reason: "RepairHeld", Episode: "old-operation"}, ok: true},
		{marker: "GangSplitRisk@#7", want: instanceannouncement.Value{Reason: "GangSplitRisk", Episode: "#7"}, ok: true},
		{marker: "FutureSafeReason@#0", want: instanceannouncement.Value{Reason: "FutureSafeReason", Episode: "#0"}, ok: true},
		{marker: "FutureSafeReason@UPDATE_2.9", want: instanceannouncement.Value{Reason: "FutureSafeReason", Episode: "UPDATE_2.9"}, ok: true},
		{marker: "RepairHeld@update-2-9@extra"},
		{marker: "missing-separator"},
		{marker: "bad reason@#7"},
		{marker: "RepairHeld@ghp_0123456789abcdefghijklmnopqrstuvwxyz"},
		{marker: "RepairHeld@" + slackCredential("xoxb", "123456789012-1234567890123-abcdefghijklmnopqrstuvwx")},
		{marker: "RepairHeld@migrate-" + slackCredential("xoxb", "123456789012-1234567890123-abcdefghijklmnopqrstuvwx") + "-1790300000"},
		{marker: "RepairHeld@" + slackCredential("xoxp", "123456789012-1234567890123-abcdefghijklmnopqrstuvwx")},
		{marker: "RepairHeld@" + slackCredential("xapp", "1-A1234567890-1234567890-abcdefghijklmnopqrstuvwxyz")},
		{marker: "RepairHeld@AKIAIOSFODNN7EXAMPLE"},
		{marker: "RepairHeld@eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJl"},
		{marker: "RepairHeld@password:hunter2"},
		{marker: "AKIAIOSFODNN7EXAMPLE@#7"},
		{marker: "RepairHeld@#-1"},
		{marker: "RepairHeld@#07"},
	}
	for _, test := range tests {
		t.Run(test.marker, func(t *testing.T) {
			t.Parallel()
			got, ok := instanceannouncement.Parse(test.marker)
			assert.Equal(t, test.ok, ok)
			assert.Equal(t, test.want, got)
		})
	}
}

func slackCredential(prefix, suffix string) string {
	return prefix + "-" + suffix
}

func TestParseBoundsBothTokens(t *testing.T) {
	t.Parallel()

	reason := "A"
	for len(reason) < 129 {
		reason += "a"
	}
	_, ok := instanceannouncement.Parse(reason + "@#7")
	assert.False(t, ok)

	episode := "a"
	for len(episode) < 129 {
		episode += "a"
	}
	_, ok = instanceannouncement.Parse("RepairHeld@" + episode)
	assert.False(t, ok)
}
