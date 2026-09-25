package query

import (
	"testing"
)

func TestRevisionHashFromControllerRevisionName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"no separator", "abcd1234", ""},
		{"single separator", "isvc-hash", "hash"},
		{"isvc-component-hash", "llama-engine-abcd1234", "abcd1234"},
		{"trailing dash returns empty suffix", "llama-engine-", ""},
		{"isvc name with dashes", "llama-70b-instruct-engine-abcd1234", "abcd1234"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RevisionHashFromControllerRevisionName(tc.in); got != tc.want {
				t.Errorf("RevisionHashFromControllerRevisionName(%q): got %q want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestLabelPodGroup_MatchesUpstream pins the exact label key the
// scheduler-plugins coscheduler reads. The constant must match
// `scheduling.x-k8s.io/pod-group` byte-for-byte — drift here means
// the scheduler will silently NOT enforce gang placement.
func TestLabelPodGroup_MatchesUpstream(t *testing.T) {
	const want = "scheduling.x-k8s.io/pod-group"
	if LabelPodGroup != want {
		t.Errorf("LabelPodGroup: got %q want %q", LabelPodGroup, want)
	}
}

func TestAnnotationTopologyKey_MatchesSchedulerContract(t *testing.T) {
	const want = "ome.io/topology-key"
	if AnnotationTopologyKey != want {
		t.Errorf("AnnotationTopologyKey: got %q want %q", AnnotationTopologyKey, want)
	}
}
