package revision

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// composeMetaVersions is the metadata of a template at version 2 (current),
// the part of it version 2 hashes (owned), and the stored version 1. Version
// 2 hashes the app label, the label it added and the note annotation; the
// queue label and the local-queue annotation belong to no revision.
func composeMetaVersions() (current, owned, stored *metav1.ObjectMeta) {
	current = &metav1.ObjectMeta{
		Labels: map[string]string{
			"app.example.com/name":       "llama-v2",
			"example.com/added-by-v2":    "yes",
			constants.KueueQueueLabelKey: "team-b",
		},
		Annotations: map[string]string{
			"example.com/note":   "v2",
			constants.LocalQueue: "team-b",
		},
	}
	owned = &metav1.ObjectMeta{
		Labels:      map[string]string{"app.example.com/name": "llama-v2", "example.com/added-by-v2": "yes"},
		Annotations: map[string]string{"example.com/note": "v2"},
	}
	stored = &metav1.ObjectMeta{
		Labels:      map[string]string{"app.example.com/name": "llama-v1"},
		Annotations: map[string]string{"example.com/note": "v1"},
	}
	return current, owned, stored
}

func TestComposeMeta(t *testing.T) {
	current, owned, stored := composeMetaVersions()
	cases := []struct {
		name                  string
		current, owned, store *metav1.ObjectMeta
		want                  *metav1.ObjectMeta
	}{
		{
			name: "no metadata on either side composes none",
		},
		{
			name:    "revision keys come from the stored revision and the rest follow the current template",
			current: current, owned: owned, store: stored,
			want: &metav1.ObjectMeta{
				Labels: map[string]string{
					"app.example.com/name":       "llama-v1",
					constants.KueueQueueLabelKey: "team-b",
				},
				Annotations: map[string]string{
					"example.com/note":   "v1",
					constants.LocalQueue: "team-b",
				},
			},
		},
		{
			name:    "with no hashed metadata in hand every current key is kept under the stored values",
			current: current, store: stored,
			want: &metav1.ObjectMeta{
				Labels: map[string]string{
					"app.example.com/name":       "llama-v1",
					"example.com/added-by-v2":    "yes",
					constants.KueueQueueLabelKey: "team-b",
				},
				Annotations: map[string]string{
					"example.com/note":   "v1",
					constants.LocalQueue: "team-b",
				},
			},
		},
		{
			name:  "with no current template the stored revision's metadata is all there is",
			owned: owned, store: stored,
			want: &metav1.ObjectMeta{
				Labels:      map[string]string{"app.example.com/name": "llama-v1"},
				Annotations: map[string]string{"example.com/note": "v1"},
			},
		},
		{
			name:    "a stored revision that records no metadata keeps only the keys no revision hashes",
			current: current, owned: owned,
			want: &metav1.ObjectMeta{
				Labels:      map[string]string{constants.KueueQueueLabelKey: "team-b"},
				Annotations: map[string]string{constants.LocalQueue: "team-b"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComposeMeta(tc.current, tc.owned, tc.store)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("ComposeMeta (-want +got):\n%s", diff)
			}
		})
	}
}

func TestComposeMeta_TheInputsAreNotWritten(t *testing.T) {
	current := &metav1.ObjectMeta{
		Labels:      map[string]string{"app.example.com/name": "llama-v2", constants.KueueQueueLabelKey: "team-a"},
		Annotations: map[string]string{constants.LocalQueue: "team-a"},
	}
	owned := &metav1.ObjectMeta{Labels: map[string]string{"app.example.com/name": "llama-v2"}}
	stored := &metav1.ObjectMeta{
		Labels:      map[string]string{"app.example.com/name": "llama-v1"},
		Annotations: map[string]string{"example.com/note": "v1"},
	}
	before := []*metav1.ObjectMeta{current.DeepCopy(), owned.DeepCopy(), stored.DeepCopy()}

	got := ComposeMeta(current, owned, stored)
	got.Labels["example.com/written-after"] = "x"
	got.Annotations["example.com/written-after"] = "x"

	for i, in := range []*metav1.ObjectMeta{current, owned, stored} {
		if diff := cmp.Diff(before[i], in); diff != "" {
			t.Errorf("input %d changed (-before +after):\n%s", i, diff)
		}
	}
}
