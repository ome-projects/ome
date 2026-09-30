package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestExtractDocuments(t *testing.T) {
	tests := []struct {
		name     string
		markdown string
		want     []document
	}{
		{
			name:     "yaml block",
			markdown: "Intro\n\n```yaml\na: 1\n```\n",
			want:     []document{{path: "page.md", line: 4, text: "a: 1"}},
		},
		{
			name:     "yml and attributes",
			markdown: "```YML title=\"model.yaml\"\na: 1\n```\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
		{
			name:     "tilde fence",
			markdown: "~~~yaml\na: 1\n~~~\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
		{
			name:     "check=skip",
			markdown: "```yaml check=skip\na: 1\n```\n",
		},
		{
			name:     "check=skip inside a title",
			markdown: "```yaml title=\"check=skip\"\na: 1\n```\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
		{
			name:     "other languages",
			markdown: "```bash\nkubectl apply -f model.yaml\n```\n",
		},
		{
			name:     "documents",
			markdown: "```yaml\na: 1\n---\nb: 2\n--- # comment\n\nc: 3\n\n```\n",
			want: []document{
				{path: "page.md", line: 2, text: "a: 1"},
				{path: "page.md", line: 4, text: "b: 2"},
				{path: "page.md", line: 7, text: "c: 3"},
			},
		},
		{
			name:     "indented in a tab",
			markdown: "=== \"Tab\"\n\n    ```yaml\n    a:\n      b: 1\n    ```\n",
			want:     []document{{path: "page.md", line: 4, text: "a:\n  b: 1"}},
		},
		{
			name:     "fence inside a longer fence",
			markdown: "````markdown\n```yaml\na: 1\n```\n````\n",
		},
		{
			name:     "unclosed fence",
			markdown: "```yaml\na: 1\n",
			want:     []document{{path: "page.md", line: 2, text: "a: 1"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractDocuments("page.md", tc.markdown)
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(document{})); diff != "" {
				t.Errorf("extractDocuments() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
