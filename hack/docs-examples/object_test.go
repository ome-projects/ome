package main

import (
	"strings"
	"testing"
)

func TestParseObject(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string // describe's result, or empty when the document isn't an object
	}{
		{
			name: "object",
			text: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings",
			want: "ConfigMap settings",
		},
		{
			name: "generated name",
			text: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  generateName: settings-",
			want: "ConfigMap settings-",
		},
		{
			// parse reports it: an object needs a name.
			name: "no name",
			text: "apiVersion: v1\nkind: Config\nclusters: []",
			want: "Config",
		},
		{name: "no apiVersion", text: "kind: ConfigMap\nmetadata:\n  name: settings"},
		{name: "no kind", text: "apiVersion: v1\nmetadata:\n  name: settings"},
		{name: "fragment", text: "spec:\n  model:\n    name: llama"},
		{name: "list", text: "- a\n- b"},
		{name: "comment", text: "# Nothing here yet."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := parseObject(tc.text)
			if err != nil {
				t.Fatalf("parseObject() error: %v", err)
			}
			got := ""
			if obj != nil {
				got = describe(obj)
			}
			if got != tc.want {
				t.Errorf("parseObject() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseObjectErrors(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string // in the message, with the document on line 10
	}{
		{name: "syntax", text: "a: 1\n  b: 2", want: "line 11:"},
		{name: "duplicate key", text: "a: 1\na: 2", want: `line 11: key "a" already set in map`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc := document{path: "page.md", line: 10, text: tc.text}
			_, err := parseObject(doc.text)
			if err == nil {
				t.Fatal("parseObject() succeeded, want an error")
			}
			if got := yamlMessage(doc, err); !strings.Contains(got, tc.want) {
				t.Errorf("yamlMessage() = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}
