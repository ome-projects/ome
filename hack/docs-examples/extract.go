package main

import (
	"regexp"
	"strings"
)

// A document is one YAML document from a fenced yaml block.
type document struct {
	path string
	line int // 1-based line of the document's first non-blank line
	text string
}

var (
	// fenceOpen matches a line that opens a fenced code block, with the
	// renderer's rules: any indentation, three or more backticks or tildes,
	// then the info string.
	fenceOpen = regexp.MustCompile("^(\\s*)(`{3,}|~{3,})(.*)$")
	// attribute matches one key=value pair in an info string.
	attribute = regexp.MustCompile(`([\w-]+)=(?:"([^"]*)"|(\S+))`)
)

// extractDocuments returns the YAML documents in the fenced yaml blocks of
// a Markdown file, skipping blocks marked check=skip. Each block is
// dedented by its fence's indentation, so blocks inside tabs and callouts
// keep their YAML structure. As in CommonMark, a fence that is never
// closed runs to the end of the file.
func extractDocuments(path, markdown string) []document {
	lines := strings.Split(markdown, "\n")
	var docs []document
	for i := 0; i < len(lines); i++ {
		open := fenceOpen.FindStringSubmatch(lines[i])
		// A backtick fence's info string can't contain a backtick.
		if open == nil || (open[2][0] == '`' && strings.Contains(open[3], "`")) {
			continue
		}
		indent, marker, info := open[1], open[2], open[3]
		end := i + 1
		for end < len(lines) && !closesFence(lines[end], marker) {
			end++
		}
		if isCheckedYAML(info) {
			body := make([]string, 0, end-i-1)
			for _, line := range lines[i+1 : end] {
				body = append(body, dedent(line, len(indent)))
			}
			docs = append(docs, splitDocuments(path, i+2, body)...)
		}
		i = end
	}
	return docs
}

// closesFence reports whether line closes a fence opened with marker: a
// run of the same character, at least as long, with optional whitespace.
func closesFence(line, marker string) bool {
	trimmed := strings.TrimSpace(line)
	return len(trimmed) >= len(marker) && strings.Trim(trimmed, marker[:1]) == ""
}

// isCheckedYAML reports whether a fence's info string opens a yaml (or
// yml) block that isn't marked check=skip.
func isCheckedYAML(info string) bool {
	info = strings.TrimSpace(info)
	fields := strings.Fields(info)
	if len(fields) == 0 {
		return false
	}
	if name := strings.ToLower(fields[0]); name != "yaml" && name != "yml" {
		return false
	}
	for _, m := range attribute.FindAllStringSubmatch(info[len(fields[0]):], -1) {
		if m[1] == "check" && m[3] == "skip" {
			return false
		}
	}
	return true
}

// dedent removes up to n leading spaces or tabs from line.
func dedent(line string, n int) string {
	i := 0
	for i < n && i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	return line[i:]
}

// splitDocuments splits a yaml block's lines into documents at separator
// lines, dropping blank lines around each document. first is the file
// line of body[0].
func splitDocuments(path string, first int, body []string) []document {
	var docs []document
	start := 0
	for i := 0; i <= len(body); i++ {
		if i < len(body) && !isSeparator(body[i]) {
			continue
		}
		end := i
		for start < end && strings.TrimSpace(body[start]) == "" {
			start++
		}
		for end > start && strings.TrimSpace(body[end-1]) == "" {
			end--
		}
		if start < end {
			docs = append(docs, document{
				path: path,
				line: first + start,
				text: strings.Join(body[start:end], "\n"),
			})
		}
		start = i + 1
	}
	return docs
}

// isSeparator reports whether line separates YAML documents. As in
// kubectl, only whitespace and a comment may follow the "---".
func isSeparator(line string) bool {
	rest, ok := strings.CutPrefix(line, "---")
	if !ok {
		return false
	}
	rest = strings.TrimSpace(rest)
	return rest == "" || strings.HasPrefix(rest, "#")
}
