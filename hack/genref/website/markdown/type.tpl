{{- define "kind" }}
## `{{ .Name.Name }}` {#{{ .Anchor }}{{ template "sinceattr" .CommentLines }}}
{{ template "comment" .CommentLines }}
{{- template "markers" .CommentLines }}

<table class="doc-api-fields">
<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>apiVersion</code> <span class="doc-api-required">Required</span></td>
<td><code>string</code></td>
<td>

`{{ .APIGroup }}`

</td></tr>
<tr><td><code>kind</code> <span class="doc-api-required">Required</span></td>
<td><code>string</code></td>
<td>

`{{ .Name.Name }}`

</td></tr>
<tr><td><code>metadata</code></td>
<td><a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/#objectmeta-v1-meta"><code>ObjectMeta</code></a></td>
<td>

Standard object metadata, such as `name` and `labels`.

</td></tr>
{{- template "kindmembers" . }}
</tbody>
</table>
{{- end }}

{{- define "type" }}
{{- template "typehead" . }}
{{- if .GetMembers }}

<table class="doc-api-fields">
<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
<tbody>
{{- template "members" . }}
</tbody>
</table>
{{- end }}
{{- end }}

{{- /* statustype prints a status type: its fields aren't marked Required. */}}
{{- define "statustype" }}
{{- template "typehead" . }}
{{- if .GetMembers }}

<table class="doc-api-fields">
<thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
<tbody>
{{- template "statusmembers" . }}
</tbody>
</table>
{{- end }}
{{- end }}

{{- /* typehead prints a supporting type's heading and description. */}}
{{- define "typehead" }}
### `{{ .Name.Name }}` {#{{ .Anchor }}{{ template "sinceattr" .CommentLines }}}
{{- with .References }}

Used by {{ range $i, $ref := . }}{{ if $i }}, {{ end }}[`{{ $ref.DisplayName }}`]({{ $ref.Link }}){{ end }}.
{{- end }}
{{ template "comment" .CommentLines }}
{{- if eq .Kind "Alias" }}

Underlying type: `{{ .Underlying }}`.
{{- end }}
{{- template "markers" .CommentLines }}
{{- end }}

{{- /*
	comment prints doc comment lines as Markdown without the +marker lines.
	Lines are HTML-escaped so placeholders such as spec.<component> stay
	visible, except lines with a code span: entities inside backticks would
	show literally, and marked escapes code spans itself.
*/}}
{{- define "comment" }}
{{- range . }}
{{- if or (eq . "") (ne (slice . 0 1) "+") }}
{{- $line := . }}
{{- $code := false }}
{{- range $i := len $line }}{{ if eq (index $line $i) 96 }}{{ $code = true }}{{ end }}{{ end }}
{{ if $code }}{{ $line }}{{ else }}{{ html $line }}{{ end }}
{{- end }}
{{- end }}
{{- end }}

{{- /*
	sinceattr prints a heading's since attribute, such as " since=v1.3",
	for a +ome:since marker. controller-gen ignores the marker.
*/}}
{{- define "sinceattr" }}
{{- range . }}
{{- if and (gt (len .) 11) (eq (slice . 0 11) "+ome:since=") }} since={{ slice . 11 }}{{ end }}
{{- end }}
{{- end }}

{{- /*
	sincetext starts a field's description with "Since v1.3." for a
	+ome:since marker, since a table row can't carry a since badge.
*/}}
{{- define "sincetext" }}
{{- range . }}
{{- if and (gt (len .) 11) (eq (slice . 0 11) "+ome:since=") }}
Since {{ slice . 11 }}.
{{- end }}
{{- end }}
{{- end }}

{{- /*
	markers prints what kubebuilder markers set: the default, the allowed
	values, the bounds, and the message of each validation rule. A default
	is shown as you'd write it in YAML: a quoted string that starts with a
	letter, such as "ome.io", loses its quotes, while "" and "1" keep them.
*/}}
{{- define "markers" }}
{{- range . }}
{{- if and (gt (len .) 21) (eq (slice . 0 21) "+kubebuilder:default=") }}
{{- $value := slice . 21 }}
{{- $end := 0 }}
{{- range $i := len $value }}{{ if and $i (eq (index $value $i) 34) }}{{ $end = $i }}{{ end }}{{ end }}
{{- if and (eq (index $value 0) 34) (gt $end 1) (eq (len (slice $value $end)) 1) }}
{{- $first := index $value 1 }}
{{- if or (and (ge $first 65) (le $first 90)) (and (ge $first 97) (le $first 122)) }}{{ $value = slice $value 1 $end }}{{ end }}
{{- end }}

Default: `{{ $value }}`.
{{- end }}
{{- if and (ge (len .) 29) (eq (slice . 0 29) "+kubebuilder:validation:Enum=") }}
{{- $values := slice . 29 }}

Allowed values: `{{ range $i := len $values }}{{ if eq (index $values $i) 59 }}`, `{{ else }}{{ printf "%c" (index $values $i) }}{{ end }}{{ end }}`.
{{- end }}
{{- end }}
{{- template "bounds" . }}
{{- template "rules" . }}
{{- end }}

{{- /*
	bounds prints the Minimum, Maximum, length, item count and Pattern
	markers on one line, such as "Minimum: `0`. Maximum: `100`." A marker
	is "+kubebuilder:validation:<name>=<value>"; a Pattern value is quoted
	in backticks.
*/}}
{{- define "bounds" }}
{{- $bounds := "" }}
{{- range . }}
{{- if and (gt (len .) 24) (eq (slice . 0 24) "+kubebuilder:validation:") }}
{{- $marker := slice . 24 }}
{{- $name := "" }}
{{- $value := "" }}
{{- range $i := len $marker }}{{ if eq (index $marker $i) 61 }}{{ $name = slice $marker 0 $i }}{{ $value = slice (slice $marker $i) 1 }}{{ break }}{{ end }}{{ end }}
{{- $label := "" }}
{{- if eq $name "Minimum" }}{{ $label = "Minimum" }}
{{- else if eq $name "Maximum" }}{{ $label = "Maximum" }}
{{- else if eq $name "MinLength" }}{{ $label = "Minimum length" }}
{{- else if eq $name "MaxLength" }}{{ $label = "Maximum length" }}
{{- else if eq $name "MinItems" }}{{ $label = "Minimum items" }}
{{- else if eq $name "MaxItems" }}{{ $label = "Maximum items" }}
{{- else if eq $name "MaxProperties" }}{{ $label = "Maximum keys" }}
{{- else if eq $name "Pattern" }}{{ $label = "Pattern" }}
{{- else if eq $name "items:Minimum" }}{{ $label = "Minimum per item" }}
{{- else if eq $name "items:Maximum" }}{{ $label = "Maximum per item" }}
{{- else if eq $name "items:MinLength" }}{{ $label = "Minimum length per item" }}
{{- else if eq $name "items:MaxLength" }}{{ $label = "Maximum length per item" }}
{{- end }}
{{- if and $label $value }}
{{- if eq (index $value 0) 96 }}
{{- $quoted := slice $value 1 }}
{{- range $i := len $quoted }}{{ if eq (index $quoted $i) 96 }}{{ $value = slice $quoted 0 $i }}{{ break }}{{ end }}{{ end }}
{{- end }}
{{- if $bounds }}{{ $bounds = print $bounds " " }}{{ end }}
{{- $bounds = print $bounds $label ": `" $value "`." }}
{{- end }}
{{- end }}
{{- end }}
{{- if $bounds }}

{{ $bounds }}
{{- end }}
{{- end }}

{{- /*
	rules prints the message of each CEL validation rule, the message="..."
	part of an XValidation marker, as a "Validation:" line.
*/}}
{{- define "rules" }}
{{- range . }}
{{- if and (gt (len .) 36) (eq (slice . 0 36) "+kubebuilder:validation:XValidation:") }}
{{- $line := . }}
{{- $message := "" }}
{{- range $i := len $line }}
{{- $tail := slice $line $i }}
{{- if and (ge (len $tail) 10) (eq (slice $tail 0 10) ",message=\"") }}{{ $message = slice $tail 10 }}{{ break }}{{ end }}
{{- end }}
{{- range $i := len $message }}{{ if eq (index $message $i) 34 }}{{ $message = slice $message 0 $i }}{{ break }}{{ end }}{{ end }}
{{- if $message }}
{{- $last := 0 }}
{{- range $i := len $message }}{{ $last = index $message $i }}{{ end }}

Validation: {{ html $message }}{{ if ne $last 46 }}.{{ end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
