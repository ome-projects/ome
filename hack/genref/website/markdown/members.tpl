{{- /* members prints a table row per field. */}}
{{- define "members" }}
{{- range .GetMembers }}
{{- template "member" . }}
{{- end }}
{{- end }}

{{- /* member prints a field's row. */}}
{{- define "member" }}
{{- if not .Hidden }}
{{- if .IsInline }}
{{- template "embedded" . }}
{{- else }}
<tr><td><code>{{ .FieldName }}</code>{{ template "badge" . }}</td>
{{- template "field" . }}
{{- end }}
{{- end }}
{{- end }}

{{- /*
	kindmembers prints a kind's rows. The spec and status fields of a kind
	rarely have a doc comment, so without one they get a stock description.
*/}}
{{- define "kindmembers" }}
{{- $kind := .Name.Name }}
{{- range .GetMembers }}
{{- $text := false }}
{{- range .CommentLines }}{{ if and (ne . "") (ne (slice . 0 1) "+") }}{{ $text = true }}{{ end }}{{ end }}
{{- if and (not .Hidden) (not $text) (or (eq .FieldName "spec") (eq .FieldName "status")) }}
<tr><td><code>{{ .FieldName }}</code>{{ if eq .FieldName "spec" }}{{ template "badge" . }}{{ end }}</td>
<td>{{ template "typelink" .GetType }}</td>
<td>

The {{ if eq .FieldName "spec" }}desired{{ else }}observed{{ end }} state of the `{{ $kind }}`.

</td></tr>
{{- else }}
{{- template "member" . }}
{{- end }}
{{- end }}
{{- end }}

{{- /*
	badge prints the Required badge of a field that must be set. A field is
	required on the same terms controller-gen uses for the CRD schema: it's
	marked +required, or it's none of +optional, inline and omitempty. A
	field with a default isn't marked, since the API server sets it.
*/}}
{{- define "badge" }}
{{- $omitempty := printf "json:\"%s,omitempty\"" .FieldName }}
{{- $required := not (or .IsOptional (and (ge (len .Tags) (len $omitempty)) (eq (slice .Tags 0 (len $omitempty)) $omitempty))) }}
{{- $default := false }}
{{- range .CommentLines }}
{{- if eq . "+required" }}{{ $required = true }}{{ end }}
{{- if and (ge (len .) 21) (eq (slice . 0 21) "+kubebuilder:default=") }}{{ $default = true }}{{ end }}
{{- end }}
{{- if and $required (not $default) }} <span class="doc-api-required">Required</span>{{ end }}
{{- end }}

{{- /*
	statusmembers prints the rows of a status type. OME writes these
	fields, so none is marked Required.
*/}}
{{- define "statusmembers" }}
{{- range .GetMembers }}
{{- if not .Hidden }}
{{- if .IsInline }}
{{- template "embedded" . }}
{{- else }}
<tr><td><code>{{ .FieldName }}</code></td>
{{- template "field" . }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{- /* embedded prints the row of an embedded struct. */}}
{{- define "embedded" }}
<tr><td><em>Embedded</em></td>
<td>{{ template "typelink" .GetType }}</td>
<td>
{{ template "comment" .CommentLines }}

The fields of `{{ template "shortname" .GetType }}` appear directly in this object.

</td></tr>
{{- end }}

{{- /* field prints the type and description cells of a field's row. */}}
{{- define "field" }}
<td>{{ template "typelink" .GetType }}</td>
<td>
{{ template "sincetext" .CommentLines }}{{ template "comment" .CommentLines }}
{{- template "markers" .CommentLines }}

</td></tr>
{{- end }}

{{- /*
	typelink links a field's type. genref builds a broken anchor for a map
	or slice of pointers to a local type, so those link to the element type.
*/}}
{{- define "typelink" -}}
{{- $link := .Link }}
{{- if and .Elem (eq .Elem.Kind "Pointer") (eq .Elem.Elem.Name.Package "sigs.k8s.io/ome/pkg/apis/ome/v1beta1") }}
{{- $link = printf "#ome-io-v1beta1-%s" .Elem.Elem.Name.Name }}
{{- end }}
{{- if $link }}<a href="{{ $link }}"><code>{{ template "shortname" . }}</code></a>{{ else }}<code>{{ template "shortname" . }}</code>{{ end -}}
{{- end }}

{{- /* shortname is the type's name without its package path, such as []Container. */}}
{{- define "shortname" -}}
{{- if eq .Kind "Pointer" }}{{ template "shortname" .Elem }}
{{- else if eq .Kind "Slice" }}[]{{ template "shortname" .Elem }}
{{- else if eq .Kind "Map" }}map[{{ .Key.Name.Name }}]{{ template "shortname" .Elem }}
{{- else }}{{ .Name.Name }}{{ end -}}
{{- end }}
