{{/*
Common labels. The control-plane label is the stable selector key and must
never change; the rest follow chart conventions.
*/}}
{{- define "ome-alfred.labels" -}}
control-plane: ome-alfred
app.kubernetes.io/component: "ome-alfred"
app.kubernetes.io/name: ome-alfred
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end }}

{{/*
Full image URL with optional global hub prefix — the same contract as
ome-resources' ome.imageWithHub: if the repository already contains '/', the
hub is ignored.
*/}}
{{- define "ome-alfred.image" -}}
{{- $hub := .Values.global.hub }}
{{- $repo := .Values.image.repository }}
{{- $tag := .Values.image.tag | toString }}
{{- if and $hub (not (contains "/" $repo)) -}}
{{- printf "%s/%s:%s" $hub $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end }}

{{/*
Name of the recommendations ConfigMap — single source of truth is the policy
config, so the pre-created ConfigMap and the RBAC write grant can never
drift from what the reporter writes to.
*/}}
{{- define "ome-alfred.recommendationsName" -}}
{{- .Values.alfredConfig.recommendationsConfigMapName | default "alfred-recommendations" -}}
{{- end }}
