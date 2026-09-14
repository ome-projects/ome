{{/*
Constructs an image ref with an optional global hub prefix. If the repository
already contains '/', hub is ignored. Mirrors the ome-scheduler helper.
Parameters: values, repository, tag.
*/}}
{{- define "ome-quota-manager.image" -}}
{{- $hub := .values.global.hub -}}
{{- $repo := .repository -}}
{{- $tag := .tag -}}
{{- if and $hub (not (contains "/" $repo)) -}}
{{- printf "%s/%s:%s" $hub $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end -}}

{{/*
Chart name and version, suitable for use as a Kubernetes label value.
*/}}
{{- define "ome-quota-manager.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Selector labels — minimal and stable across re-renders.
*/}}
{{- define "ome-quota-manager.selectorLabels" -}}
app.kubernetes.io/name: ome-quota-manager
app.kubernetes.io/component: quota-manager
{{- end -}}

{{/*
Full label set stamped on chart resources.
*/}}
{{- define "ome-quota-manager.labels" -}}
{{ include "ome-quota-manager.selectorLabels" . }}
helm.sh/chart: {{ include "ome-quota-manager.chart" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: ome
{{- end -}}

{{/*
The one Service in front of this component. It fronts both the webhook and the
metrics endpoint, so the name is referenced from five places: the Service, the
serving cert's CN and DNS names, the ValidatingWebhookConfiguration's
clientConfig, the --webhook-service-name flag, and the ServiceMonitor's target.
A rename that missed any one of them would fail closed — the apiserver dialling
a name the cert does not cover rejects every AcceleratorQuota write.
*/}}
{{- define "ome-quota-manager.serviceName" -}}
{{ .Values.quotaManager.name }}-service
{{- end -}}

{{/*
Where the webhook serving cert lives inside the container. Not a values knob —
it is a container-local path, not an operator decision — but it is shared
between the volume mount and the flag, and the two silently disagreeing would
mean a webhook that never serves.
*/}}
{{- define "ome-quota-manager.certDir" -}}
/tmp/k8s-webhook-server/serving-certs
{{- end -}}

{{/*
The accelerator resources capacity derivation is configured with, as a list with
blanks removed.

Nil-safe and blank-safe on purpose. A GitOps overlay switching the feature off
naturally writes `capacity:` or `capacity: {resources: null}`, and Helm deletes
the defaulted map rather than merging it, so an unguarded lookup aborts the whole
render. Blanks are dropped because the binary drops them too — without that the
chart would grant cluster-wide Node reads for a list the binary reads as empty.
*/}}
{{/*
Cover resources as name=quantity pairs. Blank names and blank quantities are
dropped, so a partially-filled overlay disables materialization rather than
rendering a flag the binary rejects at startup.
*/}}
{{- define "ome-quota-manager.coverResources" -}}
{{- $materialize := .Values.quotaManager.materialize | default dict -}}
{{- $cover := $materialize.coverResources | default dict -}}
{{- $out := list -}}
{{- range $name, $qty := $cover -}}
{{- if and (trim (toString $name)) (trim (toString $qty)) -}}
{{- $out = append $out (printf "%s=%s" (trim (toString $name)) (trim (toString $qty))) -}}
{{- end -}}
{{- end -}}
{{- join "," (sortAlpha $out) -}}
{{- end -}}

{{/*
The namespaces this cluster serves budgeted workloads from, as a comma-separated
list. Blank entries are dropped and the result is sorted, so an overlay cannot
change the flag by reordering and duplicates in an overlay collapse.
*/}}
{{- define "ome-quota-manager.enrolledNamespaces" -}}
{{- $materialize := .Values.quotaManager.materialize | default dict -}}
{{- $out := list -}}
{{- range $ns := ($materialize.enrolledNamespaces | default list) -}}
{{- if trim (toString $ns) -}}
{{- $out = append $out (trim (toString $ns)) -}}
{{- end -}}
{{- end -}}
{{- join "," (sortAlpha (uniq $out)) -}}
{{- end -}}

{{/*
The field manager that owns applied Kueue objects. Defaults to the component
name so a single-manager install needs no configuration, while two managers on
one cluster can be told apart.
*/}}
{{- define "ome-quota-manager.fieldManager" -}}
{{- $materialize := .Values.quotaManager.materialize | default dict -}}
{{- $fm := $materialize.fieldManager | default "" -}}
{{- if trim (toString $fm) -}}
{{- trim (toString $fm) -}}
{{- else -}}
{{- .Values.quotaManager.name -}}
{{- end -}}
{{- end -}}

{{- define "ome-quota-manager.acceleratorResources" -}}
{{- $capacity := .Values.quotaManager.capacity | default dict -}}
{{- $resources := $capacity.resources | default list -}}
{{- $out := list -}}
{{- range $resources -}}
{{- if trim (toString .) -}}
{{- $out = append $out (trim (toString .)) -}}
{{- end -}}
{{- end -}}
{{- join "," $out -}}
{{- end -}}

{{/*
Fails the render unless mode is one of the two the binary accepts. There is no
safe default: workload mode writes Kueue objects on this cluster and management
mode projects shares onto others, so an unset or mistyped value must stop the
install rather than produce a Deployment that crash-loops on a flag check.
*/}}
{{- define "ome-quota-manager.mode" -}}
{{- $mode := .Values.quotaManager.mode -}}
{{- if not (has $mode (list "workload" "management")) -}}
{{- fail (printf "quotaManager.mode must be \"workload\" or \"management\", got %q" $mode) -}}
{{- end -}}
{{- $mode -}}
{{- end -}}
