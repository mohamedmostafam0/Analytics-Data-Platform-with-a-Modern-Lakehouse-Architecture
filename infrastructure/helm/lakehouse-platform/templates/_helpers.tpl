{{/* Chart name, constrained to a valid Kubernetes name. */}}
{{- define "lakehouse-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Release-qualified resource name. */}}
{{- define "lakehouse-platform.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "lakehouse-platform.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Common Kubernetes labels shared by every Phase 2 resource. */}}
{{- define "lakehouse-platform.baseLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
app.kubernetes.io/name: {{ include "lakehouse-platform.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/part-of: "lakehouse-platform"
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{/* Foundation labels. */}}
{{- define "lakehouse-platform.labels" -}}
{{ include "lakehouse-platform.baseLabels" . }}
app.kubernetes.io/component: "platform-foundation"
{{- end -}}

{{/* Stable selector labels for the one-shot item seeder. */}}
{{- define "lakehouse-platform.itemsSelectorLabels" -}}
app.kubernetes.io/name: {{ include "lakehouse-platform.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/component: "items-load-generator"
{{- end -}}

{{/* Full item-seeder labels. */}}
{{- define "lakehouse-platform.itemsLabels" -}}
{{ include "lakehouse-platform.baseLabels" . }}
app.kubernetes.io/component: "items-load-generator"
{{- end -}}

{{/* Release-qualified Job name. */}}
{{- define "lakehouse-platform.itemsJobName" -}}
{{- printf "%s-items-load" (include "lakehouse-platform.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* PostgreSQL bootstrap ConfigMap name. */}}
{{- define "lakehouse-platform.postgresqlBootstrapName" -}}
{{- printf "%s-items-bootstrap" (include "lakehouse-platform.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Prefer an immutable image digest when one is supplied. */}}
{{- define "lakehouse-platform.itemsImage" -}}
{{- if .Values.components.itemsLoadGenerator.image.digest -}}
{{- printf "%s@%s" .Values.components.itemsLoadGenerator.image.repository .Values.components.itemsLoadGenerator.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.components.itemsLoadGenerator.image.repository .Values.components.itemsLoadGenerator.image.tag -}}
{{- end -}}
{{- end -}}
