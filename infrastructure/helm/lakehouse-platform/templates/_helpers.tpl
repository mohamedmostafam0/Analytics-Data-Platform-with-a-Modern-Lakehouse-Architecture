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

{{/* Common Kubernetes labels shared by every migrated resource. */}}
{{- define "lakehouse-platform.baseLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
app.kubernetes.io/name: {{ include "lakehouse-platform.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/part-of: "lakehouse-platform"
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{/* Stable selector labels for the main PostgreSQL Cluster. */}}
{{- define "lakehouse-platform.postgresqlSelectorLabels" -}}
cnpg.io/cluster: {{ .Values.components.postgresql.clusterName | quote }}
{{- end -}}

{{/* Full main PostgreSQL labels. */}}
{{- define "lakehouse-platform.postgresqlLabels" -}}
{{ include "lakehouse-platform.baseLabels" . }}
app.kubernetes.io/component: "postgresql"
{{- end -}}

{{/* Main PostgreSQL bootstrap ConfigMap name. */}}
{{- define "lakehouse-platform.mainPostgresqlBootstrapName" -}}
{{- printf "%s-main-postgresql-bootstrap" (include "lakehouse-platform.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Stable MinIO selector labels. */}}
{{- define "lakehouse-platform.minioSelectorLabels" -}}
app.kubernetes.io/name: {{ include "lakehouse-platform.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/component: "minio"
{{- end -}}

{{/* Full MinIO labels. */}}
{{- define "lakehouse-platform.minioLabels" -}}
{{ include "lakehouse-platform.baseLabels" . }}
app.kubernetes.io/component: "minio"
{{- end -}}

{{/* Stable MinIO client selector labels. */}}
{{- define "lakehouse-platform.minioClientSelectorLabels" -}}
app.kubernetes.io/name: {{ include "lakehouse-platform.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/component: "minio-client"
{{- end -}}

{{/* Full MinIO client labels. */}}
{{- define "lakehouse-platform.minioClientLabels" -}}
{{ include "lakehouse-platform.baseLabels" . }}
app.kubernetes.io/component: "minio-client"
{{- end -}}

{{/* Release-qualified MinIO names. */}}
{{- define "lakehouse-platform.minioName" -}}
{{- printf "%s-minio" (include "lakehouse-platform.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lakehouse-platform.minioClientJobName" -}}
{{- printf "%s-minio-bootstrap" (include "lakehouse-platform.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Prefer immutable image digests when supplied. */}}
{{- define "lakehouse-platform.minioImage" -}}
{{- if .Values.components.minio.image.digest -}}
{{- printf "%s@%s" .Values.components.minio.image.repository .Values.components.minio.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.components.minio.image.repository .Values.components.minio.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "lakehouse-platform.minioClientImage" -}}
{{- if .Values.components.minioClient.image.digest -}}
{{- printf "%s@%s" .Values.components.minioClient.image.repository .Values.components.minioClient.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.components.minioClient.image.repository .Values.components.minioClient.image.tag -}}
{{- end -}}
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
