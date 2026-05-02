{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "shub.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "shub.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "shub.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "shub.labels" -}}
helm.sh/chart: {{ include "shub.chart" . }}
{{ include "shub.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "shub.selectorLabels" -}}
app: {{ include "shub.name" . }}
app.kubernetes.io/name: {{ include "shub.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "shub.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "shub.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image repository.
*/}}
{{- define "shub.repository" -}}
{{- default "ghcr.io/invergent-ai/surogate-hub" .Values.image.repository }}
{{- end }}

{{/*
Image tag.
*/}}
{{- define "shub.tag" -}}
{{- default "latest" .Values.image.tag }}
{{- end }}

{{/*
Replication resource full name
*/}}
{{- define "replication.fullname" -}}
{{- $name := include "shub.fullname" . }}
{{- printf "%s-replication" $name | trunc 63 }}
{{- end }}

{{/*
Replication common labels
*/}}
{{- define "replication.labels" -}}
helm.sh/chart: {{ include "shub.chart" . }}
{{ include "replication.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Replication selector labels
*/}}
{{- define "replication.selectorLabels" -}}
app.kubernetes.io/name: {{ include "shub.name" . }}-replication
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: replication
app: {{ include "shub.name" . }}-replication
{{- end }}

{{/*
Audit maintenance resource full name
*/}}
{{- define "audit.fullname" -}}
{{- $name := include "shub.fullname" . }}
{{- printf "%s-audit-maintain" $name | trunc 63 }}
{{- end }}

{{/*
Audit maintenance common labels
*/}}
{{- define "audit.labels" -}}
helm.sh/chart: {{ include "shub.chart" . }}
{{ include "audit.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Audit maintenance selector labels
*/}}
{{- define "audit.selectorLabels" -}}
app.kubernetes.io/name: {{ include "shub.name" . }}-audit-maintain
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: audit-maintain
app: {{ include "shub.name" . }}-audit-maintain
{{- end }}

{{/*
Stats-worker resource full name
*/}}
{{- define "shub.statsWorker.fullname" -}}
{{- printf "%s-stats-worker" (include "shub.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Stats-worker selector labels
*/}}
{{- define "shub.statsWorker.selectorLabels" -}}
app: {{ include "shub.name" . }}-stats-worker
app.kubernetes.io/name: {{ include "shub.name" . }}-stats-worker
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: stats-worker
{{- end }}

{{/*
Stats-worker common labels
*/}}
{{- define "shub.statsWorker.labels" -}}
helm.sh/chart: {{ include "shub.chart" . }}
{{ include "shub.statsWorker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: {{ include "shub.name" . }}
{{- end }}

{{- define "shub.dockerConfigJson" }}
{{- $token := .Values.image.privateRegistry.secretToken }}
{{- $username := "externalshub" }}
{{- $registry := "https://index.docker.io/v1/" }}
{{- printf "{\"auths\":{\"%s\":{\"username\":\"%s\",\"password\":\"%s\",\"auth\":\"%s\"}}}" $registry $username $token (printf "%s:%s" $username $token | b64enc) | b64enc }}
{{- end }}
