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
Define which repository to use according to the following:
1. Explicitly defined
2. Otherwise if enterprise is enabled - take enterprise image
3. Otherwise use OSS image
*/}}
{{- define "shub.repository" -}}
{{- if not .Values.image.repository }}
{{- if (.Values.enterprise).enabled }}
{{- default "treeverse/shub-enterprise" .Values.image.repository }}
{{- else }}
{{- default "treeverse/shub" .Values.image.repository }}
{{- end }}
{{- else }}
{{- default .Values.image.repository }}
{{- end }}
{{- end }}

{{/*
Select the image tag. An explicit .Values.image.tag wins (back-compat override).
Otherwise pick community or enterprise tag based on the enterprise flag so each
variant can release on its own cadence.
*/}}
{{- define "shub.tag" -}}
{{- if .Values.image.tag }}
{{- .Values.image.tag }}
{{- else if (.Values.enterprise).enabled }}
{{- required "image.enterprise.tag is required when enterprise.enabled is true" (((.Values.image).enterprise).tag) }}
{{- else }}
{{- required "image.community.tag is required" (((.Values.image).community).tag) }}
{{- end }}
{{- end }}

{{- define "shub.checkDeprecated" -}}
{{- if .Values.fluffy -}}
{{- fail "Fluffy configuration detected. Please migrate to shub Enterprise auth configuration and use treeverse/shub-enterprise docker image. See migration guide: https://docs.shub.io/latest/enterprise/upgrade/#kubernetes-migrating-with-helm-from-fluffy-to-new-shub-enterprise." -}}
{{- end -}}
{{- end -}}

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

{{- define "shub.dockerConfigJson" }}
{{- $token := .Values.image.privateRegistry.secretToken }}
{{- $username := "externalshub" }}
{{- $registry := "https://index.docker.io/v1/" }}
{{- printf "{\"auths\":{\"%s\":{\"username\":\"%s\",\"password\":\"%s\",\"auth\":\"%s\"}}}" $registry $username $token (printf "%s:%s" $username $token | b64enc) | b64enc }}
{{- end }}
