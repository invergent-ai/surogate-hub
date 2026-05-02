{{- define "shub.env" -}}
env:
  {{- if and .Values.existingSecret .Values.secretKeys.databaseConnectionString }}
  - name: SHUB_DATABASE_POSTGRES_CONNECTION_STRING
    valueFrom:
      secretKeyRef:
        name: {{ .Values.existingSecret }}
        key: {{ .Values.secretKeys.databaseConnectionString }}
  {{- else if and .Values.secrets (.Values.secrets).databaseConnectionString }}
  - name: SHUB_DATABASE_POSTGRES_CONNECTION_STRING
    valueFrom:
      secretKeyRef:
        name: {{ include "shub.fullname" . }}
        key: database_connection_string
  {{- end }}
  {{- if .Values.existingSecret }}
  - name: SHUB_AUTH_ENCRYPT_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: {{ .Values.existingSecret }}
        key: {{ .Values.secretKeys.authEncryptSecretKey }}
  {{- else if and .Values.secrets (.Values.secrets).authEncryptSecretKey }}
  - name: SHUB_AUTH_ENCRYPT_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: {{ include "shub.fullname" . }}
        key: auth_encrypt_secret_key
  {{- else }}
  - name: SHUB_AUTH_ENCRYPT_SECRET_KEY
    value: asdjfhjaskdhuioaweyuiorasdsjbaskcbkj
  {{- end }}
  {{- if .Values.s3Fallback.enabled }}
  - name: SHUB_GATEWAYS_S3_FALLBACK_URL
    value: http://localhost:7001
  {{- end }}
  {{- if .Values.committedLocalCacheVolume }}
  - name: SHUB_COMMITTED_LOCAL_CACHE_DIR
    value: /shub/cache
  {{- end }}
  {{- if .Values.useDevPostgres }}
  - name: SHUB_DATABASE_TYPE
    value: postgres
  - name: SHUB_DATABASE_POSTGRES_CONNECTION_STRING
    value: 'postgres://shub:shub@postgres-server:5432/postgres?sslmode=disable'
  {{- end }}
  {{- if .Values.extraEnvVars }}
  {{- toYaml .Values.extraEnvVars | nindent 2 }}
  {{- end }}
{{- if .Values.extraEnvVarsSecret }}
envFrom:
  - secretRef:
      name: {{ .Values.extraEnvVarsSecret }}
{{- end }}
{{- end }}

{{- define "shub.volumes" -}}
{{- if .Values.extraVolumes }}
{{ toYaml .Values.extraVolumes }}
{{- end }}
{{- if .Values.committedLocalCacheVolume }}
- name: committed-local-cache
{{- toYaml .Values.committedLocalCacheVolume | nindent 2 }}
{{- end }}
{{- if not .Values.shubConfig }}
- name: {{ .Chart.Name }}-local-data
{{- end}}
{{- if .Values.shubConfig }}
- name: config-volume
  configMap:
    name: {{ include "shub.fullname" . }}
    items:
      - key: config.yaml
        path: config.yaml
{{- end }}
{{- if (.Values.enterprise).enabled }}
{{- if and .Values.existingSecret .Values.secretKeys.licenseContentsKey }}
- name: secret-volume-license-token
  secret:
    secretName: {{ .Values.existingSecret }}
    items:
      - key: {{ .Values.secretKeys.licenseContentsKey }}
        path: license.tkn
{{- else if and .Values.secrets .Values.secrets.licenseContents }}
- name: secret-volume-license-token
  secret:
    secretName: {{ include "shub.fullname" . }}
    items:
      - key: license_contents
        path: license.tkn
{{- end }}
{{- end }}
{{- if (((.Values.enterprise).auth).saml).enabled }}
- name: secret-volume-license-token
  secret:
    secretName: saml-certificates
{{- end }}
{{- end }}
