{{/*
Expand the name of the chart.
*/}}
{{- define "daft.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "daft.fullname" -}}
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
{{- define "daft.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "daft.labels" -}}
helm.sh/chart: {{ include "daft.chart" . }}
{{ include "daft.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "daft.selectorLabels" -}}
app.kubernetes.io/name: {{ include "daft.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "daft.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "daft.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Ray head service name
*/}}
{{- define "daft.headServiceName" -}}
{{- printf "%s-head" (include "daft.fullname" .) }}
{{- end }}

{{/*
Calculate Ray memory from Kubernetes memory requests
Returns memory in bytes (100% of container memory request)
*/}}
{{- define "daft.calculateRayMemory" -}}
{{- $memStr := . -}}
{{- if hasSuffix "Gi" $memStr -}}
{{- $memNum := $memStr | trimSuffix "Gi" | int -}}
{{- mul $memNum 1073741824 -}}
{{- else if hasSuffix "G" $memStr -}}
{{- $memNum := $memStr | trimSuffix "G" | int -}}
{{- mul $memNum 1000000000 -}}
{{- else if hasSuffix "Mi" $memStr -}}
{{- $memNum := $memStr | trimSuffix "Mi" | int -}}
{{- mul $memNum 1048576 -}}
{{- else if hasSuffix "M" $memStr -}}
{{- $memNum := $memStr | trimSuffix "M" | int -}}
{{- mul $memNum 1000000 -}}
{{- else if hasSuffix "Ki" $memStr -}}
{{- $memNum := $memStr | trimSuffix "Ki" | int -}}
{{- mul $memNum 1024 -}}
{{- else if hasSuffix "K" $memStr -}}
{{- $memNum := $memStr | trimSuffix "K" | int -}}
{{- mul $memNum 1000 -}}
{{- else -}}
{{- . -}}
{{- end -}}
{{- end }}

{{/*
Extract CPU count from resource specification
Returns CPU cores (supports millicores and whole cores)
*/}}
{{- define "daft.calculateRayCpus" -}}
{{- $cpuStr := . -}}
{{- if hasSuffix "m" $cpuStr -}}
{{- $cpuNum := $cpuStr | trimSuffix "m" | int -}}
{{- if eq (mod $cpuNum 1000) 0 -}}
{{- div $cpuNum 1000 -}}
{{- else if eq (mod $cpuNum 500) 0 -}}
0.5
{{- else if eq (mod $cpuNum 250) 0 -}}
0.25
{{- else if eq $cpuNum 100 -}}
0.1
{{- else -}}
1
{{- end -}}
{{- else -}}
{{- $cpuStr -}}
{{- end -}}
{{- end }}
