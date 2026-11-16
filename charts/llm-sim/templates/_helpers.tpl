{{- define "llm-sim.labels" -}}
app.kubernetes.io/name: llm-sim
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
{{- end }}
{{- define "llm-sim.selector" -}}
app: llm-sim
{{- end }}
