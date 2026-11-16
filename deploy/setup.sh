#!/usr/bin/env bash
# Поднять стенд ДЗ 11 с нуля: kind + образ + metrics-server + Prometheus + Grafana + llm-sim.
# Требуется: docker, kind v0.33.0, kubectl, helm v4.3.1. Запуск из корня репозитория (Git Bash / Linux).
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=llm-sim:0.1.0
PROM_CHART=29.47.0
GRAFANA_CHART=10.5.15
METRICS_CHART=3.14.0

echo "== свободное место и лимиты контейнерного окружения"
df -h . | tail -1
docker info --format 'docker {{.ServerVersion}}, CPU={{.NCPU}}, RAM={{.MemTotal}} bytes, root={{.DockerRootDir}}'
docker system df

echo "== образ сервиса"
docker build -t "$IMAGE" .

echo "== кластер kind"
kind get clusters | grep -qx hw11 || kind create cluster --config deploy/kind-config.yaml
kind load docker-image "$IMAGE" --name hw11
kubectl config use-context kind-hw11

echo "== helm-репозитории (версии чартов закреплены ниже)"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
helm repo add grafana https://grafana.github.io/helm-charts --force-update
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ --force-update
helm repo update

echo "== metrics-server (resource metrics API для HPA); kubelet в kind с самоподписанным сертификатом"
helm upgrade --install metrics-server metrics-server/metrics-server --version "$METRICS_CHART" \
  -n kube-system --set 'args={--kubelet-insecure-tls}' --wait

echo "== Prometheus и Grafana"
helm upgrade --install prometheus prometheus-community/prometheus --version "$PROM_CHART" \
  -n monitoring --create-namespace -f monitoring/prometheus-values.yaml --wait --timeout 10m
kubectl -n monitoring create configmap grafana-dashboard-llm-sim \
  --from-file=llm-sim.json=monitoring/grafana-dashboard.json --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install grafana grafana/grafana --version "$GRAFANA_CHART" \
  -n monitoring -f monitoring/grafana-values.yaml --wait --timeout 10m

echo "== сервис"
helm upgrade --install llm charts/llm-sim -n llm --create-namespace --wait --timeout 5m
kubectl -n llm get pods,svc,endpointslices -o wide

cat <<'EOF'
Готово. Доступ:
  kubectl -n monitoring port-forward svc/prometheus-server 9090:80
  kubectl -n monitoring port-forward svc/grafana 3000:80     # admin / hw11-local
EOF
