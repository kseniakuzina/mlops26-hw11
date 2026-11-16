#!/usr/bin/env bash
# Версии и фактический размер стенда -> results/footprint.txt
set -uo pipefail
cd "$(dirname "$0")/.."
{
  echo "# $(date -u +%FT%TZ)"
  echo "## версии"
  docker version --format 'docker client {{.Client.Version}} / server {{.Server.Version}}'
  kind version
  kubectl version 2>/dev/null
  helm version --short
  helm list -A
  echo "## ресурсы контейнерного окружения"
  docker info --format 'CPU={{.NCPU}} RAM={{.MemTotal}} bytes, cgroup={{.CgroupVersion}}, storage={{.Driver}}'
  echo "## свободное место на хосте"
  df -h . | tail -1
  echo "## образы стенда"
  docker images --format '{{.Repository}}:{{.Tag}} {{.ID}} {{.Size}}' | grep -E 'llm-sim|kindest/node' || true
  docker exec hw11-control-plane crictl images 2>/dev/null
  echo "## размер узла kind (контейнер + тома)"
  docker ps -s --filter name=hw11-control-plane --format '{{.Names}} {{.Size}}'
  docker exec hw11-control-plane du -sh /var/lib/containerd /var/local-path-provisioner 2>/dev/null
  echo "## PVC"
  kubectl get pvc -A
  echo "## потребление Pod"
  kubectl top pods -A 2>/dev/null
  echo "## узел"
  kubectl describe node hw11-control-plane | sed -n '/Capacity:/,/System Info:/p;/Allocated resources:/,/Events:/p'
} > results/footprint.txt 2>&1
cat results/footprint.txt
