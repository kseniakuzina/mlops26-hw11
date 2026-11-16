#!/usr/bin/env bash
# Удалить стенд ДЗ 11 полностью: кластер kind (вместе с PVC Prometheus) и образ сервиса.
# Чужие кластеры, образы и тома не трогаются.
set -euo pipefail
kind delete cluster --name hw11
docker image rm llm-sim:0.1.0 || true
# Образ узла kind (~1 ГБ) оставлен в кеше Docker; удалить при необходимости:
#   docker image rm kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
docker system df
