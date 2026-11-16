# ДЗ 11. Kubernetes под нагрузкой

API из ДЗ 8+9+10 (FastAPI-сервис с проверкой хеша артефакта, прогревом и ограничением очереди) перенесён в
Kubernetes: образ, Helm-чарт, пробы, три политики маршрутизации, HPA, Prometheus и Grafana.
Backend — управляемый CPU-имитатор, **`backend_mode=simulated`** в каждом ответе. Все замеры относятся
к модели нагрузки имитатора: они называются TTFC и inter-chunk latency, а не TTFT/TPOT.

| Материал | Где |
|---|---|
| Отчёт (гипотеза → воспроизведение → причина → исправление → повтор, ревью PR) | [report.html](report.html) |
| Задание | [homework11.html](homework11.html) |
| Сырые результаты (одна строка — одна попытка) | [results/requests.csv](results/requests.csv), `results/exp-*/**/*.jsonl` |
| Сводки | `results/summary-exp-*.csv/json`, [results/promql-vs-client.json](results/promql-vs-client.json) |
| Графики | [results/plots/](results/plots/) |
| Журналы проверок (красный/зелёный, все 7) | [results/tests/](results/tests/) |
| Прогон, испорченный сном ноутбука (не входит в сравнение) | `results/exp-b-invalid/` |

## Состав

| Файл | Назначение |
|---|---|
| [app/service.py](app/service.py) | HTTP API: `/v1/chat` (NDJSON-поток), `/livez`, `/startupz`, `/readyz`, `/version`, `/metrics` |
| [app/backend.py](app/backend.py) | имитатор: загрузка артефакта с проверкой SHA-256, прогрев, префиксный кеш (LRU), prefill/decode |
| [app/loadgen.py](app/loadgen.py), [app/workload.py](app/workload.py) | open-loop генератор и детерминированное расписание |
| [app/chatclient.py](app/chatclient.py) | одиночный запрос из клиентского Pod (для проверок) |
| [models/](models/) | артефакты имитатора `sim-A.json`, `sim-B.json` и `SHA256SUMS` |
| [Dockerfile](Dockerfile), [app/requirements.lock](app/requirements.lock) | образ на `python:3.12-slim@sha256:…`, полный закреплённый набор пакетов |
| [charts/llm-sim/](charts/llm-sim/) | Helm: Deployment, ConfigMap, 3 Service, HAProxy-маршрутизатор, HPA, клиентские Pod |
| [monitoring/](monitoring/) | values Prometheus (сбор, алерты, хранение) и Grafana, экспорт дашборда |
| [deploy/](deploy/) | `kind-config.yaml`, `setup.sh`, `teardown.sh`, `footprint.sh` |
| [experiments/](experiments/) | сценарии опытов A, B, C и сверки PromQL, values сравниваемых конфигураций |
| [analysis/](analysis/) | счётчики исходов, перцентили, графики |
| [tests/](tests/) | 5 собственных проверок (3 через Kubernetes, учёт отказов, изоляция диалогов) |

## Окружение

Windows 11, Docker Desktop 29.9.2 (WSL2, 16 vCPU, 15,3 ГиБ), kind v0.33.0 (Kubernetes v1.37.0, один узел,
kube-proxy iptables), kubectl v1.36.1, helm v4.3.1. Чарты: prometheus 29.47.0, grafana 10.5.15,
metrics-server 3.14.0. Python 3.12 в образе, зависимости — `app/requirements.lock`.
Фактический размер стенда и ресурсы — [results/footprint.txt](results/footprint.txt).

## Запуск

```bash
bash deploy/setup.sh          # образ, kind, metrics-server, Prometheus, Grafana, llm-sim (~5 мин без скачивания)
kubectl -n monitoring port-forward svc/grafana 3000:80      # http://localhost:3000, admin / hw11-local
kubectl -n monitoring port-forward svc/prometheus-server 9090:80
```

Опыты (результаты пишутся в `results/`, Python 3.12 на хосте: `uv venv && uv pip install -r app/requirements.txt matplotlib pytest`):

```bash
python experiments/exp_a.py warmup --probes http   # исходный дефект
python experiments/exp_a.py warmup --probes warm   # исправление, тот же опыт
python experiments/exp_a.py rollout                # A->B под нагрузкой с длинными потоками
python experiments/exp_a.py bad-release            # неготовая B, исключение, helm rollback
python experiments/exp_a.py crash                  # аварийное завершение процесса
python experiments/exp_b.py                        # 3 политики x 2 профиля x 3 прогона
python experiments/promql_compare.py               # PromQL vs клиентский журнал
python experiments/exp_c.py hpa; python experiments/exp_c.py hot; python experiments/exp_c.py alerts
python experiments/run_checks.py                   # красный/зелёный прогон + все проверки -> results/tests/
python analysis/analyze.py                         # requests.csv, сводки, 4 графика
python -m pytest tests -v                          # собственные проверки
```

## Удаление

```bash
bash deploy/teardown.sh       # kind delete cluster --name hw11 + образ llm-sim:0.1.0
```

Удаляется только свой кластер (вместе с PVC Prometheus внутри узла) и свой образ.
