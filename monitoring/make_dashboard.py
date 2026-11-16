"""Генерирует monitoring/grafana-dashboard.json (provisioning через ConfigMap grafana-dashboard-llm-sim)."""
import json
from pathlib import Path

NS = 'namespace="llm"'
panels = []


def panel(title, targets, unit, x, y, w=12, h=8, stack=False, desc=''):
    panels.append({
        'type': 'timeseries', 'title': title, 'description': desc,
        'gridPos': {'x': x, 'y': y, 'w': w, 'h': h}, 'datasource': {'type': 'prometheus', 'uid': 'prom'},
        'fieldConfig': {'defaults': {'unit': unit, 'custom': {'stacking': {'mode': 'normal' if stack else 'none'},
                                                              'lineWidth': 2, 'fillOpacity': 10}},
                        'overrides': []},
        'options': {'legend': {'displayMode': 'table', 'placement': 'bottom', 'calcs': ['lastNotNull', 'max']},
                    'tooltip': {'mode': 'multi'}},
        'targets': [{'refId': chr(65 + i), 'expr': expr, 'legendFormat': legend, 'range': True}
                    for i, (expr, legend) in enumerate(targets)]})


q = 'histogram_quantile({q}, sum by (le{by}) (rate({m}_bucket{{{ns}}}[1m])))'
panel('TTFC (до первой части), p50/p95 по Pod',
      [(q.format(q=.95, by=', pod', m='llm_ttfc_seconds', ns=NS), 'p95 {{pod}}'),
       (q.format(q=.5, by=', pod', m='llm_ttfc_seconds', ns=NS), 'p50 {{pod}}'),
       (q.format(q=.95, by='', m='llm_ttfc_seconds', ns=NS), 'p95 все Pod')], 's', 0, 0,
      desc='classic histogram: перцентиль из суммарных bucket-счётчиков, le сохраняется')
panel('Длительность полного ответа, p95 по Pod',
      [(q.format(q=.95, by=', pod', m='llm_request_duration_seconds', ns=NS), 'p95 {{pod}}'),
       (q.format(q=.95, by='', m='llm_queue_wait_seconds', ns=NS), 'p95 ожидание очереди (все)')], 's', 12, 0)
panel('running / waiting по Pod',
      [(f'sum by (pod) (llm_running{{{NS}}})', 'running {{pod}}'),
       (f'sum by (pod) (llm_waiting{{{NS}}})', 'waiting {{pod}}')], 'short', 0, 8)
panel('Завершённые запросы/с по Pod',
      [(f'sum by (pod) (rate(llm_requests_total{{{NS}, outcome="ok"}}[1m]))', 'ok {{pod}}')], 'reqps', 12, 8)
panel('Ошибки и отказы/с по исходу',
      [(f'sum by (outcome) (rate(llm_requests_total{{{NS}, outcome!="ok"}}[1m]))', '{{outcome}}'),
       (f'sum(rate(llm_requests_total{{{NS}, outcome!="ok"}}[1m])) / sum(rate(llm_requests_total{{{NS}}}[1m]))',
        'доля отказов')], 'short', 0, 16)
panel('Готовые реплики',
      [('kube_deployment_status_replicas_ready{namespace="llm", deployment="llm-sim"}', 'Ready'),
       ('kube_deployment_spec_replicas{namespace="llm", deployment="llm-sim"}', 'spec.replicas'),
       ('kube_horizontalpodautoscaler_status_desired_replicas{namespace="llm"}', 'HPA desired')], 'short', 12, 16)
panel('CPU по Pod (ядра)',
      [('sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="llm", container="api"}[1m]))', '{{pod}}')],
      'short', 0, 24)
panel('RAM по Pod (working set)',
      [('sum by (pod) (container_memory_working_set_bytes{namespace="llm", container="api"})', '{{pod}}')],
      'bytes', 12, 24)
panel('Имитация префиксного кеша: доля hit по Pod',
      [(f'sum by (pod) (rate(llm_prefix_cache_total{{{NS}, result="hit"}}[1m])) / '
        f'sum by (pod) (rate(llm_prefix_cache_total{{{NS}}}[1m]))', 'hit ratio {{pod}}')], 'percentunit', 0, 32)
panel('Алерты (ALERTS)',
      [('ALERTS{alertname=~"LLM.*"}', '{{alertname}} {{alertstate}}'),
       ('sum by (version) (llm_model_info{namespace="llm"})', 'Pod с версией {{version}}')], 'short', 12, 32)

dashboard = {'uid': 'llm-sim-hw11', 'title': 'HW11 llm-sim (backend_mode=simulated)', 'schemaVersion': 39,
             'time': {'from': 'now-30m', 'to': 'now'}, 'refresh': '10s', 'tags': ['hw11'], 'panels': panels}
out = Path(__file__).with_name('grafana-dashboard.json')
out.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(out)
