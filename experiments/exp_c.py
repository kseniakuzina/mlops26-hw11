"""Опыт C: HPA 2->3->2 по CPU, повторная проверка горячей реплики, два алерта.

python experiments/exp_c.py hpa       # ramp-нагрузка через обычный Service
python experiments/exp_c.py hot       # 80 % через одного клиента, Service с ClientIP-привязкой
python experiments/exp_c.py alerts    # перегрузка -> pending -> firing -> возврат в норму
"""
import argparse
import json
import threading
import time

from common import (RESULTS, ROOT, Watcher, ensure_prom_forward, helm_upgrade, kjson, kubectl, log, prom,
                    prom_range, run, save_pod_logs, start_loadgen, wait_procs, wait_rollout, write_json, rfc3339)

VALUES = ROOT / 'experiments' / 'values-exp-c.yaml'
PLAIN = 'http://llm-sim.llm.svc.cluster.local'
AFFINITY = 'http://llm-sim-affinity.llm.svc.cluster.local'


def prepare(out):
    helm_upgrade(VALUES)
    wait_rollout()
    out.mkdir(parents=True, exist_ok=True)
    checks = {
        'apiservice-metrics': kubectl('get', 'apiservice', 'v1beta1.metrics.k8s.io', '-o', 'wide', check=False),
        'top-pods': kubectl('top', 'pods', check=False),
        'hpa-yaml': kubectl('get', 'hpa', 'llm-sim', '-o', 'yaml'),
        'cpu-requests': kubectl('get', 'pods', '-l', 'app=llm-sim', '-o',
                                'jsonpath={range .items[*]}{.metadata.name}{" requests="}'
                                '{.spec.containers[*].resources.requests}{"\\n"}{end}'),
    }
    for name, text in checks.items():
        (out / f'{name}.txt').write_text(text, encoding='utf-8')


def wait_replicas(n, timeout=900):
    t0 = time.time()
    while time.time() - t0 < timeout:
        dep = kjson('get', 'deployment', 'llm-sim')
        if dep['spec'].get('replicas') == n and dep['status'].get('readyReplicas') == n:
            return time.time()
        time.sleep(3)
    raise TimeoutError(f'replicas != {n}')


def collect(out, start, end, since):
    save_pod_logs(out / 'pods.jsonl', since=since)
    (out / 'hpa-describe.txt').write_text(kubectl('describe', 'hpa', 'llm-sim'), encoding='utf-8')
    (out / 'events.txt').write_text(kubectl('get', 'events', '--sort-by=.lastTimestamp'), encoding='utf-8')
    exprs = {
        'cpu_by_pod': 'sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="llm", container="api"}[30s]))',
        'ready': 'kube_deployment_status_replicas_ready{namespace="llm", deployment="llm-sim"}',
        'hpa_desired': 'kube_horizontalpodautoscaler_status_desired_replicas{namespace="llm"}',
        'rps_by_pod': 'sum by (pod) (rate(llm_requests_total{namespace="llm"}[30s]))',
        'waiting': 'sum by (pod) (llm_waiting{namespace="llm"})',
        'ttfc_p95': 'histogram_quantile(0.95, sum by (le) (rate(llm_ttfc_seconds_bucket{namespace="llm"}[1m])))',
        'error_ratio': 'sum(rate(llm_requests_total{namespace="llm", outcome!="ok"}[1m])) / '
                       'sum(rate(llm_requests_total{namespace="llm"}[1m]))',
        'alerts': 'ALERTS{alertname=~"LLM.*"}',
    }
    write_json(out / 'prom.json', {k: prom_range(v, start, end, 5) for k, v in exprs.items()})


def step_hpa(args):
    out = RESULTS / 'exp-c' / 'hpa'
    prepare(out)
    wait_replicas(2)
    since = rfc3339(time.time())
    watcher = Watcher(out / 'timeline.jsonl', period=2)
    watcher.start()
    start_at = time.time() + 6
    procs = [start_loadgen(out / f'client{c}.jsonl', c, PLAIN, start_at, profile='ramp', client=c, keepalive=0,
                           timeout=90, seed=1, policy='plain', run='hpa') for c in range(4)]
    wait_procs(procs)
    t_load_end = time.time()
    try:
        t_back = wait_replicas(2, timeout=600)
    except TimeoutError:
        t_back = None
    time.sleep(10)
    watcher.stop()
    collect(out, start_at - 30, time.time(), since)
    write_json(out / 'meta.json', {'start_at': start_at, 'ramp': [[60, 1.0], [240, 6.0], [300, 0.5]],
                                   't_load_end': t_load_end, 't_back_to_min': t_back, 'target': PLAIN})


def step_hot(args):
    """Горячая реплика при HPA: клиент 0 — 80 % запросов, Service с ClientIP-привязкой, keep-alive."""
    out = RESULTS / 'exp-c' / 'hot'
    prepare(out)
    kubectl('rollout', 'restart', 'deployment/llm-sim')
    wait_rollout()
    wait_replicas(2)
    since = rfc3339(time.time())
    watcher = Watcher(out / 'timeline.jsonl', period=2)
    watcher.start()
    start_at = time.time() + 6
    procs = [start_loadgen(out / f'client{c}.jsonl', c, AFFINITY, start_at, profile='skewed', rate=4.0, warmup=0,
                           duration=300, client=c, keepalive=1, timeout=90, seed=1, policy='affinity', run='hot')
             for c in range(4)]
    wait_procs(procs)
    try:
        wait_replicas(2, timeout=600)
    except TimeoutError:
        pass
    watcher.stop()
    collect(out, start_at - 30, time.time(), since)
    write_json(out / 'meta.json', {'start_at': start_at, 'profile': 'skewed', 'rate': 4.0, 'target': AFFINITY})


def poll_alerts(path, stop):
    with path.open('w', encoding='utf-8') as f:
        while not stop.is_set():
            try:
                alerts = prom('/api/v1/alerts')['data']['alerts']
                f.write(json.dumps({'t': round(time.time(), 2), 'alerts': [
                    {'name': a['labels']['alertname'], 'state': a['state'], 'activeAt': a.get('activeAt'),
                     'value': a.get('value')} for a in alerts]}, ensure_ascii=False) + '\n')
                f.flush()
            except OSError as exc:
                f.write(json.dumps({'t': time.time(), 'error': str(exc)}) + '\n')
            stop.wait(5)


def step_alerts(args):
    out = RESULTS / 'exp-c' / 'alerts'
    prepare(out)
    wait_replicas(2)
    (out / 'rules.json').write_text(json.dumps(prom('/api/v1/rules'), ensure_ascii=False, indent=1), encoding='utf-8')
    since = rfc3339(time.time())
    stop = threading.Event()
    poller = threading.Thread(target=poll_alerts, args=(out / 'alerts-poll.jsonl', stop), daemon=True)
    poller.start()
    watcher = Watcher(out / 'timeline.jsonl', period=3)
    watcher.start()
    # 60 с нормы -> 240 с перегрузки (16 запросов/с, ёмкость ~10 при 3 Pod) -> 240 с нормы
    start_at = time.time() + 6
    phases = [('normal-1', 60, 1.0), ('overload', 240, 16.0), ('normal-2', 240, 1.0)]
    t = start_at
    marks, procs = [], []
    for name, length, rate in phases:
        # Все фазы запускаются сразу с собственными отметками начала: генератор ждёт своего времени сам.
        procs += [start_loadgen(out / f'{name}-client{c}.jsonl', c, PLAIN, t, profile='uniform', rate=rate,
                                warmup=0, duration=length, client=c, keepalive=0, timeout=60, seed=7,
                                policy='plain', run=name) for c in range(4)]
        marks.append({'phase': name, 'start': t, 'end': t + length, 'rate': rate})
        t += length
    wait_procs(procs)
    time.sleep(60)
    stop.set()
    watcher.stop()
    collect(out, start_at - 30, time.time(), since)
    write_json(out / 'meta.json', {'phases': marks})


def main():
    p = argparse.ArgumentParser()
    p.add_argument('step', choices=['hpa', 'hot', 'alerts'])
    args = p.parse_args()
    pf = ensure_prom_forward()
    {'hpa': step_hpa, 'hot': step_hot, 'alerts': step_alerts}[args.step](args)
    if pf:
        pf.terminate()


if __name__ == '__main__':
    main()
