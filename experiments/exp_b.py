"""Опыт B: привязка к Pod, горячая реплика и политика маршрутизации.

Политики (один и тот же набор из двух Pod llm-sim):
  plain    — Service без привязки (kube-proxy iptables, выбор Pod на новое TCP-соединение);
  affinity — Service с sessionAffinity: ClientIP;
  router   — HAProxy: consistent hashing по X-Session-ID с bounded loads (hash-balance-factor).
Трафик: uniform (4 клиента по 25 %) и skewed (клиент 0 — 80 %).
Каждая комбинация — 3 прогона (seed 1..3 одинаковый для всех политик), 20 с прогрева + 120 с измерения.
Начальное состояние перед каждым прогоном: rollout restart -> новые Pod, пустой префиксный кеш,
таблица привязки kube-proxy для новых IP Pod пуста.

python experiments/exp_b.py                    # все 18 прогонов
python experiments/exp_b.py --policies router --profiles skewed --runs 1
"""
import argparse
import time

from common import (RESULTS, ROOT, ensure_prom_forward, helm_upgrade, kjson, kubectl, log, prom_range, run,
                    save_pod_logs, start_loadgen, wait_procs, wait_rollout, write_json, rfc3339)

VALUES = ROOT / 'experiments' / 'values-exp-b.yaml'
TARGETS = {'plain': 'http://llm-sim.llm.svc.cluster.local',
           'affinity': 'http://llm-sim-affinity.llm.svc.cluster.local',
           'router': 'http://llm-router.llm.svc.cluster.local'}
RATE, WARMUP, DURATION = 2.0, 20, 120


def fresh_pods():
    kubectl('rollout', 'restart', 'deployment/llm-sim')
    wait_rollout()
    kubectl('rollout', 'restart', 'deployment/llm-router')
    kubectl('rollout', 'status', 'deployment/llm-router', '--timeout=120s')
    time.sleep(8)  # HAProxy/kube-proxy успевают увидеть новые адреса


def one_run(policy, profile, seed, keepalive):
    out = RESULTS / 'exp-b' / f'{policy}-{profile}-run{seed}'
    fresh_pods()
    pods = kjson('get', 'pods', '-l', 'app=llm-sim')['items']
    clients = kjson('get', 'pods', '-l', 'app=loadgen')['items']
    since = rfc3339(time.time())
    start_at = time.time() + 6
    procs = [start_loadgen(out / f'client{c}.jsonl', c, TARGETS[policy], start_at, profile=profile, rate=RATE,
                           warmup=WARMUP, duration=DURATION, seed=seed, client=c, keepalive=keepalive, timeout=90,
                           policy=policy, run=f'run{seed}') for c in range(4)]
    wait_procs(procs)
    end = time.time()
    save_pod_logs(out / 'pods.jsonl', since=since)
    if policy == 'router':
        (out / 'router.log').write_text(kubectl('logs', 'deployment/llm-router', f'--since-time={since}'),
                                        encoding='utf-8')
    series = {}
    for name, expr in {'waiting': 'sum by (pod) (llm_waiting{namespace="llm"})',
                       'running': 'sum by (pod) (llm_running{namespace="llm"})'}.items():
        try:
            series[name] = prom_range(expr, start_at, end, 5)
        except OSError as exc:
            series[name] = str(exc)
    write_json(out / 'prom.json', series)
    write_json(out / 'meta.json', {
        'policy': policy, 'profile': profile, 'seed': seed, 'target': TARGETS[policy], 'rate': RATE,
        'warmup_s': WARMUP, 'duration_s': DURATION, 'keepalive': keepalive, 'start_at': start_at, 'end': end,
        'server_pods': {p['metadata']['name']: p['status'].get('podIP') for p in pods},
        'client_pods': {c['metadata']['name']: c['status'].get('podIP') for c in clients},
        'initial_state': 'новые Pod (rollout restart), пустой префиксный кеш, прогрев backend 5 с + 20 с нагрузки'})
    log('run done', out.name)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--policies', nargs='+', default=['plain', 'affinity', 'router'])
    p.add_argument('--profiles', nargs='+', default=['uniform', 'skewed'])
    p.add_argument('--runs', nargs='+', type=int, default=[1, 2, 3])
    p.add_argument('--keepalive', type=int, default=1)
    args = p.parse_args()
    pf = ensure_prom_forward()
    helm_upgrade(VALUES)
    wait_rollout()
    net = {'kube-proxy-config': kubectl('get', 'cm', 'kube-proxy', '-n', 'kube-system', '-o',
                                        'jsonpath={.data.config\\.conf}', check=False),
           'services': kubectl('get', 'svc', '-o', 'wide'),
           'affinity-service': kubectl('get', 'svc', 'llm-sim-affinity', '-o', 'yaml'),
           'router-config': kubectl('get', 'cm', 'llm-router-config', '-o', 'jsonpath={.data.haproxy\\.cfg}')}
    out = RESULTS / 'exp-b'
    out.mkdir(parents=True, exist_ok=True)
    for name, text in net.items():
        (out / f'net-{name}.txt').write_text(text, encoding='utf-8')
    # Прогоны чередуются по политикам внутри одного seed, чтобы дрейф стенда не совпадал с политикой.
    for seed in args.runs:
        for profile in args.profiles:
            for policy in args.policies:
                one_run(policy, profile, seed, args.keepalive)
    if pf:
        pf.terminate()


if __name__ == '__main__':
    main()
