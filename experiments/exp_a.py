"""Опыт A: прогрев, rolling update A->B, неготовая B и откат, аварийное завершение процесса.

python experiments/exp_a.py warmup --probes http   # исходный дефект
python experiments/exp_a.py warmup --probes warm   # исправление, тот же опыт
python experiments/exp_a.py rollout                # A->B под нагрузкой с длинными потоками
python experiments/exp_a.py bad-release            # неготовая B, исключение, helm rollback к A
python experiments/exp_a.py crash                  # os._exit в одном Pod, обрывы, обнаружение, восстановление

Нагрузка: клиенты loadgen-0 (короткие ответы) и loadgen-1 (длинные потоки), keep-alive выключен,
чтобы каждый запрос заново выбирал Pod через Service. Результаты — results/exp-a/<шаг>/.
"""
import argparse
import json
import time

from common import (NS, RESULTS, SHA, ROOT, LogCollector, Watcher, helm_upgrade, kjson, kubectl, log, pod_names, pod_post, run,
                    save_pod_logs, start_loadgen, wait_procs, wait_rollout, write_json, rfc3339)

VALUES = ROOT / 'experiments' / 'values-exp-a.yaml'
TARGET = 'http://llm-sim.llm.svc.cluster.local'


def load(out, duration, start_at, short_rate=4.0, long_rate=0.1, long_chunks=500, retries=0, timeout=60, seed=11):
    """Короткие запросы + длинные потоки (long_chunks x 50 мс = 25 с) в одном расписании по времени."""
    common = dict(duration=duration, warmup=0, keepalive=0, timeout=timeout, retries=retries, policy='plain')
    return [
        start_loadgen(out / 'short.jsonl', 0, TARGET, start_at, profile='uniform', rate=short_rate, seed=seed,
                      run='short', **common),
        start_loadgen(out / 'long.jsonl', 1, TARGET, start_at, profile='uniform', rate=long_rate, seed=seed + 1,
                      max_chunks=long_chunks, run='long', **common),
    ]


def first_long_start(start_at, duration, seed=12, rate=0.1):
    from importlib import import_module
    import sys
    sys.path.insert(0, str(ROOT / 'app'))
    sched = import_module('workload').schedule('uniform', rate, duration, 0, seed)
    return [start_at + r['scheduled_s'] for r in sched]


def step_warmup(args):
    since = rfc3339(time.time() - 5)  # с запасом: журналы с момента до helm upgrade
    out = RESULTS / 'exp-a' / f'warmup-{args.probes}'
    helm_upgrade(VALUES, sets=[f'probes.mode={args.probes}'])
    wait_rollout()
    time.sleep(5)
    watcher = Watcher(out / 'timeline.jsonl')
    watcher.start()
    logs = LogCollector(out / 'pods-all.jsonl', since)
    logs.start()
    start_at = time.time() + 5
    procs = load(out, duration=args.duration, start_at=start_at, long_rate=0.05)
    time.sleep(5 + 15)
    save_pod_logs(out / 'pods-before.jsonl', since=since)  # журналы исходных Pod пропадут после замены
    t_restart = time.time()
    kubectl('rollout', 'restart', 'deployment/llm-sim')
    log('rollout restart')
    wait_procs(procs)
    watcher.stop()
    logs.stop()
    save_pod_logs(out / 'pods.jsonl', since=since)
    write_json(out / 'meta.json', {'step': 'warmup', 'probes': args.probes, 'start_at': start_at,
                                   't_restart': t_restart, 'values': VALUES.name, 'target': TARGET})
    log('done', out)


def step_rollout(args):
    """Штатный rollout A->B при нагрузке; длинный поток стартует до начала обновления."""
    since = rfc3339(time.time() - 5)  # с запасом: журналы с момента до helm upgrade
    out = RESULTS / 'exp-a' / 'rollout-A-to-B'
    helm_upgrade(VALUES, sets=['probes.mode=warm', 'model.version=A', f'model.sha256={SHA["A"]}'])
    wait_rollout()
    time.sleep(5)
    watcher = Watcher(out / 'timeline.jsonl')
    watcher.start()
    logs = LogCollector(out / 'pods-all.jsonl', since)
    logs.start()
    start_at = time.time() + 5
    procs = load(out, duration=args.duration, start_at=start_at)
    longs = first_long_start(start_at, args.duration)
    # Начинаем обновление через 3 с после старта первого длинного потока: он пересекает начало rollout.
    t_update = max(longs[0] + 3, start_at + 20)
    time.sleep(max(0, t_update - time.time()))
    save_pod_logs(out / 'pods-before.jsonl', since=since)
    helm_upgrade(VALUES, sets=['probes.mode=warm', 'model.version=B', f'model.sha256={SHA["B"]}'])
    t_update = time.time()
    wait_rollout()
    t_done = time.time()
    log('rollout finished in', round(t_done - t_update, 1), 's')
    wait_procs(procs)
    watcher.stop()
    logs.stop()
    save_pod_logs(out / 'pods.jsonl', since=since)
    write_json(out / 'meta.json', {'step': 'rollout', 'from': 'A', 'to': 'B', 'start_at': start_at,
                                   't_update': t_update, 't_rollout_done': t_done, 'long_starts': longs[:5]})


def step_bad_release(args):
    """Неготовая B (неверный хеш артефакта) -> исключена из трафика -> helm rollback к последней A."""
    since = rfc3339(time.time() - 5)  # с запасом: журналы с момента до helm upgrade
    out = RESULTS / 'exp-a' / 'bad-release'
    helm_upgrade(VALUES, sets=['probes.mode=warm', 'model.version=A', f'model.sha256={SHA["A"]}'])
    wait_rollout()
    history_before = run('helm', 'history', 'llm', '-n', NS, '-o', 'json')
    good_revision = json.loads(history_before)[-1]['revision']
    watcher = Watcher(out / 'timeline.jsonl')
    watcher.start()
    logs = LogCollector(out / 'pods-all.jsonl', since)
    logs.start()
    start_at = time.time() + 5
    procs = load(out, duration=args.duration, start_at=start_at, long_rate=0.05)
    time.sleep(20)
    bad_sha = '0' * 64
    save_pod_logs(out / 'pods-before.jsonl', since=since)
    helm_upgrade(VALUES, sets=['probes.mode=warm', 'model.version=B', f'model.sha256={bad_sha}'])
    t_bad = time.time()
    log('bad B released, waiting 60 s')
    time.sleep(60)
    bad_status = kubectl('get', 'pods', '-l', 'app=llm-sim', '-o', 'wide')
    slices = kubectl('get', 'endpointslices', '-l', 'kubernetes.io/service-name=llm-sim', '-o', 'yaml')
    t_rollback = time.time()
    rb = run('helm', 'rollback', 'llm', str(good_revision), '-n', NS, '--wait', '--timeout', '5m')
    t_rb_done = time.time()
    log('rolled back to revision', good_revision, round(t_rb_done - t_rollback, 1), 's')
    wait_procs(procs)
    watcher.stop()
    logs.stop()
    save_pod_logs(out / 'pods.jsonl', since=since)
    (out / 'bad-pods.txt').write_text(bad_status, encoding='utf-8')
    (out / 'bad-endpointslices.yaml').write_text(slices, encoding='utf-8')
    (out / 'helm-history.json').write_text(run('helm', 'history', 'llm', '-n', NS, '-o', 'json'), encoding='utf-8')
    (out / 'rollback.txt').write_text(rb, encoding='utf-8')
    write_json(out / 'meta.json', {'step': 'bad-release', 'good_revision': good_revision, 'bad_sha256': bad_sha,
                                   'start_at': start_at, 't_bad_release': t_bad, 't_rollback': t_rollback,
                                   't_rollback_done': t_rb_done})


def step_crash(args):
    """Аварийное завершение процесса в одном Pod при нагрузке; клиент повторяет до 2 раз."""
    since = rfc3339(time.time() - 5)  # с запасом: журналы с момента до helm upgrade
    out = RESULTS / 'exp-a' / 'crash'
    helm_upgrade(VALUES, sets=['probes.mode=warm', 'model.version=A', f'model.sha256={SHA["A"]}'])
    wait_rollout()
    watcher = Watcher(out / 'timeline.jsonl')
    watcher.start()
    logs = LogCollector(out / 'pods-all.jsonl', since)
    logs.start()
    start_at = time.time() + 5
    procs = load(out, duration=args.duration, start_at=start_at, retries=2, long_rate=0.1)
    time.sleep(5 + 30)
    victim = pod_names()[0]
    save_pod_logs(out / 'pods-before.jsonl', since=since)
    t_crash = time.time()
    pod_post(victim, '/admin/crash')
    log('crashed', victim)
    wait_procs(procs)
    watcher.stop()
    logs.stop()
    save_pod_logs(out / 'pods.jsonl', since=since)
    write_json(out / 'meta.json', {'step': 'crash', 'victim': victim, 't_crash': t_crash, 'start_at': start_at,
                                   'method': 'POST /admin/crash -> os._exit(137) внутри процесса'})


def main():
    p = argparse.ArgumentParser()
    p.add_argument('step', choices=['warmup', 'rollout', 'bad-release', 'crash'])
    p.add_argument('--probes', choices=['http', 'warm'], default='warm')
    p.add_argument('--duration', type=float, default=150)
    args = p.parse_args()
    {'warmup': step_warmup, 'rollout': step_rollout, 'bad-release': step_bad_release, 'crash': step_crash}[args.step](args)


if __name__ == '__main__':
    main()
