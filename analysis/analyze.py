"""Сводки и графики по сырым результатам.

python analysis/analyze.py
  -> results/requests.csv            все попытки всех опытов (одна строка = одна попытка)
  -> results/summary-*.csv|json      счётчики, p50/p95 с числом наблюдений, распределение по Pod
  -> results/plots/*.png             четыре графика отчёта
"""
import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402

from metrics import pct, summarize  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / 'results'
PLOTS = RES / 'plots'
OUTCOME_COLORS = {'ok': '#2a9d8f', 'rejected': '#e9c46a', 'http_5xx': '#d62828', 'truncated': '#7b2cbf',
                  'timeout': '#f77f00', 'connect_error': '#6c757d', 'http_4xx': '#adb5bd'}
CSV_FIELDS = ['experiment', 'step', 'req_id', 'attempt', 'phase', 'policy', 'profile', 'run', 'client',
              'client_pod', 'client_ip', 'session_id', 'turn', 'prompt_chars', 'max_chunks', 'keepalive',
              'scheduled_wall', 'sent_wall', 'send_lag_s', 'finished_wall', 'outcome', 'status', 'error', 'pod',
              'routed_to', 'model_version', 'backend_mode', 'request_id', 'chunks', 'headers_s', 'ttfc_s',
              'inter_chunk_mean_s', 'inter_chunk_p95_s', 'total_s', 'cache', 'server_queue_s', 'prefill_s',
              'queued_ahead']


def jl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def pod_events(d):
    """События журналов Pod: снимок до воздействия + после, без повторов."""
    seen, out = set(), []
    for e in jl(Path(d) / 'pods-before.jsonl') + jl(Path(d) / 'pods-all.jsonl') + jl(Path(d) / 'pods.jsonl'):
        key = (e.get('pod'), e.get('event'), e.get('ts'), e.get('request_id'))
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out


def jread(path):
    path = Path(path)
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}


def write_csv(path, rows, fields=None):
    fields = fields or sorted({k for r in rows for k in r})
    with Path(path).open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def r3(x):
    return None if x is None else round(x, 3)


# ---------------------------------------------------------------- сбор
def collect_requests():
    all_rows = []
    for exp_dir in sorted(p for p in RES.glob('exp-*') if p.is_dir()):
        for step_dir in sorted(p for p in exp_dir.iterdir() if p.is_dir()):
            for f in sorted(step_dir.glob('*.jsonl')):
                if f.name in ('timeline.jsonl', 'alerts-poll.jsonl') or f.name.startswith('pods'):
                    continue
                for r in jl(f):
                    if 'req_id' in r:
                        all_rows.append({**r, 'experiment': exp_dir.name, 'step': step_dir.name})
    write_csv(RES / 'requests.csv', all_rows, CSV_FIELDS)
    return all_rows


# ---------------------------------------------------------------- опыт A
def timeline_series(tl, t0, warm_at=None):
    """Ready (без завершающихся), ready в EndpointSlice, Ready по версиям и Ready+прогрет (по журналу Pod)."""
    warm_at = warm_at or {}
    xs, ready, ep_ready, warm, notready, by_version = [], [], [], [], [], defaultdict(list)
    versions = sorted({p.get('version') for s in tl for p in s.get('pods', []) if p.get('version')})
    for s in tl:
        if 'pods' not in s:
            continue
        live = [p for p in s['pods'] if p['ready'] and not p['terminating']]
        xs.append(s['t'] - t0)
        ready.append(len(live))
        ep_ready.append(sum(1 for e in s['endpoints'] if e['ready']))
        warm.append(sum(1 for p in live if warm_at.get(p['name'], float('inf')) <= s['t']))
        notready.append(sum(1 for p in s['pods'] if not p['ready'] and not p['terminating']))
        for v in versions:
            by_version[v].append(sum(1 for p in live if p.get('version') == v))
    return xs, ready, ep_ready, warm, notready, by_version


def first_time(tl, pred):
    for s in tl:
        if 'pods' in s and pred(s):
            return s['t']
    return None


def exp_a_summary():
    out = {}
    for step in sorted((RES / 'exp-a').glob('*')):
        if not step.is_dir():
            continue
        meta = jread(step / 'meta.json')
        rows = jl(step / 'short.jsonl') + jl(step / 'long.jsonl')
        pods = pod_events(step)
        tl = jl(step / 'timeline.jsonl')
        cold = [e for e in pods if e.get('event') == 'cold_request']
        res = {'meta': meta,
               'short': summarize(jl(step / 'short.jsonl')), 'long': summarize(jl(step / 'long.jsonl')),
               'all': summarize(rows),
               'cold_requests_in_pod_logs': len(cold),
               'cold_by_pod': dict(Counter(e['pod'] for e in cold)),
               'served_by_version': dict(Counter(r.get('model_version') for r in rows if r['outcome'] == 'ok')),
               'outcomes_by_pod': {f'{k[0]}|{k[1]}': v for k, v in Counter(
                   (r.get('pod'), r['outcome']) for r in rows).items()},
               'sigterm_events': [{k: e.get(k) for k in ('ts', 'pod', 'running', 'waiting')}
                                  for e in pods if e.get('event') == 'sigterm'],
               'warm_events': [{k: e.get(k) for k in ('ts', 'pod', 'version', 'after_s')}
                               for e in pods if e.get('event') == 'warm'],
               'load_failed': [{k: e.get(k) for k in ('ts', 'pod', 'error')}
                               for e in pods if e.get('event') == 'load_failed'][:3]}
        # первый запрос, обслуженный новой версией, и последний — старой
        okv = sorted((r['sent_wall'], r['model_version']) for r in rows if r['outcome'] == 'ok')
        if okv:
            res['version_switch'] = {v: [r3(min(t for t, vv in okv if vv == v) - meta.get('start_at', 0)),
                                         r3(max(t for t, vv in okv if vv == v) - meta.get('start_at', 0))]
                                     for v in {vv for _, vv in okv}}
        if step.name == 'crash' and tl:
            t_crash = meta.get('t_crash')
            victim = meta.get('victim')
            t_ep_gone = first_time(tl, lambda s: s['t'] > t_crash and not any(
                e['pod'] == victim and e['ready'] for e in s['endpoints']))
            t_back = first_time(tl, lambda s: t_ep_gone and s['t'] > t_ep_gone and any(
                e['pod'] == victim and e['ready'] for e in s['endpoints']))
            restarts = max((p['restarts'] for s in tl if 'pods' in s for p in s['pods'] if p['name'] == victim),
                           default=0)
            affected = [r for r in rows if r.get('pod') == victim and r['outcome'] != 'ok']
            res['crash'] = {'victim': victim, 'detected_endpoint_removed_s': r3(t_ep_gone and t_ep_gone - t_crash),
                            'recovered_endpoint_ready_s': r3(t_back and t_back - t_crash),
                            'restart_count': restarts,
                            'broken_attempts_on_victim': dict(Counter(r['outcome'] for r in affected)),
                            'attempts_by_outcome': dict(Counter(r['outcome'] for r in rows)),
                            'requests_recovered_by_retry': sum(1 for r in rows if r['attempt'] > 1 and
                                                               r['outcome'] == 'ok')}
        out[step.name] = res
    (RES / 'summary-exp-a.json').write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding='utf-8')
    flat = []
    for name, res in out.items():
        for part in ('short', 'long'):
            s = res[part]
            flat.append({'step': name, 'stream': part, **{k: s[k] for k in (
                'scheduled', 'sent_requests', 'attempts', 'completed', 'rejected', 'http_5xx', 'truncated', 'timeout',
                'connect_error', 'error_rate', 'n_ok', 'ttfc_s_p50', 'ttfc_s_p95', 'total_s_p50', 'total_s_p95',
                'send_lag_max')}, 'cold_in_pod_logs': res['cold_requests_in_pod_logs']})
    write_csv(RES / 'summary-exp-a.csv', flat)
    return out


def plot_exp_a():
    steps = [('warmup-http', 'Дефект: готов при старте HTTP', 't_restart', 'rollout restart'),
             ('warmup-warm', 'Исправлено: готов после прогрева', 't_restart', 'rollout restart'),
             ('rollout-A-to-B', 'Rolling update A→B', 't_update', 'helm upgrade B'),
             ('bad-release', 'Неготовая B → helm rollback', 't_bad_release', 'выпуск B (плохой хеш)')]
    steps = [s for s in steps if (RES / 'exp-a' / s[0] / 'meta.json').exists()]
    if not steps:
        return
    fig, axes = plt.subplots(2, len(steps), figsize=(5.2 * len(steps), 7.5), sharex='col', squeeze=False)
    for col, (name, title, mark, mark_label) in enumerate(steps):
        d = RES / 'exp-a' / name
        meta = jread(d / 'meta.json')
        t0 = meta['start_at']
        tl = jl(d / 'timeline.jsonl')
        ev = pod_events(d)
        warm_at = {e['pod']: e['ts'] for e in ev if e.get('event') == 'warm'}
        started_here = {e['pod'] for e in ev if e.get('event') == 'loading'}
        # Pod, запущенные до начала шага (нет события loading), к началу шага уже прогреты.
        for p in {p['name'] for s_ in tl for p in s_.get('pods', [])} - started_here:
            warm_at.setdefault(p, float('-inf'))
        xs, ready, ep, warm, notready, by_v = timeline_series(tl, t0, warm_at)
        ax = axes[0][col]
        ax.step(xs, ready, where='post', label='Pod Ready (не завершающиеся)', lw=2.5, color='#264653')
        ax.step(xs, ep, where='post', label='EndpointSlice ready', lw=1.5, ls='--', color='#e76f51')
        ax.step(xs, [w - 0.06 for w in warm], where='post', label='Ready и backend прогрет (журнал Pod)', lw=1.5,
                color='#2a9d8f')
        ax.step(xs, notready, where='post', label='Pod создан, не Ready', lw=1.5, ls=':', color='#9d0208')
        if len(by_v) > 1:
            for v, ys in by_v.items():
                ax.step(xs, [y + 0.06 for y in ys], where='post', label=f'Ready версии {v}', lw=1, alpha=.8)
        ax.set_title(title, fontsize=10)
        ax.set_ylabel('реплик')
        ax.set_ylim(-0.2, 5.2)
        ax.legend(fontsize=7, loc='upper right', ncol=2)
        rows = jl(d / 'short.jsonl') + jl(d / 'long.jsonl')
        ax2 = axes[1][col]
        for oc, color in OUTCOME_COLORS.items():
            sel = [r for r in rows if r['outcome'] == oc]
            if not sel:
                continue
            ax2.scatter([r['sent_wall'] - t0 for r in sel], [r['total_s'] for r in sel], s=9, color=color,
                        label=f'{oc} ({len(sel)})', alpha=.8)
        longs = [r for r in rows if r['max_chunks'] > 100]
        for r in longs:
            ax2.plot([r['sent_wall'] - t0, r['sent_wall'] - t0 + r['total_s']], [r['total_s']] * 2,
                     color=OUTCOME_COLORS.get(r['outcome'], 'k'), lw=1)
        ax2.set_yscale('log')
        ax2.set_xlabel('время от начала нагрузки, с')
        ax2.set_ylabel('длительность попытки, с (лог.)')
        ax2.legend(fontsize=7, loc='upper right')
        for a in (ax, ax2):
            if meta.get(mark):
                a.axvline(meta[mark] - t0, color='k', ls=':', lw=1)
            for extra in ('t_rollback',):
                if meta.get(extra):
                    a.axvline(meta[extra] - t0, color='#2a9d8f', ls=':', lw=1)
        ax.text((meta.get(mark, t0) - t0), 3.2, ' ' + mark_label, fontsize=7)
        if meta.get('t_rollback'):
            ax.text(meta['t_rollback'] - t0, 2.8, ' helm rollback', fontsize=7, color='#2a9d8f')
    fig.suptitle('Опыт A: Ready/EndpointSlice и исходы запросов (имитатор, warmup 20 с, '
                 'короткие 4 rps + длинные потоки 25 с, keep-alive выкл.)', fontsize=11)
    fig.tight_layout()
    fig.savefig(PLOTS / '1-warmup-rollout-ready.png', dpi=130)
    plt.close(fig)


# ---------------------------------------------------------------- опыт B
def exp_b_runs():
    runs = []
    for d in sorted((RES / 'exp-b').glob('*-run*')):
        meta = jread(d / 'meta.json')
        if not meta:  # прогон ещё идёт или прерван
            continue
        rows = [r for c in range(4) for r in jl(d / f'client{c}.jsonl')]
        meas = [r for r in rows if r['phase'] == 'measure']
        pods = sorted({r['pod'] for r in meas if r.get('pod')})
        s = summarize(meas)
        per_pod = {}
        for p in pods:
            pr = [r for r in meas if r.get('pod') == p]
            ok = [r for r in pr if r['outcome'] == 'ok']
            per_pod[p] = {'requests': len(pr), 'share': round(len(pr) / max(1, len(meas)), 3),
                          'ttfc_p50': pct([r['ttfc_s'] for r in ok], .5),
                          'ttfc_p95': pct([r['ttfc_s'] for r in ok], .95), 'n_ok': len(ok),
                          'queue_mean': r3(statistics.mean([r['server_queue_s'] for r in ok])) if ok else None,
                          'queue_p95': pct([r['server_queue_s'] for r in ok], .95),
                          'cache_hit_ratio': r3(sum(r['cache'] == 'hit' for r in ok) / len(ok)) if ok else None}
        by_client = defaultdict(Counter)
        for r in meas:
            by_client[f"client{r['client']} ({r.get('client_ip')})"][r.get('pod')] += 1
        hot = max(per_pod.values(), key=lambda v: v['share']) if per_pod else {}
        runs.append({'run': d.name, 'policy': meta.get('policy'), 'profile': meta.get('profile'),
                     'seed': meta.get('seed'), 'summary': s, 'per_pod': per_pod,
                     'client_to_pod': {k: dict(v) for k, v in by_client.items()},
                     'hot_share': hot.get('share'), 'pods_used': len(pods),
                     'cache_hit_ratio': r3(sum(r['cache'] == 'hit' for r in meas if r['outcome'] == 'ok') /
                                           max(1, s['n_ok'])),
                     'meta': meta})
    return runs


def exp_b_summary():
    runs = exp_b_runs()
    (RES / 'summary-exp-b.json').write_text(json.dumps(runs, ensure_ascii=False, indent=1), encoding='utf-8')
    flat = []
    for r in runs:
        s = r['summary']
        pp = sorted(r['per_pod'].values(), key=lambda v: -v['share'])
        flat.append({'policy': r['policy'], 'profile': r['profile'], 'seed': r['seed'],
                     'scheduled': s['scheduled'], 'completed': s['completed'], 'rejected': s['rejected'],
                     'http_5xx': s['http_5xx'], 'truncated': s['truncated'], 'timeout': s['timeout'],
                     'error_rate': s['error_rate'], 'n_ok': s['n_ok'], 'ttfc_p50': s['ttfc_s_p50'],
                     'ttfc_p95': s['ttfc_s_p95'], 'total_p95': s['total_s_p95'], 'queue_p95': s['server_queue_s_p95'],
                     'cache_hit_ratio': r['cache_hit_ratio'], 'hot_share': r['hot_share'],
                     'hot_ttfc_p95': pp[0]['ttfc_p95'] if pp else None,
                     'cold_ttfc_p95': pp[1]['ttfc_p95'] if len(pp) > 1 else None,
                     'hot_queue_mean': pp[0]['queue_mean'] if pp else None,
                     'cold_queue_mean': pp[1]['queue_mean'] if len(pp) > 1 else None,
                     'send_lag_max': s['send_lag_max']})
    write_csv(RES / 'summary-exp-b.csv', flat)
    agg = []
    for (pol, prof), grp in sorted(_group(flat, ('policy', 'profile')).items()):
        row = {'policy': pol, 'profile': prof, 'runs': len(grp)}
        for k in ('ttfc_p50', 'ttfc_p95', 'error_rate', 'cache_hit_ratio', 'hot_share', 'queue_p95'):
            vals = [g[k] for g in grp if g[k] is not None]
            row[f'{k}_mean'] = r3(statistics.mean(vals)) if vals else None
            row[f'{k}_min'] = r3(min(vals)) if vals else None
            row[f'{k}_max'] = r3(max(vals)) if vals else None
        agg.append(row)
    write_csv(RES / 'summary-exp-b-agg.csv', agg)
    return runs, flat


def _group(rows, keys):
    out = defaultdict(list)
    for r in rows:
        out[tuple(r[k] for k in keys)].append(r)
    return out


def plot_exp_b_trace(runs):
    """График 2: задержка и очередь по Pod во времени для одного skewed-прогона трёх политик."""
    chosen = [r for r in runs if r['profile'] == 'skewed' and r['seed'] == 1]
    if not chosen:
        return
    order = {'plain': 0, 'affinity': 1, 'router': 2}
    chosen.sort(key=lambda r: order.get(r['policy'], 9))
    fig, axes = plt.subplots(2, len(chosen), figsize=(5.4 * len(chosen), 7), sharey='row', squeeze=False)
    colors = ['#1d3557', '#e63946', '#2a9d8f', '#f4a261']
    for col, run in enumerate(chosen):
        d = RES / 'exp-b' / run['run']
        meta = run['meta']
        t0 = meta['start_at']
        rows = [r for c in range(4) for r in jl(d / f'client{c}.jsonl') if r['outcome'] == 'ok']
        pods = sorted({r['pod'] for r in rows})
        ax = axes[0][col]
        for i, p in enumerate(pods):
            sel = [r for r in rows if r['pod'] == p]
            meas = [r['ttfc_s'] for r in sel if r['phase'] == 'measure']
            ax.scatter([r['sent_wall'] - t0 for r in sel], [r['ttfc_s'] for r in sel], s=7, color=colors[i % 4],
                       label=f'{p[-5:]}: {len(meas)} отв. в измерении, p95 {pct(meas, .95):.2f} с')
        ax.axvline(meta['warmup_s'], color='k', ls=':', lw=1)
        ax.set_title(f'{run["policy"]}, skewed (80 % от client0), прогон 1', fontsize=10)
        ax.set_ylabel('TTFC, с')
        ax.legend(fontsize=7, loc='upper left')
        ax2 = axes[1][col]
        prom = jread(d / 'prom.json').get('waiting')
        if isinstance(prom, list):
            for i, sr in enumerate(sorted(prom, key=lambda s: s['metric'].get('pod', ''))):
                xs = [float(t) - t0 for t, _ in sr['values']]
                ys = [float(v) for _, v in sr['values']]
                ax2.step(xs, ys, where='post', color=colors[pods.index(sr['metric']['pod']) % 4
                                                         if sr['metric'].get('pod') in pods else i % 4],
                         label=f'waiting {sr["metric"].get("pod", "")[-5:]}')
        ax2.axvline(meta['warmup_s'], color='k', ls=':', lw=1)
        ax2.set_xlabel('время от начала, с (до пунктира — прогрев 20 с)')
        ax2.set_ylabel('очередь (llm_waiting), запросов')
        ax2.legend(fontsize=7, loc='upper left')
    fig.suptitle('Опыт B: TTFC и очередь по Pod (имитатор: 2 слота/Pod, miss 100 мс/1000 симв., hit 20 мс, '
                 '2 rps, keep-alive вкл.)', fontsize=11)
    fig.tight_layout()
    fig.savefig(PLOTS / '2-latency-queue-by-pod.png', dpi=130)
    plt.close(fig)


def plot_exp_b_policies(flat):
    """График 3: сравнение политик по трём прогонам (точки — прогоны, столбец — среднее)."""
    if not flat:
        return
    metrics = [('ttfc_p95', 'p95 TTFC, с'), ('hot_share', 'доля запросов на самый загруженный Pod'),
               ('cache_hit_ratio', 'доля cache hit (имитация)'), ('error_rate', 'доля отказов и тайм-аутов')]
    policies = ['plain', 'affinity', 'router']
    profiles = ['uniform', 'skewed']
    fig, axes = plt.subplots(1, len(metrics), figsize=(19, 4.6))
    width = 0.38
    for ai, (key, label) in enumerate(metrics):
        ax = axes[ai]
        for pi, prof in enumerate(profiles):
            for xi, pol in enumerate(policies):
                vals = [r[key] for r in flat if r['policy'] == pol and r['profile'] == prof and r[key] is not None]
                if not vals:
                    continue
                x = xi + (pi - .5) * width
                ax.bar(x, statistics.mean(vals), width * .9, color=['#8ecae6', '#fb8500'][pi],
                       label=prof if xi == 0 else None, alpha=.85)
                ax.scatter([x] * len(vals), vals, color='k', s=12, zorder=3)
        ax.set_xticks(range(len(policies)))
        ax.set_xticklabels(['Service\n(без привязки)', 'Service\nClientIP', 'HAProxy\nhash+bounded'], fontsize=8)
        ax.set_title(label, fontsize=10)
        if ai == 0:
            ax.legend(title='трафик', fontsize=8)
    fig.suptitle('Опыт B: политики маршрутизации, 3 прогона × 120 с измерения на комбинацию (точки — прогоны)',
                 fontsize=11)
    fig.tight_layout()
    fig.savefig(PLOTS / '3-policy-comparison.png', dpi=130)
    plt.close(fig)


# ---------------------------------------------------------------- опыт C
def hpa_events(tl, t0):
    ev = []
    prev = None
    for s in tl:
        if 'pods' not in s:
            continue
        n = s['deploy'].get('spec_replicas')
        if prev is not None and n != prev:
            ev.append({'t': r3(s['t'] - t0), 'spec_replicas': f'{prev}->{n}'})
        prev = n
    first_seen = {}
    ready_seen = {}
    for s in tl:
        for p in s.get('pods', []):
            first_seen.setdefault(p['name'], s['t'])
            if p['ready']:
                ready_seen.setdefault(p['name'], s['t'])
    return ev, first_seen, ready_seen


def exp_c_summary():
    out = {}
    for name in ('hpa', 'hot', 'alerts'):
        d = RES / 'exp-c' / name
        if not d.exists():
            continue
        meta = jread(d / 'meta.json')
        rows = [r for f in sorted(d.glob('*client*.jsonl')) for r in jl(f)]
        tl = jl(d / 'timeline.jsonl')
        t0 = meta.get('start_at') or (meta.get('phases') or [{}])[0].get('start')
        res = {'meta': meta, 'summary': summarize(rows) if rows else None}
        if tl and t0:
            ev, first_seen, ready_seen = hpa_events(tl, t0)
            initial = {p['name'] for p in next(s for s in tl if 'pods' in s)['pods']}
            res['replica_changes'] = ev
            res['new_pods'] = {n: {'appeared_s': r3(first_seen[n] - t0),
                                   'ready_s': r3(ready_seen[n] - t0) if n in ready_seen else None,
                                   'appear_to_ready_s': r3(ready_seen[n] - first_seen[n]) if n in ready_seen else None}
                               for n in first_seen if n not in initial}
            res['requests_by_pod'] = dict(Counter(r.get('pod') for r in rows))
            res['requests_by_pod_and_client'] = {f'{k[0]}|client{k[1]}': v for k, v in Counter(
                (r.get('pod'), r['client']) for r in rows).items()}
        if name == 'hpa' and meta.get('t_back_to_min'):
            res['back_to_min_after_load_end_s'] = r3(meta['t_back_to_min'] - meta['t_load_end'])
        if name == 'alerts':
            polls = jl(d / 'alerts-poll.jsonl')
            trans = []
            last = {}
            for p in polls:
                states = {a['name']: a['state'] for a in p.get('alerts', [])}
                for alert in ('LLMHighTTFC', 'LLMErrorRatio'):
                    st = states.get(alert, 'inactive')
                    if last.get(alert) != st:
                        trans.append({'alert': alert, 'state': st, 't': r3(p['t'] - t0)})
                        last[alert] = st
            res['alert_transitions'] = trans
            res['phase_summary'] = {ph['phase']: summarize([r for r in rows if r['run'] == ph['phase']])
                                    for ph in meta.get('phases', [])}
        out[name] = res
    (RES / 'summary-exp-c.json').write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding='utf-8')
    return out


def plot_exp_c():
    d = RES / 'exp-c' / 'hpa'
    meta = jread(d / 'meta.json')
    if not meta:
        return
    t0 = meta['start_at']
    tl = jl(d / 'timeline.jsonl')
    rows = [r for c in range(4) for r in jl(d / f'client{c}.jsonl')]
    prom = jread(d / 'prom.json')
    fig, axes = plt.subplots(3, 1, figsize=(12, 9.5), sharex=True)
    # нагрузка: запланированная интенсивность и завершённые/с
    ax = axes[0]
    t = 0
    for length, rate in meta['ramp']:
        ax.plot([t, t + length], [rate, rate], color='#adb5bd', lw=6, alpha=.6,
                label='запланировано, rps' if t == 0 else None)
        t += length
    bins = defaultdict(int)
    for r in rows:
        if r['outcome'] == 'ok':
            bins[int((r['finished_wall'] - t0) // 10)] += 1
    xs = sorted(bins)
    ax.plot([x * 10 + 5 for x in xs], [bins[x] / 10 for x in xs], color='#1d3557', label='завершено ok, rps')
    bad = defaultdict(int)
    for r in rows:
        if r['outcome'] != 'ok':
            bad[int((r['finished_wall'] - t0) // 10)] += 1
    if bad:
        ax.bar([x * 10 + 5 for x in sorted(bad)], [bad[x] / 10 for x in sorted(bad)], width=8, color='#d62828',
               label='отказы/тайм-ауты, rps')
    ax.set_ylabel('запросов/с')
    ax.legend(fontsize=8)
    ax.set_title('Нагрузка (4 клиента, keep-alive выкл., обычный Service)', fontsize=10)
    # реплики
    ax = axes[1]
    pts = [(s['t'] - t0, s['deploy'].get('spec_replicas'), s['deploy'].get('readyReplicas', 0),
            (s.get('hpa') or {}).get('desired'), (s.get('hpa') or {}).get('cpu_utilization')) for s in tl
           if 'pods' in s]
    if pts:
        ax.step([p[0] for p in pts], [p[1] for p in pts], where='post', label='Deployment spec.replicas', lw=2)
        ax.step([p[0] for p in pts], [p[2] for p in pts], where='post', label='Ready', lw=1.5, ls='--')
        ax3 = ax.twinx()
        ax3.plot([p[0] for p in pts], [p[4] for p in pts], color='#f4a261', lw=1, label='HPA: средний CPU, % request')
        ax3.axhline(60, color='#f4a261', ls=':', lw=1)
        ax3.set_ylabel('CPU, % от request (цель 60 %)')
        ax3.legend(fontsize=8, loc='upper right')
    ev, first_seen, ready_seen = hpa_events(tl, t0)
    initial = {p['name'] for p in next(s for s in tl if 'pods' in s)['pods']} if tl else set()
    for n in first_seen:
        if n not in initial:
            ax.axvline(first_seen[n] - t0, color='#6a4c93', ls=':', lw=1)
            if n in ready_seen:
                ax.axvline(ready_seen[n] - t0, color='#2a9d8f', ls=':', lw=1)
                ax.text(first_seen[n] - t0, 3.25, f' Pod создан +{ready_seen[n] - first_seen[n]:.0f} с до Ready',
                        fontsize=8)
    ax.set_ylim(0, 3.6)
    ax.set_ylabel('реплик')
    ax.legend(fontsize=8, loc='upper left')
    # CPU по Pod
    ax = axes[2]
    for sr in prom.get('cpu_by_pod', []):
        ax.plot([float(t) - t0 for t, _ in sr['values']], [float(v) for _, v in sr['values']],
                label=sr['metric'].get('pod', '')[-5:])
    ax.axhline(0.5, color='k', ls=':', lw=1)
    ax.text(5, 0.52, 'request 500m', fontsize=8)
    ax.set_ylabel('CPU, ядер (cAdvisor)')
    ax.set_xlabel('время от начала нагрузки, с')
    ax.legend(fontsize=8, title='Pod')
    fig.suptitle('Опыт C: HPA 2→3→2 по CPU (имитатор WORK_MODE=cpu, min 2, max 3, цель 60 %, '
                 'окно стабилизации scale-down 60 с)', fontsize=11)
    fig.tight_layout()
    fig.savefig(PLOTS / '4-hpa-scaling.png', dpi=130)
    plt.close(fig)


def main():
    PLOTS.mkdir(parents=True, exist_ok=True)
    rows = collect_requests()
    print('requests.csv:', len(rows), 'attempts')
    if (RES / 'exp-a').exists():
        a = exp_a_summary()
        for k, v in a.items():
            print('A', k, {x: v['all'][x] for x in ('attempts', 'completed', 'http_5xx', 'truncated', 'rejected',
                                                    'connect_error', 'timeout')}, 'cold', v['cold_requests_in_pod_logs'])
        plot_exp_a()
    if (RES / 'exp-b').exists():
        runs, flat = exp_b_summary()
        for f in flat:
            print('B', f['policy'], f['profile'], f['seed'], 'p95', f['ttfc_p95'], 'hot', f['hot_share'],
                  'hit', f['cache_hit_ratio'], 'err', f['error_rate'])
        plot_exp_b_trace(runs)
        plot_exp_b_policies(flat)
    if (RES / 'exp-c').exists():
        c = exp_c_summary()
        print('C', json.dumps({k: {kk: v.get(kk) for kk in ('replica_changes', 'new_pods',
                                                            'back_to_min_after_load_end_s', 'alert_transitions')}
                               for k, v in c.items()}, ensure_ascii=False)[:2000])
        plot_exp_c()


if __name__ == '__main__':
    main()
