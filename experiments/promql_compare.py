"""Сверка перцентилей: classic histogram в Prometheus vs клиентский журнал за один и тот же период.

Для каждого прогона опыта B берётся фаза измерения [start_at+warmup, end]. Prometheus:
  histogram_quantile(q, sum by (le) (increase(llm_ttfc_seconds_bucket{namespace="llm"}[W])))
вычисляется в момент end с окном W = end - t0 (суммарные bucket-счётчики всех Pod, le сохранён).
Клиент: те же ответы (outcome=ok, первая часть получена в окне), nearest-rank p50/p95 по ttfc_s.
Расхождение ожидаемо: (1) сервер мерит от приёма запроса, клиент — от отправки (сеть, Service);
(2) histogram_quantile линейно интерполирует внутри bucket; (3) границы окна по scrape_interval 5 с.
"""
import json
import sys
from pathlib import Path

from common import RESULTS, ensure_prom_forward, prom_query, write_json

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'analysis'))
from metrics import pct  # noqa: E402

Q = ('histogram_quantile({q}, sum by (le) (increase(llm_ttfc_seconds_bucket{{namespace="llm"}}[{w}s])))')
COUNT = 'sum(increase(llm_ttfc_seconds_count{{namespace="llm"}}[{w}s]))'
BUCKETS = 'sum by (le) (increase(llm_ttfc_seconds_bucket{{namespace="llm"}}[{w}s]))'


def main():
    pf = ensure_prom_forward()
    rows = []
    for run_dir in sorted((RESULTS / 'exp-b').glob('*-run*')):
        meta = json.loads((run_dir / 'meta.json').read_text(encoding='utf-8'))
        t0 = meta['start_at'] + meta['warmup_s']
        end = meta['end']
        w = int(end - t0)
        client = []
        for f in run_dir.glob('client*.jsonl'):
            for line in f.read_text(encoding='utf-8').splitlines():
                r = json.loads(line)
                if r['outcome'] == 'ok' and r['ttfc_s'] is not None and r['sent_wall'] + r['ttfc_s'] >= t0 - 0:
                    if r['sent_wall'] + r['ttfc_s'] <= end:
                        client.append(r['ttfc_s'])
        res = {'run': run_dir.name, 'window_s': w, 'client_n': len(client),
               'client_p50': pct(client, .5), 'client_p95': pct(client, .95)}
        for q in (.5, .95):
            v = prom_query(Q.format(q=q, w=w), at=end)
            res[f'prom_p{int(q * 100)}'] = float(v[0]['value'][1]) if v else None
        c = prom_query(COUNT.format(w=w), at=end)
        res['prom_n'] = round(float(c[0]['value'][1]), 1) if c else None
        b = prom_query(BUCKETS.format(w=w), at=end)
        res['prom_buckets'] = {x['metric']['le']: round(float(x['value'][1]), 1) for x in b}
        rows.append(res)
        print(f"{res['run']:28s} n client={res['client_n']:4d} prom={res['prom_n']}  "
              f"p50 {res['client_p50']} vs {res['prom_p50']:.3f}  p95 {res['client_p95']} vs {res['prom_p95']:.3f}")
    write_json(RESULTS / 'promql-vs-client.json', {
        'promql': Q.format(q='0.95', w='W'), 'note': __doc__, 'rows': rows})
    if pf:
        pf.terminate()


if __name__ == '__main__':
    main()
