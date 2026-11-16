"""Учёт исходов и перцентили по сырым строкам генератора (results/**/*.jsonl).

Запрос (req_id) и попытка (attempt) различаются: повторы клиента — отдельные попытки,
итог запроса определяется последней попыткой.
"""
import math

OUTCOMES = ('ok', 'rejected', 'http_5xx', 'http_4xx', 'truncated', 'timeout', 'connect_error')


def pct(values, q):
    """Перцентиль методом nearest-rank: наблюдение с номером ceil(q*n)."""
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    k = max(1, math.ceil(q * len(values)))
    return values[k - 1]


def summarize(rows, scheduled=None):
    """Счётчики по запросам и попыткам + p50/p95 TTFC/полного ответа только по успешным."""
    by_req = {}
    for r in rows:
        prev = by_req.get(r['req_id'])
        if prev is None or r['attempt'] > prev['attempt']:
            by_req[r['req_id']] = r
    final = list(by_req.values())
    ok = [r for r in final if r['outcome'] == 'ok']
    out = {'scheduled': scheduled if scheduled is not None else len(final), 'sent_requests': len(final),
           'attempts': len(rows), 'retries': len(rows) - len(final)}
    for name in OUTCOMES:
        out[name] = sum(r['outcome'] == name for r in final)
        out[f'attempts_{name}'] = sum(r['outcome'] == name for r in rows)
    out['completed'] = out.pop('ok')
    n = len(final) or 1
    out['error_rate'] = round(1 - len(ok) / n, 4) if final else None
    out['timeout_rate'] = round(out['timeout'] / n, 4) if final else None
    out['n_ok'] = len(ok)
    for key in ('ttfc_s', 'total_s', 'server_queue_s', 'inter_chunk_mean_s'):
        vals = [r[key] for r in ok if r.get(key) is not None]
        out[f'{key}_p50'] = pct(vals, .5)
        out[f'{key}_p95'] = pct(vals, .95)
    # Отставание генератора — только по первым попыткам: у повторов задержка включает backoff.
    lags = [r['send_lag_s'] for r in rows if r.get('send_lag_s') is not None and r['attempt'] == 1]
    out['send_lag_p95'] = pct(lags, .95)
    out['send_lag_max'] = max(lags) if lags else None
    return out
