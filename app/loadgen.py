"""Открытый (open-loop) генератор нагрузки: запросы уходят по расписанию, не дожидаясь предыдущих.

Каждая попытка — строка JSONL в stdout. Длительности меряются time.perf_counter() (монотонные часы),
абсолютные отметки time.time() нужны только для сопоставления с журналами Pod и Prometheus.
Запланированное и фактическое время отправки сохраняются оба: send_lag_s показывает, не отстаёт ли
сам генератор.

TTFC — от отправки запроса до первой строки type=chunk; inter-chunk latency — интервалы между
последовательными частями (N-1 интервал для N частей). Это измерения имитатора, а не токенов модели.

Исходы попытки (outcome):
  ok            200 и получена финальная строка type=done
  truncated     200, поток оборвался до done (оборванный)
  timeout       не уложился в --timeout (просроченный)
  rejected      429 queue_full (отклонён допуском)
  http_5xx      ответ 5xx (в т. ч. 503 backend_warming)
  http_4xx      прочие 4xx
  connect_error соединение не установлено / сброшено до заголовков
Повторы (--retries) пишутся отдельными строками с attempt=1, 2, ...
"""
import argparse
import asyncio
import json
import os
import statistics
import sys
import time

import httpx

from workload import build_messages, schedule


def percentile(values, q):
    if not values:
        return None
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round(q * (len(values) - 1)))))
    return values[k]


async def one_attempt(client, args, item, attempt, scheduled_wall):
    row = {**{k: item[k] for k in ('req_id', 'client', 'session_id', 'turn', 'phase', 'max_chunks', 'prompt_chars')},
           'scheduled_s': item['scheduled_s'], 'attempt': attempt, 'target': args.target, 'policy': args.policy,
           'profile': args.profile, 'run': args.run, 'client_pod': os.environ.get('POD_NAME'),
           'client_ip': os.environ.get('POD_IP'), 'keepalive': args.keepalive,
           'scheduled_wall': round(scheduled_wall, 4), 'outcome': None, 'status': None, 'pod': None,
           'model_version': None, 'request_id': None, 'chunks': 0, 'ttfc_s': None, 'headers_s': None,
           'total_s': None, 'inter_chunk_mean_s': None, 'inter_chunk_p95_s': None, 'cache': None,
           'server_queue_s': None, 'prefill_s': None, 'queued_ahead': None, 'error': None}
    body = {'session_id': item['session_id'], 'messages': build_messages(item['session_id'], item['turn']),
            'max_chunks': item['max_chunks']}
    request_id = f"{item['req_id']}.{attempt}"
    headers = {'X-Request-ID': request_id, 'X-Session-ID': item['session_id']}
    row['sent_wall'] = round(time.time(), 4)
    row['send_lag_s'] = round(row['sent_wall'] - scheduled_wall, 4)
    t0 = time.perf_counter()
    chunk_times = []
    done = None

    async def run():
        nonlocal done
        async with client.stream('POST', args.target + '/v1/chat', json=body, headers=headers) as resp:
            row['status'] = resp.status_code
            row['headers_s'] = round(time.perf_counter() - t0, 4)
            row['pod'] = resp.headers.get('x-pod')
            row['request_id'] = resp.headers.get('x-request-id')
            row['model_version'] = resp.headers.get('x-model-version') or None
            row['routed_to'] = resp.headers.get('x-routed-to')
            if resp.status_code != 200:
                text = (await resp.aread()).decode('utf-8', 'replace')
                try:
                    row['error'] = json.loads(text).get('error')
                except ValueError:
                    row['error'] = text[:200]
                return
            buf = b''
            async for raw in resp.aiter_raw():
                buf += raw
                while b'\n' in buf:
                    line, buf = buf.split(b'\n', 1)
                    if not line.strip():
                        continue
                    msg = json.loads(line)
                    if msg.get('type') == 'chunk':
                        chunk_times.append(time.perf_counter())
                        row.setdefault('first_text', msg['text'])
                        row.setdefault('texts', []).append(msg['text'])
                    elif msg.get('type') == 'done':
                        done = msg

    try:
        await asyncio.wait_for(run(), args.timeout)
        if row['status'] == 200:
            row['outcome'] = 'ok' if done else 'truncated'
            if not done:
                row['error'] = 'stream ended without done'
        elif row['status'] == 429:
            row['outcome'] = 'rejected'
        elif row['status'] >= 500:
            row['outcome'] = 'http_5xx'
        else:
            row['outcome'] = 'http_4xx'
    except asyncio.TimeoutError:
        row['outcome'] = 'timeout'
        row['error'] = f'no done within {args.timeout}s'
    except (httpx.HTTPError, OSError, json.JSONDecodeError) as exc:
        row['outcome'] = 'truncated' if row['status'] == 200 else 'connect_error'
        row['error'] = f'{type(exc).__name__}: {exc}'[:300]
    row['total_s'] = round(time.perf_counter() - t0, 4)
    row['chunks'] = len(chunk_times)
    if chunk_times:
        row['ttfc_s'] = round(chunk_times[0] - t0, 4)
        gaps = [b - a for a, b in zip(chunk_times, chunk_times[1:])]
        if gaps:
            row['inter_chunk_mean_s'] = round(statistics.mean(gaps), 4)
            row['inter_chunk_p95_s'] = round(percentile(gaps, .95), 4)
    if done:
        row.update(pod=done['pod'], model_version=done['model_version'], cache=done.get('cache'),
                   server_queue_s=done.get('queue_s'), prefill_s=done.get('prefill_s'),
                   queued_ahead=done.get('queued_ahead'), model_sha256=done.get('model_sha256'),
                   backend_mode=done.get('backend_mode'))
    texts = row.pop('texts', None)
    if args.keep_text and texts:
        row['text'] = ' '.join(texts)
    row['finished_wall'] = round(time.time(), 4)
    return row


async def run_item(client, args, item, start_wall, out):
    scheduled_wall = start_wall + item['scheduled_s']
    delay = scheduled_wall - time.time()
    if delay > 0:
        await asyncio.sleep(delay)
    for attempt in range(1, args.retries + 2):
        row = await one_attempt(client, args, item, attempt, scheduled_wall)
        out(row)
        if row['outcome'] == 'ok' or attempt > args.retries:
            return
        await asyncio.sleep(args.retry_backoff * attempt)


async def main_async(args):
    rows = schedule(args.profile, args.rate, args.duration, args.warmup, args.seed)
    mine = [r for r in rows if args.client is None or r['client'] == args.client]
    if args.max_chunks:
        mine = [{**r, 'max_chunks': args.max_chunks} for r in mine]
    start_wall = args.start_at or time.time() + 1
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=100 if args.keepalive else 0)
    timeout = httpx.Timeout(args.timeout, connect=5)

    def out(row):
        sys.stdout.write(json.dumps(row, ensure_ascii=False) + '\n')
        sys.stdout.flush()

    print(f'loadgen: {len(mine)}/{len(rows)} requests for client={args.client} start_at={start_wall:.3f} '
          f'target={args.target} keepalive={args.keepalive}', file=sys.stderr, flush=True)
    async with httpx.AsyncClient(limits=limits, timeout=timeout, http2=False) as client:
        await asyncio.gather(*(run_item(client, args, item, start_wall, out) for item in mine))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--target', required=True)
    p.add_argument('--profile', choices=['uniform', 'skewed', 'ramp'], default='uniform')
    p.add_argument('--rate', type=float, default=2.0, help='суммарно по всем клиентам, запросов/с')
    p.add_argument('--duration', type=float, default=120, help='фаза измерения, с')
    p.add_argument('--warmup', type=float, default=20, help='фаза прогрева перед измерением, с')
    p.add_argument('--seed', type=int, default=1)
    p.add_argument('--client', type=int, help='индекс клиента; без него — все запросы расписания')
    p.add_argument('--start-at', type=float, help='общая отметка начала (epoch), одинаковая у всех клиентов')
    p.add_argument('--timeout', type=float, default=60)
    p.add_argument('--retries', type=int, default=0)
    p.add_argument('--retry-backoff', type=float, default=0.5)
    p.add_argument('--keepalive', type=int, choices=[0, 1], default=1)
    p.add_argument('--max-chunks', type=int, help='переопределить длину ответа (длинные потоки в опыте A)')
    p.add_argument('--keep-text', action='store_true')
    p.add_argument('--policy', default='')
    p.add_argument('--run', default='')
    asyncio.run(main_async(p.parse_args()))


if __name__ == '__main__':
    main()
