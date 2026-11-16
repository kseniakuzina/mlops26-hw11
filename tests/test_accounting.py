"""Проверка 4: корректный учёт отказов, оборванных и просроченных потоков и повторов.

Сервис запускается локально тем же кодом, что в образе; генератор — тот же loadgen.one_attempt.
"""
import asyncio
import os
import socket
import subprocess
import sys
import time
import types
import urllib.request

import httpx
import pytest

from conftest import ROOT
import loadgen
from metrics import summarize


def _free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _start(**env):
    port = _free_port()
    full = {**os.environ, 'MODEL_DIR': str(ROOT / 'models'), 'MODEL_VERSION': 'A', 'WARMUP_SECONDS': '0',
            'PORT': str(port), 'FAULT_INJECTION': '1', 'POD_NAME': 'local-test', 'PYTHONUTF8': '1',
            **{k: str(v) for k, v in env.items()}}
    proc = subprocess.Popen([sys.executable, str(ROOT / 'app' / 'service.py')], env=full,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f'http://127.0.0.1:{port}'
    for _ in range(100):
        try:
            urllib.request.urlopen(url + '/readyz', timeout=1)
            return proc, url
        except OSError:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError('сервис не стартовал')


@pytest.fixture
def service(request):
    procs = []

    def start(**env):
        proc, url = _start(**env)
        procs.append(proc)
        return url
    yield start
    for p in procs:
        p.kill()


def _item(i, max_chunks=5):
    return {'req_id': f'r{i}', 'client': 0, 'session_id': f's{i}', 'turn': 0, 'phase': 'measure',
            'max_chunks': max_chunks, 'prompt_chars': 0, 'scheduled_s': 0}


def _args(url, timeout=10, retries=0):
    return types.SimpleNamespace(target=url, policy='t', profile='t', run='t', keepalive=0, timeout=timeout,
                                 keep_text=False, retries=retries, retry_backoff=0.05)


async def _attempts(url, items, **kw):
    async with httpx.AsyncClient() as client:
        return await asyncio.gather(*(loadgen.one_attempt(client, _args(url, **kw), it, 1, time.time())
                                      for it in items))


def test_outcomes_are_classified(service):
    url = service(MAX_CONCURRENCY=1, MAX_QUEUE=0, CHUNK_INTERVAL_MS=50)
    # 1 обрабатывается, 2-й одновременно получает 429 (очередь 0)
    pair = asyncio.run(_attempts(url, [_item(1, 10), _item(2, 10)]))
    ok, rejected = sorted(pair, key=lambda r: r['outcome'] != 'ok')
    assert ok['outcome'] == 'ok' and ok['chunks'] == 10 and ok['ttfc_s'] is not None
    assert rejected['outcome'] == 'rejected' and rejected['status'] == 429
    # просроченный: prefill ~1 с + 100 частей x 50 мс не укладываются в 2,5 с
    (late,) = asyncio.run(_attempts(url, [_item(3, 100)], timeout=2.5))
    assert late['outcome'] == 'timeout' and late['chunks'] > 0
    rows = [ok, rejected, late]
    s = summarize(rows, scheduled=3)
    assert (s['completed'], s['rejected'], s['timeout'], s['n_ok']) == (1, 1, 1, 1)
    assert s['error_rate'] == round(2 / 3, 4)


def test_crash_mid_stream_is_truncated(service):
    url = service(MAX_CONCURRENCY=4, CHUNK_INTERVAL_MS=50)

    async def scenario():
        task = asyncio.create_task(_attempts(url, [_item(1, 200)]))
        await asyncio.sleep(1.0)
        try:
            async with httpx.AsyncClient() as c:
                await c.post(url + '/admin/crash', timeout=2)
        except httpx.HTTPError:
            pass
        return (await task)[0]

    row = asyncio.run(scenario())
    assert row['status'] == 200 and row['outcome'] == 'truncated', row
    assert 0 < row['chunks'] < 200
    assert summarize([row])['truncated'] == 1


def test_retries_are_separate_attempts(service):
    url = service(MAX_CONCURRENCY=1, MAX_QUEUE=0, CHUNK_INTERVAL_MS=50)
    out = []

    async def scenario():
        async with httpx.AsyncClient() as client:
            blocker = asyncio.create_task(loadgen.one_attempt(client, _args(url), _item(0, 40), 1, time.time()))
            await asyncio.sleep(0.2)
            await loadgen.run_item(client, _args(url, retries=2), _item(9, 2), time.time(), out.append)
            await blocker

    asyncio.run(scenario())
    assert [r['attempt'] for r in out] == [1, 2, 3]
    assert all(r['outcome'] == 'rejected' for r in out)
    s = summarize(out, scheduled=1)
    assert (s['sent_requests'], s['attempts'], s['retries'], s['rejected']) == (1, 3, 2, 1)
