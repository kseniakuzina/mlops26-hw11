"""Проверка 1 (Kubernetes): новый Pod не получает рабочий трафик, пока идёт прогрев.

Три независимых свидетельства: условие Ready, EndpointSlice обычного Service и журнал запросов Pod.
На исходной конфигурации (probes.mode=http) проверка падает — это красный прогон исправленного дефекта.
"""
import json
import threading
import time
from datetime import datetime

from conftest import chat, kjson, kubectl, ready_pods, wait_ready


def _ts(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp()


def test_replacement_pod_gets_no_traffic_while_warming(cluster):
    old = {p['metadata']['name'] for p in ready_pods()}
    victim = sorted(old)[0]
    stop = threading.Event()
    sent = []

    def traffic():
        # Постоянные запросы через Service всё время замены Pod.
        while not stop.is_set():
            sent.extend(chat(f'warm-{len(sent)}', [{'role': 'user', 'content': 'проверка прогрева'}], repeat=3,
                             max_chunks=2))

    th = threading.Thread(target=traffic, daemon=True)
    th.start()
    kubectl('delete', 'pod', victim, '--wait=false')
    new, seen_ready_ep = None, []
    t0 = time.time()
    while time.time() - t0 < 120:
        pods = {p['metadata']['name']: p for p in ready_pods()}
        cand = [n for n in pods if n not in old]
        if cand:
            new = cand[0]
            pod = pods[new]
            ip = pod['status'].get('podIP')
            slices = kjson('get', 'endpointslices', '-l', 'kubernetes.io/service-name=llm-sim')['items']
            in_ep = any(e['addresses'][0] == ip and e.get('conditions', {}).get('ready')
                        for s in slices for e in s.get('endpoints') or [])
            seen_ready_ep.append((time.time(), in_ep))
            conds = {c['type']: c for c in pod['status'].get('conditions', [])}
            if conds.get('Ready', {}).get('status') == 'True':
                break
        time.sleep(0.5)
    stop.set()
    th.join(timeout=60)
    assert new, 'замещающий Pod не появился'
    wait_ready()
    # При дефекте Pod становится Ready раньше прогрева: ждём событие warm, чтобы сравнить моменты.
    for _ in range(60):
        logs = [json.loads(line) for line in kubectl('logs', new).splitlines() if line.startswith('{')]
        warm = [e['ts'] for e in logs if e['event'] == 'warm']
        if warm:
            break
        time.sleep(1)
    assert warm, f'{new}: прогрев не завершился'
    pod = kjson('get', 'pod', new)
    ready_at = _ts(next(c['lastTransitionTime'] for c in pod['status']['conditions'] if c['type'] == 'Ready'))
    cold = [e for e in logs if e['event'] == 'cold_request']
    early_ep = [t for t, in_ep in seen_ready_ep if in_ep and t < warm[0]]
    from_new = [r for r in sent if r['pod'] == new]
    print(f'new={new} warm_at={warm[0]:.1f} ready_at={ready_at:.0f} cold={len(cold)} '
          f'early_endpoint={len(early_ep)} requests_total={len(sent)} served_by_new={len(from_new)}')
    # lastTransitionTime округлён до секунды вниз
    assert ready_at >= int(warm[0]), 'Pod стал Ready раньше конца прогрева'
    assert not early_ep, 'IP Pod был ready в EndpointSlice до конца прогрева'
    assert not cold, f'Pod получил {len(cold)} запросов до конца прогрева'
    assert all(r['outcome'] == 'ok' for r in sent), [r for r in sent if r['outcome'] != 'ok'][:3]
