import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / 'app'), str(ROOT / 'analysis')]
NS = 'llm'


def kubectl(*args, check=True, timeout=120):
    res = subprocess.run(['kubectl', '-n', NS, *args], capture_output=True, text=True, encoding='utf-8',
                         errors='replace', timeout=timeout)
    if check and res.returncode != 0:
        raise RuntimeError(f'kubectl {" ".join(args)}: {res.stderr}')
    return res.stdout


def kjson(*args):
    return json.loads(kubectl(*args, '-o', 'json'))


def chat(session, messages, target='http://llm-sim.llm.svc.cluster.local', repeat=1, max_chunks=8):
    """Запрос через Service изнутри кластера (клиентский Pod loadgen-0)."""
    out = kubectl('exec', 'loadgen-0', '--', 'python', '/app/chatclient.py', '--target', target, '--session', session,
                  '--messages', json.dumps(messages, ensure_ascii=False), '--repeat', str(repeat),
                  '--max-chunks', str(max_chunks), '--request-id', f'test-{session}-{int(time.time())}')
    return json.loads(out)


def ready_pods():
    return [p for p in kjson('get', 'pods', '-l', 'app=llm-sim')['items']
            if 'deletionTimestamp' not in p['metadata']]


def wait_ready(n=None, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        dep = kjson('get', 'deployment', 'llm-sim')
        want = n or dep['spec']['replicas']
        if dep['status'].get('readyReplicas') == want and dep['status'].get('updatedReplicas') == want \
                and dep['status'].get('replicas') == want:
            return
        time.sleep(2)
    raise TimeoutError('Deployment не вернулся к готовности')


@pytest.fixture(scope='session')
def cluster():
    try:
        kubectl('get', 'deployment', 'llm-sim')
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f'кластер недоступен: {exc}')
    wait_ready()
    return True
