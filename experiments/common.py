"""Общие функции опытов: kubectl/helm, наблюдатель Ready/EndpointSlice/HPA, запуск клиентских Pod."""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / 'results'
NS = 'llm'
CHART = ROOT / 'charts' / 'llm-sim'
PROM_URL = 'http://127.0.0.1:9090'
SHA = {'A': '2c17c26d29def96e518f0320a8045111d49c562248d376f2899706159a69abc0',
       'B': '17bf9f2e1d96dd8d8d221b5622bfd4ed626cb9e04dcafb87a968b08d2eb5540b'}


def log(*parts):
    print(time.strftime('%H:%M:%S'), *parts, file=sys.stderr, flush=True)


def run(*cmd, check=True, capture=True, timeout=600):
    res = subprocess.run(cmd, capture_output=capture, text=True, encoding='utf-8', errors='replace', timeout=timeout)
    if check and res.returncode != 0:
        raise RuntimeError(f'{" ".join(cmd)} -> {res.returncode}\n{res.stdout}\n{res.stderr}')
    return res.stdout


def kubectl(*args, **kw):
    return run('kubectl', '-n', NS, *args, **kw)


def kjson(*args):
    return json.loads(kubectl(*args, '-o', 'json'))


def helm_upgrade(*values_files, sets=(), wait=False, timeout='5m'):
    cmd = ['helm', 'upgrade', '--install', 'llm', str(CHART), '-n', NS, '--create-namespace']
    for f in values_files:
        cmd += ['-f', str(f)]
    for s in sets:
        cmd += ['--set', s]
    if wait:
        cmd += ['--wait', '--timeout', timeout]
    out = run(*cmd, timeout=900)
    log('helm upgrade', ' '.join(str(v) for v in values_files), ' '.join(sets))
    return out


def wait_rollout(timeout=300):
    kubectl('rollout', 'status', 'deployment/llm-sim', f'--timeout={timeout}s', timeout=timeout + 30)


def snapshot():
    """Один срез: Pod сервиса, EndpointSlice обычного Service, Deployment и HPA."""
    t = time.time()
    pods = kjson('get', 'pods', '-l', 'app=llm-sim')['items']
    slices = kjson('get', 'endpointslices', '-l', 'kubernetes.io/service-name=llm-sim')['items']
    dep = kjson('get', 'deployment', 'llm-sim')
    row = {'t': round(t, 3), 'pods': [], 'endpoints': [],
           'deploy': {k: dep['status'].get(k, 0) for k in ('replicas', 'readyReplicas', 'updatedReplicas',
                                                           'availableReplicas', 'unavailableReplicas')}}
    row['deploy']['generation'] = dep['metadata'].get('generation')
    row['deploy']['spec_replicas'] = dep['spec'].get('replicas')
    for p in pods:
        conds = {c['type']: c['status'] for c in p['status'].get('conditions', [])}
        cs = (p['status'].get('containerStatuses') or [{}])[0]
        row['pods'].append({'name': p['metadata']['name'], 'ip': p['status'].get('podIP'),
                            'version': p['metadata']['labels'].get('model-version'),
                            'phase': p['status'].get('phase'), 'ready': conds.get('Ready') == 'True',
                            'started': cs.get('started'), 'restarts': cs.get('restartCount', 0),
                            'terminating': 'deletionTimestamp' in p['metadata'],
                            'created': p['metadata']['creationTimestamp']})
    for s in slices:
        for e in s.get('endpoints') or []:
            c = e.get('conditions', {})
            row['endpoints'].append({'pod': (e.get('targetRef') or {}).get('name'), 'ip': e['addresses'][0],
                                     'ready': c.get('ready'), 'serving': c.get('serving'),
                                     'terminating': c.get('terminating')})
    try:
        hpa = kjson('get', 'hpa', 'llm-sim')
        st = hpa['status']
        cpu = [m['resource']['current'].get('averageUtilization') for m in st.get('currentMetrics') or []
               if m.get('type') == 'Resource']
        row['hpa'] = {'current': st.get('currentReplicas'), 'desired': st.get('desiredReplicas'),
                      'cpu_utilization': cpu[0] if cpu else None}
    except RuntimeError:
        pass
    return row


class Watcher(threading.Thread):
    """Пишет срезы каждую ~1 с в JSONL: линия Ready/EndpointSlice/HPA по времени."""

    def __init__(self, path, period=1.0):
        super().__init__(daemon=True)
        self.path = Path(path)
        self.period = period
        self.stop_event = threading.Event()

    def run(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open('w', encoding='utf-8') as f:
            while not self.stop_event.is_set():
                started = time.time()
                try:
                    f.write(json.dumps(snapshot(), ensure_ascii=False) + '\n')
                    f.flush()
                except Exception as exc:  # наблюдатель не должен ронять опыт
                    f.write(json.dumps({'t': round(started, 3), 'error': str(exc)[:300]}) + '\n')
                self.stop_event.wait(max(0.0, self.period - (time.time() - started)))

    def stop(self):
        self.stop_event.set()
        self.join(timeout=10)


def loadgen_cmd(pod_index, target, start_at, **opts):
    cmd = ['kubectl', '-n', NS, 'exec', f'loadgen-{pod_index}', '--', 'python', '/app/loadgen.py',
           '--target', target, '--start-at', f'{start_at:.3f}']
    for key, value in opts.items():
        if value is None or value is False:
            continue
        flag = '--' + key.replace('_', '-')
        cmd += [flag] if value is True else [flag, str(value)]
    return cmd


def start_loadgen(out_path, pod_index, target, start_at, **opts):
    """Запускает генератор в клиентском Pod; stdout (JSONL) пишется прямо в файл на хосте."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    f = out_path.open('w', encoding='utf-8')
    err = out_path.with_suffix('.stderr.log').open('w', encoding='utf-8')
    proc = subprocess.Popen(loadgen_cmd(pod_index, target, start_at, **opts), stdout=f, stderr=err)
    proc._files = (f, err)
    return proc


def wait_procs(procs, timeout=None):
    for p in procs:
        p.wait(timeout=timeout)
        for f in p._files:
            f.close()


def pod_names(selector='app=llm-sim'):
    return [p['metadata']['name'] for p in kjson('get', 'pods', '-l', selector)['items']
            if 'deletionTimestamp' not in p['metadata']]


def pod_post(pod, path):
    code = (f"import urllib.request;print(urllib.request.urlopen(urllib.request.Request("
            f"'http://127.0.0.1:8000{path}', method='POST'), timeout=5).read().decode())")
    return kubectl('exec', pod, '--', 'python', '-c', code, check=False)


def save_pod_logs(path, selector='app=llm-sim', since=None):
    """Журналы всех Pod по селектору (включая предыдущий контейнер после перезапуска)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        for pod in kjson('get', 'pods', '-l', selector)['items']:
            name = pod['metadata']['name']
            args = ['logs', name, '--timestamps=false']
            if since:
                args.append(f'--since-time={since}')
            for prev in (True, False):
                out = kubectl(*args, *(['--previous'] if prev else []), check=False)
                for line in out.splitlines():
                    if line.startswith('{'):
                        f.write(line + '\n')


class LogCollector(threading.Thread):
    """Каждые period секунд перечитывает журналы всех Pod: журналы завершённых Pod не теряются."""

    def __init__(self, path, since, selector='app=llm-sim', period=3.0):
        super().__init__(daemon=True)
        self.path, self.since, self.selector, self.period = Path(path), since, selector, period
        self.texts = {}
        self.stop_event = threading.Event()

    def poll(self):
        try:
            names = [p['metadata']['name'] for p in kjson('get', 'pods', '-l', self.selector)['items']]
        except RuntimeError:
            return
        for name in names:
            for prev in (False, True):
                out = kubectl('logs', name, f'--since-time={self.since}', *(['--previous'] if prev else []),
                              check=False, timeout=30)
                key = (name, prev)
                if out and len(out) >= len(self.texts.get(key, '')):
                    self.texts[key] = out

    def run(self):
        while not self.stop_event.is_set():
            self.poll()
            self.stop_event.wait(self.period)

    def stop(self):
        self.stop_event.set()
        self.join(timeout=60)
        self.poll()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open('w', encoding='utf-8') as f:
            for text in self.texts.values():
                for line in text.splitlines():
                    if line.startswith('{'):
                        f.write(line + '\n')


def rfc3339(t):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t))


def prom(path, **params):
    url = f'{PROM_URL}{path}?{urllib.parse.urlencode(params)}'
    return json.load(urllib.request.urlopen(url, timeout=30))


def prom_range(expr, start, end, step=5):
    return prom('/api/v1/query_range', query=expr, start=start, end=end, step=step)['data']['result']


def prom_query(expr, at=None):
    params = {'query': expr}
    if at:
        params['time'] = at
    return prom('/api/v1/query', **params)['data']['result']


def ensure_prom_forward():
    try:
        urllib.request.urlopen(PROM_URL + '/-/ready', timeout=2)
        return None
    except OSError:
        pass
    proc = subprocess.Popen(['kubectl', '-n', 'monitoring', 'port-forward', 'svc/prometheus-server', '9090:80'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(30):
        time.sleep(1)
        try:
            urllib.request.urlopen(PROM_URL + '/-/ready', timeout=2)
            return proc
        except OSError:
            continue
    raise RuntimeError('Prometheus port-forward не поднялся')


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


os.environ.setdefault('PYTHONUTF8', '1')
