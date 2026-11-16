"""Собственные проверки: красный прогон на исходном дефекте проб и зелёный после исправления, затем все 5.

python experiments/run_checks.py   -> results/tests/{red-probes-http,green-probes-warm,all}.log
"""
import json
import subprocess
import sys
import time

from common import NS, RESULTS, ROOT, SHA, helm_upgrade, log, run, wait_rollout

VALUES = ROOT / 'experiments' / 'values-exp-a.yaml'
OUT = RESULTS / 'tests'


def pytest(name, *args):
    OUT.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, '-m', 'pytest', '-v', '-s', '-p', 'no:cacheprovider', *args]
    t0 = time.strftime('%Y-%m-%dT%H:%M:%S')
    res = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding='utf-8', errors='replace')
    text = f'# {t0}  {" ".join(cmd[2:])}\n# exit code {res.returncode}\n{res.stdout}\n{res.stderr}'
    (OUT / f'{name}.log').write_text(text, encoding='utf-8')
    log(name, 'exit', res.returncode, res.stdout.strip().splitlines()[-1] if res.stdout.strip() else '')
    return res.returncode


def main():
    base = ['model.version=A', f'model.sha256={SHA["A"]}']
    helm_upgrade(VALUES, sets=['probes.mode=http', *base])
    wait_rollout()
    time.sleep(25)  # Pod уже «Ready», но прогреваются: ждём конца прогрева перед проверкой
    red = pytest('red-probes-http', 'tests/test_k8s_warmup.py')
    helm_upgrade(VALUES, sets=['probes.mode=warm', *base])
    wait_rollout()
    green = pytest('green-probes-warm', 'tests/test_k8s_warmup.py')
    # Версия после отката: выпустить B, дождаться, откатить helm rollback на ревизию с A — и только потом все проверки.
    rev_a = json.loads(run('helm', 'history', 'llm', '-n', NS, '-o', 'json'))[-1]['revision']
    helm_upgrade(VALUES, sets=['probes.mode=warm', 'model.version=B', f'model.sha256={SHA["B"]}'])
    wait_rollout()
    run('helm', 'rollback', 'llm', str(rev_a), '-n', NS, '--wait', '--timeout', '5m')
    wait_rollout()
    (OUT / 'helm-history-before-checks.json').write_text(run('helm', 'history', 'llm', '-n', NS, '-o', 'json'),
                                                        encoding='utf-8')
    total = pytest('all', 'tests')
    log('red (ожидается 1):', red, 'green (ожидается 0):', green, 'all:', total)


if __name__ == '__main__':
    main()
