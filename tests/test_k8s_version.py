"""Проверка 2 (Kubernetes): фактически загруженная версия совпадает с выпущенной (в т. ч. после отката).

Ожидание берётся из текущей ревизии Helm (после `helm rollback` это значения целевой ревизии),
хеш — из models/SHA256SUMS. Ответы через Service и /version каждого Pod должны совпасть с ними.
"""
import json
import subprocess

from conftest import ROOT, NS, chat, kubectl, ready_pods


def helm_state():
    values = json.loads(subprocess.run(['helm', 'get', 'values', 'llm', '-n', NS, '-a', '-o', 'json'],
                                       capture_output=True, text=True, check=True).stdout)
    history = json.loads(subprocess.run(['helm', 'history', 'llm', '-n', NS, '-o', 'json'],
                                        capture_output=True, text=True, check=True).stdout)
    return values['model'], history[-1]


def test_served_version_matches_release(cluster):
    model, revision = helm_state()
    sums = dict(reversed(line.split()) for line in (ROOT / 'models' / 'SHA256SUMS').read_text().splitlines())
    expected_sha = sums[f'sim-{model["version"]}.json']
    assert model['sha256'] == expected_sha, 'в values хеш не той версии'
    rows = chat('version-check', [{'role': 'user', 'content': 'какая версия?'}], repeat=12, max_chunks=2)
    print(f'revision={revision["revision"]} ({revision["description"]}) expected={model["version"]} '
          f'served={sorted({(r["pod"], r["model_version"]) for r in rows})}')
    assert all(r['outcome'] == 'ok' for r in rows)
    assert {r['model_version'] for r in rows} == {model['version']}
    assert {r['model_sha256'] for r in rows} == {expected_sha}
    assert all(r['first_text'] == f'[{model["version"]}]' for r in rows)
    for pod in ready_pods():
        name = pod['metadata']['name']
        info = json.loads(kubectl('exec', name, '--', 'python', '-c',
                                  'import urllib.request;print(urllib.request.urlopen('
                                  '"http://127.0.0.1:8000/version").read().decode())'))
        assert info['model_version'] == model['version'] and info['model_sha256'] == expected_sha, info
