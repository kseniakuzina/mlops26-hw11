"""Проверка 3 (Kubernetes): после замены Pod сервис восстанавливается с той же версией,
а диалог продолжается — история передаётся в запросе, а не хранится в Pod.
"""
from conftest import chat, kubectl, ready_pods, wait_ready

CODE = 'RECOV-4242'


def test_dialog_continues_after_pod_replacement(cluster):
    history = [{'role': 'user', 'content': f'Запомни код {CODE}. Первый вопрос.'}]
    first = chat('recovery-dialog', history, max_chunks=6)[0]
    assert first['outcome'] == 'ok'
    served_by, version = first['pod'], first['model_version']
    kubectl('delete', 'pod', served_by, '--wait=true', timeout=180)
    wait_ready()
    assert served_by not in {p['metadata']['name'] for p in ready_pods()}
    history += [{'role': 'assistant', 'content': first['text']},
                {'role': 'user', 'content': 'Второй вопрос: какой код я назвал?'}]
    rows = chat('recovery-dialog', history, repeat=4, max_chunks=6)
    print(f'first pod={served_by} version={version}; after: {[(r["pod"], r["model_version"]) for r in rows]}')
    assert all(r['outcome'] == 'ok' for r in rows)
    assert all(r['model_version'] == version for r in rows), 'после замены Pod версия изменилась'
    assert all(CODE in r['text'] for r in rows), 'продолжение диалога потеряло контекст'
    assert served_by not in {r['pod'] for r in rows}
