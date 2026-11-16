"""Проверка 5: два параллельных диалога с различимыми контрольными строками не смешиваются.

Запросы чередуются и идут через Service в Kubernetes (оба Pod, общий префиксный кеш каждого Pod).
"""
from conftest import chat

A, B = 'ALPHA-1111', 'BRAVO-2222'


def test_two_dialogs_do_not_leak(cluster):
    hist = {A: [{'role': 'user', 'content': f'Диалог 1, код {A}.'}],
            B: [{'role': 'user', 'content': f'Диалог 2, код {B}.'}]}
    seen = {A: [], B: []}
    for turn in range(3):
        for code, other in ((A, B), (B, A)):
            row = chat(f'iso-{code}', hist[code], max_chunks=6)[0]
            assert row['outcome'] == 'ok', row
            seen[code].append((row['pod'], row['text']))
            assert code in row['text'], f'в ответе диалога {code} нет его кода: {row["text"]}'
            assert other not in row['text'], f'утечка {other} в диалог {code}: {row["text"]}'
            hist[code] += [{'role': 'assistant', 'content': row['text']},
                           {'role': 'user', 'content': f'Ход {turn + 1}: повтори код.'}]
    print({k: [p for p, _ in v] for k, v in seen.items()})
