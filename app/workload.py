"""Детерминированная нагрузка: одинаковые запросы, длины и расписание для сравниваемых вариантов.

Расписание строится из (profile, rate, warmup, duration, seed) одинаково в каждом клиентском Pod
и на хосте, поэтому файл расписания не нужно копировать в кластер — его можно пересчитать и сохранить.

Профили трафика:
  uniform — каждый из 4 клиентов отправляет 25 % запросов;
  skewed  — клиент 0 отправляет 80 %, остальные по ~6,7 %;
  ramp    — опыт C: интенсивность меняется по фазам (см. RAMP), клиенты равномерно.
У каждого клиента свои SESSIONS_PER_CLIENT диалогов. Запрос продолжает диалог: история растёт по ходу.
"""
import hashlib
import random

CLIENTS = 4
SESSIONS_PER_CLIENT = 12
DOC_CHARS = (4000, 12000)        # длина «документа» в первом сообщении диалога
TURN_CHARS = 180                 # длина следующих вопросов пользователя
ANSWER_CHARS = 150               # длина ответа ассистента в истории
MAX_CHUNKS = 20
CLIENT_WEIGHTS = {'uniform': [.25, .25, .25, .25], 'skewed': [.8, .2 / 3, .2 / 3, .2 / 3]}
# Опыт C: (длительность фазы, rps) — низкая, высокая, снова низкая нагрузка.
RAMP = [(60, 1.0), (240, 6.0), (300, 0.5)]


def _text(seed, n):
    words = ['журнал', 'заказ', 'оплата', 'доставка', 'договор', 'сервер', 'ошибка', 'отчёт', 'склад', 'клиент',
             'счёт', 'пароль', 'модель', 'релиз', 'метрика', 'очередь']
    rnd = random.Random(seed)
    out, size = [], 0
    while size < n:
        w = rnd.choice(words)
        out.append(w)
        size += len(w) + 1
    return ' '.join(out)[:n]


def session_id(client, k):
    return f'c{client}-s{k:02d}'


def control_code(sid):
    return 'CTL-' + str(int(hashlib.sha256(sid.encode()).hexdigest()[:8], 16) % 10000).zfill(4)


def doc_chars(sid):
    lo, hi = DOC_CHARS
    return lo + int(hashlib.sha256(('doc' + sid).encode()).hexdigest()[:8], 16) % (hi - lo)


def build_messages(sid, turn):
    """История для хода turn (0, 1, ...): одинаковый префикс у последовательных ходов одного диалога."""
    msgs = [{'role': 'user', 'content': f'Документ {sid} [{control_code(sid)}]: ' + _text(sid, doc_chars(sid))}]
    for t in range(1, turn + 1):
        msgs.append({'role': 'assistant', 'content': _text(f'{sid}/a{t}', ANSWER_CHARS)})
        msgs.append({'role': 'user', 'content': f'Вопрос {t}: ' + _text(f'{sid}/q{t}', TURN_CHARS)})
    return msgs


def _arrivals(rnd, rate, start, end):
    t = start
    while True:
        t += rnd.expovariate(rate)
        if t >= end:
            return
        yield t


def schedule(profile, rate, duration, warmup, seed):
    """Список запросов: req_id, client, session_id, turn, scheduled_s, phase, max_chunks, prompt_chars."""
    rnd = random.Random(f'{profile}:{rate}:{duration}:{warmup}:{seed}')
    if profile == 'ramp':
        times, t0 = [], 0.0
        for length, phase_rate in RAMP:
            times += list(_arrivals(rnd, phase_rate, t0, t0 + length))
            t0 += length
        weights = CLIENT_WEIGHTS['uniform']
    else:
        times = list(_arrivals(rnd, rate, 0.0, warmup + duration))
        weights = CLIENT_WEIGHTS[profile]
    turns = {}
    rows = []
    for i, t in enumerate(times):
        client = rnd.choices(range(CLIENTS), weights)[0]
        sid = session_id(client, rnd.randrange(SESSIONS_PER_CLIENT))
        turn = turns.get(sid, 0)
        turns[sid] = turn + 1
        rows.append({'req_id': f'{profile[:1]}{seed}-{i:05d}', 'client': client, 'session_id': sid, 'turn': turn,
                     'scheduled_s': round(t, 4),
                     'phase': 'warmup' if profile != 'ramp' and t < warmup else 'measure',
                     'max_chunks': MAX_CHUNKS,
                     'prompt_chars': sum(len(m['content']) for m in build_messages(sid, turn))})
    return rows
