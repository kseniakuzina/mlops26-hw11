"""Управляемый CPU-backend (backend_mode=simulated).

Интерфейс продолжает backends.py из ДЗ 8+9+10: load() / warmup() / check_input() плюс потоковый
stream() вместо generate(). Настоящей модели нет: время работы задаётся параметрами.

Модель нагрузки имитатора:
  * prefill: стоимость пропорциональна числу символов, которых нет в локальном префиксном кеше
    (PREFILL_MS_PER_KCHAR за 1000 символов) + CACHE_HIT_MS при попадании;
  * decode: max_chunks частей, между частями CHUNK_INTERVAL_MS;
  * WORK_MODE=sleep — время «тратится» ожиданием (детерминированная очередь, опыты A и B);
    WORK_MODE=cpu — то же время тратится занятым циклом на CPU (опыт C, HPA видит нагрузку).
Префиксный кеш — явно обозначенная имитация: LRU из PREFIX_CACHE_ENTRIES хешей префиксов
по границам сообщений. Он локален для процесса и пропадает при замене Pod.
"""
import asyncio
import hashlib
import json
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

CONTROL = re.compile(r'\b[A-Z]{3,}-\d{4}\b')
MAX_PROMPT_CHARS = 64000


class InputRejected(ValueError):
    pass


class ArtifactMismatch(RuntimeError):
    pass


@dataclass
class Settings:
    model_dir: str = '/models'
    model_version: str = 'A'
    model_sha256: str = ''
    warmup_seconds: float = 20.0
    work_mode: str = 'sleep'
    prefill_ms_per_kchar: float = 100.0
    cache_hit_ms: float = 20.0
    prefix_cache_entries: int = 128
    chunk_interval_ms: float = 50.0
    cpu_ms_per_chunk: float = 0.0
    default_max_chunks: int = 16
    max_chunks_limit: int = 600

    @classmethod
    def from_env(cls, env):
        out = cls()
        for name, field in cls.__dataclass_fields__.items():
            raw = env.get(name.upper())
            if raw is not None and raw != '':
                setattr(out, name, type(getattr(out, name))(raw))
        return out


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _prefix_key(messages, k):
    data = json.dumps(messages[:k], ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(data).hexdigest()


async def burn(seconds, mode):
    """Потратить seconds: ожиданием или занятым циклом с уступкой event loop каждые 5 мс."""
    if seconds <= 0:
        return
    if mode != 'cpu':
        await asyncio.sleep(seconds)
        return
    deadline = time.perf_counter() + seconds
    while True:
        slice_end = min(deadline, time.perf_counter() + 0.005)
        x = 0
        while time.perf_counter() < slice_end:
            x += 1
        if time.perf_counter() >= deadline:
            return
        await asyncio.sleep(0)


class PrefixCache:
    """LRU хешей префиксов: при попадании пересчитываются только символы после префикса."""

    def __init__(self, entries):
        self.entries = entries
        self._lru = OrderedDict()

    def lookup(self, messages):
        for k in range(len(messages) - 1, 0, -1):
            key = _prefix_key(messages, k)
            if key in self._lru:
                self._lru.move_to_end(key)
                return k
        return 0

    def insert(self, messages):
        # Хранится только полный префикс запроса: следующий ход диалога продолжает именно его.
        key = _prefix_key(messages, len(messages))
        self._lru[key] = True
        self._lru.move_to_end(key)
        while len(self._lru) > self.entries:
            self._lru.popitem(last=False)

    def clear(self):
        self._lru.clear()

    def __len__(self):
        return len(self._lru)


class SimulatedBackend:
    kind = 'simulated'

    def __init__(self, settings: Settings):
        self.s = settings
        self.manifest = None
        self.loaded_version = None
        self.loaded_sha256 = None
        self.cache = PrefixCache(settings.prefix_cache_entries)
        self.warm = False

    def load(self):
        """Загрузить артефакт и сверить хеш с ожидаемым из Helm values. Несовпадение — отказ."""
        path = Path(self.s.model_dir) / f'sim-{self.s.model_version}.json'
        if not path.exists():
            raise ArtifactMismatch(f'нет артефакта {path}')
        actual = sha256_file(path)
        if self.s.model_sha256 and actual != self.s.model_sha256:
            raise ArtifactMismatch(f'sha256 {path.name}: ожидался {self.s.model_sha256[:12]}, фактически {actual[:12]}')
        manifest = json.loads(path.read_text(encoding='utf-8'))
        if manifest.get('version') != self.s.model_version:
            raise ArtifactMismatch(f'в артефакте версия {manifest.get("version")}, ожидалась {self.s.model_version}')
        self.manifest = manifest
        self.loaded_version = manifest['version']
        self.loaded_sha256 = actual

    async def warmup(self):
        """Прогрев: заполнение «весов», компиляция и т. п. Здесь — управляемая пауза."""
        await burn(self.s.warmup_seconds, 'sleep')
        self.warm = True

    def check_input(self, messages, max_chunks):
        if not messages or messages[-1].get('role') != 'user':
            raise InputRejected('последнее сообщение должно быть от user')
        total = sum(len(m.get('content', '')) for m in messages)
        if total > MAX_PROMPT_CHARS:
            raise InputRejected(f'история длиннее {MAX_PROMPT_CHARS} символов')
        if max_chunks > self.s.max_chunks_limit:
            raise InputRejected(f'max_chunks > {self.s.max_chunks_limit}')

    def plan(self, messages):
        """Стоимость prefill с учётом кеша. Возвращает (cache, prefill_seconds, cached_chars)."""
        full = [{'role': 'system', 'content': self.manifest['system_prompt']}, *messages]
        k = self.cache.lookup(full)
        cached_chars = sum(len(m['content']) for m in full[:k])
        fresh_chars = sum(len(m['content']) for m in full[k:])
        seconds = fresh_chars / 1000 * self.s.prefill_ms_per_kchar / 1000
        if k:
            seconds += self.s.cache_hit_ms / 1000
        return ('hit' if k else 'miss'), seconds, cached_chars, full

    def answer_words(self, messages, max_chunks):
        """Детерминированный ответ: версия, контрольные строки этого диалога, слова словаря."""
        codes = []
        for m in messages:
            for code in CONTROL.findall(m.get('content', '')):
                if code not in codes:
                    codes.append(code)
        seed = int(hashlib.sha256(messages[-1]['content'].encode('utf-8')).hexdigest()[:8], 16)
        vocab = self.manifest['vocab']
        words = [f'[{self.loaded_version}]']
        if codes:
            words.append('коды:' + ','.join(codes))
        while len(words) < max_chunks:
            words.append(vocab[(seed + len(words)) % len(vocab)])
        return words[:max_chunks]

    async def stream(self, messages, max_chunks, info):
        cache, prefill, cached_chars, full = self.plan(messages)
        info.update(cache=cache, prefill_s=round(prefill, 4), cached_chars=cached_chars)
        await burn(prefill, self.s.work_mode)
        self.cache.insert(full)
        for i, word in enumerate(self.answer_words(messages, max_chunks)):
            if i:
                await asyncio.sleep(self.s.chunk_interval_ms / 1000)
            await burn(self.s.cpu_ms_per_chunk / 1000, self.s.work_mode)
            yield word
