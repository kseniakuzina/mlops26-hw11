"""HTTP-сервис ДЗ 11: потоковые ответы имитатора, очередь, пробы и метрики Prometheus.

Продолжение service.py из ДЗ 8+9+10: та же схема «загрузка -> проверка хеша -> прогрев -> готовность»,
ограничение одновременной обработки и очереди с отказом 429, версия в ответе берётся из памяти.

Эндпоинты:
  GET  /livez     процесс и event loop живы (liveness); не зависит от прогрева
  GET  /startupz  артефакт загружен и прогрев завершён (startupProbe в исправленной конфигурации)
  GET  /readyz    прогрет и не завершается (readinessProbe в исправленной конфигурации)
  GET  /version   фактически загруженная версия, хеш, Pod
  GET  /metrics   Prometheus
  POST /v1/chat   {session_id, messages:[{role, content}], max_chunks} -> NDJSON-поток частей
  POST /admin/crash        аварийное завершение процесса (только при FAULT_INJECTION=1)
  POST /admin/reset-cache  очистить имитацию префиксного кеша перед прогоном (FAULT_INJECTION=1)
"""
import asyncio
import json
import os
import sys
import time
import traceback
import uuid

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field, StrictStr, ValidationError
from starlette.responses import Response

from backend import ArtifactMismatch, InputRejected, Settings, SimulatedBackend

POD = os.environ.get('POD_NAME', 'local')
MAX_BODY_BYTES = 256 * 1024

# Границы подобраны под шкалу имитатора: части через 50 мс, prefill до ~2 с, очередь до десятков секунд.
LAT_BUCKETS = (.025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 5, 7.5, 10, 15, 20, 30, 60)
REQUESTS = Counter('llm_requests_total', 'Завершённые запросы по исходу', ['outcome'])
TTFC = Histogram('llm_ttfc_seconds', 'От приёма запроса до первой части ответа (TTFC, сервер)', buckets=LAT_BUCKETS)
DURATION = Histogram('llm_request_duration_seconds', 'От приёма до последней части', buckets=LAT_BUCKETS)
QUEUE_WAIT = Histogram('llm_queue_wait_seconds', 'Ожидание слота обработки', buckets=(0,) + LAT_BUCKETS)
INTER_CHUNK = Histogram('llm_inter_chunk_seconds', 'Интервал между частями (inter-chunk latency)',
                        buckets=(.01, .025, .04, .05, .06, .075, .1, .15, .25, .5, 1))
RUNNING = Gauge('llm_running', 'Запросы в обработке')
WAITING = Gauge('llm_waiting', 'Запросы в очереди')
CACHE = Counter('llm_prefix_cache_total', 'Имитация префиксного кеша', ['result'])
COLD = Counter('llm_cold_requests_total', 'Запросы, пришедшие до завершения прогрева')
READY = Gauge('llm_ready', '1 если прогрет и не завершается')
INFO = Gauge('llm_model_info', 'Фактически загруженная версия', ['version', 'sha256', 'backend_mode'])


def log(event, **fields):
    print(json.dumps({'ts': round(time.time(), 3), 'event': event, 'pod': POD, **fields}, ensure_ascii=False), flush=True)


class Message(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role: StrictStr = Field(pattern='^(user|assistant)$')
    content: StrictStr


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    session_id: StrictStr = Field(min_length=1, max_length=128)
    messages: list[Message] = Field(min_length=1)
    max_chunks: int | None = Field(default=None, ge=1)


class State:
    def __init__(self, settings, max_concurrency, max_queue):
        self.settings = settings
        self.backend = SimulatedBackend(settings)
        self.max_concurrency = max_concurrency
        self.max_queue = max_queue
        self.slots = asyncio.Semaphore(max_concurrency)
        self.running = 0
        self.waiting = 0
        self.load_error = None
        self.draining = False
        self.started = time.time()

    @property
    def ready(self):
        return self.backend.warm and self.load_error is None and not self.draining

    async def boot(self):
        log('loading', version=self.settings.model_version, warmup_seconds=self.settings.warmup_seconds)
        try:
            self.backend.load()
            INFO.labels(self.backend.loaded_version, self.backend.loaded_sha256, 'simulated').set(1)
            log('loaded', version=self.backend.loaded_version, sha256=self.backend.loaded_sha256)
            await self.backend.warmup()
            READY.set(1)
            log('warm', version=self.backend.loaded_version, after_s=round(time.time() - self.started, 2))
        except ArtifactMismatch as exc:
            self.load_error = str(exc)
            log('load_failed', error=self.load_error)
        except Exception as exc:
            traceback.print_exc()
            self.load_error = f'{type(exc).__name__}: {exc}'
            log('load_failed', error=self.load_error)


def error(status, code, request_id, detail=None):
    return JSONResponse({'error': code, 'detail': detail, 'request_id': request_id, 'pod': POD}, status_code=status,
                        headers={'X-Request-ID': request_id, 'X-Pod': POD})


def create_app(state: State):
    async def lifespan(app):
        task = asyncio.create_task(state.boot())
        yield
        task.cancel()

    app = FastAPI(title='llm-sim', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.get('/livez')
    def livez():
        return {'alive': True, 'pod': POD}

    @app.get('/startupz')
    def startupz():
        if state.backend.warm:
            return {'started': True}
        return JSONResponse({'started': False, 'error': state.load_error}, status_code=503)

    @app.get('/readyz')
    def readyz():
        if state.ready:
            return {'ready': True, 'version': state.backend.loaded_version}
        reason = 'load_failed' if state.load_error else 'draining' if state.draining else 'warming_up'
        return JSONResponse({'ready': False, 'reason': reason}, status_code=503)

    @app.get('/version')
    def version():
        b = state.backend
        return {'pod': POD, 'model_version': b.loaded_version, 'model_sha256': b.loaded_sha256,
                'expected_version': state.settings.model_version, 'warm': b.warm, 'backend_mode': 'simulated',
                'load_error': state.load_error, 'prefix_cache_size': len(b.cache)}

    @app.get('/metrics')
    def metrics():
        RUNNING.set(state.running)
        WAITING.set(state.waiting)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post('/admin/crash')
    def crash():
        if os.environ.get('FAULT_INJECTION') != '1':
            return JSONResponse({'error': 'disabled'}, status_code=404)
        log('crash', running=state.running, waiting=state.waiting)
        os._exit(137)

    @app.post('/admin/reset-cache')
    def reset_cache():
        if os.environ.get('FAULT_INJECTION') != '1':
            return JSONResponse({'error': 'disabled'}, status_code=404)
        size = len(state.backend.cache)
        state.backend.cache.clear()
        log('cache_reset', dropped=size)
        return {'pod': POD, 'dropped': size}

    @app.post('/v1/chat')
    async def chat(request: Request):
        received = time.perf_counter()
        request_id = request.headers.get('x-request-id') or str(uuid.uuid4())
        backend = state.backend
        if not backend.warm:
            # Сюда попадают только при пробах, считающих готовым любой поднятый HTTP-процесс.
            COLD.inc()
            REQUESTS.labels('cold').inc()
            log('cold_request', request_id=request_id, load_error=state.load_error,
                uptime_s=round(time.time() - state.started, 2))
            return error(503, 'backend_warming', request_id)
        body = await request.body()
        if len(body) > MAX_BODY_BYTES:
            return error(413, 'body_too_large', request_id)
        try:
            payload = ChatRequest.model_validate_json(body)
        except ValidationError as exc:
            return error(422, 'invalid_request', request_id, [e['msg'] for e in exc.errors()])
        messages = [m.model_dump() for m in payload.messages]
        max_chunks = payload.max_chunks or state.settings.default_max_chunks
        try:
            backend.check_input(messages, max_chunks)
        except InputRejected as exc:
            return error(422, 'invalid_request', request_id, str(exc))
        # Допуск: в обработке + в очереди не больше слотов + длины очереди (waiting растёт сразу при приёме).
        if state.running + state.waiting >= state.max_concurrency + state.max_queue:
            REQUESTS.labels('rejected_queue_full').inc()
            log('rejected', request_id=request_id, session_id=payload.session_id, running=state.running,
                waiting=state.waiting)
            return error(429, 'queue_full', request_id)

        state.waiting += 1
        WAITING.set(state.waiting)
        queued_ahead = state.waiting - 1 + state.running
        log('accepted', request_id=request_id, session_id=payload.session_id, version=backend.loaded_version,
            running=state.running, waiting=state.waiting, turns=len(messages))

        async def body_stream():
            info = {}
            acquired = in_queue = True
            outcome = 'client_aborted'
            chunks = 0
            try:
                await state.slots.acquire()
                state.waiting -= 1
                in_queue = False
                state.running += 1
                RUNNING.set(state.running)
                WAITING.set(state.waiting)
                queue_wait = time.perf_counter() - received
                QUEUE_WAIT.observe(queue_wait)
                info['queue_s'] = round(queue_wait, 4)
                last = None
                async for word in backend.stream(messages, max_chunks, info):
                    now = time.perf_counter()
                    if last is None:
                        TTFC.observe(now - received)
                        CACHE.labels(info['cache']).inc()
                    else:
                        INTER_CHUNK.observe(now - last)
                    last = now
                    yield json.dumps({'type': 'chunk', 'i': chunks, 'text': word}, ensure_ascii=False) + '\n'
                    chunks += 1
                outcome = 'ok'
                total = time.perf_counter() - received
                DURATION.observe(total)
                yield json.dumps({'type': 'done', 'request_id': request_id, 'pod': POD,
                                  'model_version': backend.loaded_version, 'model_sha256': backend.loaded_sha256,
                                  'backend_mode': 'simulated', 'session_id': payload.session_id, 'chunks': chunks,
                                  'queued_ahead': queued_ahead, 'server_total_s': round(total, 4), **info},
                                 ensure_ascii=False) + '\n'
            except asyncio.CancelledError:
                outcome = 'client_aborted'
                raise
            except Exception as exc:
                outcome = 'error'
                log('stream_error', request_id=request_id, error=f'{type(exc).__name__}: {exc}')
                raise
            finally:
                if in_queue:
                    state.waiting -= 1
                elif acquired:
                    state.running -= 1
                    state.slots.release()
                RUNNING.set(state.running)
                WAITING.set(state.waiting)
                REQUESTS.labels(outcome).inc()
                log('finished', request_id=request_id, session_id=payload.session_id, outcome=outcome,
                    chunks=chunks, version=backend.loaded_version, total_s=round(time.perf_counter() - received, 4),
                    **info)

        return StreamingResponse(body_stream(), media_type='application/x-ndjson', headers={
            'X-Request-ID': request_id, 'X-Pod': POD, 'X-Model-Version': backend.loaded_version or '',
            'X-Backend-Mode': 'simulated', 'Cache-Control': 'no-cache'})

    return app


class DrainingServer(uvicorn.Server):
    """SIGTERM: сразу перестать быть готовым, затем штатная остановка uvicorn (ждёт активные потоки)."""

    def __init__(self, config, state):
        super().__init__(config)
        self.state = state

    def handle_exit(self, sig, frame):
        if not self.state.draining:
            self.state.draining = True
            READY.set(0)
            log('sigterm', signal=int(sig), running=self.state.running, waiting=self.state.waiting)
        super().handle_exit(sig, frame)


def main():
    env = os.environ
    settings = Settings.from_env(env)
    state = State(settings, int(env.get('MAX_CONCURRENCY', 2)), int(env.get('MAX_QUEUE', 16)))
    log('config', settings=settings.__dict__, max_concurrency=state.max_concurrency, max_queue=state.max_queue,
        python=sys.version.split()[0])
    config = uvicorn.Config(create_app(state), host='0.0.0.0', port=int(env.get('PORT', 8000)), log_level='warning',
                            access_log=False, timeout_graceful_shutdown=int(env.get('GRACEFUL_SHUTDOWN_SECONDS', 40)),
                            timeout_keep_alive=int(env.get('KEEPALIVE_SECONDS', 5)))
    DrainingServer(config, state).run()


if __name__ == '__main__':
    main()
