"""Один потоковый запрос к сервису; печатает итог JSON. Используется проверками внутри кластера.

python /app/chatclient.py --target http://llm-sim --session s1 --messages '[{"role":"user","content":"..."}]'
"""
import argparse
import asyncio
import json
import types

import httpx

from loadgen import one_attempt


async def main_async(args):
    item = {'req_id': args.request_id, 'client': 0, 'session_id': args.session, 'turn': 0, 'phase': 'check',
            'max_chunks': args.max_chunks, 'prompt_chars': 0, 'scheduled_s': 0}
    opts = types.SimpleNamespace(target=args.target, policy='check', profile='check', run='check', keepalive=0,
                                 timeout=args.timeout, keep_text=True)
    messages = json.loads(args.messages)
    import loadgen
    loadgen.build_messages = lambda sid, turn: messages
    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=0)) as client:
        rows = []
        for i in range(args.repeat):
            item['req_id'] = f'{args.request_id}-{i}'
            rows.append(await one_attempt(client, opts, item, 1, 0.0))
    print(json.dumps(rows, ensure_ascii=False))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--target', default='http://llm-sim.llm.svc.cluster.local')
    p.add_argument('--session', required=True)
    p.add_argument('--messages', required=True)
    p.add_argument('--max-chunks', type=int, default=8)
    p.add_argument('--timeout', type=float, default=30)
    p.add_argument('--repeat', type=int, default=1)
    p.add_argument('--request-id', default='check')
    asyncio.run(main_async(p.parse_args()))


if __name__ == '__main__':
    main()
