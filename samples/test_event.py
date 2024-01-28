import asyncio
from asyncio import Event

import pytest

pytestmark = pytest.mark.asyncio


# TODO реализовать функцию выполнения короутин
async def do_until_event(aw: list[asyncio.Task], event: asyncio.Event, timeout: float = None):
    tasks = [asyncio.create_task(t) for t in aw]
    await asyncio.wait_for(event.wait(), timeout=timeout)
    for t in tasks:
        if not t.done():
            t.cancel()


async def test():
    stop_event = Event()
    stop_event.set()

    async def set_event(event: Event):
        await asyncio.sleep(1)
        event.set()

    async def worker():
        while True:
            await asyncio.sleep(1)

    coros = [*[worker() for i in range(10)], set_event(stop_event)]
    await asyncio.wait_for(
        do_until_event(coros, event=stop_event),
        timeout=1.2,
    )
