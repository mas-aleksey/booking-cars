import asyncio
import pytest


pytestmark = pytest.mark.asyncio

counter = 0

# TODO используя Semaphore избежать длительного сна
async def do_request():
    global counter

    counter += 1
    if counter > 5:
        await asyncio.sleep(10)
    await asyncio.sleep(0.1)
    counter -= 1


async def safe_request(sem):
    async with sem:
        return await do_request()


async def test():
    sem = asyncio.Semaphore(5)
    await asyncio.wait_for(
        asyncio.gather(*[safe_request(sem) for _ in range(10)]),
        timeout=1.2,
    )

