import asyncio
import time
async def a():
    await asyncio.sleep(0.1)
    print('a')

async def b():
    time.sleep(3)
    await asyncio.sleep(2)
    print('b')

async def c():
    await asyncio.sleep(1)
    print('c')

loop = asyncio.get_event_loop()
loop.create_task(a())
loop.create_task(b())
loop.create_task(c())
loop.run_forever()
