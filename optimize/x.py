import asyncio
import itertools
loop = asyncio.get_event_loop()

it = (i for i in range(100))

def a(it):
    while True:
        c = itertools.islice(it, 2)
        if not c:
            return 
        for i in c:
                print(i)

f0 = loop.run_in_executor(None, a,it)
f1 = loop.run_in_executor(None, a, it)

loop.run_until_complete(asyncio.wait([f0,f1]))

