import asyncio
import time
import concurrent.futures

executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

q = asyncio.Queue(maxsize=100)
var = 0
@asyncio.coroutine
def gen(q):
    for i in range(1000):
        yield from q.put(i)
    return

@asyncio.coroutine
def user(q,gen_fu,loop):
    
    while True:
        if gen_fu.done() and q.empty():
             return 
        else:
            data = yield from  q.get()
            print(data)
            yield from loop.run_in_executor(None,time.sleep, 0.1)

loop = asyncio.get_event_loop()    
loop.set_default_executor(executor)

gen_fu = loop.create_task(gen(q))
ad = [loop.create_task(user(q,gen_fu,loop)) for i in range(20)]
user_fus = asyncio.wait(ad,return_when=asyncio.FIRST_COMPLETED)

done,depending = loop.run_until_complete(user_fus)
for i in depending:
    i.cancel()
    print(i.cancelled())
print(len(depending))
#loop.create_task(cl(loop,depending))
loop.run_until_complete(asyncio.wait(depending))
time.sleep(0.1)
loop.close()

for i in depending:
    print(i.cancelled())
executor.shutdown()
