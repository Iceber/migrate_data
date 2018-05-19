import asyncio
from concurrent.futures import ThreadPoolExecutor
from models.db_conn_pool import Conn_pool
conn = Conn_pool(user="root",passwd="root", db='t',maxconn=10)
executor = ThreadPoolExecutor(max_workers=10)

q = asyncio.Queue(maxsize=1000)

sql = "insert into tb1(title,count,st) values(%s,%s,%s)"
def query_data():
    for a in range(10):
        yield (("Conn_Pool_Test", i, "C%s" % i) for i in range(1000))


async def gen_data( q, loop,):
    for result in query_data():
        await q.join()
        for row in result:
            q.put_nowait(row)

total = 0
async def handle_data(q, conn, loop, gen_fu):
    global total
    while True:
        total += 1
        if gen_fu.done() and q.empty():
            return
        data = await q.get()
        await loop.run_in_executor(None, conn.exec_sql,sql, data)
        q.task_done()
        print(total)

loop = asyncio.get_event_loop()
loop.set_default_executor(executor)

gen_fu = loop.create_task(gen_data(q, loop))
handle_fus = [ handle_data(q, conn, loop, gen_fu) for i in range(10)]
loop.run_until_complete(asyncio.wait(handle_fus,return_when=asyncio.FIRST_COMPLETED))

