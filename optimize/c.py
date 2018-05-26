import pymysql
import time
conn = pymysql.connect(user="root", passwd="root",db="t")
sql = "insert into tb1 values(%s,%s,%s)"

def query_data():
    for a in range(10):
        yield  (("Conn_Pool_Test", i, "C%s" % i) for i in range(10000))
#        yield (sql%("Conn_pool_Test", i , "C%s"%i) for i in range(10000))
total = 0
start = time.time()

for i in query_data():
    s = time.time()
    with conn.cursor() as cur:    
        cur.executemany(sql,i)
    conn.commit()
    print(time.time()-s)
#        a = map(lambda a: a, i)
#        cur.executemany(sql, a)
#        for c in i:
#            cur.execute(sql, c)
rt = time.time()
print(rt-start)

#conn.commit()  
#print(time.time()-rt) 


import itertools
def h(row):
    a = handle_row(row, table_info)
    if isinstance(result, tuple):
        pass
    else:
        return 

async def gen_sql(rows,table_info):
    rows = filter(None, map(h, rows))
    while True:
        data = itertools.islice(rows, 10000)
        if data:
            break
        await loop.run_in_executor(None, conn.exec_sql_many, data)


fus = [gen_sql(rows, table_info,) for i in range(2)]



