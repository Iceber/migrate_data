import time
import asyncio
import datetime
import leancloud
from concurrent.futures import ThreadPoolExecutor

from libs.tables_info import get_tables_info
from libs.handle_rows import handle_row,trans_row

result_count = 1000
tables = get_tables_info()

def gen_inserted_sql(table_name, columns):
    values = map(lambda name: "%({0})s".format(name),columns)
    return "INSERT INTO {0}({1}) VALUES ({2})".format(table_name,','.join(columns),','.join(values))


def gen_updated_sql(table_name, columns):
    update_data = ('='.join([name,'%{name}s']) for name in columns)
    return "UPDATE {0} SET {1} WHERE object_id=%('object_id')s".format(table_name,','.join(update_data))

def query_data(table_name, table_poll_info, loop ):

    skip = 0
    now = datetime.datetime.now()

    Table = leancloud.Object.extend(table_name)
    query1 = Table.query
    query2 = Table.query
    query1.greater_than_or_equal_to('updatedAt',  table_poll_info[0])
    query2.less_than('updatedAt', now)
    table_poll_info['last_time'] = now

    query = leancloud.Query.and_(query1, query2)

    def _get_result(skip,loop):
        query.skip(result_count * skip)
        query.limit(result_count)
        result = loop.run_in_executor(None, query.find)
        yield result
        if len(result) == result_count:
            table_poll_info['time_interval'] /= 2
            skip += 1
            yield from _get_result(query,skip)
        elif skip == 0  and len(result) < result_count / 2 : 
            table_poll_info['time_interval'] *= 2

    return _get_result(skip,loop)

async def gen_data(table_name, table_info, table_poll_info, q, loop, fault_fp):
    for result in query_data(table_name, table_poll_info, loop):
        await q.join()
        for row in result:
            row = row._attributes
            data = handle_row(row,table_info)
            if isinstance(data, tuple):
                fault_info = ''.join(['[Handle data fault]\n',data[0],'\n',row,'\n\n'])
                fault_fp.write(fault_info)
            else:
                q.put_nowait(trans_row(data))


def compile_data(table_name, data, mysql_data):
    update_data = {name: value for name,value in data.items() if mysql_data[name]!= value}
    sql = gen_updated_sql(table_name, update_data.keys())
    update_data['object_id'] = data['object_id']
    return sql, update_data

async def handle_data(table_name, q, conn, loop, fault_fp):
    while True:
        #
        data = await q.pop()
        extra,mysql_data = conn.select_sql('select * from {0} where object_id={1}'.format(table_name,data["object_id"]))
        sql = gen_inserted_sql(table_name,data.keys())
        
        if extra:
            sql,data = compile_data(table_name, data, mysql_data)
        try:
            await loop.run_in_executor(None, conn.exec_sql,sql, data)
        except Exception:
            fault_info = ''.join(['[Error] insert or update data fault\n','object_id: ',data['object_id'],'\n\n'])
            fault_fp.write(fault_info)
        finally:
            q.task_done()


def every_table(table_name, table_info, table_poll_info, conn):
    q = asyncio.Queue(maxsize=1000)
    executor = ThreadPoolExecutor(max_workers=5)
    loop = asyncio.new_event_loop()
    loop.set_default_executor(executor)
    with open(table_name, 'w') as fault_fp:
        loop.create_task(gen_data(table_name,table_info, table_poll_info, q, loop,fault_fp))
        handle_data_fus = [handle_data(table_name, q, conn, loop, fault_fp) for i in range(10)]
        loop.run_until_complete(asyncio.wait(handle_data_fus))
    loop.close()

    return 