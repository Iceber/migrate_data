import asyncio
import sys
from sync_tables_poll import TablesPoll
from tables_info import get_tables_info
from models.conn_poll import Conn_Pool
from functools import partial

tables_info = get_tables_info()


def main(args):
    loop = asyncio.get_event_loop()

    for table_name in args.table:
        loop.create_task(table_poll(table_name))
    try:
        loop.run_forever()
    except:
        TablesPoll.storage_tables_poll_info()
        return None

async def table_poll(table_name):
    conn = Conn_Poll.get()
    table_info = tables_info[talbe_name]
    table_poll = TablesPoll.get_table_poll(table_name)

    filter_data = partial(filter_data, table_info = table_info)
    data_sync = partial(data_sync, table_name=table_poll.table_name, conn)

    while True:
        try:
            table_sync(filter_data, data_sync)
        except Exception:
            table_poll.sync_failed()
        else:
            table_poll.sync_successed()

        await asyncio.sleep(table_poll.time_interval)



def table_sync(filter_data, data_sync):

    for result in table_poll.get_query_result():
        for data in filter(None, map(filter_data, result)):
            data_sync(trans_lean_to_sql(data))




def compile_data(table_name, data, mysql_data):
    update_data = {name: value for name, value in data.items() if mysql_data[name] != value}
    sql = gen_update_sql(table_name, update_data.keys())
    update_date['object_id'] = data['object_id']
    return sql, update_data


    
def data_sync(data, table_name, conn, fault_fp):
    extra,mysql_data = conn.select_sql()
    
    if extra:
        if mysql_data['updated_at'] == data['updated_at']:
            return None
        sql, data = compile_data(table_name, data, mysql_data)
    else:
        sql = gen_inserted_sql(table_name, data.keys())
    try:
        cur.execute(sql, data)
    except Exception:
        pass

def filter_data(data, table_info,fault_func=None, fault_fp = None):
    data = handle_row(row, table_info)
    if isinstance(data, tuple):
        if fault_func:
            fault_func(data[0])
        if fault_fp:
            return None
        else:
            fault_info = ''
            fault_fp.write(fault_info)
    else:
        return data
