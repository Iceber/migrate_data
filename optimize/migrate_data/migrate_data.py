import sys
import os.path
import asyncio
import pprint
from concurrent.futures import ThreadPoolExecutor

sys.path.append("../")
from models.db_conn_pool import Conn_pool
from libs.handle_rows import handle_row
from libs.myjson import ijson



conn = Conn_pool.get_pool(user="root", passwd="root", db="test", maxconn=10, charset="utf8")
executor = ThreadPoolExecutor(max_workers=1)
q = asyncio.Queue(maxsize=10000)

def handle_fault_data(fault_row, fault_columns):
    for k, v in fault_row.items():
        if v:
            fault_columns[k] |= v


def _print_fault_type(fault_columns, table_info):
    a = set()
    for v in fault_columns.values():
        a |= v
    if not a:
        print("good table")
    else:
        print("*" * 5, table_info["name"])
        pprint.pprint(fault_columns)
        for name in a:
            print(name, ": ", table_info["columns"].get(name, {}).get("type"))


def get_insert_query(table_info):
    columns = table_info["columns"]
    columns_name = map(lambda name: columns[name]["name"], columns)
    values = map(lambda name: "%({0})s".format(name), columns)

    return "INSERT INTO {0}(`{1}`) VALUES ({2})".format(
        table_info["name"], "`,`".join(columns_name), ",".join(values)
    )


from functools import partial
import itertools
total = 0
def gen_sql_data(row, table_info,fault_columns, fault_fp=None):
    data = handle_row(row, table_info)
    if isinstance(data, tuple):
        handle_fault_data(data[0], fault_columns)
        if fault_fp:
            return None
        else:
            fault_info = ''
            fault_fp.write(fault_info)
    else:
        return data

async def execs(rows, table_info,fault_columns,fault_fp, query, loop):
    sql_data = partial(gen_sql_data,table_info=table_info, fault_columns=fault_columns, fault_fp=fault_fp)
    rows = filter(None, map(sql_data,rows))
    while True:
        data = list(itertools.islice(rows,1000))
        if not data:
            break
        await loop.run_in_executor(None, conn.exec_sql_many, query, data)
        

        
def migrate_data(args, table, table_info):
    fault_columns = {
        "more_columns": set(), "short_columns": set(), "fault_types": set()
    }
    query = get_insert_query(table_info)
    pprint.pprint(table_info)
    with open(os.path.join(args.fault_dir, table.name), "w") as fault_fp:
        with open(os.path.join(args.data_dir, "%s.json" % table.name)) as f:
            rows = ijson.items(f, "results.item")
            if False:
                
                loop = asyncio.get_event_loop()
                loop.set_default_executor(executor)
                fus = [execs(rows, table_info,fault_columns,fault_fp,query,loop) for i in range(2)]
                loop.run_until_complete(asyncio.wait(fus)) 
                loop.close()
            else:
                sql_data = partial(gen_sql_data,table_info=table_info, fault_columns=fault_columns, fault_fp=fault_fp)
                rows = filter(None, map(sql_data,rows))
                while True:
                    data = list(itertools.islice(rows,10000))
                    if not data:
                        break
                    try:
                        conn.exec_sql_many(query, data)
                    except Exception as e:
                        print(query)
                        pprint.pprint(data[0])
                        raise e
    _print_fault_type(fault_columns, table_info)





