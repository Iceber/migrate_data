import sys
import os.path
import asyncio
import pprint
from concurrent.futures import ThreadPoolExecutor

sys.path.append("../")
from models.db_conn_pool import Conn_pool
from libs.handle_rows import handle_row
from libs.myjson import ijson


conn = Conn_pool.get_pool(user="root", passwd="root", db="t", maxconn=10)
executor = ThreadPoolExecutor(max_workers=10)
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

    return "INSERT INTO {0}({1}) VALUES ({2})".format(
        table_info["name"], ",".join(columns_name), ",".join(values)
    )


async def gen_sql(rows, table_info, fault_columns, fault_fp):
    for row in rows:
        result = handle_row(row, table_info)
        if isinstance(result, tuple):
            handle_fault_data(result[0], fault_columns)
            fault_fp.write(str(result[0]) + "\n")
            fault_fp.write(str(row))
            fault_fp.write("\n\n")
            fault_fp.flush()
        else:
            await q.put(result)


async def insert_data(query, q, gen_fu, loop, fault_fp):
    while True:
        if gen_fu.done() and q.empty():
            return

        else:
            data = await q.get()
            # await loop.run_in_executor(None,time.sleep,0.1)
            try:
                await loop.run_in_executor(None, conn.exec_sql, data)
            except Exception:
                fault = ''.join(["[Insert Error] objectId: ", data["objectId"], "\n\n"])
                fault_fp.write(fault)


def migrate_data(args, table, table_info):
    fault_columns = {
        "more_columns": set(), "short_columns": set(), "fault_types": set()
    }
    query = get_insert_query(table_info)

    with open(os.path.join(args.fault_dir, table.name), "w") as fault_fp:
        with open(os.path.join(args.data_dir, "%s.json" % table.name)) as f:
            rows = ijson.items(f, "results.item")
            loop = asyncio.get_event_loop()
            loop.set_default_executor(executor)
            gen_fu = loop.create_task(
                gen_sql(rows, table_info, fault_columns, fault_fp)
            )
            insert_data_fus = asyncio.wait(
                [insert_data(query, q, gen_fu, loop, fault_fp) for i in range(20)],
                return_when=asyncio.FIRST_COMPLETED,
            )
            _, depending = loop.run_until_complete(insert_data_fus)

            for i in depending:
                i.cancel()
            # 再次运行事件循环来取消futures
            loop.run_until_complete(asyncio.sleep(0.1))
            loop.close()

    _print_fault_type(fault_columns, table_info)
