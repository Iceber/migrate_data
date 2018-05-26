import copy
import asyncio
import leancloud

from talbes_info import get_tables_info,get_tables_poll_info, storage_tables_poll_info

from models.conn_poll import Conn_Pool
from tables_poll import every_table
time_interval_init = 20
result_count = 1000


try:
    tables_poll_info = get_tables_poll_info()
except Exception:
    #
    pass

tables_info = get_tables_info()

async def table_poll(table_name):
    conn = Conn_Pool.get()
    table_info = tables_info[table_name]
    table_poll_info = tables_poll_info[table_name]

    while True:
        try:
            last_time, time_interval = every_table(
                table_name, copy.deepcopy(table_info), table_poll_info, conn
            )
        except Exception:
            print("[Error] ")
            

        table_info["last_time"], table_info["time_interval"] = last_time, time_interval
        await asyncio.sleep(table_poll_info["time_interval"] * 60)


def main(args):
    loop = asyncio.get_event_loop()

    for i, table_name in enumerate(tables_info.keys()):
        if i not in range(args.count, 1000, args.total):
            continue

        loop.create_task(table_poll(table_name))
    try:
        loop.run_forever()
    except Exception:
        # storge table_poll_info
        # 程序可能会人工停止(Ctrl-C),需要对一些信息做存储处理
        storage_tables_poll_info(tables_poll_info)

        return None


class Args(object):

    def __init__(self, err_dir, total, count):
        self.err_dir = err_dir
        self.total = total
        self.count = count


if __name__ == "__main__":
    args = Args("./", 2, 1)
    main(args)
