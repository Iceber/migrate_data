import leancloud
import pymysql
from datetime import datetime
from talbes import table_info

import asyncio

time_interval_init = 20
result_count = 1000
last_time = datetime.now()
with open("tables_info.json") as f:
    tables_info = json.loads(f.read())

try: 
    with open('tables_poll_info') as f:
        tables_poll_info=json.loads(f.read())
except FileNotFoundError:
    tables_poll_info = {table: {'last_time':last_time,'time_interval':time_interval_init} for table in tables_info.keys()}


def query_data(table_name,table_poll_info):

    skip = 0
    table_info = tables_info[table_name]
    now = datetime.datetime.now()

    Table = leancloud.Object.extend(table_name)
    query1 = Table.query
    query2 = Table.query
    query1.greater_than_or_equal_to('updatedAt',  table_poll_info[0])
    query2.less_than('updatedAt', now)
    table_poll_info['last_time'] = now

    query = leancloud.Query.and_(query1, query2)

    def _get_result(query):
        query.skip(result_count * skip)
        result = query.find()
        yield result
        if len(result) == result_count:
            table_poll_info['time_interval'] /= 2
            result_count += 1
            q()
        elif skip == 0  and len(result) < result_count / 2 : 
            table_poll_info['time_interval'] *= 2

    for result in _get_result(query):
        for row in result:
            handle_row(row,table_info)

def handle_row(row):
    filter_columns(row, table_info["ignore_columns"])
    fault_columns = handle_columns(row, table_info["columns"])
    handle_data(row.table_info["columns"])
    

async
def table_poll(table_name):
    table_poll_info = table_poll_info[table_name]
    query_data(table_name, table_poll_info)
    await asyncio.sleep(table_poll_info['time_interval'] * 60)


def main():
    leancloud.init()

    loop = asyncio.get_event_loop()
    
    for table_name in tables_info.keys():
        loop.create_task(table_poll(table_name))

    loop.run_forever()
