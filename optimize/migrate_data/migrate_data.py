import sys
sys.path.append('../')

from models.columns import TableInfo
from handle_row import handle_data, handle_columns
import pprint
import os.path
import multiprocessing
import ijson
import pymysql
import logging

table_info = None
gargs = None

def ijson_number(str_value):
    number = float(str_value)
    if not ('.' in str_value or 'e' in str_value or 'E' in str_value):
        number = int(number)
    return number
ijson.common.number = ijson_number

def change_table_info(table, config):
    global table_info
    table_info = TableInfo.from_mongodb(table, config)


def filter_columns(mongo_data, ignore_columns):

    for i in ignore_columns:
        mongo_data.pop(i, "")

def handle_rows(row):
    global table_info
    filter_columns(row, table_info.ignore_columns)
    fault_columns = handle_columns(row, table_info.columns)
    if any(fault_columns.values()):
        return (fault_columns,)
    fault_types = handle_data(row, table_info.columns)
    fault_columns.update(fault_types)
    if gargs.test or  any(fault_columns.values()):
        return (fault_columns,)
    return row

def get_sql(table_info):
    columns = table_info.columns
    columns_name = list(map(lambda name: columns[name]["name"], columns))
    values = list(map(lambda name: "%({0})s".format(name), columns))

    return "INSERT INTO {0}({1}) VALUES ({2})".format(
        table_info.table_name, ",".join(columns_name), ",".join(values)
    )


def handle_fault_data(fault_row,fault_columns):
    for k,v in fault_row.items():
        if v:
            fault_columns[k] |= v
            
def _print_fault_type(fault_columns):
    a = set()
    for v in fault_columns.values():
        a |= v
    if not a:
        print('good table')
    else:
        print('*'*5,table_info.table_name)
        pprint.pprint(fault_columns)
        for name in a:
            print(name,': ',table_info.columns.get(name,{}).get("type"))
def insert_data(query,row):
    pass

def migrate_data(args, table, config):
    global gargs
    gargs = args
    change_table_info(table, config)
    fault_columns = {
        "more_columns": set(),
        "short_columns": set(),
        "fault_types": set()
    }
    total = 0
    query = get_sql(table_info)
    with open(os.path.join(args.data_dir, "%s.json" % table.name)) as f:
        rows = ijson.items(f, "results.item")
        pool = multiprocessing.Pool()
        result = pool.imap(handle_rows, rows)
        for row in result:
            if isinstance(row, tuple):
#                total += 1
#                print(total)
                handle_fault_data(row[0],fault_columns)
            else:
                try:
                    insert_data(query,row)
                except Exception as e:
                    logging.warning(e)
#                    logging.warning(f'[insert fault]: {row}')
                else:
                    print('ok')

    _print_fault_type(fault_columns)

from collections import deque
import asyncio    

q = deque(maxlen=10000)

def migrate_data(args):
    global gargs
    gargs = args
    change_table_info(table, config)
    fault_columns = {
        "more_columns": set(),
        "short_columns": set(),
        "fault_types": set()
    }
    total = 0
    loop = asyncio.get_event_loop()

    query = get_sql(table_info)
    with open(os.path.join(args.data_dir, "%s.json" % table.name)) as f:
        rows = ijson.items(f, "results.item")

        loop.run_in_executor(None, handle_rows, rows)

        
def handle_rows(rows):
    global q
    while True:
        if len(q) < 500:
            for row in rows:
                row = handle_row(row)
                if isinstance(row, tuple):
                    handle_fault_data(row[0],fault_columns)
                else:
                    q.append(row)
                    if len(q) == 10000:
                        break
                
def insert_data(query):
    global q
    conn = pymysql.connect(user = user, passwd = passwd, db = db)
    conn.autocommit(True)
    
    while True:
        data = q.pop()
        try:
            with conn.cursot() as cur:
                cur.execute(query, data)

            
