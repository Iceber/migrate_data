from columns import TableInfo
from handle_row import handle_data, handle_columns
import pprint
import os.path
import multiprocessing
import ijson

table_info = None
gargs = None


def change_table_info(table, config):
    global table_info
    table_info = TableInfo.from_mongodb(table, config)


def filter_columns(mongo_data, ignore_columns):

    for i in ignore_columns:
        mongo_data.pop(i, "")


def get_sql(table_info):
    columns = table_info.columns
    columns_name = list(map(lambda name: columns[name]["name"], columns.keys()))
    values = list(map(lambda name: "%({0})s".format(name), columns_name))

    return "INSERT INTO {0} VALUES ({1})".format(
        table_info.table_name, ",".join(columns_name), ",".join(values)
    )


def handle_rows(row):
    global table_info
    filter_columns(row, table_info.ignore_columns)
    fault_columns = handle_columns(row, table_info)

    fault_types = handle_data(row, table_info.columns)
    fault_columns.update(fault_types)
    if gargs.test or not any(fault_columns.values()):
        return fault_columns

    sql = get_sql(table_info)


def insert_data(row):
    pass


def handle_fault_data(fault_columns):
    if not any(fault_columns.values()):
        print("good table")
    else:
        pprint.pprint(fault_columns)


def migrate_data(args, table, config):
    global gargs
    gargs = args
    change_table_info(table, config)

    with open(os.path.join(args.data_dir, "%s.json" % table.name)) as f:
        rows = ijson.items(f, "results.item")
        pool = multiprocessing.Pool()
        result = pool.imap(handle_rows, rows)
        for row in result:
            if isinstance(row, dict):
                handle_fault_data(row)
            else:
                insert_data(row)
