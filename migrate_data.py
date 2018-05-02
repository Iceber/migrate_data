import ijson
import pymysql
import functools
import logging
import json
import os.path
import copy
from handle_type import handle_data
from handle_columns import translate_columns_name, handle_columns
from conf import translate_table_name, get_columns, get_columns_info


user = "root"
password = "root"
host = "localhost"
database = "test"

db = pymysql.connect(user=user, password=password, host=host, database=database)


def _retry(f):

    @functools.wraps(f)
    def _f(*args, **kwargs):
        _num = 3
        while True:
            try:
                return f(*args, **kwargs)

            except Exception as e:
                _num -= 1
                if _num == 0:
                    print(e)
                    raise e

    return _f

@_retry
def insert_data(db, sql):
    with db.cursor() as cur:
        cur.execute(sql)
    return True


def migrate_data(path_name,db = db,database = database):
    file_name = os.path.basename(path_name)
    table_name = translate_table_name(file_name)
    col_type = get_columns(db, database, table_name)
    col_info = get_columns_info(db,database, table_name)

    with open(path_name+'.json') as f:
        rows = ijson.items(f, "results.item")

        diff_column_data = {}
        diff_type_data = {}
        error_inserts_data = []


        for row in rows:
            sql_cols = copy.deepcopy(col_type)
            row = translate_columns_name(file_name, row)
            diff_column = handle_columns(row, col_info,sql_cols)

            if diff_column:
                diff_column_data[str(row)] = diff_column
                logging.warning(f"[diff columns] {diff_column}")
                continue
            diff_type = handle_data(row, sql_cols)


            if diff_type:
                logging.warning(f"[diff type] {diff_type}")
                diff_type_data[str(row)] = diff_type
                continue

            sql = "INSERT INTO {0}({1}) VALUES {2};".format(
                table_name, ','.join(row.keys()), tuple(row.values())
            )
            print(sql)
            try:
                insert_data(db, sql)
            except:
                logging.error(f"[insert fail] {row}")
                error_inserts_data.append(row)
            break
            


    return (diff_column_data, diff_type_data, error_inserts_data)


def test():

    db = pymysql.connect(host=host, user=user, password=password, db=database)
    diff_columns_data, diff_type_data, error_inserts = migrate_data(db, table)

    db.close()

if __name__ == "__main__":

    migrate_data('/home/pybeef/workspace/dump_data/Beef')