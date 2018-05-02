import ijson
import pymysql
import functools
import logging
import json
import datetime

from handle_type import handle_data
from handle_columns import translate_columns_name, compare_columns
from conf import translate_columns_name

user = "root"
password = "root"
host = "localhost"
database = "dump_mongo"

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
                    raise e

    return _f


@_retry
def insert_data(db, sql, row):
    with db.cursor() as cur:
        cur.execute(sql)
        return True


def migrate_data(db, database, file_name):
    table_name = translate_table_name(file_name)

    sql_cols = cc.get_columns(db, database, table_name)
    with open("dump_file") as f:
        rows = ijson.items(f, "sesults")
        diff_column_data = {}
        diff_type_data = {}
        error_inserts_data = []

        for row in rows:
            row = cc.translate_columns_name(file_name, row)
            diff_column = cc.compare_column(row, sql_cols)
            if diff_column:
                diff_column_data[row] = diff_column
                logging.warning(f"[diff columns] {diff_column}")
                continue

            diff_type = ct.handle_data(row, sql_cols)
            if diff_type:
                logging.warning(f"[diff type] {diff_type}")
                diff_types_data[row] = diff_type
                continue

            sql = "INSERT INTO {0}{1} VALUES ({2})".format(
                table_name, tuple(row.keys()), tuple(row.values())
            )
            try:
                insert_data(db, sql)
            except:
                logging.error(f"[insert fail] {row}")
                error_inserts_data.append(row)

    return (diff_columns_data, diff_types_data, error_inserts_data)


def test():

    db = pymysql.connect(host=host, user=user, password=password, db=database)
    diff_columns_data, diff_type_data, error_inserts = migrate_data(db, table)

    db.close()
