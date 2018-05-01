import ijson
import pymysql
import functools
import logging
import json
import datetime

import compare_type as ct
import compare_columns as cc

user = "root"
password = "root"
host = "localhost"
database = "test"

def handle_table_name(file_name):
    table_name = customize.get(file_name, {}).get(
        "table_name", _snake_case(file_name) + "s"
    )
    return table_name


def _to_json(column_data):
    return json.dumps(column_data)
def _to_datetime(column_data):
    date = column_data.get("iso", "") if isinstance(column_data, dict) else column_data
    date = date.replace("T", " ")[:-1]
def _pointer_to_char(column_data):
    return column_data["ojbectID"]

def handle_data(data, columns_type):
    for column_name, column_type in columns_type:
        if column_type is "json":
            data[column_name] = _to_json(data[column_name])
        elif column_type is "Datetime":
            data[column_name] = _to_datetime(data[column_name])
        elif column_type is "char(24)":
            if isinstance(data[column_name], dict):
                data[column_name] = _pointer_to_char(data[column_name])


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
        cur.execute(sql, tuple(row.values()))
        return True


def migrate_data(db, database, file_name):
    table_name = handle_table_name(file_name)

    columns_name = cc.get_columns_name(db, database, table_name)
    columns_type = ct.get_columns_type(db, database, table_name)

    with open("dump_file") as f:
        rows = ijson.items(f, "sesults")
        diff_columns_data = {}
        diff_types_data = {}
        error_inserts_data = []

        for row in rows:
            row = cc.handle_columns_name(row)
            handle_data_type(row)
            diff_column = cc.compare_column(row, sql_cols)
            if diff_column:
                diff_columns_data[row] = diff_column
                logging.warning(f"[diff columns] {diff_column}")
                continue

            diff_type = ct.compare_type(row, columns_type)
            if diff_type:
                logging.warning(f"[diff type] {diff_type}")
                diff_types_data[row] = diff_type
                continue

            _s = ",".join(tuple("?" * len(columns_name)))
            insert_columns = str(tuple(row.keys()))
            sql = "INSERT INTO {0}{1} VALUES ({2})".format(
                table_name, insert_columns, _s
            )

            row = handle_data

            try:
                insert_data(db, sql, row)
            except:
                logging.error(f"[insert fail] {row}")
                error_inserts_data.append(row)

    return (diff_columns_data, diff_types_data, error_inserts_data)


def test():

    db = pymysql.connect(host=host, user=user, password=password, db=database)
    diff_columns_data, diff_type_data, error_inserts = migrate_data(db, table)

    db.close()
