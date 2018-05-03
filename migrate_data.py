import ijson
import pymysql
import functools
import logging
import json
import os.path
import copy
from handle_type import handle_data
from handle_columns import translate_columns_name, handle_columns
from conf import translate_table_name, get_columns_type, get_columns_info,\
                dump_data_dir,dump_data_file


user = "root"
password = "root"
host = "localhost"
database = "t"

db = pymysql.connect(user=user, password=password, host=host, database=database)

def insert_data(db, sql):
    try:
        with db.cursor() as cur:
            cur.execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()
        print(e)
        raise e

def test(row,diff_columns, sql_cols):
    for column in diff_columns:
        print(column,': ' ,sql_cols[column])

def migrate_data(file_name,db = db,database = database):
    path_name = os.path.join(dump_data_dir,file_name+'.json')
    table_name = translate_table_name(file_name)

    col_type = get_columns_type(db, database, table_name)
    col_info = get_columns_info(db,database, table_name)

    with open(path_name) as f:
        rows = ijson.items(f, "results.item")

        diff_column_data = {}
        diff_type_data = {}
        error_inserts_data = []

        test_diff_columns = set()
        fail = 0
        total = 0

        print('start')
        for row in rows:
            sql_cols = copy.deepcopy(col_type)
            row = translate_columns_name(file_name, row)
            diff_columns = handle_columns(row, col_info, col_type, sql_cols)

            total += 1
            if diff_columns:
                test_diff_columns |= diff_columns
                print(diff_columns)
                print(row)
                fail += 1
            continue

            if diff_columns:
                diff_column_data[str(row)] = diff_columns
                logging.warning(f"[diff columns] {diff_columns}")
                continue
            diff_type = handle_data(row, sql_cols)



            if diff_type:
                logging.warning(f"[diff type] {diff_type}")
                diff_type_data[str(row)] = diff_type
                continue

            sql = "INSERT INTO {0}({1}) VALUES {2};".format(
                table_name, ','.join(row.keys()), tuple(row.values())
            )
            try:
                insert_data(db, sql)
            except:
                logging.error(f"[insert fail] {row}")
                error_inserts_data.append(row)
            
    print(test_diff_columns)
    print('total: fail')
    print("%s:\t %s",total, fail)
    test(row,test_diff_columns, sql_cols)


    return (diff_column_data, diff_type_data, error_inserts_data)




if __name__ == "__main__":
    migrate_data(dump_data_file)


