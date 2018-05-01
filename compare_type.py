import pymysql
import re


host = "localhost"
user = "root"
password = "root"
database = "test"

# db = pymysql.connect(host=host, user=user, password=password, db=database)


def get_columns_type(db,database,table_name):

    with db.cursor() as cur:
        cur.execute(
            "select column_name,column_type,data_type from information_schema.columns where table_schema = %s and table_name=%s",
            (database,table_name)
        )
        columns_type = cur.fetchall()
    return columns_type


def _str_compare(str_data, str_type):
    str_len = re.findall("\d+", str_type)
    str_len = int(str_len[0])
    var_char = 0

    if "varchar" in str_type:
            var_char = 1
    if len(str_data) >= (str_len - var_char):
        return False

    return True


def _int_compare(int_data, int_type):
    int_len = re.findall("\d+", int_type)
    int_len = int(str_len[0])

    if int_data * 2 >= (256 ** int_len) or int_data < (-256 ** int_len):
        return False

    return True


def compare_type(mongo_data, sql_type):

    change_column = []
    for column_name, column_type,data_type in sql_type:
        data = mongo_data[column_name]
'''
        if isinstance(data, int):
            if not _int_compare(data, column_type):
                change_column.append(column_name)
                continue

        elif isinstance(data, str):
            if not _str_compare(data, column_type):
                change_column.append(column_name)
                continue
'''
        if 'int' in data_type:
            if not _int_compare(data,column_type):
                change_column.append(column_name)
        elif 'char' in data_type:
            if not _str_compare(data,column_type):
                change_column.append(column_name)


    if  change_column:
        return change_column

    return True



























def test():

    mongo_data = {"id": 10, "min": 256 ** 16, "test_string": "343434343"}
    sql_type = (("id", "int(32)"), ("min", "int(16)"), ("test_string", "varchar(5)"))

    change_column = compare_type(mongo_data, sql_type)

    if change_column:
        print(change_column)


if __name__ == "__main__":
    test()
