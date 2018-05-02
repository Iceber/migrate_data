import pymysql
import re



def _str_compare(str_data, str_type):
    str_len = re.findall("\d+", str_type)
    str_len = int(str_len[0])
    var_char = 1 if "varchar" in str_type else 0

    if len(str_data) >= (str_len - var_char):
        return False

    return True


def _int_compare(int_data, int_type):
    int_len = re.findall("\d+", int_type)
    int_len = int(str_len[0])

    if int_data * 2 >= (256 ** int_len) or int_data < (-256 ** int_len):
        return False

    return True


def _to_json(column_data):
    return json.dumps(column_data)


def _to_datetime(column_data):
    date = column_data.get("iso", "") if isinstance(column_data, dict) else column_data
    date = date.replace("T", " ")[:-1]


def _pointer_to_char(column_data):
    return column_data["ojbectID"]


def _compare_type(data, column_type):
    if "int" in column_type:
        return _int_compare(data, column_type)

    elif "char" in column_type:
        return _str_compare(data, conlumn_type)


def handle_data(data, sql_columns):

    diff_columns = []

    for column_name, column_type in sql_columns:
        if column_type is "json":
            data[column_name] = _to_json(data[column_name])
        elif column_type is "Datetime":
            data[column_name] = _to_datetime(data[column_name])
        elif column_type is "char(24)":
            if isinstance(data[column_name], dict):
                data[column_name] = _pointer_to_char(data[column_name])

        if not _compare_type(data, column_type):
            diff_clumns.append(column_name)

        return diff_columns


"""
def test():

    mongo_data = {"id": 10, "min": 256 ** 16, "test_string": "343434343"}
    sql_type = (("id", "int(32)"), ("min", "int(16)"), ("test_string", "varchar(5)"))

    change_column = compare_type(mongo_data, sql_type)

    if change_column:
        print(change_column)


if __name__ == "__main__":
    test()
"""