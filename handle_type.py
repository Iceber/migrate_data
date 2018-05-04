import re
import json


def _str_compare(str_data, str_type):
    str_len = re.findall("\d+", str_type)
    str_len = int(str_len[0])
    var_char = 1 if "varchar" in str_type else 0

    if len(str_data) > (str_len - var_char):
        return False

    return True


def _int_compare(int_data, int_type):
    int_len = re.findall("\d+", int_type)
    int_len = int(int_len[0])

    if int_data * 2 >= (256 ** int_len) or int_data < (-256 ** int_len):
        return False

    return True


def _compare_type(data, column_type):
    if "int" in column_type:
        return _int_compare(data, column_type)

    elif "char" in column_type:
        return _str_compare(data, column_type)

    return True


def _to_json(column_data):
    return json.dumps(column_data)


def _to_datetime(column_data):
    date = column_data.get("iso", "") if isinstance(column_data, dict) else column_data
    date = date.replace("T", " ")[:-1]
    return date


def _pointer_to_char(column_data):
    return column_data["objectId"]


def handle_data(data, sql_columns):

    diff_columns = []
    for column_name, column_type in sql_columns.items():
        if column_type == "json":
            data[column_name] = _to_json(data[column_name])
        elif column_type == "datetime":
            data[column_name] = _to_datetime(data[column_name])
        elif column_type == "char(24)":
            if isinstance(data[column_name], dict):
                data[column_name] = _pointer_to_char(data[column_name])
        elif "int" in column_type and "tinyint" not in column_type:
            data[column_name] = int(data[column_name])
        if not _compare_type(data[column_name], column_type):
            diff_columns.append(column_name)

    return diff_columns
