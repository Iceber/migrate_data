import re
import json


def filter_columns(mongo_data, ignore_columns):
    
    for i in ignore_columns:
        mongo_data.pop(i, "")
        

def handle_columns(mongo_data, columns):

    def _fill_short_columns(mongo_data):
        for col in short_columns:
            col_info = columns[col]
            default = col_info["default"]
            if default is not None:
                mongo_data[col] = default
            elif col_info["nullable"]:
                mongo_data[col] = None
            else:
                yield col

    more_columns = set(mongo_data.keys()) - set(columns.keys())

    short_columns = set(columns.keys()) - set(mongo_data.keys())

    short_columns = short_columns if not short_columns else set(
        _fill_short_columns(mongo_data)
    )

    return {"more_columns": more_columns, "short_columns": short_columns}


def compare_type(data, column_type):

    def _str_compare(char_data, char_type):
        char_len = re.findall("\d+", char_type)
        char_len = int(char_len[0])
        if "VAR" in char_type:
            return len(char_data) < char_len

        else:
            return len(char_data) == char_len

    def _int_compare(int_data, int_type):
        int_len = re.findall("\d+", int_type)
        int_len = int(int_len[0])

        return int_data * 2 < (256 ** int_len) and int_data >= (-256 ** int_len)

    if "INT" in column_type:
        return _int_compare(data, column_type)

    elif "CHAR" in column_type:
        return _str_compare(data, column_type)
    else:
        return True

def handle_data(data, columns):

    def _to_json(column_data):
        return json.dumps(column_data)

    def _to_datetime(column_data):

        date = column_data.get("iso", "") if isinstance(
            column_data, dict
        ) else column_data
        date = date.replace("T", " ")[:-1]
        return date

    def _pointer_to_char(column_data):
        return column_data["objectId"]

    def _to_char(column_data):
        return str(column_data).strip()

    def _get_trans_func(column_type):
        if "INT" in column_type:
            func = _to_int
        elif "CHAR" in column_type:
            func = _to_char
        return func

    def _to_decimal_12_2(column_data):
        column_data = "%.2f" % column_data
        if len(re.findall('\d+',column_data)[0]) > 10:
            raise TypeError("Float length is too long")

        return float(column_data)
    def _to_int(column_data):
        if column_data is '':
            return 0
        return int(column_data)

    type_trans_func = {
        "JSON": _to_json,
        "DATETIME": _to_datetime,
        "CHAR(24)": _pointer_to_char,
        "DECIMAL(12,2)": _to_decimal_12_2,
        "BOOLEAN": bool,
        "TEXT": str,
        "FLOAT": float,
    }

    def _handle(data, columns):
        fault_types = set()
        for column_name, column_info in columns.items():
            if data[column_name] is None:
                continue
            column_type = column_info["type"]
            func = type_trans_func.get(column_type) or _get_trans_func(column_type)

            try:
                if (
                    column_type == "CHAR(24)"
                    and not isinstance(data[column_name], dict)
                ):
                    # 如果mongo数据中Pointer类型的字段不为dict类型，则不需要进行类型转换
                    continue

                data[column_name] = func(data[column_name])

                if compare_type(data[column_name], column_type):
                    continue

                fault_types.add(column_name)
                print(data)
            except Exception as e:
                print(column_name, column_type)
                print(data)
                print("[raise exception]", e)
                fault_types.add(column_name)

        return {"fault_types":fault_types}

    return _handle(data, columns)


