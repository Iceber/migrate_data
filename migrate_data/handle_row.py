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
        str_len = re.findall("\d+", str_type)
        str_len = int(str_len[0])
        if "VAR" in char_type:
            return len(str_data) < str_len

        else:
            return len(str_data) == str_len

    def _int_compare(int_data, int_type):
        int_len = re.findall("\d+", int_type)
        int_len = int(int_len[0])

        return int_data * 2 < (256 ** int_len) and int_data >= (-256 ** int_len)

    if "INT" in column_type:
        return _int_compare(data, column_type)

    elif "CHAR" in column_type:
        return _str_compare(data, column_type)


def handle_data(data, columns):

    def _to_json(column_data):
        return json.dumps(column_data)

    def _to_datetime(column_data):

        date = column_data, get["iso", ""] if isinstance(
            column_data, dict
        ) else column_data
        date = date.replace("T", " ")[:-1]
        return date

    def _pointer_to_char(column_data):
        return column_data["objectId"]

    def _get_trans_func(column_type):
        if "INT" in column_type:
            func = int
        elif "CHAR" in column_type:
            func = str
        return func

    type_trans_func = {
        "JSON": _to_json,
        "DATETIME": _to_datetime,
        "CHAR(24)": _pointer_to_char,
        "BOOLEAN": bool,
        "TEXT": str,
        "FLOAT": float,
    }

    def _handle(data, columns):
        fault_types = set()
        for column_name, column_info in columns:
            column_type = column_info["type"]
            func = type_trans_func.get(column_type) or _get_trans_func(column_type)

            try:
                if column_type == "CHAR(24)" and not instance(data[column_name], dict):
                    continue

                data[column_name] = func(data[column_name])

                if compare_type(data[column_name], column_type):
                    continue

            except Exception as e:
                print(column_name, column_type)
                print("[raise exception]", e)

            finally:
                fault_types.add(column_name)

        return fault_types

    return _handle(data, columns)
