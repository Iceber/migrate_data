from conf import (
    _snake_case,
    common,
    customize,
    type_default_value,
    dump_schema_dir,
    dump_data_file as file_name,
)


def translate_columns_name(file_name, mongo_data):
    ignore_columns = customize.get(file_name, {}).get("ignore_columns", {}).get(
        "name", []
    )

    ignore_columns.extend(common.get("ignore_columns", {}).get("type", []))

    data = {}
    for key in mongo_data.keys():
        if key in ignore_columns:
            continue

        name = customize.get(file_name, {}).get("columns", {}).get(key, {}).get(
            "name", _snake_case(key)
        )
        data[name] = mongo_data[key]

    return data


def _get_columns_default(columns_attr_file):
    import json, os.path

    with open(os.path.join(dump_schema_dir, columns_attr_file + "_all.json")) as f:
        columns_attr = json.load(f)["schema"]
    columns_default = {}
    for name, attr in columns_attr.items():
        if "default" in attr:
            columns_default[name] = attr["default"] if not isinstance(
                attr["default"], dict
            ) else attr[
                "default"
            ].get(
                "iso", ""
            ) or attr[
                "default"
            ][
                "objectId"
            ]
    return translate_columns_name(columns_attr_file, columns_default)


# columns_default = _get_columns_default(file_name)


def handle_columns(mongo_data, columns_info, columns_type, columns, columns_default):
    mongo_cols_only = set(mongo_data.keys()) - set(columns_info.keys())
    if mongo_cols_only:

        return ('more_columns',mongo_cols_only)

    sql_cols_only = set(columns_info.keys()) - set(mongo_data.keys())
    null_columns = set()
    if sql_cols_only:
        for col in sql_cols_only:
            if col in columns_default:
                mongo_data[col] = columns_default[col]
            elif "int" in columns_type[col]:
                mongo_data[col] = type_default_value["int"]
            elif "float" == columns_type[col]:
                mongo_data[col] = type_default_value["float"]
            elif "varchar" in columns_type[col]:
                mongo_data[col] = type_default_value["varchar"]
            elif columns_type[col]:
                mongo_data[col] = type_default_value["json"]
            elif any(columns_info[col]):
                columns.pop(col)
            else:
                null_columns.add(col)

    return ('diff_columns',null_columns)
