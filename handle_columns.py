from conf import _snake_case, common, customize



def handle_columns(mongo_data,columns_info,columns):
    mongo_cols_only = set(mongo_data.keys()) - set(columns_info.keys())
    if mongo_cols_only:
        print("mongo data more than sql",mongo_cols_only,'\n',mongo_data.keys(),columns_info.keys())
        return set()
    sql_cols_only = set(columns_info.keys())-set(mongo_data.keys())
    null_columns = set()
    if sql_cols_only:
        for col in sql_cols_only:
            if any(columns_info[col]):
                columns.pop(col)
                continue
            null_columns.add(col)
    
    return null_columns
                    
            

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

