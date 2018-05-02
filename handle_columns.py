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


"""
def _dump_compare(dump_items, sql_cols):
    for item in dump_items:
        diff_key = columns_compare(item, sql_cols)
        if diff_key:
            print(diff_key)
            yield item

def test(dump_name, table_name, database_name=database):
    import pymysql
    import ijson

    db = pymysql.connect(host=host, user=user, password=password, db=database)
    sql_cols = get_columns_name(database_name, table_name)


    with open(dump_name) as f:
        dump_items = ijson.items(f, "results")
        for item in _dump_compare(dump_items, sql_cols):
            print(item)


if __name__ == "__main__":
    test("Activity_all.json", "ok")
"""
