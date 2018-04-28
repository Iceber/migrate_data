def get_columns_name(db, database_name, table_name):
    with db.cursor() as cur:
        cur.execute(
            "select  column_name from information_schema.columns where table_schema =%s  \
                    and table_name = %s;",
            (database_name, table_name),
        )
        col = cur.fetchone()
    return set(col)

def compare_columns(dump_data, sql_cols):
    return set(dump_data.keys()) - sql_cols 







































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
