from sync_talbes_poll import TablesPoll
from tables_info import get_tables_info

def gen_inserted_sql(table_name, columns):
    values = map(lambda name: "%({0})s".format(name),columns)
    return "INSERT INTO {0}({1}) VALUES ({2})".format(table_name,','.join(columns),','.join(values))


def gen_updated_sql(table_name, columns):
    update_data = ('='.join([name,'%{name}s']) for name in columns)
    return "UPDATE {0} SET {1} WHERE object_id=%('object_id')s".format(table_name,','.join(update_data))

def compile_data(table_name, data, mysql_data):
    update_data = {name: value for name, value in data.items() if mysql_data[name] != value}
    sql = gen_updated_sql(table_name, update_data.keys())
    update_data['object_id'] = data['object_id']

    return sql, update_data

def data_sync(data, table_name, conn, fault_fp):
    extra,mysql_data = conn.select_sql()
    
    if extra:
        if mysql_data['updated_at'] == data['updated_at']:
            return None
        sql, data = compile_data(table_name, data, mysql_data)
    else:
        sql = gen_inserted_sql(table_name, data.keys())
    try:
        cur.execute(sql, data)
    except Exception:
        pass    


def table_sync(table_poll, filter_data, data_sync):
    for result in table_poll.get_query_result():
        for data in filter(None, map(filter_data, result)):
            data_sync(trans_lean_to_sql(data))
    