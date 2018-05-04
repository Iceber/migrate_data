import asyncio
import multiprocessing


diff_columns = set()
diff_types = set()


def handle(row):
    #被并发执行函数
    sql_cols = copy.deepcopy(col_type)
    row = translate_columns_name(file_name,row)
    diff_columns = handle_columns(row,col_info, col_type,sql_cols)

    if diff_columns:
#            print(diff_columns)
#            print(row)
        return ("diff_columns",diff_columns)

    diff_types = handle_data(row, sql_cols)
    if diff_types:
        return ("diff_type",diff_types)
    sql = "INSERT INTO {0}({1}) VALUES {2};".format(
        table_name, ','.join(row.keys()), tuple(row.values())
        )
    return sql


async
def insert_data(sql):
    try:
        with db.cursor() as cur:
            cur.execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()
        print(e)

def handle_pool(rows):
    pool = multiprocessing.Pool()
    async_result = pool.imap(handle,rows)
    for row in async_result:
        if isinstance(row, tuple):
            #问题数据
            if row[0] == "diff_columns":
                pass
            elif row[0] == "diff_types":
                pass
        yield sql


def migrate_data():
    db = 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(insert_data())

