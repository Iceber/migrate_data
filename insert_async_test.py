import asyncio
import pymysql

import concurrent.futures

user = "root"
password = "root"
host = "localhost"
database = "test"

db = pymysql.connect(user=user, password=password, host=host, database=database)
file_name = "/home/pybeef/workspace/migrate_data/test.sql"

def get_row():
    with open(file_name) as f:
        for row in f.readlines():
            yield row


def insert_data(sql):
    try:
        with db.cursor() as cur:
            cur.execute(sql)
        db.commit()
        print('ok')
    except Exception as e:
        db.rollback()
        print(e)



def main():
    exe = concurrent.futures.ThreadPoolExecutor()
    for row in get_row():
        print('x')
        exe.submit(insert_data,row)
    
if __name__ == "__main__":
    main()