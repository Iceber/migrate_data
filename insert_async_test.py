import asyncio
import pymysql
from DBUtils.PersistentDB import PersistentDB
import concurrent.futures

user = "root"
password = "root"
host = "localhost"
database = "t"

db1 = pymysql.connect(user=user, password=password, host=host, database=database)
db2 = pymysql.connect
file_name = "/home/pybeef/workspace/migrate_data/test.sql"
loop =PersistentDB(pymysql, user=user, password=password, host=host, database=database)


def get_row():
    with open(file_name) as f:
        for row in f.readlines():
            yield row

def get_row_test():
    for i in range(1000):
        yield  "insert into ran(id,con,vc,c) values({0},{1},'{2}','{3}')".format(i,i*3,str(i)* 10,'haha'*10)

def insert_data(sql):
    
    db = loop.connection()
    
    try:
        db.cursor().execute(sql)
        db.commit()
    except Exception as e:
        db.rollback()
        print(e)


def use_dbutils():
    for row in get_row_test():
        insert_data(row)



def use_com():
    db = pymysql.connect(user=user, password=password, host=host, database=database)
    for row in get_row_test():
        try:
            with db.cursor() as cur:
                cur.execute(row)
            db.commit()
        except Exception as e:
            print(e)
            db.rollback()

    
 ojb = 
def test_func(num):
    data=(("男",i,"张小凡%s" %i) for i in range(num))
    sql="insert into tb1(gender,class_id,sname) values(%s,%s,%s)"
    print(obj.exec_sql_many(sql,data))

job_list=[]
for i in range(10):
    t=threading.Thread(target=test_func,args=(100000,))
    t.start()
    job_list.append(t)
for j in job_list:
    j.join()

obj.close_conn()

print("totol time:",time.time()-start)


if __name__ == "__main__":
    use_dbutils()
#    use_com()