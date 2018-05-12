
import pymysql
from queue import Queue


class Conn_pool(object):
    __p = None

    def __init__(
        self, host=None, port=None, user=None, passwd=None, db=None, maxconn=5
    ):
        self.maxconn = maxconn
        self.pool = Queue(maxconn)
        for i in range(maxconn):
            try:
                conn = pymysql.connect(
                    host=host, port=port, user=user, passwd=passwd, db=db
                )
                conn.autocommit(True)
                self.pool.put(conn)
            except Exception as e:
                IOError(e)

    @classmethod
    def get_pool(cls, *args, **kwargs):
        if cls.__p:
            return cls.__p

        else:
            cls.__p = Conn_pool(*args, **kwargs)
            return cls.__p

    def insert_sql(self, sql, arg=None):
        try:
            conn = self.pool.get()
            with conn.cursor() as cur:
                response = cur.execute(sql, arg) if arg else cur.execute(sql)
        except Exception as e:
            print(e)
        else:
            return response

        finally:
            self.pool.put(conn)

    def insert_many(self, sql, arg):
        try:
            conn = self.pool.get()
            with conn.cursor() as cur:
                response = cur.executemany(sql, arg)
        except Exception as e:
            print(e)
        else:
            return response

        finally:
            self.pool.put(conn)

    def close_conn(self):
        for i in range(self.maxconn):
            self.pool.get().close()


conn = Conn_pool.get_pool(user="root", passwd="root", db="t", maxconn=100)

import time
import threading

data = (("Conn_Pool_Test", i, "C%s" % i) for i in range(1000000))


def test_func(num):
    #    data = (("Conn_Pool_Test", i, "C%s" % i) for i in range(num))
    while True:
        try:
            d = next(data)
        except StopIteration:
            return

        sql = "insert into tb1(title,count,st) values(%s,%s,%s)"
        conn.insert_sql(sql, d)


def threading_test():
    job_list = []

    for i in range(100):
        t = threading.Thread(target=test_func, args=(10000,))
        t.start()
        job_list.append(t)
    for j in job_list:
        j.join()

    conn.close_conn()


def async_test():
    import asyncio

    loop = asyncio.get_event_loop()
    for i in range(100):
        loop.run_in_executor(None, test_func, 10000)
    loop.run_until_complete(asyncio.sleep(0.1))

    conn.close_conn()


db = pymysql.connect(user="root", passwd="root", db="t")
db.autocommit(True)


def com_test():
    with db.cursor() as cur:
        data = (("Conn_Pool_Test", i, "C%s" % i) for i in range(1000000))
        sql = "insert into tb1(title,count,st) values(%s,%s,%s)"
        cur.executemany(sql, data)
    db.close()


if __name__ == "__main__":
    start = time.time()
    #   threading_test()
    async_test()
    #    com_test()
    print("total: ", time.time() - start)
