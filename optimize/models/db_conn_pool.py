

import pymysql
from queue import Queue


class Conn_pool(object):
    __p = None

    def __init__(
        self, host=None, port=None, user=None, passwd=None, db=None, maxconn=5,charset=None
    ):
        self.maxconn = maxconn
        self.pool = Queue(maxconn)
        for i in range(maxconn):
            try:
                conn = pymysql.connect(
                    host=host, port=port, user=user, passwd=passwd, db=db, charset=charset
                )
                conn.autocommit(True)
                self.pool.put(conn)
            except Exception as e:
                raise IOError(e)

    @classmethod
    def get_pool(cls, *args, **kwargs):
        if cls.__p:
            return cls.__p

        else:
            cls.__p = Conn_pool(*args, **kwargs)
            return cls.__p

    def select_sql(self, sql):
        try:
            conn = self.pool.get()
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                response = cur.execute(sql)
                data = cur.fetchone()
            return response, data

        finally:
            self.pool.put(conn)

    def exec_sql(self, sql, arg=None):
        try:
            conn = self.pool.get()
            with conn.cursor() as cur:
                response = cur.execute(sql, arg) if arg else cur.execute(sql)
            return response

        finally:
            self.pool.put(conn)

    def exec_sql_many(self,sql,operation):
        try:
            conn=self.pool.get()
            with conn.cursor() as cur:
                response=cur.executemany(sql,operation)
            return response
        finally:
            self.pool.put(conn)

    def close_conn(self):
        for i in range(self.maxconn):
            self.pool.get().close()
