import pymysql
from collections import deque


class Conn_pool(object):
    __p = None

    def __init__(
        self, host=None, port=None, user=None, passwd=None, db=None, maxconn=5
    ):
        self.maxconn = maxconn
        self.pool = deque(maxlen=maxconn)
        for i in range(maxconn):
            try:
                conn = pymysql.connect(
                    host=host, port=port, user=user, passwd=passwd, db=db
                )
                conn.autocommit(True)
                self.pool.append(conn)
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
            conn = self.pool.pop()
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                response = cur.execute(sql)
                data = cur.fetchone()
            return response, data
        finally:
            self.pool.append(conn)
            

    def exec_sql(self, sql, arg=None):
        try:
            conn = self.pool.pop()
            with conn.cursor() as cur:
                response = cur.execute(sql, arg) if arg else cur.execute(sql)
            return response

        finally:
            self.pool.append(conn)
    def exec_sql_many(self,sql,operation=None):
        """
            执行多个sql，主要是insert into 多条数据的时候
        """
        try:
            conn=self.pool.pop()
            cursor=conn.cursor()
            response=cursor.executemany(sql,operation) if operation else cursor.executemany(sql)
        except Exception as e:
            print(e)
            cursor.close()
            self.pool.append(conn)
        else:
            cursor.close()
            self.pool.append(conn)
            return response

    def close_conn(self):
        for i in range(self.maxconn):
            self.pool.pop().close()


