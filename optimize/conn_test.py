import pymysql
from queue import Queue

class Conn_pool(object):

    def __init__(self):
        self.maxconn = maxconn
        self.pool = Queue(maxconn)
        for i in range(maxconn):
            try:
                conn = pymysql.connect(host, port=port,user, passwd, db = db)
                self.pool.put(conn)

            except Exception as e:
                raise IOError(e)

            
        
   def get_conn():
        conn = self.pool.get()
        return conn.cursor()
        