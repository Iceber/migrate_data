from gevent import monkey
monkey.patch_all()

import threading

import pymysql
from queue import Queue
import time
start = time.time()
class Exec_db:

    __v=None

    def __init__(self,host=None,port=None,user=None,passwd=None,db=None,charset=None,maxconn=5):
        self.host,self.port,self.user,self.passwd,self.db,self.charset=host,port,user,passwd,db,charset
        self.maxconn=maxconn
        self.pool=Queue(maxconn)
        for i in range(maxconn):
            try:
                conn=pymysql.connect(host=self.host,port=self.port,user=self.user,passwd=self.passwd,db=self.db,charset=self.charset)
                conn.autocommit(True)
                # self.cursor=self.conn.cursor(cursor=pymysql.cursors.DictCursor)
                self.pool.put(conn)
            except Exception as e:
                raise IOError(e)

    @classmethod
    def get_instance(cls,*args,**kwargs):
        if cls.__v:
            return cls.__v
        else:
            cls.__v=Exec_db(*args,**kwargs)
            return cls.__v

    def exec_sql(self,sql,operation=None):
        """
            执行无返回结果集的sql，主要有insert update delete
        """
        try:
            conn=self.pool.get()
            cursor=conn.cursor(cursor=pymysql.cursors.DictCursor)
            response=cursor.execute(sql,operation) if operation else cursor.execute(sql)
        except Exception as e:
            print(e)
            cursor.close()
            self.pool.put(conn)
            return None
        else:
            cursor.close()
            self.pool.put(conn)
            return response


    def exec_sql_feach(self,sql,operation=None):
        """
            执行有返回结果集的sql,主要是select
        """
        try:
            conn=self.pool.get()
            cursor=conn.cursor(cursor=pymysql.cursors.DictCursor)
            response=cursor.execute(sql,operation) if operation else cursor.execute(sql)
        except Exception as e:
            print(e)
            cursor.close()
            self.pool.put(conn)
            return None,None
        else:
            data=cursor.fetchall()
            cursor.close()
            self.pool.put(conn)
            return response,data

    def exec_sql_many(self,sql,operation=None):
        """
            执行多个sql，主要是insert into 多条数据的时候
        """
        try:
            conn=self.pool.get()
            cursor=conn.cursor(cursor=pymysql.cursors.DictCursor)
            response=cursor.executemany(sql,operation) if operation else cursor.executemany(sql)
        except Exception as e:
            print(e)
            cursor.close()
            self.pool.put(conn)
        else:
            cursor.close()
            self.pool.put(conn)
            return response

    def close_conn(self):
        for i in range(self.maxconn):
            self.pool.get().close()

obj=Exec_db.get_instance(host="localhost",user="root",passwd="root",db="t",charset="utf8",maxconn=10)

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
