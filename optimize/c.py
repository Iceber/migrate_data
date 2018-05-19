import pymysql

conn = pymysql.connect(user="root", passwd="root",db="t")

sql = "insert into tb1(title,count,st) values(%s,%s,%s)"

def query_data():
    for a in range(10):
        yield  (("Conn_Pool_Test", i, "C%s" % i) for i in range(10000))
total = 0

for i in query_data():



   
#        total += 1
#        print(total)
    with conn.cursor() as cur:
        for c in i:
            cur.execute(sql,c)
        conn.commit()  
 
