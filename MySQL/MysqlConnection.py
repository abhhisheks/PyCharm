#!/usr/bin/python3
import pymysql


db = pymysql.connect("127.0.0.1","root","12345","user",port=3306)
cur = db.cursor()
for i in range(1000000):

    sql = """INSERT INTO USER (name,add_date) VALUES ("Abhishek Singh",'2008-7-04')"""
    #print(sql)
    cur.execute(sql)
    db.commit()
    print("Hello: "+str(i))

#print(p1.x)

