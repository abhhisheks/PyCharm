#!/usr/bin/python3

import pymysql
import sys
import csv
import os
import html
import urllib.parse
import contextlib
db = pymysql.connect("127.0.0.1","root","12345","user",port=3306)
cur = db.cursor()
sql = "SELECT NAME,user_id FROM USER;"
print(sql)
cur.execute(sql)

user_data = cur.fetchall()
print("Hello World")
#tplfinal=[]
tplfinal=[('name','id','stype')]
for i in user_data:
    lst = list(i)
    lst[0] = lst[0]+'300'
    lst.append('3')
    #lst.append("10")
    i = tuple(lst)
    tplfinal.append(i) 

print(tplfinal) 

#writer.writerow(['name','id','stype'])
with open('people1.csv', 'a') as csvFile:
    writer = csv.writer(csvFile)
    writer.writerows(tplfinal)

csvFile.close()

os.system("""curl -u admin 'http://localhost:8983/solr/test_core/update/csv?commit=true' --data-binary @people1.csv -H 'Content-type:text/plain; charset=utf-8'""")

