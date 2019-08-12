#!/usr/bin/python3
import pymysql

from ConfigManagement.configtest import MyClass

p1=MyClass()

print(p1.Abc())

db = pymysql.connect("127.0.0.1","root","12345","user",port=3306)
cur = db.cursor()
sql = "SELECT NAME,empid FROM employee1;"
print(sql)
cur.execute(sql)

user_data = cur.fetchall()

print(user_data)

print("User Name: "+user_data[0][0])

str="{\"User Name\": "+user_data[0][0]+",\"Data\":"+str(user_data[0][1])+"}"

print("{\"User Name\": "+user_data[0][0]+",\"Data\":"+str(user_data[0][1])+"}")

#print(p1.x)

