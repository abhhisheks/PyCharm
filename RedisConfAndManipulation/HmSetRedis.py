import redis
import ConfigManagement.configtest as con

r=con.Redis

r_con = redis.Redis(host="localhost",port="6379",db=0)
#Multiple Key Values
user = {"Name":"Pradeep", "Company":"SCTL", "Address":"Mumbai", "Location":"RCP"}

#Inserting Data using HMSET
r_con.hmset("pythonDict", user)

#Getting value of key
print(r_con.hget("pythonDict","Name"))

if(r_con.hget("pythonDict","Name")=="Pradeep"):
    print("Pradeep")
else:
    print("Hello Not Equal")








