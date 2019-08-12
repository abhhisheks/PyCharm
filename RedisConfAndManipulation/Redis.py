import redis
import ConfigManagement.configtest as con

r=con.Redis

r_con = redis.Redis(host="localhost",port="6379",db=0)

#r_con.set("a","three")
d = {'foo': 1.5, 'bar': 2.5}
r_con.zadd("abc",d);

d1 = r_con.zrange('abc', 0, -1, withscores=True)

print(r_con.exists("abc"))

print(d1)
print("Hello")







