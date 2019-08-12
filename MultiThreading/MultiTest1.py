#!/usr/bin/python3

import MySQLdb
import sys
from multiprocessing import Process
from argparse import ArgumentParser, ArgumentTypeError
import re
from math import ceil


#MySQL connection
mysql_connection_slave = MySQLdb.connect(host="192.168.0.235", user="scripts_user",
                                     passwd="scr!pts@34#", db="staging_study247_4.0.0.1",
                                     port=3308)

# Create a Cursor object to execute queries.
cursor_slave = mysql_connection_slave.cursor()

# Here num1 is(starting data),num2 is (ending data),and num3 is(Thread Count)
def suggestion_mobile_contacts_mobile(num1, num2, num3):
    # MySQL connection
    # mysql_connection_slave = MySQLdb.connect(host="192.168.0.171", user="root",
    #                                  passwd="123456", db="staging_study247_2.0.0.7",
    #                                  port=3308)
    # MySQL connection
    mysql_connection_slave = MySQLdb.connect(host="192.168.0.235", user="scripts_user",
                                             passwd="scr!pts@34#", db="staging_study247_4.0.0.1",
                                             port=3308)

    # mysql_connection_master = MySQLdb.connect(host="192.168.0.171", user="root",
    #                                  passwd="123456", db="staging_study247_2.0.0.7",
    #                                  port=3307)
    # MySQL connection
    mysql_connection_master = MySQLdb.connect(host="192.168.0.235", user="scripts_user",
                                              passwd="scr!pts@34#", db="staging_study247_4.0.0.1",
                                              port=3307)

    # Create a Cursor object to execute queries.
    cursor_slave = mysql_connection_slave.cursor()

    # Create a Cursor object to execute queries.
    cursor_master = mysql_connection_master.cursor()

    n = (num2 - num1) / 100

    n1 = ceil(n)

    for n2 in range(n1):

        sql1 = ""

        if n2 == (n1 - 1):
            sql1 = (
                "SELECT MOBILE,USER_ID FROM SUGGESTIONS_MOBILE_CONTACTS WHERE MOBILE IS NOT NULL AND ISREGISTERED = 0 LIMIT %s,%s" % (
                    num1 + (n2 * 100), (num2 - (num1 + (n2 * 100)))))

        else:
            # Select data from table using SQL query.
            sql1 = (
                "SELECT MOBILE,USER_ID FROM SUGGESTIONS_MOBILE_CONTACTS WHERE MOBILE IS NOT NULL AND ISREGISTERED = 0 LIMIT %s,100" % (
                    num1 + (n2 * 100)))

        # Execute the sql query
        cursor_slave.execute(sql1)

        mobile_contacts_data = cursor_slave.fetchall()

        for mobile_contacts in mobile_contacts_data:

            # print("MOBILE ISREGISTERED = 0: " + str(mobile_contacts[0]))

            sql2 = ("SELECT EXISTS(SELECT 1 FROM USER WHERE MOBILE = '" + str(mobile_contacts[0]) + "');")

            # Execute the sql query
            cursor_slave.execute(sql2)

            mobileexists = cursor_slave.fetchall()

            a = mobileexists[0][0]

            if a == 1:
                # Creating cursor object
                #cursor = mysql_connection_master.cursor()

                sql3 = ("UPDATE SUGGESTIONS_MOBILE_CONTACTS SET ISREGISTERED = 1 WHERE MOBILE = '" + str(
                    mobile_contacts[0]) + "';")

                # Execute the sql query
                cursor_master.execute(sql3)

                mysql_connection_master.commit()

                # Create a Cursor object to execute queries.
                #cursor = mysql_connection_slave.cursor()

        sql4 = ""
        if n2 == (n1 - 1):
            sql4 = (
                "SELECT MOBILE,USER_ID FROM SUGGESTIONS_MOBILE_CONTACTS WHERE MOBILE IS NOT NULL AND ISREGISTERED = 1 LIMIT %s,%s" % (
                    num1 + (n2 * 100), (num2 - (num1 + (n2 * 100)))))

        else:
            # Select data from table using SQL query.
            sql4 = (
                "SELECT MOBILE,USER_ID FROM SUGGESTIONS_MOBILE_CONTACTS WHERE MOBILE IS NOT NULL AND ISREGISTERED = 1 LIMIT %s,100" % (
                    num1 + (n2 * 100)))

        # Execute the sql query
        cursor_slave.execute(sql4)

        mobile_contacts_registered_data = cursor_slave.fetchall()

        for mobile_contacts_registered in mobile_contacts_registered_data:

            # print("MOBILE ISREGISTERED = 1: " + str(mobile_contacts_registered[0]))

            sql5 = ("SELECT USER_ID FROM USER WHERE MOBILE = '" + str(mobile_contacts_registered[0]) + "';")

            cursor_slave.execute(sql5)

            user_id_data = cursor_slave.fetchall()

            for user_id in user_id_data:

                # Creating cursor object
                #cursor = mysql_connection_master.cursor()

                sql6 = (
                "UPDATE SUGGESTIONS_MOBILE_CONTACTS SET CONN_USER = " + str(user_id[0]) + " WHERE MOBILE = '" + str(
                    mobile_contacts_registered[0]) + "';")

                cursor_master.execute(sql6)

                mysql_connection_master.commit()

                # Create a Cursor object to execute queries.
                #cursor = mysql_connection_slave.cursor()

                sql7 = ("SELECT EXISTS(SELECT 1 FROM USER_CONNECTION WHERE (USER_ID = " + str(
                    mobile_contacts_registered[1]) + " AND CONN_USER = " + str(
                    user_id[0]) + " AND STATUS=1 ) OR (USER_ID = " + str(user_id[0]) + " AND CONN_USER = " + str(
                    mobile_contacts_registered[1]) + " AND STATUS = 1));")

                cursor_slave.execute(sql7)

                is_friend = cursor_slave.fetchall()

                b = is_friend[0][0]

                if b == 1:
                    # Creating cursor object
                    #cursor = mysql_connection_master.cursor()

                    sql8 = ("UPDATE SUGGESTIONS_MOBILE_CONTACTS SET ISREGISTERED = 2 WHERE MOBILE = '" + str(
                        mobile_contacts_registered[0]) + "' AND USER_ID = " + str(mobile_contacts_registered[1]) + ";")

                    # Execute the sql query
                    cursor_master.execute(sql8)

                    mysql_connection_master.commit()

                    # Create a Cursor object to execute queries.
                    #cursor = mysql_connection_slave.cursor()

        sql9 = ""
        if n2 == (n1 - 1):
            sql9 = (
                "SELECT MOBILE,USER_ID FROM SUGGESTIONS_MOBILE_CONTACTS WHERE MOBILE IS NOT NULL AND ISREGISTERED = 2 LIMIT %s,%s" % (
                    num1 + (n2 * 100), (num2 - (num1 + (n2 * 100)))))

        else:
            # Select data from table using SQL query.
            sql9 = (
                "SELECT MOBILE,USER_ID FROM SUGGESTIONS_MOBILE_CONTACTS WHERE MOBILE IS NOT NULL AND ISREGISTERED = 2 LIMIT %s,100" % (
                    num1 + (n2 * 100)))
            # Execute the sql query
        cursor_slave.execute(sql9)
        mobile_contacts_friends_data = cursor_slave.fetchall()

        for mobile_contacts_friends in mobile_contacts_friends_data:

            # print("MOBILE ISREGISTERED = 2: " + str(mobile_contacts_friends[0]))

            sql10 = ("SELECT USER_ID FROM USER WHERE MOBILE = '" + str(mobile_contacts_friends[0]) + "';")

            cursor_slave.execute(sql10)

            user_friend_id_data = cursor_slave.fetchall()

            for user_friend_id in user_friend_id_data:

                sql11 = ("SELECT EXISTS(SELECT 1 FROM USER_CONNECTION WHERE (USER_ID = " + str(
                    mobile_contacts_friends[1]) + " AND CONN_USER = " + str(
                    user_friend_id[0]) + " AND STATUS =1) OR (USER_ID = " + str(
                    user_friend_id[0]) + " AND CONN_USER = " + str(mobile_contacts_friends[1]) + " AND STATUS =1));")

                cursor_slave.execute(sql11)

                is_user_friend = cursor_slave.fetchall()

                c = is_user_friend[0][0]

                if c == 0:
                    # Creating cursor object
                    #cursor = mysql_connection_master.cursor()

                    sql12 = ("UPDATE SUGGESTIONS_MOBILE_CONTACTS SET ISREGISTERED = 1 WHERE MOBILE = '" + str(
                        mobile_contacts_friends[0]) + "' AND USER_ID = " + str(mobile_contacts_friends[1]) + ";")

                    # Execute the sql query
                    cursor_master.execute(sql12)

                    mysql_connection_master.commit()

                    # Create a Cursor object to execute queries.
                    #cursor = mysql_connection_slave.cursor()

    # close the cursor object
    cursor_slave.close()
    cursor_master.close()
    # close the mysql connection
    mysql_connection_slave.close()
    # close the mysql connection
    mysql_connection_master.close()


# Select data from table using SQL query.
sql8 = ("""SELECT COUNT(*) FROM SUGGESTIONS_MOBILE_CONTACTS WHERE MOBILE IS NOT NULL""")

# Execute the sql query
cursor_slave.execute(sql8)

# Number of threads to be created given at run time
try:
    num_threads = int(sys.argv[1])
except:
    num_threads=3
# num_threads = 8
sql8_count = cursor_slave.fetchall()

# close the cursor object
cursor_slave.close()
# close the mysql connection
mysql_connection_slave.close()

for user_id_count in sql8_count:
    c = user_id_count[0]

# Dividing the post_id_count with number of threads
perthread_post = ceil(c / num_threads)

# Multiprocessing
for i in range(num_threads):
    # print (i)
    # Creating a process object
    p = Process(target=suggestion_mobile_contacts_mobile, args=(i * perthread_post, (i + 1) * perthread_post, i,))
    # Calling the start method
    p.start()
    # friend_suggestion(i*perthread_post,(i+1)*perthread_post,i,)
sys.exit()
