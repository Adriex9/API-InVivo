import mysql.connector
from mysql.connector import Error
import pandas as pd



try:
    connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            port = 3306
    )

    if connection.is_connected():
        print("Connection to MySQL server successful")
        cursor = connection.cursor(buffered=True)
except Error as e:
    print(f"The error '{e}' occurred")


try:

    cursor.execute(f"""USE hdb""")
    print("hdb connected successfully")
except Error as e:
    print(f"The error '{e}' occurred")

try:
    cursor.execute("SELECT * FROM hdata LIMIT 3")
    result = cursor.fetchall()
    if result is not None:
        for row in result:
           print("résult:")
           print(row)

    else: 
        print("none is the result")
except Error as e:
    print(f"The error '{e}' occurred")  

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")