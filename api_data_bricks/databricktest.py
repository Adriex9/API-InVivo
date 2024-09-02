from databricks import sql
from mysql.connector import Error
import pyodbc
import os
import pandas as pd


try:     
    host = "adb-7710764419102719.19.azuredatabricks.net"
    http_path = "/sql/1.0/warehouses/9054e24293b2ec21"
    access_token = "dapi9ad58e89f78852130a06af6ffd580c2a-2"

    connection = sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=access_token)

    print("Connection successful")

except Error as e:
    print(f"The error '{e}' occurred")

try :
    query = "select count(*) from bronze_dev.naia_agrfr.naia_adresse limit 10"
    cursor= connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    for rows in result:
        print(rows)

except Error as e:
    print(f"The error '{e}' occurred")

