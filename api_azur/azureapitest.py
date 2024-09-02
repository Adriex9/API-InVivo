from databricks import sql
from mysql.connector import Error
import pyodbc
import os


try: 
    acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=sql-dataplatform-common-dev-data.database.windows.net;'
        f'DATABASE=sqldb-shared-management;'
        f'UID=amontreer@invivo-group.com;'
        f'Authentication=ActiveDirectoryInteractive;')
    
    connection = pyodbc.connect(acc)
    print("Connection successful")

except Error as e:
    print(f"The error '{e}' occurred")

try :
    cursor = connection.cursor()
    cursor.execute("SELECT TOP 1 * FROM mgmt.testAPI_S_Geography")
    print(cursor.fetchall())

    """cursor.close()
    connection.close()
"""
except Error as e:
    print(f"The error '{e}' occurred")

