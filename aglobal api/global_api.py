from flask import Flask, request,jsonify, render_template
import mysql.connector
from mysql.connector import Error
import pandas as pd
from databricks import sql
import pyodbc
import json
import time


app = Flask(__name__)



@app.route("/")
def home():
    return render_template("main.html")

tpa= ''#acess token
mail=''#mail connexion

@app.route("/get_Azure_s", methods=["GET"])
def get_azure_s():  
        start_time = time.time()
        acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=sql-dataplatform-common-dev-data.database.windows.net;'
        f'DATABASE=sqldb-shared-management;'
        f'UID={mail};'
        f'Authentication=ActiveDirectoryInteractive;'
    )
        try :
            query="SELECT * FROM mgmt.testAPI_S_Geography"
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    row_dict[column[0]] = str(row[i])
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_azure_mgmt.testAPI_S_Geography",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        



@app.route("/get_Databricks_s", methods=["GET"])
def get_databricks_s():  
        start_time = time.time()
        host = "adb-7710764419102719.19.azuredatabricks.net"
        http_path = "/sql/1.0/warehouses/fc7f10b40c958f2d"
        access_token = tpa
        try :
            connection = sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=access_token)
        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        
        print()
        try :
            query="select * from bronze_dev.api.api_s_geography"
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_databricks_api_s_geography",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)

@app.route("/get_Serveless_s", methods=["GET"])
def get_serverless_s():  
        start_time = time.time()
        acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=synw-dataplatform-common-dev-ondemand.sql.azuresynapse.net,1433;'
        f'DATABASE=master;'
        f'UID={mail};'
        f'Authentication=ActiveDirectoryInteractive;'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'HostNameInCertificate=*.sql.azuresynapse.net;'
        f'Connection Timeout=45;')

        try :
            query="SELECT * FROM Serverlessdb_API.ldb.API_S_GEOGRAPHY"
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    row_dict[column[0]] = str(row[i])
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_azure_serverless_S",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)


@app.route("/get_Azure_m", methods=["GET"])
def get_azure_m():  
        start_time = time.time()
        acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=sql-dataplatform-common-dev-data.database.windows.net;'
        f'DATABASE=sqldb-shared-management;'
        f'UID={mail};'
        f'Authentication=ActiveDirectoryInteractive;'
    )
        try :
            query="SELECT * FROM mgmt.testAPI_M_Geography"
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    row_dict[column[0]] = str(row[i])
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_azure_mgmt.testAPI_M_Geography",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        



@app.route("/get_Databricks_m", methods=["GET"])
def get_databricks_m():  
        start_time = time.time()
        host = "adb-7710764419102719.19.azuredatabricks.net/"
        http_path = "/sql/1.0/warehouses/fc7f10b40c958f2d"
        access_token = tpa
        try :
            connection = sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=access_token)
        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        
        print()
        try :
            query="select * from bronze_dev.api.api_m_geography"
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_databricks_api_m_geography",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)

@app.route("/get_Serveless_m", methods=["GET"])
def get_serverless_m():  
        start_time = time.time()
        acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=synw-dataplatform-common-dev-ondemand.sql.azuresynapse.net,1433;'
        f'DATABASE=master;'
        f'UID={mail};'
        f'Authentication=ActiveDirectoryInteractive;'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'HostNameInCertificate=*.sql.azuresynapse.net;'
        f'Connection Timeout=45;')

        try :
            query="SELECT * FROM Serverlessdb_API.ldb.API_M_GEOGRAPHY"
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    if not(row[i] is None): 
                        row_dict[column[0]] = str(row[i])
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_azure_serverless_M",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)


@app.route("/get_Azure_l", methods=["GET"])
def get_azure_l():  
        start_time = time.time()
        acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=sql-dataplatform-common-dev-data.database.windows.net;'
        f'DATABASE=sqldb-shared-management;'
        f'UID={mail};'
        f'Authentication=ActiveDirectoryInteractive;'
    )
        try :
            query="SELECT * FROM mgmt.testAPI_L_Geography"
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    row_dict[column[0]] = str(row[i])
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_azure_mgmt.testAPI_L_Geography",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        



@app.route("/get_Databricks_l", methods=["GET"])
def get_databricks_l():  
        start_time = time.time()
        host = "adb-7710764419102719.19.azuredatabricks.net"
        http_path = "/sql/1.0/warehouses/fc7f10b40c958f2d"
        access_token = tpa
        try :
            connection = sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=access_token)
        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        
        print()
        try :
            query="select * from bronze_dev.api.api_l_geography"
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_databricks_api_l_geography",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)

'''
@app.route("/get_Sql", methods=["GET"])
def get_sql():  
        start_time = time.time()
        try :
            query=""
            connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            port = 3306
            )
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    row_dict[column[0]] = str(row[i])
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_sql",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": result,
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        
'''

@app.route("/get_Serveless_l", methods=["GET"])
def get_serverless_l():  
        start_time = time.time()
        acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=synw-dataplatform-common-dev-ondemand.sql.azuresynapse.net,1433;'
        f'DATABASE=master;'
        f'UID={mail};'
        f'Authentication=ActiveDirectoryInteractive;'
        f'Encrypt=yes;'
        f'TrustServerCertificate=no;'
        f'HostNameInCertificate=*.sql.azuresynapse.net;'
        f'Connection Timeout=45;')

        try :
            query="SELECT * FROM Serverlessdb_API.ldb.API_L_GEOGRAPHY"
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    row_dict[column[0]] = str(row[i])
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET_azure_serverless_L",
                "temps de compilation": time.time()-start_time,
                "query": query,
                "result": "data stored in variable result",
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
        
if __name__ == "__main__":
    app.run(debug=True)