from flask import Flask, request,jsonify, render_template
import mysql.connector
from mysql.connector import Error
import pandas as pd
from databricks import sql
from mysql.connector import Error
import pyodbc
import json


acc = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'Server=sql-dataplatform-common-dev-data.database.windows.net;'
        f'DATABASE=sqldb-shared-management;'
        f'UID=amontreer@invivo-group.com;'
        f'Authentication=ActiveDirectoryInteractive;')

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("home.html")

@app.route("/get_data", methods=["GET","POST"])
def get():  
    if request.method == "POST":
        try :
            form = request.form.get("query")
            query = ""
            query += form
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            print(rows)
            # Conversion des résultats en une liste de dictionnaires
            result = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    row_dict[column[0]] = row[i]
                result.append(row_dict)
            json_result = json.dumps(result, indent=4)

            data = {
                "method": "GET",
                "query": query,
                "result": result,
            }
            return data

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
    else:
        return render_template("get.html")
    

@app.route("/post_data", methods=["POST", "GET"])
def post():
    if request.method == "POST":
        
        try :
            form = request.form.get("query")
            query = "INSERT INTO  "
            query += form
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            connection.commit()


            data = {
                "method": "POST",
                "query": query,
                "result": result,
            }
            return (data)

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
    else:
        return render_template("post.html")


@app.route("/put_data", methods=["PUT","GET","POST"])
def put():
    if request.method == "POST":
        
        try :
            form = request.form.get("query")
            query = "UPDATE hdata SET "
            query += form
            connection = pyodbc.connect(acc)
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            connection.commit()


            data = {
                "method": "PUT",
                "query": query,
                "result": result,
            }
            return (data)

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
    else:
        return render_template("put.html")


@app.route("/delete_data", methods=["GET", "POST"])
def delete():
    if request.method == "POST":
        
        try :
            form = request.form.get("query")
            query = "DELETE FROM hdata where "
            query += form
            connection = pyodbc.connect(acc)

            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()


            data = {
                "method": "DELETE",
                "query": query,
                "result": "done",
            }
            return (data)

        except Error as e:
            data = {
                "error":str(e)
            }
            return(data)
    else:
        return render_template("delete.html")


if __name__ == "__main__":
    app.run(debug=True)