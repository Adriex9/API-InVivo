from flask import Flask, request,jsonify, render_template
import mysql.connector
from databricks import sql
from mysql.connector import Error
import json
#api for Databricks using access token http path and host to connect

host = "adb-7710764419102719.19.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/9054e24293b2ec21"
access_token = "dapie8206932ae0faa2b77c23131437bbf39-2"

connection = sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=access_token)
        

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
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            #json_result = json.dumps(result, indent=4) #to json serialize result if needed

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
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            #json_result = json.dumps(result, indent=4) #to json serialize result if needed


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
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            #json_result = json.dumps(result, indent=4) #to json serialize result if needed



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
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            #json_result = json.dumps(result, indent=4) #to json serialize result if needed


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
