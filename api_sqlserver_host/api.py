from flask import Flask, request,jsonify, render_template
import mysql.connector
from mysql.connector import Error
import pandas as pd


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
            connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            port = 3306
            )
            if connection.is_connected():
                cursor = connection.cursor(buffered=True)
                cursor.execute("use hdb")
                cursor.execute("SELECT COUNT(*) FROM hdata")
                row_count = cursor.fetchone()[0]
                cursor.execute(query)
                result = cursor.fetchall()

            data = {
                "method": "GET",
                "query": query,
                "result": result,
                "rows":row_count
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
            query = "INSERT INTO hdata "
            query += form
            connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            port = 3306
            )
            if connection.is_connected():
                cursor = connection.cursor(buffered=True)
                cursor.execute("use hdb")
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
            connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            port = 3306
            )
            if connection.is_connected():
                cursor = connection.cursor(buffered=True)
                cursor.execute("use hdb")
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
            connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            port = 3306
            )
            if connection.is_connected():
                cursor = connection.cursor(buffered=True)
                cursor.execute("use hdb")
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