import os

from flask import Flask, render_template
from db import DatabaseApp

# Flask App
app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24).hex()

# Database App
uri = "neo4j+s://0a95c956.databases.neo4j.io"
user = "neo4j"
password = "2asnUpmxsbe6IGrt4PAbDpnCgK0GVLZqjMFIGS3nIiM"
db = DatabaseApp(uri, user, password)

@app.route("/")
def home():
    header = "Strona Główna"
    return render_template('base.html', header=header)

@app.route("/listBooks", methods=['GET'])
def list_books_get():
    header = "Lista Książek"
    table_data = db.find_all_books()
    return render_template('list_books.html', data=table_data, header=header)

if __name__ == '__main__':
    app.run()