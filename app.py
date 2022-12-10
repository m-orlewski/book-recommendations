import os

from flask import Flask, render_template, flash, redirect, url_for, request
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

@app.route("/addBook", methods=['GET'])
def add_book_get():
    header = "Dodaj Książkę"
    return render_template('add_book.html', header=header)

@app.route("/addBook", methods=['POST'])
def add_book_post():

    book = {}
    book['title'] = request.form.get('title')
    book['author'] = request.form.get('author').split('/')
    book['genre'] = request.form.get('genre').split('/')
    
    if book['title'] == '':
        flash('Formularz wypełniony niepoprawnie. Podaj tytuł książki.')
        return redirect(url_for('add_book_get'))

    db.add_book(book['title'], book['author'], book['genre'])
    flash('Książka dodana do bazy.')
    return redirect(url_for('list_books_get'))

@app.route("/listPeople", methods=['GET'])
def list_people_get():
    header = "Lista Osób"
    people = db.find_people()
    print(people)
    return render_template('list_people.html', data=people, header=header)

@app.route("/addPerson", methods=['GET'])
def add_person_get():
    header = "Dodaj Osobę"
    return render_template('add_person.html', header=header)

@app.route("/addPerson", methods=['POST'])
def add_person_post():

    person = request.form.get('person')
    
    if person == '':
        flash('Formularz wypełniony niepoprawnie. Podaj imię.')
        return redirect(url_for('add_person_get'))

    db.add_person(person)
    flash('Osoba dodana do bazy.')
    return redirect(url_for('add_person_get'))

if __name__ == '__main__':
    app.run()