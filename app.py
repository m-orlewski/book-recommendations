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
    table_data = sorted(db.find_all_books(), key=lambda x: x[0])
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
    people = sorted(db.find_people())
    d = {}
    
    for person in people:
        d[person] = db.find_books_liked_by_person(person)

    table_data = []
    for k, v in d.items():
        table_data.append([])
        table_data[-1].append(k)
        s = ''
        v.sort()
        for book in v:
            s += book
            s += ', '
        table_data[-1].append(s[:-2])
    return render_template('list_people.html', data=table_data, header=header)

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

@app.route("/likeBook", methods=['GET'])
def like_book_get():
    header = "Polub Książkę"
    people = sorted(db.find_people())
    books = sorted([x[0] for x in db.find_all_books()])

    return render_template('like_book.html', people=people, books=books, header=header)

@app.route("/likeBook", methods=['POST'])
def like_book_post():
    
    person = request.form.get('select-person')
    book = request.form.get('select-book')


    db.add_like(person, book)
    flash('Dodano polubienie do bazy')
    return redirect(url_for('like_book_get'))

@app.route("/getRecommendation", methods=['GET'])
def get_recommendation_get():
    header = "Otrzymaj rekomendację"
    people = sorted(db.find_people())

    return render_template('get_recommendation.html', people=people, header=header)

@app.route("/getRecommendation", methods=['POST'])
def get_recommendation_post():

    person = request.form.get('select-person')
    header = f"Książki rekomendowane dla {person}"

    rec_by_author = [rec[0] for rec in db.find_recommended_books_by_author(person)]
    rec_by_genre =  [rec[0] for rec in db.find_recommended_books_by_genre(person)]

    unique_rec_by_author = []
    for book in rec_by_author:
        if book not in unique_rec_by_author:
            unique_rec_by_author.append(book)
        
    unique_rec_by_genre = []
    for book in rec_by_genre:
        if book not in unique_rec_by_genre:
            unique_rec_by_genre.append(book)

    unique_all = set(unique_rec_by_author.copy())
    unique_all.update(set(unique_rec_by_genre))

    rec = {}
    for book in unique_all:
        rec[book] = rec_by_author.count(book) + rec_by_genre.count(book)

    rec = sorted(rec)

    return render_template('show_recommendations.html', rec=rec, rec_by_author=rec_by_author, rec_by_genre=rec_by_genre, header=header)

if __name__ == '__main__':
    app.run()