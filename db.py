from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class DatabaseApp:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_book(self, book_name, authors, genres):
        with self.driver.session(database="neo4j") as session:
            for author_name in authors:
                if author_name != '':
                    result = session.execute_write(self._add_book_with_author, book_name, author_name)
                    for row in result:
                        print(f'Added book: {row["b"]}, written by: {row["a"]}')
            for genre_name in genres:
                if genre_name != '':
                    result = session.execute_write(self._add_book_with_genre, book_name, genre_name)
                    for row in result:
                        print(f'Added book: {row["b"]}, with genre: {row["g"]}')

    @staticmethod
    def _add_book_with_author(tx, book_name, author_name):
        query = (
            "MERGE (b:Book {name: $book_name }) "
            "MERGE (a:Author {name: $author_name }) "
            "MERGE (a)-[:AUTHOR_OF]->(b)"
            "RETURN b, a"
        )
        result = tx.run(query, book_name=book_name, author_name=author_name)
        try:
            return [{"b": row["b"]["name"], "a": row["a"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _add_book_with_genre(tx, book_name, genre_name):
        query = (
            "MERGE (b:Book {name: $book_name }) "
            "MERGE (g:Genre {name: $genre_name }) "
            "MERGE (g)-[:GENRE_OF]->(b)"
            "RETURN b, g"
        )
        result = tx.run(query, book_name=book_name, genre_name=genre_name)
        try:
            return [{"b": row["b"]["name"], "g": row["g"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def _add_and_return_book(tx, book_name, author_name, genre_name):
        query = (
            "MERGE (b:Book {name: $book_name }) "
            "MERGE (a:Author {name: $author_name }) "
            "MERGE (g:Genre {name: $genre_name }) "
            "MERGE (a)-[:AUTHOR_OF]->(b)"
            "MERGE (g)-[:GENRE_OF]->(b)"
            "RETURN b, a, g"
        )
        result = tx.run(query, book_name=book_name, author_name=author_name, genre_name=genre_name)
        try:
            return [{"b": row["b"]["name"], "a": row["a"]["name"], "g": row["g"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


    def find_book(self, book_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_book, book_name)
            for row in result:
                print(f'Found book: {row["b"]}, written by: {row["a"]}, with genre: {row["g"]}')

    @staticmethod
    def _find_and_return_book(tx,book_name):
        query = (
            "MATCH "
            "(g:Genre)--(b:Book {name: $book_name})--(a:Author) "
            "RETURN b, a, g"
        )
        result = tx.run(query, book_name=book_name)
        try:
            return [{"b": row["b"]["name"], "a": row["a"]["name"], "g": row["g"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_all_books(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_all_books)     
            grouped = {}
            for row in result:
                for k, v in row.items():
                    if k not in grouped.keys():
                        grouped[k] = [v[0], v[1]]
                    else:
                        if v[0] not in grouped[k][0]:
                            grouped[k][0] += f', {v[0]}'
                        elif v[1] not in grouped[k][1]:
                            grouped[k][1] += f', {v[1]}'
            result = [[k, v[0], v[1]] for k, v in grouped.items()]
            return result

    @staticmethod
    def _find_and_return_all_books(tx):
        query = (
            "MATCH "
            "(g:Genre)--(b:Book)--(a:Author) "
            "RETURN b, a, g"
        )
        result = tx.run(query)
        try:
            return [{row["b"]["name"]: [f'{row["a"]["name"]}', f'{row["g"]["name"]}']} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_people(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_people)
            for row in result:
                print(f'Found person: {row}')
            return result

    @staticmethod
    def _find_and_return_people(tx):
        query = (
            "MATCH "
            "(p:Person)"
            "RETURN p"
        )
        result = tx.run(query)
        try:
            return [row["p"]["name"] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def add_person(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._add_person, person_name)
            for row in result:
                print(f'Added person: {row}')

    @staticmethod
    def _add_person(tx, person_name):
        query = (
            "MERGE (p:Person {name: $person_name }) "
            "RETURN p"
        )
        result = tx.run(query, person_name=person_name)
        try:
            return [row["p"] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_books_liked_by_person(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_books_liked_by_person, person_name)
            print(f'Found books liked by {person_name}: {result}')
            return result
    
    @staticmethod
    def _find_books_liked_by_person(tx,person_name):
        query = (
            "MATCH (p:Person {name: $person_name})-[:LIKED]->(b:Book) "
            "RETURN b"
        )
        result = tx.run(query, person_name=person_name)
        try:
            return [row["b"]["name"] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def add_like(self, person_name, book_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._add_like, person_name, book_name)
            for row in result:
                print(f'Person: {row[0]} liked book: {row[1]}')
            return result

    @staticmethod
    def _add_like(tx, person_name, book_name):
        query = (
            "MERGE (p:Person {name: $person_name}) "
            "MERGE (b:Book {name: $book_name}) "
            "MERGE (p)-[:LIKED]->(b) "
            "RETURN p, b"
        )
        result = tx.run(query, person_name=person_name, book_name=book_name)
        try:
            return [[row["p"]["name"], row["b"]["book"]] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_recommended_books_by_author(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_recommended_books_by_author, person_name)
            for row in result:
                print(f'Recommended book: {row[0]} because of author: {row[1]}')
            return result

    @staticmethod
    def _find_recommended_books_by_author(tx, person_name):
        query = (
            "MATCH (p:Person {name: $person_name})-[:LIKED]->(b)<-[:AUTHOR_OF]-(a)-[:AUTHOR_OF]->(rec) "
            "WHERE NOT (p)-[:LIKED]->(rec) "
            "RETURN DISTINCT rec, a "
            "ORDER BY rec.name"
        )
        result = tx.run(query, person_name=person_name)
        try:
            return [[row["rec"]["name"], row["a"]["name"]] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_recommended_books_by_genre(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_recommended_books_by_genre, person_name)
            for row in result:
                print(f'Recommended book: {row[0]} because of genre: {row[1]}')
            return result

    @staticmethod
    def _find_recommended_books_by_genre(tx, person_name):
        query = (
            "MATCH (p:Person {name: $person_name})-[:LIKED]->(b)<-[:GENRE_OF]-(g)-[:GENRE_OF]->(rec) "
            "WHERE NOT (p)-[:LIKED]->(rec) "
            "RETURN DISTINCT rec, g "
            "ORDER BY rec.name"
        )
        result = tx.run(query, person_name=person_name)
        try:
            return [[row["rec"]["name"], row["g"]["name"]] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise



if __name__ == "__main__":
    uri = "neo4j+s://0a95c956.databases.neo4j.io"
    user = "neo4j"
    password = "2asnUpmxsbe6IGrt4PAbDpnCgK0GVLZqjMFIGS3nIiM"
    app = DatabaseApp(uri, user, password)
    #app.add_book('Book10', 'Author3', 'Genre7')
    #app.find_book('Book1')
    #book_titles = [row[0] for row in app.find_all_books()]
    #print(book_titles)
    #print(app.find_people())
    app.find_books_liked_by_person('Person1')
    app.close()
