from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class DatabaseApp:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_book(self, book_name, author_name, genre_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._add_and_return_book, book_name, author_name, genre_name)
            for row in result:
                print(f'Added book: {row["b"]}, written by: {row["a"]}, with genre: {row["g"]}')

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


if __name__ == "__main__":
    uri = "neo4j+s://0a95c956.databases.neo4j.io"
    user = "neo4j"
    password = "2asnUpmxsbe6IGrt4PAbDpnCgK0GVLZqjMFIGS3nIiM"
    app = DatabaseApp(uri, user, password)
    #app.add_book('Book10', 'Author3', 'Genre7')
    #app.find_book('Book1')
    app.close()
