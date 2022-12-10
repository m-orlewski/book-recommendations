from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def add_book(self, book_name, author_name, genre_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._add_and_return_book, book_name, author_name, genre_name)
            for row in result:
                print(f'Added book: {row["b"]}, written by: {row["a"]}, with genre: {row["g"]}')

    @staticmethod
    def _add_and_return_book(tx, book_name, author_name, genre_name):
        query = (
            "MERGE (b:Book {name:'$book_name'}) "
            "MERGE (a:Author {name:'$author_name'}) "
            "MERGE (g:Genre {name:'$genre_name'}) "
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


    def create_friendship(self, person1_name, person2_name):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.execute_write(
                self._create_and_return_friendship, person1_name, person2_name)
            for row in result:
                print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[:KNOWS]->(p2) "
            "RETURN p1, p2"
        )
        result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://fbf92c48.databases.neo4j.io"
    user = "neo4j"
    password = "zDVavOL7eo8dt5tJqhMLluXfPkPVP7H-qt0POr8EJcQ"
    app = App(uri, user, password)
    app.add_book('Book10', 'Author3', 'Genre7')
    app.close()
