import configparser
import psycopg2

from .sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(*, connector: psycopg2.connect, cursor, queries: [str, ...]) -> None:
    """
    Loads staging tables on the database by executing the different copying queries

    Args:
        connector: database connector
        cursor: database cursor
        queries: database load's queries

    Returns: None, uploads data into a database system
    """
    for query in queries:
        cursor.execute(query)
        connector.commit()


def insert_tables(*, connector: psycopg2.connect, cursor, queries: [str, ...]) -> None:
    """
    Inserts tables on the database by executing the different inserting queries

    Args:
        connector: database connector
        cursor: database cursor
        queries: database insert table's queries

    Returns: None, uploads data into a database system
    """
    for query in queries:
        cursor.execute(query)
        connector.commit()


def main():
    """
    ETL program method
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    connector = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                 .format(*config['CLUSTER'].values()))
    cursor = connector.cursor()

    load_staging_tables(cursor=cursor, connector=connector, queries=copy_table_queries)
    insert_tables(cursor=cursor, connector=connector, queries=insert_table_queries)
    connector.close()


if __name__ == "__main__":
    main()
