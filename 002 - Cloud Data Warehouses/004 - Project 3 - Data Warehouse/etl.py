import configparser
import psycopg2

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(*, connector: psycopg2.connect, cursor, copy_queries: [str, ...]) -> None:
    """
    Drops tables on the DB Schema

    Args:
        connector: database connector
        cursor: database cursor
        copy_queries: copy information form the S3 bucket (landing) into DB table (staging)
                      SQL queries' list

    Returns: None
    """

    for query in copy_queries:
        cursor.execute(query)
        connector.commit()


def insert_tables(*, connector: psycopg2.connect, cursor, insert_queries: [str, ...]) -> None:
    """
    Drops tables on the DB Schema

    Args:
        connector: database connector
        cursor: database cursor
        insert_queries: insert information form the staging into golden DB tables queries' list

    Returns: None
    """

    for query in insert_queries:
        cursor.execute(query)
        connector.commit()


def main():
    """
    Load tables main method; introduces data to the appropiate tables on the database
    Returns: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    connector = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                 .format(*config['CLUSTER'].values()))
    cursor = connector.cursor()

    load_staging_tables(connector=connector, cursor=cursor, copy_queries=copy_table_queries)
    insert_tables(connector=connector, cursor=cursor, insert_queries=insert_table_queries)

    connector.close()


if __name__ == "__main__":
    main()
