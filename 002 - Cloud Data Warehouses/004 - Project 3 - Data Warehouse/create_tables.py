import configparser
import psycopg2

from .sql_queries import create_table_queries, drop_table_queries


def drop_tables(*, connector: psycopg2.connect, cursor, drop_queries: [str, ...]) -> None:
    """
    Drops tables on the DB Schema

    Args:
        connector: database connector
        cursor: database cursor
        drop_queries: drop tables SQL queries list

    Returns: None
    """
    for query in drop_queries:
        cursor.execute(query)
        connector.commit()


def create_tables(*, connector: psycopg2.connect, cursor, create_queries: [str, ...]) -> None:
    """
    Creates tables on the DB schema

    Args:
        connector: database connector
        cursor: database cursor
        create_queries: create tables SQL queries list

    Returns: None
    """
    for query in create_queries:
        cursor.execute(query)
        connector.commit()


def main():
    """
    Create tables main method; resets tables to zero for a given data base
    Returns: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    connector = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                 .format(*config['CLUSTER'].values()))
    cursor = connector.cursor()

    drop_tables(connector=connector, cursor=cursor, drop_queries=drop_table_queries)
    create_tables(connector=connector, cursor=cursor, create_queries=create_table_queries)

    connector.close()


if __name__ == "__main__":
    main()
