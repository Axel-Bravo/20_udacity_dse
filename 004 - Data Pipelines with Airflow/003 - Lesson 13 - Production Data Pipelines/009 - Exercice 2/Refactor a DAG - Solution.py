import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def log_youngest(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1""")
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")


def log_oldest(*args, **kwarg):
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1""")
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)

log_youngest_task = PythonOperator(
    task_id='log_youngest',
    dag=dag,
    python_callable=log_youngest,
    provide_context=True,
)

log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest,
    provide_context=True,
)


create_younger_riders = PostgresOperator(
    task_id = "create_younger_riders",
    dag=dag,
    sql="""
            CREATE TABLE younger_riders AS (
                SELECT * 
                FROM trips 
                WHERE birthyear > 2000
            );
        """,
    autocommit=True,
    postgres_conn_id="redshift"
)

create_lifetime_rides = PostgresOperator(
    task_id = "create_lifetime_rides",
    dag=dag,
    sql="""
            CREATE TABLE lifetime_rides AS (
                SELECT bikeid, COUNT(bikeid)
                FROM trips
                GROUP BY bikeid
            );
        """,
    autocommit=True,
    postgres_conn_id="redshift"
)

create_city_station_counts = PostgresOperator(
    task_id = "create_city_station_counts",
    dag=dag,
    sql="""
            CREATE TABLE city_station_counts AS(
                SELECT city, COUNT(city)
                FROM stations
                GROUP BY city
            );
        """,
    autocommit=True,
    postgres_conn_id="redshift"
)

create_older_riders = PostgresOperator(
    task_id="create_older_riders",
    dag=dag,
    sql="""
            CREATE TABLE older_riders AS (
                SELECT * 
                FROM trips 
                WHERE birthyear > 0 
                    AND birthyear <= 1945
            );
        """,
    autocommit=True,
    postgres_conn_id="redshift"
)

# DAG
create_younger_riders >> log_youngest_task
create_older_riders >> log_oldest_task
create_lifetime_rides
create_city_station_counts
