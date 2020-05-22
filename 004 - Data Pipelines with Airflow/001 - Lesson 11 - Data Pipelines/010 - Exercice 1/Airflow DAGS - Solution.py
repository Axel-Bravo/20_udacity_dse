import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def my_function():
    logging.info("Hello Axel")


dag = DAG(
        "lesson1.exercise1",
        start_date=datetime.datetime.now() - datetime.timedelta(days=2),
        schedule_interval='@daily')

greet_task = PythonOperator(
    task_id="hello_world_task",
    python_callable=my_function,
    dag=dag
)
