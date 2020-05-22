import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (FactsCalculatorOperator, HasRowsOperator, S3ToRedshiftOperator)

dag = DAG("lesson3.exercise4",
          start_date=datetime.datetime.now() - datetime.timedelta(days=2)
          )

copy_trips_task = S3ToRedshiftOperator(
        task_id="load_trips_from_s3_to_redshift",
        dag=dag,
        table="trips",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=f"{Variable.get('s3_bucket')}/{Variable.get('s3_prefix')}",
        s3_key="divvy/unpartitioned/divvy_trips_2018.csv"
        )

check_trips = HasRowsOperator(
        task_id="check_trips",
        dag=dag,
        redshift_conn_id="redshift",
        table="trips"
        )

calculate_facts = FactsCalculatorOperator(
        task_id="calculate_facts",
        dag=dag,
        redshift_conn_id="redshift",
        origin_table='trips',
        destination_table='facts',
        fact_column='tripduration',
        groupby_column='bikeid'
        )

copy_trips_task >> check_trips >> calculate_facts
