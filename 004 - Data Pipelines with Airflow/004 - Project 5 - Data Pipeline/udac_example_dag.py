import datetime

from airflow import DAG
from airflow.operators import (
    DataQualityOperator, StageToRedshiftOperator,
    )
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

default_args = {
        'owner': 'udacity',
        'start_date': datetime.datetime(2019, 1, 12),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=5),
        'catchup': False,
        'email_on_retry': False
        }

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag
        )

stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag
        )

load_songplays_table = PostgresOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        sql=SqlQueries.songplay_table_insert,
        autocommit=True,
        postgres_conn_id="redshift"
        )

load_user_dimension_table = PostgresOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        sql=SqlQueries.user_table_insert,
        autocommit=True,
        postgres_conn_id="redshift"
        )

load_song_dimension_table = PostgresOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        sql=SqlQueries.song_table_insert,
        autocommit=True,
        postgres_conn_id="redshift"
        )

load_artist_dimension_table = PostgresOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        sql=SqlQueries.artist_table_insert,
        autocommit=True,
        postgres_conn_id="redshift"
        )

load_time_dimension_table = PostgresOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        sql=SqlQueries.time_table_insert,
        autocommit=True,
        postgres_conn_id="redshift"
        )

run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        tables=["songplay", "users", "song", "artist", "time"],
        redshift_conn_id='redshift',
        )

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# DAG
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table]
load_songplays_table >> [load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table] >> run_quality_checks
[load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
