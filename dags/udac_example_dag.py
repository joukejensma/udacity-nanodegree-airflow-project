from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date' : datetime(2021, 7, 21),
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
    'depends_on_past': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1,
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events", 
    s3_bucket="udacity-dend",
    s3_key="log_data",
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs", 
    s3_bucket="udacity-dend",
    s3_key="song_data",
    provide_context=True,    
)

load_songplays_table = LoadFactOperator(
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.songplay_table_insert,
    task_id='Load_songplays_fact_table',
    destination_table='songplays',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql_stmt=SqlQueries.user_table_insert,
    destination_table='users',
    redshift_conn_id="redshift",
    truncate_before_insert=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql_stmt=SqlQueries.song_table_insert,
    destination_table='songs',
    redshift_conn_id="redshift",
    truncate_before_insert=False    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql_stmt=SqlQueries.artist_table_insert,
    destination_table='artists',
    redshift_conn_id="redshift",
    truncate_before_insert=False    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    sql_stmt=SqlQueries.time_table_insert,
    destination_table='time',
    redshift_conn_id="redshift",
    truncate_before_insert=False    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['staging_events', 'staging_songs', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator