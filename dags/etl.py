from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'Catchup':False
}
## Initiating DAG
# The following DAG performs the following functions:
#       1. Loads Song data and Log from S3 to RedShift staging tables
#       2. Load from Redshift staging tables to facts and dimension tables
#       3. Performs a data quality check on the facts table
#      

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'          
           )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# The following code will load log  data from S3 to RedShift. Use the s3_key
#       "log_data" and the s3_bucket "udacity-dend"
#

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = 'staging_events',
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    json="s3://udacity-dend/log_json_path.json"
)

#
# The following code will load song data from S3 to RedShift. Use the s3_key
#       "song_data" and the s3_bucket "udacity-dend"
#

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'staging_songs',
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    json="auto"
)

#
# The following code will insert the data in fact table 
#

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    query_nm=SqlQueries.songplay_table_insert,
    table = 'songplays',
    delete_load = False
    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query_nm=SqlQueries.user_table_insert,
    table = 'users',
    delete_load = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query_nm=SqlQueries.song_table_insert,
    table = 'songs',
    delete_load = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query_nm=SqlQueries.artist_table_insert,
    table = 'artists',
    delete_load = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query_nm=SqlQueries.time_table_insert,
    table = 'time',
    delete_load = True
)

#
#Data quality check
#
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table = "songplays",
    redshift_conn_id = "redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks 
#
start_operator >>  stage_events_to_redshift 
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
