from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (StageToRedshiftOperatorMRT
                               , LoadDimensionOperatorMRT
                               , LoadFactOperatorMRT
                               , DataQualityOperatorMRT)

from helpers.mrt_sql_queries import MrtSqlQueries

default_args = {
    'owner': 'shih-wen',
    'start_date': datetime(2022, 1, 1),
    'end_date': datetime(2022,3,31),
    'depends_on_past' : False,
    'retries' : 0,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry' : False
}

dag = DAG('mrt_hourly_traffic_dag'
          , default_args = default_args
          , description = 'Load and transform Tapei MRT traffic data, station data and station exit data'
          , schedule_interval = '0 5 2 * *'
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

'''
stage_station_to_redshift = StageToRedshiftOperatorMRT(
    task_id='stage_station',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_station",
    s3_bucket="s3://mrt-traffic",
    s3_key="staging-data/mrt_station_v4.csv",
    region="us-west-2"
)

stage_station_exit_to_redshift = StageToRedshiftOperatorMRT(
    task_id='stage_station_exit',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_station_exit",
    s3_bucket="s3://mrt-traffic",
    s3_key="staging-data/mrt_exit_v3.csv",
    region="us-west-2"
)
'''

stage_traffic_to_redshift = StageToRedshiftOperatorMRT(
    task_id='stage_traffic',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_traffic",
    s3_bucket="s3://mrt-traffic",
    s3_key="staging-data/traffic",
    region="us-west-2"
)

'''
load_station_dimension_table = LoadDimensionOperatorMRT(
    task_id='Load_station_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="mrt_station_dim",
    sql_statement=MrtSqlQueries.insert_station_dim,
    truncate_table=True
)

load_station_exit_dimension_table = LoadDimensionOperatorMRT(
    task_id='Load_station_exit_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="mrt_station_exit_dim",
    sql_statement=MrtSqlQueries.insert_station_exit_dim,
    truncate_table=True
)
'''
load_time_dimension_table = LoadDimensionOperatorMRT(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="time_dim",
    sql_statement=MrtSqlQueries.insert_time_dim,
    truncate_table=False
)

load_traffic_fact_table = LoadFactOperatorMRT(
    task_id='Load_traffic_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="mrt_traffic_fact"
)

run_quality_checks = DataQualityOperatorMRT(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[ {'check_sql': MrtSqlQueries.check_station_number_dim
                 , 'expected_sql': MrtSqlQueries.check_station_number_staging}
                , {'check_sql': MrtSqlQueries.check_traffic_fact
                 , 'expected_sql': MrtSqlQueries.check_station_number_staging}
                ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> stage_traffic_to_redshift

stage_traffic_to_redshift >> load_time_dimension_table

load_time_dimension_table >> load_traffic_fact_table

load_traffic_fact_table >> run_quality_checks

run_quality_checks >> end_operator
