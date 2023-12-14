from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from weather_pipeline.tasks import *
from weather_pipeline.utils import _get_logger

logger = _get_logger("Airflow DAG")

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Adjust the start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    'weather_pipeline_dag',
    default_args=default_args,
    description='Weather Pipeline DAG',
    schedule_interval=timedelta(days=30),  # Adjust the schedule interval
)

def execute_extract_stations(*args, **kwargs):
    logger.info("Executing extract_stations task")
    ingest_extract_stations()

def execute_load_stations(*args, **kwargs):
    logger.info("Executing load_stations task")
    ingest_load_stations()

def execute_extract_station_data(*args, **kwargs):
    logger.info("Executing extract_station_data task")
    ingest_extract_station_data()

def execute_load_station_data(*args, **kwargs):
    logger.info("Executing load_station_data task")
    ingest_load_station_data()

def execute_create_dimension_tables(*args, **kwargs):
    logger.info("Executing create_dimension_tables task")
    transform_create_dimention_tables()

def execute_station_monthly_temp_avg(*args, **kwargs):
    logger.info("Executing get_monthly_temp_avg task")
    transform_station_monthly_temp_avg()

# Define PythonOperators for each task
extract_stations_task = PythonOperator(
    task_id='extract_stations',
    python_callable=execute_extract_stations,
    provide_context=True,
    dag=dag,
)

load_stations_task = PythonOperator(
    task_id='load_stations',
    python_callable=execute_load_stations,
    provide_context=True,
    dag=dag,
)

extract_station_data_task = PythonOperator(
    task_id='extract_station_data',
    python_callable=execute_extract_station_data,
    provide_context=True,
    dag=dag,
)

load_station_data_task = PythonOperator(
    task_id='load_station_data',
    python_callable=execute_load_station_data,
    provide_context=True,
    dag=dag,
)

create_dimension_tables_task = PythonOperator(
    task_id='create_dimension_tables',
    python_callable=execute_create_dimension_tables,
    provide_context=True,
    dag=dag,
)

get_monthly_temp_avg_task = PythonOperator(
    task_id='get_monthly_temp_avg',
    python_callable=execute_get_monthly_temp_avg,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_stations_task >> load_stations_task
load_stations_task >> extract_station_data_task
extract_station_data_task >> load_station_data_task
load_station_data_task >> create_dimension_tables_task
create_dimension_tables_task >> get_monthly_temp_avg_task

if __name__ == "__main__":
    dag.cli()
