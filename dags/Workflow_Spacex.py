import pandas as pd
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator

TIMEZONE = timezone(timedelta(hours=-4))  # UTC-4

with DAG(dag_id="Workflow_Spacex",
         description="Project_satelite_data",
         schedule_interval="@daily",
         max_active_runs=1,
         default_args = { 
            'owner': 'airflow', 
            'start_date': datetime(2023, 5, 1, tzinfo=TIMEZONE),
            'end_date': datetime(2023, 6, 30, tzinfo=TIMEZONE),
            'depends_on_past': False, 
            'retries': 1}) as dag:
    
  
    
    external_sensor = ExternalTaskSensor(task_id="Waiting_for_Nasa",
							external_dag_id="Workflow_Nasa",
							external_task_id="Read_response_Nasa",
                            mode="reschedule",
							poke_interval=20# Cada 20 segundos pregunta si ya termino la tarea
							)
    
    task_1 = BashOperator(
        task_id="Obtain_spacex_data",
        bash_command="curl https://api.spacexdata.com/v4/launches/past > /tmp/spacex_{{ds_nodash}}.json"
        )
    
    sensor_response_spacex = FileSensor(task_id="Waiting_file_Spacex",
                                        filepath="/tmp/spacex_{{ds_nodash}}.json")
            

    external_sensor >> task_1 >> sensor_response_spacex