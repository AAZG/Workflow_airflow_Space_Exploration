import pandas as pd
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

TIMEZONE = timezone(timedelta(hours=-4))  # UTC-4     
         
with DAG(dag_id="Workflow_Nasa",
         description="Project_satelite_data",
         schedule_interval="@daily",
         max_active_runs=1,
         default_args = {
            'owner': 'airflow', 
            'start_date': datetime(2023, 5, 1, tzinfo=TIMEZONE),
            'end_date': datetime(2023, 6, 30, tzinfo=TIMEZONE),
            'depends_on_past': False, 
            'retries': 1}) as dag:
    
    task_1 = BashOperator(
        task_id="Nasa_confirmation_permision",
        bash_command="sleep 5 && echo 'OK' > /tmp/response_{{ds_nodash}}.txt"
        )
    
    
    sensor_response_nasa = FileSensor(task_id="Waiting_file_confirmation_NASA",
                                      filepath="/tmp/response_{{ds_nodash}}.txt")
    
    task_2 = BashOperator(task_id = "Read_response_Nasa",
                          bash_command='ls /tmp && head /tmp/response_{{ds_nodash}}.txt')
           

    task_1 >> sensor_response_nasa >> task_2