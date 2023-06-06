import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor

TIMEZONE = timezone(timedelta(hours=-4))  # UTC-4

def _generate_platzi_data(**kwargs):
    data = pd.DataFrame({"Student": ["Maria Cruz", "Daniel Crema","Elon Musk", "Karol Castrejon", "Freddy Vega","Felipe Duque"], 
                         "timestamp": [kwargs['logical_date'], kwargs['logical_date'], 
                                       kwargs['logical_date'], kwargs['logical_date'],
                                       kwargs['logical_date'], kwargs['logical_date']]})
    
    data.to_csv(f"/tmp/platzi_data_{kwargs['ds_nodash']}.csv",header=True, index=False)
         

load_dotenv()
mail_to = os.getenv('MAIL_TO')
         
with DAG(dag_id="Workflow_Platzi",
         description="Project_satelite_data",
         schedule_interval="@daily",
         max_active_runs=1,
         default_args = { 
            'owner': 'airflow', 
            'start_date': datetime(2023, 5, 1, tzinfo=TIMEZONE),
            'end_date': datetime(2023, 6, 30, tzinfo=TIMEZONE),
            'depends_on_past': False, 
            'email_on_failure': False, 
            'email_on_retry': False, 
            'retries': 1}) as dag:
    
    external_sensor = ExternalTaskSensor(task_id="Waiting_for_Spacex",
							external_dag_id="Workflow_Spacex",
							external_task_id="Waiting_file_Spacex",
                            mode="reschedule",
							poke_interval=20 # Cada 20 segundos pregunta si ya termino la tarea
							)
    
    task_1 = PythonOperator(task_id="Satellite_response",
                    python_callable=_generate_platzi_data)
    
    task_2 = BashOperator(task_id = "Read_satellite_response_data",
                    bash_command='ls /tmp && head /tmp/platzi_data_{{ds_nodash}}.csv')

    email_analistas  = EmailOperator(task_id='Notify_analysts_Platzi',
                    to = [mail_to],
                    subject = "Notification Satellite Data",
                    html_content = "Notice to analysts, the data is available")  
    
    
    external_sensor >> task_1 >> task_2 >> email_analistas