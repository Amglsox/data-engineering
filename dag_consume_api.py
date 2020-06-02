# Importando as bibliotecas
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.bash_operator import BashOperator
dag_id = 'consume-api'
default_args = {'owner': 'Lucas Mari',
                'start_date': datetime(2019, 1, 1)
               }
schedule = None
dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args)
with dag:
    t1 = BashOperator(task_id='consume-api',bash_command="""python3 /usr/local/airflow/dags/consume-api.py""")


