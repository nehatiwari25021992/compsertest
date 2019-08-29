"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from subdags.subdag import load_subdag

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'main',
    default_args=default_args,
    description='main dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60))

start = BashOperator(
    task_id='start',
    bash_command='echo start-dag',
    dag=dag,
    )


load_tasks = SubDagOperator(
        task_id='load_tasks',
        subdag=load_subdag('main',
                           'load_tasks', default_args),
        default_args=default_args,
        dag=dag,
    )

load_tasks1 = SubDagOperator(
        task_id='load_tasks1',
        subdag=load_subdag('main',
                           'load_tasks1', default_args),
        default_args=default_args,
        dag=dag,
    )

load_tasks2 = SubDagOperator(
        task_id='load_tasks2',
        subdag=load_subdag('main',
                           'load_tasks2', default_args),
        default_args=default_args,
        dag=dag,
    )

end = BashOperator(
    task_id='end',
    bash_command='echo end-dag',
    dag=dag,
    )


start >> [load_tasks,load_tasks1,load_tasks2] >> end