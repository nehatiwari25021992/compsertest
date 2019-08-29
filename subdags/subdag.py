print("SUBDAG")

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import python_operator
import time 

def sleepFortask(**kwargs):
    print(kwargs['key1'])
    i = kwargs['key1']
    time.sleep(1*i)


def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
    )
    with dag_subdag:
        for i in range(3):
            t =  python_operator.PythonOperator(
                 task_id='load_subdag_{0}'.format(i), 
                 python_callable=sleepFortask,
                 op_kwargs={'key1': i},
                 dag=dag_subdag)

    return dag_subdag