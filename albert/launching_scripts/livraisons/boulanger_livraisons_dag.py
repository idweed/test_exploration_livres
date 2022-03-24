#!/usr/bin/env python2.7
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime
from boulanger_prod_script import transfer_file

start_date = datetime(2020, 6, 9)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 3
}

dag_name = 'livraisons_push_boulanger'
schedule_interval = '45 6 * * *'

dag = DAG(dag_name,
          description='crawl livraisons boulanger 8h30',
          default_args=default_args,
          schedule_interval=schedule_interval,
          catchup=False,
          start_date=start_date,
          max_active_runs=1)


with dag:
    dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

    transfer = PythonOperator(task_id='filter',
                              python_callable=transfer_file)

    dummy_operator >> transfer
