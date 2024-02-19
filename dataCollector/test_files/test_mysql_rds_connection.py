from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta
import json
import logging


def retrieve_data(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="get_data_using_query")
    return data


# test & load to s3 template
with DAG(
    "get_from_rds",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 2, 18),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="@once",
    catchup=False,
) as dag:

    task_get_data = MySqlOperator(
        task_id="get_data_using_query",
        sql="select * from STREAMER_INFO limit 10;",
        mysql_conn_id="aws_rds_conn_id",
    )

    task_print_data = PythonOperator(
        task_id="print",
        provide_context=True,
        python_callable=retrieve_data,
    )

task_get_data >> task_print_data
