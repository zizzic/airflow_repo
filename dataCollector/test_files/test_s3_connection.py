from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from datetime import datetime, timedelta
import json

data_dict = {"apple": 0.5, "milk": 2.5, "bread": 4.0}
data_json = json.dumps(data_dict)
bucket_name = "de-2-1-bucket"


# test & load to s3 template
with DAG(
    "load_to_s3",
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

    current_time = "{{ data_interval_end}}"
    year = "{{ data_interval_end.year }}"
    month = "{{ data_interval_end.month }}"
    day = "{{ data_interval_end.day }}"
    table_name = "raw_live_viewer"

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )

task_load_raw_data
