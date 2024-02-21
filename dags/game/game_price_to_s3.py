from datetime import datetime, timedelta
import json
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator


# from plugins import slack
from top_300_games import games


@task
def get_ccu():
    data = []  # 모든 API 응답을 저장할 리스트

    for appid in games.keys():
        url = f"http://store.steampowered.com/api/appdetails?appids={appid}&cc=kr"
        response = requests.get(url)

        temp = dict()
        temp[appid] = response.json()

        data.append(temp)  # API 응답을 리스트에 추가

    return data


@task
def save_to_json(data):
    result = json.dumps(data)  # API 응답들이 담긴 리스트를 JSON으로 저장

    return result


with DAG(
    dag_id="game_price_to_s3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Steam_API"],
    schedule_interval="@once",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=15),
        # 'on_failure_callback': slack.on_failure_callback,
    },
) as dag:

    data = get_ccu()
    data_json = save_to_json(data)

    bucket_name = "de-2-1-bucket"

    current_time = "{{ data_interval_end }}"
    year = "{{ data_interval_end.year }}"
    month = "{{ data_interval_end.month }}"
    day = "{{ data_interval_end.day }}"
    table_name = "RAW_GAME_PRICE"

    task_load_raw_data = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=f"source/json/table_name={table_name}/year={year}/month={month}/day={day}/{table_name}_{current_time}.json",
        data=data_json,
        replace=True,
        aws_conn_id="aws_conn_id",
    )
